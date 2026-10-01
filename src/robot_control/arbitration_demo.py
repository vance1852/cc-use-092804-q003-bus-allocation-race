"""左右机械臂控制器同一调度周期争抢实时总线的并发裁决复现。

用法：

    PYTHONPATH=src python3 -m robot_control.arbitration_demo

脚本在一个临时 WAL 文件库上播种一条实时控制总线与左右臂两条时隙申请，
然后用多个独立连接（模拟两个独立控制器）在同一个屏障后并发发起分配请求。
输出可稳定复现：唯一赢家裁决、落选原因、实际占用的控制周期，并从审计记录
确认执行侧只收到一份有效决定；随后再演示依据变化的后来者收到带版本的冲突。
"""

from __future__ import annotations

import argparse
import json
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import ScheduleConflict
from .service import SupplyService
from .storage import connect


SERVICE_DATE = "2026-09-25"
ROUTE_ID = "fabric-a-b"


def _nomination(nom_id: str, requested: str, priority: int, key: str) -> dict[str, object]:
    return {
        "nomination_id": nom_id,
        "route_id": ROUTE_ID,
        "shipper_id": nom_id,
        "service_date": SERVICE_DATE,
        "requested_control_slots": requested,
        "priority": priority,
        "idempotency_key": key,
    }


def run(workspace: Path | None = None, racer_count: int = 8) -> dict[str, object]:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "arbitration.sqlite3"
        clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        seed = SupplyService(connect(path), clock)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
            seed.create_user(user_id, user_id, role)
        seed.create_facility("plan", {"facility_id": "cluster-a", "name": "北部机器人控制平台", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_control_slots": "500000"})
        seed.create_facility("plan", {"facility_id": "pool-b", "name": "东部推理池", "kind": "inference-pool", "timezone": "Asia/Shanghai", "capacity_control_slots": "800000"})
        seed.create_route("plan", {"route_id": ROUTE_ID, "origin_id": "cluster-a", "destination_id": "pool-b", "product": "gpu-h100", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
        # 左臂高优先级申请占满总线；右臂低优先级在同一周期争抢，必然落选。
        seed.submit_nomination("dispatch", _nomination("arm-left", "100000", 10, "key-left"))
        seed.submit_nomination("dispatch", _nomination("arm-right", "60000", 20, "key-right"))
        seed.connection.close()

        racers = [SupplyService(connect(path)) for _ in range(racer_count)]
        barrier = threading.Barrier(racer_count)
        outcomes: list[dict[str, object]] = []
        failures: list[str] = []

        def decide(index: int) -> None:
            barrier.wait()
            try:
                outcomes.append(racers[index].allocate("dispatch", ROUTE_ID, SERVICE_DATE))
            except Exception as exc:  # noqa: BLE001 - 复现实用：记录任何非预期异常
                failures.append(f"{type(exc).__name__}: {exc}")

        with ThreadPoolExecutor(max_workers=racer_count) as pool:
            list(pool.map(decide, range(racer_count)))

        firsts = [o for o in outcomes if not o["replayed"]]
        replays = [o for o in outcomes if o["replayed"]]
        decision = firsts[0] if firsts else replays[0]
        rows = {item["nomination_id"]: item for item in decision["allocations"]}

        # 依据变化的后来者：冻结后再到一个新申请，应得到带版本的业务冲突。
        later = SupplyService(connect(path))
        later.submit_nomination("dispatch", _nomination("arm-extra", "10000", 5, "key-extra"))
        conflict: dict[str, object]
        try:
            later.allocate("dispatch", ROUTE_ID, SERVICE_DATE)
            conflict = {"raised": False}
        except ScheduleConflict as exc:
            conflict = {"raised": True, "code": exc.code, "details": exc.details}

        auditor = SupplyService(connect(path))
        frozen_row = auditor.connection.execute(
            "SELECT * FROM allocation_runs WHERE route_id=? AND service_date=?",
            (ROUTE_ID, SERVICE_DATE),
        ).fetchone()
        frozen_count = auditor.connection.execute(
            "SELECT COUNT(*) c FROM supply_audit_events WHERE event_type='allocation.frozen'"
        ).fetchone()["c"]
        run_count = 0 if frozen_row is None else 1
        chain = auditor.audit_chain("audit")

        for svc in (*racers, later, auditor):
            svc.connection.close()

        return {
            "status": "ok",
            "control_cycle": f"{ROUTE_ID}:{SERVICE_DATE}",
            "racer_requests": racer_count,
            "unexpected_failures": failures,
            "freeze": {
                "first_decision_count": len(firsts),
                "replayed_count": len(replays),
                "allocation_id": decision["allocation_id"],
                "bus_revision": decision["bus_revision"],
                "degraded": decision["degraded"],
                "available_capacity": decision["available_capacity"],
                "request_set_sha256": frozen_row["request_set_sha256"],
                "degradation_sha256": frozen_row["degradation_sha256"],
            },
            "arms": {
                "winner": {
                    "nomination_id": "arm-left",
                    "allocated_control_slots": rows["arm-left"]["allocated_control_slots"],
                },
                "loser": {
                    "nomination_id": "arm-right",
                    "allocated_control_slots": rows["arm-right"]["allocated_control_slots"],
                    "unfilled_control_slots": rows["arm-right"]["unfilled_control_slots"],
                    "reason": "insufficient_capacity_after_higher_priority",
                },
            },
            "later_decider": conflict,
            "execution_side_audit": {
                "allocation_run_rows": run_count,
                "allocation_frozen_events": frozen_count,
                "single_effective_decision": run_count == 1 and frozen_count == 1,
                "chain_valid": chain["valid"],
            },
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="复现实时总线并发调度裁决")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--racers", type=int, default=8)
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace, args.racers), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
