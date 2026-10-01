"""贯通控制算力单价、实时控制总线、控制算力库存、提名和情景分析的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import Conflict
from .service import SupplyService


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    service = SupplyService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    for index, close in enumerate(("108", "105", "102", "100", "98", "96"), start=18):
        service.record_quote("plan", {"market_index": "PEAK_VALLEY", "trade_date": f"2026-09-{index}", "close_cny": close, "source_revision": f"rev-{index}", "observed_at": f"2026-09-{index}T21:00:00Z"})
    service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部机器人控制平台", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_control_slots": "500000"})
    service.create_facility("plan", {"facility_id": "pool-b", "name": "东部推理池", "kind": "inference-pool", "timezone": "Asia/Shanghai", "capacity_control_slots": "800000"})
    service.create_route("plan", {"route_id": "fabric-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "gpu-h100", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
    service.add_inventory_lot("dispatch", {"lot_id": "lot-001", "facility_id": "cluster-a", "product": "gpu-h100", "grade": "PEAK_VALLEY", "quantity_control_slots": "150000", "unit_cost_cny": "91.25", "received_at": "2026-09-24T06:00:00Z"})
    service.submit_nomination("dispatch", {"nomination_id": "nom-001", "route_id": "fabric-a-b", "shipper_id": "tenant-east", "service_date": "2026-09-25", "requested_control_slots": "80000", "priority": 10, "idempotency_key": "nom-key-001"})
    allocation = service.allocate("dispatch", "fabric-a-b", "2026-09-25")
    transfer = service.dispatch_transfer("dispatch", "transfer-001", "nom-001", "lot-001", 2)
    service.create_scenario("plan", {"scenario_id": "fabric-recovery", "name": "关键机组检修恢复与需求回落", "market_index_drop_percent": "9", "route_capacity_changes": {"fabric-a-b": "20"}, "demand_changes": {"cluster-a:gpu-h100": "-5"}})
    service.approve_scenario("risk", "fabric-recovery", 1)
    scenario = service.run_scenario("plan", "fabric-recovery", "2026-09-23")
    arbitration = _concurrent_arbitration(service)
    result = {"status": "ok", "price": service.price_summary("PEAK_VALLEY"), "allocation_id": allocation["allocation_id"], "transfer": transfer, "scenario_run_id": scenario["run_id"], "arbitration": arbitration, "audit": service.audit_chain("audit"), "workspace": workspace.name}
    connection.close()
    return result


def _concurrent_arbitration(service: SupplyService) -> dict[str, object]:
    """同一容量快照的并发申请必须收敛为一份冻结决定，过期依据收到冲突。"""

    service.submit_nomination("dispatch", {"nomination_id": "nom-arm-left", "route_id": "fabric-a-b", "shipper_id": "tenant-east", "service_date": "2026-09-26", "requested_control_slots": "100000", "priority": 5, "idempotency_key": "nom-key-arm-left"})
    service.submit_nomination("dispatch", {"nomination_id": "nom-arm-right", "route_id": "fabric-a-b", "shipper_id": "tenant-west", "service_date": "2026-09-26", "requested_control_slots": "70000", "priority": 9, "idempotency_key": "nom-key-arm-right"})
    snapshot = service.capacity_snapshot("fabric-a-b", "2026-09-26")
    decisions: list[dict[str, object]] = []
    failures: list[Exception] = []

    def attempt() -> None:
        try:
            decisions.append(service.allocate("dispatch", "fabric-a-b", "2026-09-26", expected_snapshot=snapshot["snapshot_sha256"]))
        except Exception as exc:  # noqa: BLE001
            failures.append(exc)

    threads = [threading.Thread(target=attempt) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    fresh = [item for item in decisions if not item["replayed"]]
    if failures or len(decisions) != 4 or len(fresh) != 1 or len({item["allocation_id"] for item in decisions}) != 1:
        raise RuntimeError("并发裁决验收失败：同一快照的并发申请未收敛为一份决定")
    stale = None
    try:
        service.allocate("dispatch", "fabric-a-b", "2026-09-26", expected_snapshot="0" * 64)
    except Conflict as exc:
        stale = exc
    if stale is None or not stale.details or stale.details["snapshot_sha256"] != snapshot["snapshot_sha256"]:
        raise RuntimeError("并发裁决验收失败：过期依据未返回包含当前版本的冲突")
    completed = [
        event
        for event in service.audit_events("audit", "route", "fabric-a-b")["events"]
        if event["event_type"] == "allocation.completed" and event["payload"].get("service_date") == "2026-09-26"
    ]
    if len(completed) != 1:
        raise RuntimeError("并发裁决验收失败：审计记录中的有效决定不是一份")
    decision = fresh[0]
    winner = next(item for item in decision["allocations"] if item["outcome"] == "allocated")
    loser = next(item for item in decision["allocations"] if item["outcome"] == "rejected")
    return {
        "attempts": len(decisions),
        "allocation_id": decision["allocation_id"],
        "service_date": decision["service_date"],
        "snapshot_sha256": decision["snapshot_sha256"],
        "winner": winner["nomination_id"],
        "loser": {"nomination_id": loser["nomination_id"], "reason": loser["reason"]},
        "audit_decisions": len(completed),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行机器人控制平台调度服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
