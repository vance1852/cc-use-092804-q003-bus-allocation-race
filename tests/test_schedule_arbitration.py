"""控制周期并发裁决：左右机械臂控制器争抢实时总线时隙的业务裁决回归。"""

from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from robot_control.api import JsonApplication
from robot_control.clock import FrozenClock
from robot_control.errors import InvalidState, ScheduleConflict
from robot_control.service import SupplyService
from robot_control.storage import connect


def nomination(nom_id: str, requested: str, priority: int, key: str) -> dict[str, object]:
    return {
        "nomination_id": nom_id,
        "route_id": "fabric-a-b",
        "shipper_id": nom_id,
        "service_date": "2026-09-25",
        "requested_control_slots": requested,
        "priority": priority,
        "idempotency_key": key,
    }


class ArbitrationFixture(unittest.TestCase):
    def setUp(self) -> None:
        # 裁决由服务内进程锁串行化，故允许工作线程共享同一连接（与 HTTP 服务一致）。
        self.connection = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部机器人控制平台", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_control_slots": "500000"})
        self.service.create_facility("plan", {"facility_id": "pool-b", "name": "东部推理池", "kind": "inference-pool", "timezone": "Asia/Shanghai", "capacity_control_slots": "800000"})
        self.service.create_route("plan", {"route_id": "fabric-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "gpu-h100", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})

    def tearDown(self) -> None:
        self.connection.close()

    def seed_arms(self) -> None:
        # 左臂高优先级占满总线，右臂低优先级落选。
        self.service.submit_nomination("dispatch", nomination("arm-left", "100000", 10, "key-left"))
        self.service.submit_nomination("dispatch", nomination("arm-right", "50000", 20, "key-right"))


class ConcurrentCycleArbitrationTests(ArbitrationFixture):
    def test_concurrent_identical_deciders_freeze_exactly_one_decision(self) -> None:
        self.seed_arms()
        barrier = threading.Barrier(8)

        def decide(_: int) -> dict[str, object]:
            barrier.wait()
            return self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(decide, range(8)))

        winners = [o for o in outcomes if not o["replayed"]]
        replays = [o for o in outcomes if o["replayed"]]
        self.assertEqual(len(winners), 1, "同一控制周期只能有一份首次裁决")
        self.assertEqual(len(replays), 7)
        winning_id = winners[0]["allocation_id"]
        self.assertTrue(all(o["allocation_id"] == winning_id for o in outcomes))

        # 周期表只有一行；申请只被推进一次（revision 各加 1），不存在单侧动作。
        runs = self.connection.execute(
            "SELECT COUNT(*) c FROM allocation_runs WHERE route_id='fabric-a-b' AND service_date='2026-09-25'"
        ).fetchone()
        self.assertEqual(runs["c"], 1)
        states = {
            row["nomination_id"]: (row["state"], row["revision"])
            for row in self.connection.execute("SELECT nomination_id,state,revision FROM nominations").fetchall()
        }
        self.assertEqual(states["arm-left"], ("allocated", 2))
        self.assertEqual(states["arm-right"], ("cancelled", 2))

        # 执行侧只收到一份有效决定：审计链里只有一个 allocation.frozen。
        frozen_events = self.connection.execute(
            "SELECT COUNT(*) c FROM supply_audit_events WHERE event_type='allocation.frozen'"
        ).fetchone()
        self.assertEqual(frozen_events["c"], 1)
        self.assertTrue(self.service.audit_chain("audit")["valid"])

    def test_winner_takes_slot_and_loser_reason_is_in_decision(self) -> None:
        self.seed_arms()
        decision = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        rows = {item["nomination_id"]: item for item in decision["allocations"]}
        self.assertEqual(rows["arm-left"]["allocated_control_slots"], "100000.000")
        # 右臂落选：0 时隙、未满足量即落选原因，可被联调人员稳定复现。
        self.assertEqual(rows["arm-right"]["allocated_control_slots"], "0.000")
        self.assertEqual(rows["arm-right"]["unfilled_control_slots"], "50000.000")
        right = self.connection.execute(
            "SELECT state FROM nominations WHERE nomination_id='arm-right'"
        ).fetchone()
        self.assertEqual(right["state"], "cancelled")
        # 联调人员可从裁决稳定读到实际占用的控制周期。
        self.assertEqual(decision["route_id"], "fabric-a-b")
        self.assertEqual(decision["service_date"], "2026-09-25")
        self.assertEqual(decision["bus_revision"], 1)

    def test_identical_retry_returns_first_decision(self) -> None:
        self.seed_arms()
        first = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        retry = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        self.assertFalse(first["replayed"])
        self.assertTrue(retry["replayed"])
        self.assertEqual(retry["allocation_id"], first["allocation_id"])
        self.assertEqual(retry["allocations"], first["allocations"])

    def test_later_decider_with_new_request_gets_versioned_conflict(self) -> None:
        self.seed_arms()
        first = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        # 冻结后又到了一个新申请，依据已经变化。
        self.service.submit_nomination("dispatch", nomination("arm-extra", "10000", 5, "key-extra"))
        with self.assertRaises(ScheduleConflict) as caught:
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        details = caught.exception.details
        self.assertEqual(details["reason"], ["new_requests_after_freeze"])
        self.assertEqual(details["frozen_decision"]["allocation_id"], first["allocation_id"])
        self.assertEqual(details["frozen_decision"]["bus_revision"], 1)
        self.assertEqual(details["current_basis"]["new_request_ids"], ["arm-extra"])
        # 冲突本身不产生第二个决定，也没有半更新。
        runs = self.connection.execute("SELECT COUNT(*) c FROM allocation_runs").fetchone()
        self.assertEqual(runs["c"], 1)

    def test_bus_revision_change_is_reported_in_conflict(self) -> None:
        self.seed_arms()
        self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        self.connection.execute("UPDATE routes SET revision=revision+1 WHERE route_id='fabric-a-b'")
        with self.assertRaises(ScheduleConflict) as caught:
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        details = caught.exception.details
        self.assertIn("bus_revision_changed", details["reason"])
        self.assertEqual(details["frozen_decision"]["bus_revision"], 1)
        self.assertEqual(details["current_basis"]["bus_revision"], 2)

    def test_degradation_window_is_frozen_with_decision(self) -> None:
        self.seed_arms()
        self.service.announce_outage(
            "risk", "fabric-a-b", "2026-09-25T00:00:00Z", "2026-09-25T23:59:59Z", "50", "链路检修"
        )
        decision = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        self.assertTrue(decision["degraded"])
        self.assertEqual(decision["available_capacity"], "50000.000")
        self.assertEqual(len(decision["degradation_windows"]), 1)
        snapshots = self.connection.execute(
            "SELECT outage_id,capacity_percent FROM freeze_outage_snapshots WHERE allocation_id=?",
            (decision["allocation_id"],),
        ).fetchall()
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0]["capacity_percent"], "50")

        # 冻结后新增降级窗口：后来者得到降级窗口变更冲突，含当前窗口与版本。
        self.service.announce_outage(
            "risk", "fabric-a-b", "2026-09-25T12:00:00Z", "2026-09-25T18:00:00Z", "80", "二次限速"
        )
        with self.assertRaises(ScheduleConflict) as caught:
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        details = caught.exception.details
        self.assertIn("degradation_window_changed", details["reason"])
        # 首次裁决冻结时已处于降级态（1 个窗口）；后来者看到的是 2 个窗口。
        self.assertTrue(details["frozen_decision"]["degraded"])
        self.assertTrue(details["current_basis"]["degraded"])
        self.assertEqual(len(details["current_basis"]["degradation_windows"]), 2)

    def test_empty_cycle_is_invalid_state_without_decision(self) -> None:
        with self.assertRaises(InvalidState):
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) c FROM allocation_runs").fetchone()["c"], 0)


class CrossConnectionArbitrationTests(unittest.TestCase):
    def test_two_connections_racing_over_file_db_yield_single_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bus.sqlite3"
            svc_a = SupplyService(connect(path), FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
            for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
                svc_a.create_user(user_id, user_id, role)
            svc_a.create_facility("plan", {"facility_id": "cluster-a", "name": "北", "kind": "storage", "timezone": "UTC", "capacity_control_slots": "500000"})
            svc_a.create_facility("plan", {"facility_id": "pool-b", "name": "东", "kind": "inference-pool", "timezone": "UTC", "capacity_control_slots": "800000"})
            svc_a.create_route("plan", {"route_id": "fabric-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "gpu-h100", "daily_capacity": "100000", "loss_basis_points": 0, "transit_hours": 1})
            svc_a.submit_nomination("dispatch", nomination("arm-left", "60000", 10, "key-left"))
            svc_a.submit_nomination("dispatch", nomination("arm-right", "60000", 20, "key-right"))

            svc_b = SupplyService(connect(path))  # 第二个控制器进程，独立连接
            barrier = threading.Barrier(2)
            results: dict[str, dict[str, object]] = {}
            errors: dict[str, object] = {}

            def run(name: str, svc: SupplyService) -> None:
                barrier.wait()
                try:
                    results[name] = svc.allocate("dispatch", "fabric-a-b", "2026-09-25")
                except Exception as exc:  # noqa: BLE001 - 断言中分类
                    errors[name] = exc

            t_a = threading.Thread(target=run, args=("A", svc_a))
            t_b = threading.Thread(target=run, args=("B", svc_b))
            t_a.start(); t_b.start(); t_a.join(); t_b.join()

            self.assertEqual(errors, {}, "跨连接竞争不得抛出任何（含 SQLite）异常")
            ids = {results["A"]["allocation_id"], results["B"]["allocation_id"]}
            self.assertEqual(len(ids), 1, "两个控制器进程必须落在同一份裁决上")
            self.assertEqual(len({results["A"]["replayed"], results["B"]["replayed"]}), 2)
            svc_a.connection.close(); svc_b.connection.close()


class ApiArbitrationTests(ArbitrationFixture):
    def test_api_returns_business_conflict_never_storage_error(self) -> None:
        self.seed_arms()
        app = JsonApplication(self.service)
        first = app.handle("POST", "/routes/fabric-a-b/allocate", {"X-Actor-Id": "dispatch"}, b'{"service_date":"2026-09-25"}')
        self.assertEqual(first.status, 200)
        self.assertFalse(first.body["replayed"])

        # 相同重试：业务层回放，200。
        replay = app.handle("POST", "/routes/fabric-a-b/allocate", {"X-Actor-Id": "dispatch"}, b'{"service_date":"2026-09-25"}')
        self.assertEqual(replay.status, 200)
        self.assertTrue(replay.body["replayed"])

        # 依据变更：结构化 409，带冻结版本与当前版本，不含任何 SQLite 字样。
        self.connection.execute("UPDATE routes SET revision=revision+1 WHERE route_id='fabric-a-b'")
        conflict = app.handle("POST", "/routes/fabric-a-b/allocate", {"X-Actor-Id": "dispatch"}, b'{"service_date":"2026-09-25"}')
        self.assertEqual(conflict.status, 409)
        self.assertEqual(conflict.body["error"]["code"], "schedule_conflict")
        self.assertIn("details", conflict.body["error"])
        encoded = str(conflict.body)
        self.assertNotIn("sqlite", encoded.lower())
        self.assertNotIn("UNIQUE", encoded)
        self.assertEqual(conflict.body["error"]["details"]["current_basis"]["bus_revision"], 2)

    def test_api_masks_any_escaped_storage_exception(self) -> None:
        class ExplodingService:
            def allocate(self, *args: object) -> object:
                raise sqlite3.IntegrityError("UNIQUE constraint failed: allocation_runs.route_id")

        app = JsonApplication(ExplodingService())  # type: ignore[arg-type]
        response = app.handle("POST", "/routes/fabric-a-b/allocate", {"X-Actor-Id": "dispatch"}, b'{"service_date":"2026-09-25"}')
        self.assertEqual(response.status, 503)
        self.assertEqual(response.body["error"]["code"], "temporarily_unavailable")
        self.assertNotIn("UNIQUE", str(response.body))
        self.assertNotIn("sqlite", str(response.body).lower())


class LegacyMigrationTests(unittest.TestCase):
    def test_legacy_allocation_table_migrates_to_cycle_decision(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.executescript(
            """
            CREATE TABLE routes (route_id TEXT PRIMARY KEY, daily_capacity TEXT NOT NULL, revision INTEGER DEFAULT 1);
            CREATE TABLE supply_users (user_id TEXT PRIMARY KEY);
            CREATE TABLE allocation_runs (
                allocation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                route_id TEXT NOT NULL,
                service_date TEXT NOT NULL,
                input_sha256 TEXT NOT NULL,
                available_capacity TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(route_id, service_date, input_sha256)
            );
            INSERT INTO routes(route_id,daily_capacity,revision) VALUES('fabric-a-b','100000',1);
            INSERT INTO supply_users(user_id) VALUES('dispatch');
            INSERT INTO allocation_runs(route_id,service_date,input_sha256,available_capacity,result_json,created_by,created_at)
            VALUES('fabric-a-b','2026-09-25','abc','100000','{}','dispatch','t');
            """
        )
        from robot_control.storage import initialize

        initialize(connection)
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(allocation_runs)").fetchall()}
        self.assertIn("bus_revision", columns)
        self.assertIn("degradation_sha256", columns)
        self.assertIn("request_set_sha256", columns)
        row = connection.execute("SELECT * FROM allocation_runs").fetchone()
        self.assertEqual(row["bus_revision"], 1)
        self.assertEqual(row["degraded"], 0)
        self.assertEqual(row["request_set_sha256"], "abc")
        # 新的周期级唯一约束生效：同周期第二行必须被拒绝。
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO allocation_runs(route_id,service_date,bus_revision,degraded,degradation_sha256,"
                "request_set_sha256,input_sha256,available_capacity,result_json,created_by,created_at) "
                "VALUES('fabric-a-b','2026-09-25',1,0,'x','y','z','1','{}','dispatch','t2')"
            )
        connection.close()


if __name__ == "__main__":
    unittest.main()
