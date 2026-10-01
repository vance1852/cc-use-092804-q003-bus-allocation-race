"""同一调度周期并发裁决的业务仲裁契约测试。"""

from __future__ import annotations

import json
import sqlite3
import threading
import unittest
from datetime import datetime, timezone

from robot_control.api import JsonApplication
from robot_control.clock import FrozenClock
from robot_control.errors import Conflict, InvalidState, ValidationFailed
from robot_control.service import SupplyService
from robot_control.storage import initialize


class AllocationArbitrationTests(unittest.TestCase):
    """左右机械臂控制器在同一调度周期争抢实时总线的裁决行为。"""

    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部机器人控制平台", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_control_slots": "500000"})
        self.service.create_facility("plan", {"facility_id": "pool-b", "name": "东部推理池", "kind": "inference-pool", "timezone": "Asia/Shanghai", "capacity_control_slots": "800000"})
        self.service.create_route("plan", {"route_id": "fabric-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "gpu-h100", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
        self.service.submit_nomination("dispatch", {"nomination_id": "arm-left", "route_id": "fabric-a-b", "shipper_id": "tenant", "service_date": "2026-09-25", "requested_control_slots": "100000", "priority": 10, "idempotency_key": "key-left"})
        self.service.submit_nomination("dispatch", {"nomination_id": "arm-right", "route_id": "fabric-a-b", "shipper_id": "tenant", "service_date": "2026-09-25", "requested_control_slots": "70000", "priority": 20, "idempotency_key": "key-right"})

    def tearDown(self) -> None:
        self.connection.close()

    def snapshot(self) -> dict:
        return self.service.capacity_snapshot("fabric-a-b", "2026-09-25")

    def allocation_events(self) -> list[dict]:
        rows = self.connection.execute(
            "SELECT payload_json FROM supply_audit_events WHERE event_type='allocation.completed' ORDER BY event_id"
        ).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]

    def submit_late_nomination(self) -> None:
        self.service.submit_nomination("dispatch", {"nomination_id": "arm-late", "route_id": "fabric-a-b", "shipper_id": "tenant", "service_date": "2026-09-25", "requested_control_slots": "5000", "priority": 30, "idempotency_key": "key-late"})

    def test_concurrent_identical_requests_share_one_frozen_decision(self) -> None:
        snapshot = self.snapshot()
        results: list[dict] = []
        errors: list[Exception] = []

        def attempt() -> None:
            try:
                results.append(self.service.allocate("dispatch", "fabric-a-b", "2026-09-25", expected_snapshot=snapshot["snapshot_sha256"]))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=attempt) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 6)
        fresh = [item for item in results if not item["replayed"]]
        self.assertEqual(len(fresh), 1)
        self.assertEqual({item["allocation_id"] for item in results}, {fresh[0]["allocation_id"]})
        runs = self.connection.execute(
            "SELECT * FROM allocation_runs WHERE route_id='fabric-a-b' AND service_date='2026-09-25'"
        ).fetchall()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["input_sha256"], snapshot["snapshot_sha256"])
        self.assertEqual(runs[0]["route_revision"], 1)
        self.assertEqual(len(json.loads(runs[0]["request_set_json"])), 2)
        self.assertEqual(json.loads(runs[0]["outage_window_json"]), [])
        self.assertEqual(len(self.allocation_events()), 1)
        states = {
            row["nomination_id"]: (row["state"], row["revision"])
            for row in self.connection.execute("SELECT nomination_id,state,revision FROM nominations")
        }
        self.assertEqual(states["arm-left"], ("allocated", 2))
        self.assertEqual(states["arm-right"], ("cancelled", 2))
        winner, loser = fresh[0]["allocations"]
        self.assertEqual((winner["nomination_id"], winner["outcome"]), ("arm-left", "allocated"))
        self.assertEqual((loser["nomination_id"], loser["outcome"], loser["reason"]), ("arm-right", "rejected", "capacity_exhausted"))
        self.assertEqual(fresh[0]["service_date"], "2026-09-25")

    def test_identical_retry_replays_first_decision(self) -> None:
        snapshot = self.snapshot()
        first = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25", expected_snapshot=snapshot["snapshot_sha256"])
        second = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25", expected_snapshot=snapshot["snapshot_sha256"])
        third = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertTrue(third["replayed"])
        self.assertEqual(first["allocation_id"], second["allocation_id"])
        self.assertEqual(first["allocation_id"], third["allocation_id"])
        self.assertEqual(first["snapshot_sha256"], third["snapshot_sha256"])
        self.assertEqual(len(self.allocation_events()), 1)

    def test_stale_basis_conflict_carries_current_version(self) -> None:
        snapshot = self.snapshot()
        self.submit_late_nomination()
        current = self.snapshot()
        self.assertNotEqual(snapshot["snapshot_sha256"], current["snapshot_sha256"])
        with self.assertRaises(Conflict) as ctx:
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25", expected_snapshot=snapshot["snapshot_sha256"])
        self.assertEqual(ctx.exception.details["snapshot_sha256"], current["snapshot_sha256"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM allocation_runs").fetchone()[0], 0)

    def test_latecomer_after_freeze_gets_frozen_version(self) -> None:
        snapshot = self.snapshot()
        decision = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25", expected_snapshot=snapshot["snapshot_sha256"])
        self.submit_late_nomination()
        live = self.snapshot()
        with self.assertRaises(Conflict) as ctx:
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25", expected_snapshot=live["snapshot_sha256"])
        details = ctx.exception.details
        self.assertEqual(details["snapshot_sha256"], snapshot["snapshot_sha256"])
        self.assertEqual(details["allocation_id"], decision["allocation_id"])
        self.assertEqual(details["live_snapshot_sha256"], live["snapshot_sha256"])

    def test_blind_retry_after_basis_change_conflicts(self) -> None:
        self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        self.submit_late_nomination()
        with self.assertRaises(Conflict):
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")

    def test_failed_commit_leaves_no_partial_updates(self) -> None:
        def broken_audit(*args: object, **kwargs: object) -> None:
            raise sqlite3.IntegrityError("simulated storage failure")

        self.service._audit = broken_audit
        with self.assertRaises(Conflict):
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25")
        self.assertEqual(self.connection.execute("SELECT count(*) FROM allocation_runs").fetchone()[0], 0)
        rows = self.connection.execute("SELECT state,allocated_control_slots,revision FROM nominations ORDER BY nomination_id").fetchall()
        self.assertEqual([(row["state"], row["allocated_control_slots"], row["revision"]) for row in rows], [("submitted", "0", 1)] * 2)
        self.assertEqual(self.allocation_events(), [])

    def test_outage_window_is_frozen_with_decision(self) -> None:
        self.service.announce_outage("risk", "fabric-a-b", "2026-09-25T00:00:00Z", "2026-09-25T23:59:59Z", "50", "总线检修")
        snapshot = self.snapshot()
        self.assertEqual(snapshot["available_capacity"], "50000.000")
        decision = self.service.allocate("dispatch", "fabric-a-b", "2026-09-25", expected_snapshot=snapshot["snapshot_sha256"])
        self.assertEqual(decision["outage_window"][0]["capacity_percent"], "50")
        self.assertEqual(decision["allocations"][0]["allocated_control_slots"], "50000.000")
        self.assertEqual(decision["allocations"][0]["outcome"], "partial")
        row = self.connection.execute("SELECT outage_window_json FROM allocation_runs").fetchone()
        self.assertEqual(json.loads(row["outage_window_json"])[0]["reason"], "总线检修")

    def test_allocate_validates_input(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.allocate("dispatch", "fabric-a-b", "2026-13-40")
        with self.assertRaises(ValidationFailed):
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-25", expected_snapshot=123)
        with self.assertRaises(InvalidState):
            self.service.allocate("dispatch", "fabric-a-b", "2026-09-30")


class AllocationApiContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.service = SupplyService(self.connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部机器人控制平台", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_control_slots": "500000"})
        self.service.create_facility("plan", {"facility_id": "pool-b", "name": "东部推理池", "kind": "inference-pool", "timezone": "Asia/Shanghai", "capacity_control_slots": "800000"})
        self.service.create_route("plan", {"route_id": "fabric-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "gpu-h100", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
        self.service.submit_nomination("dispatch", {"nomination_id": "arm-left", "route_id": "fabric-a-b", "shipper_id": "tenant", "service_date": "2026-09-25", "requested_control_slots": "100000", "priority": 10, "idempotency_key": "key-left"})
        self.service.submit_nomination("dispatch", {"nomination_id": "arm-right", "route_id": "fabric-a-b", "shipper_id": "tenant", "service_date": "2026-09-25", "requested_control_slots": "70000", "priority": 20, "idempotency_key": "key-right"})
        self.app = JsonApplication(self.service)
        self.headers = {"X-Actor-Id": "dispatch"}

    def tearDown(self) -> None:
        self.connection.close()

    def allocate_body(self, snapshot_sha256: str | None = None) -> bytes:
        payload: dict[str, str] = {"service_date": "2026-09-25"}
        if snapshot_sha256 is not None:
            payload["expected_snapshot"] = snapshot_sha256
        return json.dumps(payload).encode()

    def test_snapshot_allocate_replay_and_conflict_contract(self) -> None:
        snapshot = self.app.handle("GET", "/routes/fabric-a-b/snapshot?service_date=2026-09-25", self.headers)
        self.assertEqual(snapshot.status, 200)
        self.assertEqual(len(snapshot.body["snapshot_sha256"]), 64)
        self.assertEqual(len(snapshot.body["request_set"]), 2)
        first = self.app.handle("POST", "/routes/fabric-a-b/allocate", self.headers, self.allocate_body(snapshot.body["snapshot_sha256"]))
        self.assertEqual(first.status, 200)
        self.assertFalse(first.body["replayed"])
        replay = self.app.handle("POST", "/routes/fabric-a-b/allocate", self.headers, self.allocate_body(snapshot.body["snapshot_sha256"]))
        self.assertEqual(replay.status, 200)
        self.assertTrue(replay.body["replayed"])
        self.assertEqual(replay.body["allocation_id"], first.body["allocation_id"])
        stale = self.app.handle("POST", "/routes/fabric-a-b/allocate", self.headers, self.allocate_body("0" * 64))
        self.assertEqual(stale.status, 409)
        self.assertEqual(stale.body["error"]["code"], "conflict")
        self.assertEqual(stale.body["error"]["details"]["snapshot_sha256"], snapshot.body["snapshot_sha256"])
        self.assertNotIn("sqlite", json.dumps(stale.body).lower())
        denied = self.app.handle("GET", "/audit/events?entity_type=route&entity_id=fabric-a-b", self.headers)
        self.assertEqual(denied.status, 403)
        audit = self.app.handle("GET", "/audit/events?entity_type=route&entity_id=fabric-a-b", {"X-Actor-Id": "audit"})
        self.assertEqual(audit.status, 200)
        completed = [event for event in audit.body["events"] if event["event_type"] == "allocation.completed"]
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0]["payload"]["allocation_id"], first.body["allocation_id"])

    def test_concurrent_http_requests_share_one_decision(self) -> None:
        snapshot = self.app.handle("GET", "/routes/fabric-a-b/snapshot?service_date=2026-09-25", self.headers)
        responses = []

        def attempt() -> None:
            responses.append(self.app.handle("POST", "/routes/fabric-a-b/allocate", self.headers, self.allocate_body(snapshot.body["snapshot_sha256"])))

        threads = [threading.Thread(target=attempt) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(responses), 8)
        self.assertTrue(all(response.status == 200 for response in responses))
        self.assertEqual(len({response.body["allocation_id"] for response in responses}), 1)
        self.assertEqual(sum(1 for response in responses if not response.body["replayed"]), 1)

    def test_storage_errors_do_not_leak(self) -> None:
        def broken(*args: object, **kwargs: object) -> None:
            raise sqlite3.OperationalError("database is locked")

        self.service.allocate = broken
        response = self.app.handle("POST", "/routes/fabric-a-b/allocate", self.headers, self.allocate_body())
        self.assertEqual(response.status, 500)
        self.assertEqual(response.body["error"]["code"], "storage_error")
        self.assertNotIn("locked", response.body["error"]["message"])
        self.assertNotIn("sqlite", json.dumps(response.body).lower())


class AllocationSchemaMigrationTests(unittest.TestCase):
    def test_legacy_allocation_runs_table_is_rebuilt(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.executescript(
                """
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
                """
            )
            for token in ("a" * 64, "b" * 64):
                connection.execute(
                    "INSERT INTO allocation_runs(route_id,service_date,input_sha256,available_capacity,result_json,created_by,created_at) "
                    "VALUES('r1','2026-09-25',?,'1','{}','u1','2026-09-24T08:00:00Z')",
                    (token,),
                )
            initialize(connection)
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(allocation_runs)")}
            self.assertIn("route_revision", columns)
            self.assertIn("request_set_json", columns)
            self.assertIn("outage_window_json", columns)
            rows = connection.execute("SELECT * FROM allocation_runs ORDER BY allocation_id").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["input_sha256"], "a" * 64)
            self.assertEqual(rows[0]["route_revision"], 1)
            with self.assertRaises(sqlite3.IntegrityError) as ctx:
                connection.execute(
                    "INSERT INTO allocation_runs(route_id,service_date,input_sha256,available_capacity,result_json,created_by,created_at) "
                    "VALUES('r1','2026-09-25',?, '1','{}','u1','2026-09-24T09:00:00Z')",
                    ("c" * 64,),
                )
            self.assertIn("UNIQUE", str(ctx.exception))
            initialize(connection)
            self.assertEqual(connection.execute("SELECT count(*) FROM allocation_runs").fetchone()[0], 1)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
