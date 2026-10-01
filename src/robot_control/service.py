"""控制算力单价、控制算力库存、实时控制总线和提名的事务用例。"""

from __future__ import annotations

import functools
import hashlib
import json
import sqlite3
import threading
from datetime import timedelta
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import (
    IndexQuote,
    Facility,
    InventoryLot,
    NominationRequest,
    Route,
    SupplyScenario,
    date_text,
    required_text,
)
from .planning import (
    AllocationRequest,
    PricePoint,
    allocate_capacity,
    canonical_json,
    decimal_text,
    delivered_after_loss,
    digest,
    effective_capacity,
    latest_streak,
    moving_average,
    quantize_volume,
    scenario_projection,
    weighted_inventory_cost,
)
from .storage import initialize, transaction


def _serialized(operation: Callable) -> Callable:
    """串行化服务方法：共享连接上的并发请求按到达顺序逐个执行。"""

    @functools.wraps(operation)
    def wrapper(self: "SupplyService", *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            return operation(self, *args, **kwargs)

    return wrapper


ROLE_PERMISSIONS = {
    "planner": {"quote.write", "catalog.write", "scenario.write", "scenario.run"},
    "dispatcher": {"nomination.write", "allocation.run", "transfer.write", "inventory.write"},
    "risk": {"outage.write", "scenario.approve", "report.read"},
    "auditor": {"report.read", "audit.read"},
}


class SupplyService:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self._lock = threading.RLock()
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM supply_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound("用户不存在")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM supply_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO supply_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    @_serialized
    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO supply_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    @_serialized
    def record_quote(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "quote.write")
        quote = IndexQuote.from_dict(raw)
        previous = self.connection.execute(
            "SELECT quote_id,source_revision FROM market_index_quotes WHERE market_index=? AND trade_date=? "
            "ORDER BY quote_id DESC LIMIT 1",
            (quote.market_index, quote.trade_date),
        ).fetchone()
        if previous is not None and previous["source_revision"] == quote.source_revision:
            raise Conflict("同一来源修订已登记")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO market_index_quotes(market_index,trade_date,close_cny,source_revision,observed_at,"
                    "supersedes_quote_id,recorded_by,recorded_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        quote.market_index,
                        quote.trade_date,
                        decimal_text(quote.close_cny),
                        quote.source_revision,
                        quote.observed_at,
                        None if previous is None else previous["quote_id"],
                        actor_id,
                        self._now(),
                    ),
                )
                quote_id = int(cursor.lastrowid)
                self._audit(
                    "quote",
                    str(quote_id),
                    "quote.recorded",
                    actor_id,
                    {"market_index": quote.market_index, "trade_date": quote.trade_date},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("控制算力单价版本冲突") from exc
        return {"quote_id": quote_id, "market_index": quote.market_index, "trade_date": quote.trade_date}

    @_serialized
    def price_summary(self, market_index: str, sessions: int = 20) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT q.trade_date,q.close_cny FROM market_index_quotes q "
            "JOIN (SELECT trade_date,max(quote_id) quote_id FROM market_index_quotes "
            "WHERE market_index=? GROUP BY trade_date) latest ON latest.quote_id=q.quote_id "
            "ORDER BY q.trade_date DESC LIMIT ?",
            (market_index.upper(), sessions),
        ).fetchall()
        points = [PricePoint(row["trade_date"], Decimal(row["close_cny"])) for row in rows]
        if not points:
            raise NotFound("没有基准控制算力单价")
        streak = latest_streak(points)
        average = moving_average(points, min(5, len(points)))
        latest = max(points, key=lambda item: item.trade_date)
        return {
            "market_index": market_index.upper(),
            "latest": {"trade_date": latest.trade_date, "close_cny": decimal_text(latest.close)},
            "latest_streak": None if streak is None else streak.as_dict(),
            "moving_average": None if average is None else decimal_text(average),
            "observations": len(points),
        }

    @_serialized
    def create_facility(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        facility = Facility.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO facilities(facility_id,name,kind,timezone,capacity_control_slots,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        facility.facility_id,
                        facility.name,
                        facility.kind,
                        facility.timezone,
                        decimal_text(facility.capacity_control_slots),
                        self._now(),
                    ),
                )
                self._audit("facility", facility.facility_id, "facility.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("设施编号已经存在") from exc
        return dict(raw)

    @_serialized
    def create_route(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        route = Route.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO routes(route_id,origin_id,destination_id,product,daily_capacity,"
                    "loss_basis_points,transit_hours,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        route.route_id,
                        route.origin_id,
                        route.destination_id,
                        route.product,
                        decimal_text(route.daily_capacity),
                        route.loss_basis_points,
                        route.transit_hours,
                        self._now(),
                    ),
                )
                self._audit("route", route.route_id, "route.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("实时控制总线编号冲突或设施不存在") from exc
        return self.route(route.route_id)

    @_serialized
    def route(self, route_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM routes WHERE route_id=?", (route_id,)).fetchone()
        if row is None:
            raise NotFound("实时控制总线不存在")
        return dict(row)

    @_serialized
    def announce_outage(
        self,
        actor_id: str,
        route_id: str,
        starts_at: str,
        ends_at: str | None,
        capacity_percent: object,
        reason: str,
    ) -> dict[str, Any]:
        self._require(actor_id, "outage.write")
        self.route(route_id)
        try:
            start = parse_utc(starts_at, "starts_at")
            end = None if ends_at is None else parse_utc(ends_at, "ends_at")
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        if end is not None and end <= start:
            raise ValidationFailed("ends_at 必须晚于 starts_at")
        percentage = Decimal(str(capacity_percent))
        if percentage < 0 or percentage > 100:
            raise ValidationFailed("capacity_percent 必须在 0 到 100 之间")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO route_outages(route_id,starts_at,ends_at,capacity_percent,reason,created_by,created_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (route_id, utc_text(start), None if end is None else utc_text(end), decimal_text(percentage), reason, actor_id, self._now()),
            )
            outage_id = int(cursor.lastrowid)
            self._audit("route", route_id, "outage.announced", actor_id, {"outage_id": outage_id})
        return {"outage_id": outage_id, "route_id": route_id, "state": "announced"}

    @_serialized
    def add_inventory_lot(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "inventory.write")
        lot = InventoryLot.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO inventory_lots(lot_id,facility_id,product,grade,quantity_control_slots,available_control_slots,"
                    "unit_cost_cny,received_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        lot.lot_id,
                        lot.facility_id,
                        lot.product,
                        lot.grade,
                        decimal_text(lot.quantity_control_slots),
                        decimal_text(lot.quantity_control_slots),
                        decimal_text(lot.unit_cost_cny),
                        lot.received_at,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("inventory_lot", lot.lot_id, "inventory.received", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("国产控制器资源批次冲突或设施不存在") from exc
        return self.inventory_lot(lot.lot_id)

    @_serialized
    def inventory_lot(self, lot_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM inventory_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if row is None:
            raise NotFound("国产控制器资源批次不存在")
        return dict(row)

    @_serialized
    def inventory_summary(self, facility_id: str, product: str) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT * FROM inventory_lots WHERE facility_id=? AND product=? ORDER BY received_at,lot_id",
            (facility_id, product),
        ).fetchall()
        return {"facility_id": facility_id, "product": product, **weighted_inventory_cost(rows)}

    @_serialized
    def submit_nomination(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "nomination.write")
        nomination = NominationRequest.from_dict(raw)
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM supply_idempotency WHERE scope='nomination' AND idempotency_key=?",
            (nomination.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同提名内容")
            return json.loads(stored["response_json"])
        route = self.route(nomination.route_id)
        if route["state"] != "active":
            raise InvalidState("实时控制总线当前不可提名")
        response = {
            "nomination_id": nomination.nomination_id,
            "route_id": nomination.route_id,
            "state": "submitted",
            "revision": 1,
        }
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO nominations(nomination_id,route_id,shipper_id,service_date,requested_control_slots,"
                    "priority,idempotency_key,submitted_by,submitted_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        nomination.nomination_id,
                        nomination.route_id,
                        nomination.shipper_id,
                        nomination.service_date,
                        decimal_text(nomination.requested_control_slots),
                        nomination.priority,
                        nomination.idempotency_key,
                        actor_id,
                        self._now(),
                    ),
                )
                self.connection.execute(
                    "INSERT INTO supply_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('nomination',?,?,?,?)",
                    (nomination.idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                self._audit("nomination", nomination.nomination_id, "nomination.submitted", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("提名编号或幂等键冲突") from exc
        return response

    def _allocation_snapshot(self, route: sqlite3.Row, service_date: str) -> dict[str, Any]:
        """汇总调度周期的容量快照：申请集合、降级窗口、总线版本和有效容量。

        快照指纹只覆盖申请内容（不随裁决改变的字段），因此裁决提交后
        同一周期的快照版本保持稳定，内容相同的重试可以据此识别。
        """

        start = service_date + "T00:00:00Z"
        end = service_date + "T23:59:59Z"
        outage_rows = self.connection.execute(
            "SELECT outage_id,starts_at,ends_at,capacity_percent,reason FROM route_outages "
            "WHERE route_id=? AND state IN ('announced','active') "
            "AND starts_at<=? AND (ends_at IS NULL OR ends_at>=?) ORDER BY outage_id",
            (route["route_id"], end, start),
        ).fetchall()
        outage_window = [
            {
                "outage_id": row["outage_id"],
                "starts_at": row["starts_at"],
                "ends_at": row["ends_at"],
                "capacity_percent": row["capacity_percent"],
                "reason": row["reason"],
            }
            for row in outage_rows
        ]
        nomination_rows = self.connection.execute(
            "SELECT nomination_id,requested_control_slots,priority,submitted_at FROM nominations "
            "WHERE route_id=? AND service_date=? ORDER BY priority,submitted_at,nomination_id",
            (route["route_id"], service_date),
        ).fetchall()
        request_set = [
            {
                "nomination_id": row["nomination_id"],
                "requested_control_slots": row["requested_control_slots"],
                "priority": row["priority"],
                "submitted_at": row["submitted_at"],
            }
            for row in nomination_rows
        ]
        available = effective_capacity(
            Decimal(route["daily_capacity"]),
            [Decimal(row["capacity_percent"]) for row in outage_rows],
        )
        snapshot: dict[str, Any] = {
            "route_id": route["route_id"],
            "service_date": service_date,
            "route_revision": route["revision"],
            "available_capacity": decimal_text(available),
            "outage_window": outage_window,
            "request_set": request_set,
        }
        snapshot["snapshot_sha256"] = digest(snapshot)
        return snapshot

    @_serialized
    def capacity_snapshot(self, route_id: str, service_date: str) -> dict[str, Any]:
        """读取调度周期当前的容量快照和快照版本，供控制器作为裁决依据。"""

        service_date = date_text(service_date, "service_date")
        route = self.connection.execute("SELECT * FROM routes WHERE route_id=?", (route_id,)).fetchone()
        if route is None:
            raise NotFound("实时控制总线不存在")
        return self._allocation_snapshot(route, service_date)

    @staticmethod
    def _allocation_response(row: sqlite3.Row, *, replayed: bool) -> dict[str, Any]:
        return {"allocation_id": row["allocation_id"], **json.loads(row["result_json"]), "replayed": replayed}

    @_serialized
    def allocate(
        self,
        actor_id: str,
        route_id: str,
        service_date: str,
        expected_snapshot: str | None = None,
    ) -> dict[str, Any]:
        """裁决一个调度周期的时隙分配。

        整个读取-裁决-提交在单个 IMMEDIATE 事务内完成：同一周期只存在一份
        有效决定，申请集合、降级窗口和总线版本随决定一起冻结。携带相同
        快照版本的并发申请拿回首次裁决（replayed=True）；依据已经变化的
        申请收到包含当前版本的冲突响应。
        """

        self._require(actor_id, "allocation.run")
        service_date = date_text(service_date, "service_date")
        if expected_snapshot is not None and not isinstance(expected_snapshot, str):
            raise ValidationFailed("expected_snapshot 必须是快照版本字符串")
        try:
            with transaction(self.connection, immediate=True):
                route = self.connection.execute(
                    "SELECT * FROM routes WHERE route_id=?", (route_id,)
                ).fetchone()
                if route is None:
                    raise NotFound("实时控制总线不存在")
                snapshot = self._allocation_snapshot(route, service_date)
                existing = self.connection.execute(
                    "SELECT * FROM allocation_runs WHERE route_id=? AND service_date=?",
                    (route_id, service_date),
                ).fetchone()
                if existing is not None:
                    if expected_snapshot == existing["input_sha256"]:
                        return self._allocation_response(existing, replayed=True)
                    if expected_snapshot is None and snapshot["snapshot_sha256"] == existing["input_sha256"]:
                        return self._allocation_response(existing, replayed=True)
                    details: dict[str, Any] = {
                        "route_id": route_id,
                        "service_date": service_date,
                        "snapshot_sha256": existing["input_sha256"],
                        "allocation_id": existing["allocation_id"],
                    }
                    if snapshot["snapshot_sha256"] != existing["input_sha256"]:
                        details["live_snapshot_sha256"] = snapshot["snapshot_sha256"]
                    raise Conflict(
                        "本调度周期已完成裁决，申请集合、降级窗口与总线版本已冻结",
                        details=details,
                    )
                if expected_snapshot is not None and expected_snapshot != snapshot["snapshot_sha256"]:
                    raise Conflict(
                        "容量快照已变化，请基于当前版本重新申请",
                        details={
                            "route_id": route_id,
                            "service_date": service_date,
                            "snapshot_sha256": snapshot["snapshot_sha256"],
                        },
                    )
                submitted = self.connection.execute(
                    "SELECT * FROM nominations WHERE route_id=? AND service_date=? AND state='submitted' "
                    "ORDER BY priority,submitted_at,nomination_id",
                    (route_id, service_date),
                ).fetchall()
                if not submitted:
                    raise InvalidState("没有待分配提名")
                requests = [
                    AllocationRequest(
                        row["nomination_id"],
                        Decimal(row["requested_control_slots"]),
                        int(row["priority"]),
                        row["submitted_at"],
                    )
                    for row in submitted
                ]
                allocations = allocate_capacity(Decimal(snapshot["available_capacity"]), requests)
                result = {
                    "route_id": route_id,
                    "service_date": service_date,
                    "snapshot_sha256": snapshot["snapshot_sha256"],
                    "route_revision": snapshot["route_revision"],
                    "available_capacity": snapshot["available_capacity"],
                    "outage_window": snapshot["outage_window"],
                    "allocations": allocations,
                }
                cursor = self.connection.execute(
                    "INSERT INTO allocation_runs(route_id,service_date,input_sha256,route_revision,"
                    "available_capacity,request_set_json,outage_window_json,result_json,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        route_id,
                        service_date,
                        snapshot["snapshot_sha256"],
                        snapshot["route_revision"],
                        snapshot["available_capacity"],
                        canonical_json(snapshot["request_set"]),
                        canonical_json(snapshot["outage_window"]),
                        canonical_json(result),
                        actor_id,
                        self._now(),
                    ),
                )
                allocation_id = int(cursor.lastrowid)
                for item in allocations:
                    state = "allocated" if Decimal(item["allocated_control_slots"]) > 0 else "cancelled"
                    updated = self.connection.execute(
                        "UPDATE nominations SET allocated_control_slots=?,state=?,revision=revision+1 "
                        "WHERE nomination_id=? AND state='submitted'",
                        (item["allocated_control_slots"], state, item["nomination_id"]),
                    )
                    if updated.rowcount != 1:
                        raise InvalidState("提名在裁决期间被改动，裁决已整体回滚")
                self._audit(
                    "route",
                    route_id,
                    "allocation.completed",
                    actor_id,
                    {
                        "allocation_id": allocation_id,
                        "service_date": service_date,
                        "snapshot_sha256": snapshot["snapshot_sha256"],
                    },
                )
                return {"allocation_id": allocation_id, **result, "replayed": False}
        except sqlite3.IntegrityError as exc:
            raise Conflict("裁决提交未成功，本调度周期可能已被占用，请基于当前容量快照重试") from exc
        except sqlite3.OperationalError as exc:
            raise Conflict("实时控制总线正在被其他控制器裁决，请稍后重试") from exc

    @_serialized
    def dispatch_transfer(
        self,
        actor_id: str,
        transfer_id: str,
        nomination_id: str,
        lot_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        self._require(actor_id, "transfer.write")
        nomination = self.connection.execute(
            "SELECT n.*,r.loss_basis_points,r.transit_hours,r.origin_id FROM nominations n "
            "JOIN routes r ON r.route_id=n.route_id WHERE n.nomination_id=?",
            (nomination_id,),
        ).fetchone()
        if nomination is None:
            raise NotFound("提名不存在")
        if nomination["state"] != "allocated" or nomination["revision"] != expected_revision:
            raise InvalidState("提名不是当前可交付版本")
        lot = self.connection.execute("SELECT * FROM inventory_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if lot is None:
            raise NotFound("国产控制器资源批次不存在")
        allocated = Decimal(nomination["allocated_control_slots"])
        available = Decimal(lot["available_control_slots"])
        if lot["facility_id"] != nomination["origin_id"] or lot["product"] != self.route(nomination["route_id"])["product"]:
            raise Conflict("国产控制器资源批次与实时控制总线起点或资源类型不匹配")
        if available < allocated:
            raise Conflict("控制算力库存不足以完成分配")
        expected_delivery = delivered_after_loss(allocated, int(nomination["loss_basis_points"]))
        departed_at = self._now()
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE inventory_lots SET available_control_slots=?,revision=revision+1 WHERE lot_id=? AND revision=?",
                (decimal_text(quantize_volume(available - allocated)), lot_id, lot["revision"]),
            )
            self.connection.execute(
                "UPDATE nominations SET state='in_transit',revision=revision+1 WHERE nomination_id=? AND revision=?",
                (nomination_id, expected_revision),
            )
            self.connection.execute(
                "INSERT INTO transfers(transfer_id,nomination_id,inventory_lot_id,loaded_control_slots,"
                "expected_delivered_control_slots,departed_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    transfer_id,
                    nomination_id,
                    lot_id,
                    decimal_text(allocated),
                    decimal_text(expected_delivery),
                    departed_at,
                    actor_id,
                    departed_at,
                ),
            )
            self._audit("transfer", transfer_id, "transfer.dispatched", actor_id, {"nomination_id": nomination_id})
        return {
            "transfer_id": transfer_id,
            "state": "in_transit",
            "loaded_control_slots": decimal_text(allocated),
            "expected_delivered_control_slots": decimal_text(expected_delivery),
            "expected_arrival": utc_text(parse_utc(departed_at) + timedelta(hours=int(nomination["transit_hours"]))),
        }

    @_serialized
    def create_scenario(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "scenario.write")
        scenario = SupplyScenario.from_dict(raw)
        definition = canonical_json(raw)
        content_sha256 = hashlib.sha256(definition.encode("utf-8")).hexdigest()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO supply_scenarios(scenario_id,name,definition_json,content_sha256,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (scenario.scenario_id, scenario.name, definition, content_sha256, actor_id, self._now()),
                )
                self._audit("scenario", scenario.scenario_id, "scenario.created", actor_id, {"sha256": content_sha256})
        except sqlite3.IntegrityError as exc:
            raise Conflict("情景编号或内容已经存在") from exc
        return {"scenario_id": scenario.scenario_id, "state": "draft", "sha256": content_sha256}

    @_serialized
    def approve_scenario(self, actor_id: str, scenario_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "scenario.approve")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE supply_scenarios SET state='approved',revision=revision+1 "
                "WHERE scenario_id=? AND state='draft' AND revision=?",
                (scenario_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("情景不是当前草稿版本")
            self._audit("scenario", scenario_id, "scenario.approved", actor_id, {})
        return {"scenario_id": scenario_id, "state": "approved", "revision": expected_revision + 1}

    @_serialized
    def run_scenario(self, actor_id: str, scenario_id: str, as_of_date: str) -> dict[str, Any]:
        self._require(actor_id, "scenario.run")
        row = self.connection.execute(
            "SELECT * FROM supply_scenarios WHERE scenario_id=?", (scenario_id,)
        ).fetchone()
        if row is None:
            raise NotFound("情景不存在")
        if row["state"] != "approved":
            raise InvalidState("只有已批准情景可以运行")
        scenario = SupplyScenario.from_dict(json.loads(row["definition_json"]))
        price_row = self.connection.execute(
            "SELECT close_cny FROM market_index_quotes WHERE trade_date<=? ORDER BY trade_date DESC,quote_id DESC LIMIT 1",
            (as_of_date,),
        ).fetchone()
        if price_row is None:
            raise InvalidState("截止日期没有可用控制算力单价")
        routes = self.connection.execute("SELECT * FROM routes WHERE state='active' ORDER BY route_id").fetchall()
        inventory = self.connection.execute(
            "SELECT facility_id,product,sum(CAST(available_control_slots AS REAL)) available_control_slots "
            "FROM inventory_lots GROUP BY facility_id,product ORDER BY facility_id,product"
        ).fetchall()
        input_value = {
            "scenario_sha256": row["content_sha256"],
            "as_of_date": as_of_date,
            "price": price_row["close_cny"],
            "routes": [dict(item) for item in routes],
            "inventory": [dict(item) for item in inventory],
        }
        input_sha256 = digest(input_value)
        existing = self.connection.execute(
            "SELECT run_id,result_json FROM scenario_runs WHERE scenario_id=? AND as_of_date=? AND input_sha256=?",
            (scenario_id, as_of_date, input_sha256),
        ).fetchone()
        if existing is not None:
            return {"run_id": existing["run_id"], **json.loads(existing["result_json"]), "replayed": True}
        result = scenario_projection(
            current_price=Decimal(price_row["close_cny"]),
            market_index_drop_percent=scenario.market_index_drop_percent,
            routes=routes,
            inventory=inventory,
            route_capacity_changes=scenario.route_capacity_changes,
            demand_changes=scenario.demand_changes,
        )
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO scenario_runs(scenario_id,as_of_date,input_sha256,result_json,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (scenario_id, as_of_date, input_sha256, canonical_json(result), actor_id, self._now()),
            )
            run_id = int(cursor.lastrowid)
            self._audit("scenario", scenario_id, "scenario.executed", actor_id, {"run_id": run_id})
        return {"run_id": run_id, **result, "replayed": False}

    @_serialized
    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute("SELECT * FROM supply_audit_events ORDER BY event_id").fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        return {"valid": valid, "events": len(rows), "head_hash": previous_hash}

    @_serialized
    def audit_events(self, actor_id: str, entity_type: str, entity_id: str) -> dict[str, Any]:
        """按实体读取审计事件，供联调人员核对执行侧收到的有效决定。"""

        self._require(actor_id, "audit.read")
        entity_type = required_text(entity_type, "entity_type", 64)
        entity_id = required_text(entity_id, "entity_id", 64)
        rows = self.connection.execute(
            "SELECT event_type,actor_id,payload_json,created_at FROM supply_audit_events "
            "WHERE entity_type=? AND entity_id=? ORDER BY event_id",
            (entity_type, entity_id),
        ).fetchall()
        return {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "events": [
                {
                    "event_type": row["event_type"],
                    "actor_id": row["actor_id"],
                    "payload": json.loads(row["payload_json"]),
                    "created_at": row["created_at"],
                }
                for row in rows
            ],
        }
