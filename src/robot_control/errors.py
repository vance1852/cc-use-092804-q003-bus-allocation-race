"""供应服务向 API 和 CLI 暴露的稳定错误。"""

from __future__ import annotations

from typing import Any, Mapping


class SupplyError(RuntimeError):
    code = "supply_error"
    status = 400

    def __init__(self, message: str = "", *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details: dict[str, Any] | None = None if details is None else dict(details)


class NotFound(SupplyError):
    code = "not_found"
    status = 404


class Conflict(SupplyError):
    code = "conflict"
    status = 409


class ScheduleConflict(Conflict):
    """控制周期已有冻结裁决，后来者的申请依据与当前周期版本不一致。"""

    code = "schedule_conflict"
    status = 409


class Forbidden(SupplyError):
    code = "forbidden"
    status = 403


class InvalidState(SupplyError):
    code = "invalid_state"
    status = 409


class ValidationFailed(SupplyError):
    code = "validation_failed"
    status = 422
