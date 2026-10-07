"""Orchestrated control cycle endpoints.

The run endpoint is the idempotent entry point: submit a cycle id and its
parameters and the cycle either starts or resumes from its persisted
checkpoint. Resume, compensate and approve rebuild the step plan from the
parameters frozen at creation, so a cycle always continues as the plan it
started with.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from coldstore.orchestrate import Orchestrator
from coldstore.orchestrate.line import CYCLE_NAME, PARAM_FIELDS, build_steps
from coldstore.orchestrate.state import FailureStrategy

from .http import Request, RequestError, Response, json_response

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from .server import Server


def _params_from(request: Request) -> dict[str, object]:
    return {
        "room_id": request.str_field("room_id"),
        "comp_id": request.str_field("comp_id"),
        "temp": request.float_field("temp"),
        "ppm": request.float_field("ppm"),
        "amount": request.float_field("amount"),
        "defrost": request.int_field("defrost"),
    }


def _strategy_from(request: Request) -> FailureStrategy:
    raw = request.str_field("strategy", str(FailureStrategy.HOLD))
    try:
        return FailureStrategy(raw)
    except ValueError:
        raise RequestError(400, "strategy must be hold or compensate") from None


def _require_cycle(orchestrator: Orchestrator, cycle_id: str):
    record = orchestrator.get(cycle_id)
    if record is None:
        raise RequestError(404, f"unknown cycle {cycle_id}")
    return record


def run(server: "Server", request: Request) -> Response:
    cycle_id = request.str_field("cycle_id") or uuid.uuid4().hex
    params = _params_from(request)
    orchestrator = Orchestrator(server.runtime.store)
    record = orchestrator.run(
        cycle_id,
        CYCLE_NAME,
        build_steps(server.runtime),
        params,
        _strategy_from(request),
    )
    return json_response(record.as_dict())


def resume(server: "Server", request: Request) -> Response:
    cycle_id = request.str_field("cycle_id")
    if not cycle_id:
        raise RequestError(400, "cycle_id is required")
    orchestrator = Orchestrator(server.runtime.store)
    _require_cycle(orchestrator, cycle_id)
    record = orchestrator.resume(cycle_id, build_steps(server.runtime))
    return json_response(record.as_dict())


def compensate(server: "Server", request: Request) -> Response:
    cycle_id = request.str_field("cycle_id")
    if not cycle_id:
        raise RequestError(400, "cycle_id is required")
    orchestrator = Orchestrator(server.runtime.store)
    _require_cycle(orchestrator, cycle_id)
    record = orchestrator.compensate(cycle_id, build_steps(server.runtime))
    return json_response(record.as_dict())


def approve(server: "Server", request: Request) -> Response:
    cycle_id = request.str_field("cycle_id")
    if not cycle_id:
        raise RequestError(400, "cycle_id is required")
    orchestrator = Orchestrator(server.runtime.store)
    _require_cycle(orchestrator, cycle_id)
    record = orchestrator.approve(
        cycle_id, build_steps(server.runtime), step_name=request.str_field("step")
    )
    return json_response(record.as_dict())


def lookup(server: "Server", request: Request) -> Response:
    cycle_id = request.str_field("cycle_id")
    if not cycle_id:
        raise RequestError(400, "cycle_id is required")
    orchestrator = Orchestrator(server.runtime.store)
    record = _require_cycle(orchestrator, cycle_id)
    return json_response(record.as_dict())


def listing(server: "Server", request: Request) -> Response:
    orchestrator = Orchestrator(server.runtime.store)
    cycles = orchestrator.cycles()
    attention = [
        record.cycle_id
        for record in cycles
        if record.status in ("failed", "awaiting_approval", "compensation_failed")
    ]
    return json_response(
        {
            "cycles": [record.summary() for record in cycles],
            "count": len(cycles),
            "attention": attention,
            "param_fields": list(PARAM_FIELDS),
        }
    )
