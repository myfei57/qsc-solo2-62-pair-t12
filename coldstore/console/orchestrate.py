"""Operator endpoints for orchestrated control cycles."""

from __future__ import annotations

from typing import TYPE_CHECKING

from coldstore.orchestrator import cycle_summary
from coldstore.orchestrator.model import CYCLE_COMPENSATING

from .cycle import cycle_params, cycle_payload
from .http import Request, RequestError, Response, json_response

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from .server import Server


def _call(func, *args):
    """Map a missing cycle to a 404 and let validation errors stay 400s."""

    try:
        return func(*args)
    except LookupError as exc:
        raise RequestError(404, str(exc)) from exc


def _cycle_id(request: Request) -> str:
    cycle_id = request.str_field("id") or request.str_field("cycle_id")
    if not cycle_id:
        values = request.query.get("id", [])
        if values:
            cycle_id = values[0]
    if not cycle_id:
        raise RequestError(400, "id is required")
    return cycle_id


def _flag(request: Request, name: str) -> bool:
    return request.str_field(name).lower() in ("1", "true", "yes")


def run(server: "Server", request: Request) -> Response:
    rt = server.runtime
    params = cycle_params(request)
    cycle_id = request.str_field("cycle_id") or request.str_field("id")
    record, replayed = _call(
        rt.orchestrator.start,
        rt,
        params,
        cycle_id,
        request.str_field("compensation", "auto"),
    )
    return json_response(cycle_payload(rt, record, replayed))


def status(server: "Server", request: Request) -> Response:
    rt = server.runtime
    record = _call(rt.orchestrator.status, _cycle_id(request))
    payload = record.as_dict()
    payload["compensations"] = [
        comp.as_dict() for comp in rt.orchestrator.compensations(record.id)
    ]
    return json_response(payload)


def listing(server: "Server", request: Request) -> Response:
    records = server.runtime.orchestrator.list_cycles()
    return json_response(
        {"cycles": [cycle_summary(record) for record in records], "count": len(records)}
    )


def resume(server: "Server", request: Request) -> Response:
    rt = server.runtime
    record = _call(rt.orchestrator.resume, rt, _cycle_id(request))
    return json_response(cycle_payload(rt, record))


def rollback(server: "Server", request: Request) -> Response:
    rt = server.runtime
    record = _call(rt.orchestrator.rollback, rt, _cycle_id(request))
    payload = record.as_dict()
    payload["compensations"] = [
        comp.as_dict() for comp in rt.orchestrator.compensations(record.id)
    ]
    return json_response(payload)


def restart(server: "Server", request: Request) -> Response:
    rt = server.runtime
    rerun = _flag(request, "rerun")
    record, follow = _call(rt.orchestrator.restart, rt, _cycle_id(request), rerun)
    payload = record.as_dict()
    payload["compensations"] = [
        comp.as_dict() for comp in rt.orchestrator.compensations(record.id)
    ]
    payload["rerun"] = follow.as_dict() if follow else None
    payload["rerun_pending"] = (
        rerun and follow is None and record.status == CYCLE_COMPENSATING
    )
    return json_response(payload)


def rerun(server: "Server", request: Request) -> Response:
    rt = server.runtime
    record = _call(rt.orchestrator.rerun, rt, _cycle_id(request))
    return json_response(cycle_payload(rt, record))


def compensations(server: "Server", request: Request) -> Response:
    rt = server.runtime
    comps = _call(rt.orchestrator.compensations, _cycle_id(request))
    return json_response(
        {
            "compensations": [comp.as_dict() for comp in comps],
            "pending": sum(1 for comp in comps if comp.status == "pending"),
        }
    )


def compensate(server: "Server", request: Request) -> Response:
    rt = server.runtime
    record, comps = _call(
        rt.orchestrator.approve,
        rt,
        _cycle_id(request),
        request.str_field("compensation_id"),
    )
    payload = record.as_dict()
    payload["compensations"] = [comp.as_dict() for comp in comps]
    return json_response(payload)
