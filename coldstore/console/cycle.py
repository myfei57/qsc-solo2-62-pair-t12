"""One full refrigeration control cycle, run through the orchestrator."""

from __future__ import annotations

from coldstore.ammonia import validate_ppm
from coldstore.orchestrator.model import CycleRecord
from coldstore.quota import validate_amount
from coldstore.temp import validate_temp

from .http import Request, Response, json_response
from .runtime import Runtime


def cycle_params(request: Request) -> dict[str, object]:
    """Extract and validate the control inputs every cycle runs with."""

    room_id = request.str_field("room_id")
    comp_id = request.str_field("comp_id")
    temp = request.float_field("temp")
    ppm = request.float_field("ppm")
    amount = request.float_field("amount")
    defrost = request.int_field("defrost")

    validate_temp(temp)
    validate_ppm(ppm)
    validate_amount(amount)

    return {
        "room_id": room_id,
        "comp_id": comp_id,
        "temp": temp,
        "ppm": ppm,
        "amount": amount,
        "defrost": defrost,
    }


def cycle_payload(rt: Runtime, record: CycleRecord, replayed: bool = False) -> dict:
    """Render a cycle record with the live readings the console reports."""

    details = {step.stage: step.detail for step in record.steps}
    temp_detail = details.get("temp", {})
    ammonia_detail = details.get("ammonia", {})
    verdict = rt.temp.verdict()
    comp_id = str(record.params.get("comp_id", ""))
    return {
        "cycle_id": record.id,
        "cycle_status": record.status,
        "replayed": replayed,
        "temp": float(temp_detail.get("value", verdict.value)),
        "rate": float(temp_detail.get("rate", verdict.rate)),
        "within_window": bool(temp_detail.get("within", verdict.within)),
        "ammonia_level": str(ammonia_detail.get("level", "")),
        "tripped": bool(ammonia_detail.get("trip", False)),
        "stage": rt.compressors.stage(comp_id)[0] if comp_id else 0,
        "supply_open": rt.supply.is_open(comp_id) if comp_id else False,
        "defrosting": rt.defrost.defrosting(),
        "quota": rt.accumulator.value(),
        "cooling_load": rt.load.load_for(float(record.params.get("temp", 0.0))),
        "audit_count": rt.auditor.count(),
        "steps": [step.as_dict() for step in record.steps],
    }


def run_cycle(rt: Runtime, request: Request) -> Response:
    """Walk the whole control line once, journaled step by step."""

    params = cycle_params(request)
    record, replayed = rt.orchestrator.start(
        rt, params, compensation=request.str_field("compensation", "auto")
    )
    return json_response(cycle_payload(rt, record, replayed))
