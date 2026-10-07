"""The refrigeration control line expressed as orchestrated steps.

The plan mirrors the canonical pipeline order and replaces the one-shot
cycle: every stage is a named step with its own state, and every effect that
is not naturally idempotent runs through the cycle's effect log.

Compensation policy per step:

* measurements (temp, ammonia, evap, audit) leave nothing to undo;
* staging, the supply valve and the quota meter undo themselves
  automatically, in reverse;
* reversing a defrost is declared manual: warming a room back up is an
  operator decision, so the cycle parks for approval there;
* the ammonia response is never compensated automatically — clearing a
  safety latch stays an explicit operator action on the interlock endpoint.
"""

from __future__ import annotations

from coldstore.ammonia import validate_ppm
from coldstore.quota import validate_amount
from coldstore.temp import validate_temp

from .state import CompensationMode, HaltCycle, SkipStep
from .steps import Step, StepContext

CYCLE_NAME = "control"

PARAM_FIELDS = ("room_id", "comp_id", "temp", "ppm", "amount", "defrost")


def _validate(ctx: StepContext) -> object:
    params = ctx.params
    validate_temp(float(params.get("temp", 0.0)))
    validate_ppm(float(params.get("ppm", 0.0)))
    validate_amount(float(params.get("amount", 0.0)))
    return "inputs valid"


def build_steps(rt) -> list[Step]:
    """Bind the control line steps to one console runtime."""

    def record_temp(ctx: StepContext) -> object:
        temp = float(ctx.params.get("temp", 0.0))
        verdict = ctx.effect_once("reading", lambda: rt.temp.record(temp).as_dict())
        return verdict

    def detect_ammonia(ctx: StepContext) -> object:
        ppm = float(ctx.params.get("ppm", 0.0))
        verdict = rt.ammonia.detect(ppm)
        return verdict.as_dict()

    def respond_ammonia(ctx: StepContext) -> object:
        ppm = float(ctx.params.get("ppm", 0.0))
        comp_id = str(ctx.params.get("comp_id", ""))
        verdict = rt.ammonia.detect(ppm)
        if not verdict.trip:
            return "no trip"
        ctx.effect_once(
            "trip", lambda: rt.interlock.trip(f"ammonia {verdict.calibrated:.2f} ppm").as_dict()
        )
        if comp_id and rt.compressors.compressor(comp_id) is not None:
            rt.supply.stop_with_supply(rt.compressors, comp_id)
        return f"tripped at {verdict.calibrated:.2f} ppm"

    def stage_comp(ctx: StepContext) -> object:
        comp_id = str(ctx.params.get("comp_id", ""))
        if not comp_id:
            raise SkipStep("no comp_id")
        if rt.interlock.is_tripped():
            raise HaltCycle("interlock latched")
        temp = float(ctx.params.get("temp", 0.0))

        def apply() -> dict[str, object]:
            before, _ = rt.compressors.stage(comp_id)
            if rt.load.is_high(temp):
                after = rt.compressors.stage_up(comp_id)
            else:
                after = rt.compressors.stage_down(comp_id)
            return {"before": before, "after": after}

        return ctx.effect_once("stage", apply)

    def restore_comp(ctx: StepContext) -> object:
        comp_id = str(ctx.params.get("comp_id", ""))
        staged, present = ctx.effect_result("stage")
        if not present or not isinstance(staged, dict):
            return "nothing staged"
        before = int(staged.get("before", 0))
        return ctx.effect_once(
            "restored", lambda: {"restored": rt.compressors.set_stage(comp_id, before)}
        )

    def open_supply(ctx: StepContext) -> object:
        comp_id = str(ctx.params.get("comp_id", ""))
        if not comp_id:
            raise SkipStep("no comp_id")
        if rt.interlock.is_tripped():
            raise HaltCycle("interlock latched")
        rt.supply.start_with_supply(rt.compressors, comp_id)
        return f"supply open for {comp_id}"

    def close_supply(ctx: StepContext) -> object:
        comp_id = str(ctx.params.get("comp_id", ""))
        if not comp_id:
            return "no comp_id"
        if rt.compressors.compressor(comp_id) is None:
            return "compressor gone"
        rt.supply.stop_with_supply(rt.compressors, comp_id)
        return f"supply closed for {comp_id}"

    def toggle_defrost(ctx: StepContext) -> object:
        room_id = str(ctx.params.get("room_id", ""))
        if not room_id:
            raise SkipStep("no room_id")
        if int(ctx.params.get("defrost", 0) or 0):
            rt.defrost.start(room_id)
            return f"defrost started for {room_id}"
        rt.defrost.finish(room_id)
        return f"defrost finished for {room_id}"

    def revert_defrost(ctx: StepContext) -> object:
        room_id = str(ctx.params.get("room_id", ""))
        if not room_id:
            return "no room_id"
        if int(ctx.params.get("defrost", 0) or 0):
            rt.defrost.finish(room_id)
            return f"defrost finished for {room_id}"
        rt.defrost.start(room_id)
        return f"defrost started for {room_id}"

    def hold_evap(ctx: StepContext) -> object:
        temp = float(ctx.params.get("temp", 0.0))
        rt.evap.set_temperature(temp)
        return {"temperature": temp, "above_baseline": rt.evap.above_baseline()}

    def verify_interlock(ctx: StepContext) -> object:
        if rt.interlock.is_tripped():
            raise HaltCycle(f"interlock latched: {rt.interlock.reason()}")
        return "interlock clear"

    def meter_quota(ctx: StepContext) -> object:
        amount = float(ctx.params.get("amount", 0.0))
        value = ctx.effect_once("metered", lambda: rt.accumulator.add(amount))
        return {"quota": value}

    def refund_quota(ctx: StepContext) -> object:
        amount = float(ctx.params.get("amount", 0.0))
        value = ctx.effect_once("refunded", lambda: rt.accumulator.add(-amount))
        return {"quota": value}

    def record_audit(ctx: StepContext) -> object:
        temp = float(ctx.params.get("temp", 0.0))
        entry = ctx.effect_once(
            "audited", lambda: rt.auditor.record("cycle", f"{temp:.4f}").as_dict()
        )
        return entry

    return [
        Step("validate", _validate, max_attempts=2),
        Step("temp.record", record_temp, max_attempts=2),
        Step("ammonia.detect", detect_ammonia, max_attempts=2),
        Step("ammonia.respond", respond_ammonia),
        Step(
            "comp.stage",
            stage_comp,
            compensate=restore_comp,
            compensation_mode=CompensationMode.AUTO,
        ),
        Step(
            "valve.supply",
            open_supply,
            compensate=close_supply,
            compensation_mode=CompensationMode.AUTO,
        ),
        Step(
            "defrost.toggle",
            toggle_defrost,
            compensate=revert_defrost,
            compensation_mode=CompensationMode.MANUAL,
        ),
        Step("evap.hold", hold_evap),
        Step("interlock.verify", verify_interlock),
        Step(
            "quota.meter",
            meter_quota,
            compensate=refund_quota,
            compensation_mode=CompensationMode.AUTO,
        ),
        Step("audit.record", record_audit),
    ]
