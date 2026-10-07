"""Binding between the canonical control stages and their cycle behaviour.

Each step declares how to run, how to prepare an idempotency memo before
running, and how to compensate afterwards. The memo is journaled before
the action executes, so a step interrupted between its effect and the
journal write recognises its own work on re-entry instead of doubling it.

The runtime is passed duck typed: the orchestrator package must not import
the console wiring that owns it.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable

from coldstore.ns import Stage, control_line
from coldstore.ns.pipeline import STAGE_ACTIONS
from coldstore.quota.accumulator import EPOCH_CAP

from . import model


@dataclass(frozen=True)
class CycleContext:
    """What a step knows about the cycle it runs in."""

    cycle_id: str
    params: dict
    details: dict  # stage -> detail of every finished step

    def tripped(self) -> bool:
        return bool(self.details.get(Stage.AMMONIA.value, {}).get("trip", False))

    def str_param(self, name: str) -> str:
        return str(self.params.get(name, ""))

    def float_param(self, name: str) -> float:
        try:
            return float(self.params.get(name, 0.0))
        except (TypeError, ValueError):
            return 0.0

    def int_param(self, name: str) -> int:
        try:
            return int(self.params.get(name, 0))
        except (TypeError, ValueError):
            return 0


@dataclass(frozen=True)
class Outcome:
    """Result of one step action."""

    status: str = model.DONE
    detail: dict = field(default_factory=dict)


def skipped(reason: str) -> Outcome:
    return Outcome(status=model.SKIPPED, detail={"reason": reason})


@dataclass(frozen=True)
class StepSpec:
    """Action, compensation and execution mode for one stage."""

    stage: Stage
    action: str
    mode: str  # MODE_AUTO, MODE_MANUAL, or "" when there is nothing to undo
    run: Callable[[object, CycleContext, dict], Outcome]
    prepare: Callable[[object, CycleContext], dict] | None = None
    compensate: Callable[[object, CycleContext, dict], str | None] | None = None


# --- temp --------------------------------------------------------------------


def _temp_prepare(rt: object, ctx: CycleContext) -> dict:
    return {"at": int(time.time())}


def _temp_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    temp = ctx.float_param("temp")
    at = int(memo.get("at", 0)) or int(time.time())
    readings = rt.temp.readings()
    if readings and readings[-1].value == temp and readings[-1].at == at:
        # the reading for this attempt is already persisted
        verdict = rt.temp.verdict()
        recorded = False
    else:
        verdict = rt.temp.record(temp, at=at)
        recorded = True
    return Outcome(
        detail={
            "value": verdict.value,
            "rate": verdict.rate,
            "within": verdict.within,
            "recorded": recorded,
        }
    )


# --- ammonia -----------------------------------------------------------------


def _ammonia_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    verdict = rt.ammonia.detect(ctx.float_param("ppm"))
    return Outcome(
        detail={
            "level": verdict.level,
            "trip": verdict.trip,
            "calibrated": verdict.calibrated,
            "threshold": verdict.threshold,
        }
    )


# --- compressors -------------------------------------------------------------


def _comp_prepare(rt: object, ctx: CycleContext) -> dict:
    comp_id = ctx.str_param("comp_id")
    if not comp_id:
        return {}
    stage, _ = rt.compressors.stage(comp_id)
    return {"prior": stage}


def _comp_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    comp_id = ctx.str_param("comp_id")
    if not comp_id:
        return skipped("no comp_id")
    compressor = rt.compressors.compressor(comp_id)
    if compressor is None:
        raise ValueError(f"compressor {comp_id} not found")
    prior = int(memo.get("prior", 0))
    if ctx.tripped():
        rt.compressors.stop(comp_id)
        return Outcome(
            detail={"comp_id": comp_id, "stage": 0, "prior": prior, "tripped": True}
        )
    high = rt.load.is_high(ctx.float_param("temp"))
    target = prior + 1 if high else prior - 1
    target = max(0, min(target, compressor.stages))
    rt.compressors.set_stage(comp_id, target)
    return Outcome(
        detail={"comp_id": comp_id, "stage": target, "prior": prior, "high": high}
    )


def _comp_undo(rt: object, ctx: CycleContext, detail: dict) -> str | None:
    if detail.get("tripped"):
        return None  # a tripped machine stays stopped
    comp_id = str(detail.get("comp_id", ""))
    if not comp_id:
        return None
    prior = int(detail.get("prior", 0))
    rt.compressors.set_stage(comp_id, prior)
    return f"restored stage {prior} on {comp_id}"


# --- supply valve ------------------------------------------------------------


def _valve_prepare(rt: object, ctx: CycleContext) -> dict:
    comp_id = ctx.str_param("comp_id")
    if not comp_id:
        return {}
    compressor = rt.compressors.compressor(comp_id)
    return {
        "prior_open": rt.supply.is_open(comp_id),
        "prior_running": bool(compressor.running) if compressor else False,
    }


def _valve_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    comp_id = ctx.str_param("comp_id")
    if not comp_id:
        return skipped("no comp_id")
    detail = {
        "comp_id": comp_id,
        "prior_open": bool(memo.get("prior_open", False)),
        "prior_running": bool(memo.get("prior_running", False)),
    }
    if ctx.tripped():
        rt.supply.close_valve(comp_id)
        return Outcome(detail={**detail, "position": "closed"})
    rt.supply.start_with_supply(rt.compressors, comp_id)
    return Outcome(detail={**detail, "position": "open"})


def _valve_undo(rt: object, ctx: CycleContext, detail: dict) -> str | None:
    comp_id = str(detail.get("comp_id", ""))
    if not comp_id or detail.get("position") != "open":
        return None  # a closed valve is the safe state; nothing to undo
    restored = []
    if not detail.get("prior_running", False):
        rt.compressors.stop(comp_id)
        restored.append("stopped the compressor")
    if not detail.get("prior_open", False):
        rt.supply.close_valve(comp_id)
        restored.append("closed the supply valve")
    return ", ".join(restored) if restored else None


# --- defrost -----------------------------------------------------------------


def _defrost_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    if ctx.tripped():
        return skipped("interlock tripped")
    room_id = ctx.str_param("room_id")
    if not room_id:
        return skipped("no room_id")
    if ctx.int_param("defrost"):
        rt.defrost.start(room_id)
        return Outcome(detail={"room_id": room_id, "action": "started"})
    rt.defrost.finish(room_id)
    return Outcome(detail={"room_id": room_id, "action": "finished"})


def _defrost_undo(rt: object, ctx: CycleContext, detail: dict) -> str | None:
    room_id = str(detail.get("room_id", ""))
    action = str(detail.get("action", ""))
    if not room_id:
        return None
    if action == "started":
        rt.defrost.finish(room_id)
        return f"finished the defrost on {room_id}"
    if action == "finished":
        rt.defrost.start(room_id)
        return f"restarted the defrost on {room_id}"
    return None


# --- evaporative condenser ---------------------------------------------------


def _evap_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    state = rt.evap.state()
    return Outcome(detail=state.as_dict())


# --- interlock ---------------------------------------------------------------


def _interlock_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    if not ctx.tripped():
        return skipped("no ammonia trip")
    if rt.interlock.is_tripped():
        # the latch is already held; re-tripping would only duplicate the log
        return Outcome(
            detail={"tripped": True, "reason": rt.interlock.reason(), "already": True}
        )
    calibrated = float(ctx.details.get(Stage.AMMONIA.value, {}).get("calibrated", 0.0))
    reason = f"ammonia {calibrated:.2f} ppm"
    rt.interlock.trip(reason)
    return Outcome(detail={"tripped": True, "reason": reason, "already": False})


def _interlock_undo(rt: object, ctx: CycleContext, detail: dict) -> str | None:
    if not detail.get("tripped") or detail.get("already"):
        return None  # this cycle did not set the latch
    rt.interlock.reset()
    return "released the interlock latch"


# --- quota -------------------------------------------------------------------


def _quota_prepare(rt: object, ctx: CycleContext) -> dict:
    amount = ctx.float_param("amount")
    baseline = rt.accumulator.value()
    target = baseline + amount
    if 0 < EPOCH_CAP <= target:
        target -= EPOCH_CAP  # mirror the accumulator wrap
    return {"baseline": baseline, "target": target}


def _quota_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    amount = ctx.float_param("amount")
    target = float(memo.get("target", 0.0))
    current = rt.accumulator.value()
    if abs(current - target) < 1e-9:
        # the meter already shows this attempt's effect
        return Outcome(detail={"value": current, "amount": amount, "applied": False})
    value = rt.accumulator.add(amount)
    return Outcome(detail={"value": value, "amount": amount, "applied": True})


def _quota_undo(rt: object, ctx: CycleContext, detail: dict) -> str | None:
    if not detail.get("applied"):
        return None
    amount = float(detail.get("amount", 0.0))
    value = rt.accumulator.add(-amount)
    return f"returned the accumulator to {value:.4f}"


# --- audit -------------------------------------------------------------------


def _audit_run(rt: object, ctx: CycleContext, memo: dict) -> Outcome:
    # append-only log: the journal guards against re-entry, and a crash in
    # the narrow window after this write fails toward one extra audit line
    entry = rt.auditor.record("cycle", f"{ctx.float_param('temp'):.4f}")
    return Outcome(detail={"entry": entry.id})


SPECS: dict[Stage, StepSpec] = {
    Stage.TEMP: StepSpec(
        stage=Stage.TEMP,
        action=STAGE_ACTIONS[Stage.TEMP],
        mode="",
        run=_temp_run,
        prepare=_temp_prepare,
    ),
    Stage.AMMONIA: StepSpec(
        stage=Stage.AMMONIA,
        action=STAGE_ACTIONS[Stage.AMMONIA],
        mode="",
        run=_ammonia_run,
    ),
    Stage.COMP: StepSpec(
        stage=Stage.COMP,
        action=STAGE_ACTIONS[Stage.COMP],
        mode=model.MODE_AUTO,
        run=_comp_run,
        prepare=_comp_prepare,
        compensate=_comp_undo,
    ),
    Stage.VALVE: StepSpec(
        stage=Stage.VALVE,
        action=STAGE_ACTIONS[Stage.VALVE],
        mode=model.MODE_AUTO,
        run=_valve_run,
        prepare=_valve_prepare,
        compensate=_valve_undo,
    ),
    Stage.DEFROST: StepSpec(
        stage=Stage.DEFROST,
        action=STAGE_ACTIONS[Stage.DEFROST],
        mode=model.MODE_AUTO,
        run=_defrost_run,
        compensate=_defrost_undo,
    ),
    Stage.EVAP: StepSpec(
        stage=Stage.EVAP,
        action=STAGE_ACTIONS[Stage.EVAP],
        mode="",
        run=_evap_run,
    ),
    Stage.INTERLOCK: StepSpec(
        stage=Stage.INTERLOCK,
        action=STAGE_ACTIONS[Stage.INTERLOCK],
        mode=model.MODE_MANUAL,  # a safety latch is released by a human, not a saga
        run=_interlock_run,
        compensate=_interlock_undo,
    ),
    Stage.QUOTA: StepSpec(
        stage=Stage.QUOTA,
        action=STAGE_ACTIONS[Stage.QUOTA],
        mode=model.MODE_AUTO,
        run=_quota_run,
        prepare=_quota_prepare,
        compensate=_quota_undo,
    ),
    Stage.AUDIT: StepSpec(
        stage=Stage.AUDIT,
        action=STAGE_ACTIONS[Stage.AUDIT],
        mode="",
        run=_audit_run,
    ),
}


def ordered_specs() -> list[StepSpec]:
    """Step specs in canonical control line order."""

    return [SPECS[stage] for stage in control_line().stages]


def spec_for(stage: str) -> StepSpec:
    return SPECS[Stage(stage)]
