"""Persisted records describing one orchestrated cycle.

A cycle record is the breakpoint: the cursor says which step runs next, and
each step record carries its own state, attempts and error. Everything here
serialises to plain JSON so the whole cycle fits in a single store key.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .state import CycleStatus, StepStatus


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class StepRecord:
    """Persisted state of one step execution."""

    name: str
    status: str = StepStatus.PENDING.value
    attempts: int = 0
    error: str = ""
    note: str = ""
    result: object = None
    started_at: str = ""
    finished_at: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "status": self.status,
            "attempts": self.attempts,
            "error": self.error,
            "note": self.note,
            "result": self.result,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "StepRecord":
        return cls(
            name=str(payload.get("name", "")),
            status=str(payload.get("status", StepStatus.PENDING.value)),
            attempts=int(payload.get("attempts", 0) or 0),
            error=str(payload.get("error", "")),
            note=str(payload.get("note", "")),
            result=payload.get("result"),
            started_at=str(payload.get("started_at", "")),
            finished_at=str(payload.get("finished_at", "")),
        )


@dataclass
class CompensationRecord:
    """One compensation action taken (or awaited) for a completed step."""

    step: str
    mode: str
    status: str
    approved: bool = False
    error: str = ""
    at: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "step": self.step,
            "mode": self.mode,
            "status": self.status,
            "approved": self.approved,
            "error": self.error,
            "at": self.at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "CompensationRecord":
        return cls(
            step=str(payload.get("step", "")),
            mode=str(payload.get("mode", "")),
            status=str(payload.get("status", "")),
            approved=bool(payload.get("approved", False)),
            error=str(payload.get("error", "")),
            at=str(payload.get("at", "")),
        )


@dataclass
class CycleRecord:
    """The persisted breakpoint and outcome of one control cycle."""

    cycle_id: str
    name: str
    status: str
    strategy: str
    params: dict[str, object] = field(default_factory=dict)
    plan: list[str] = field(default_factory=list)
    steps: dict[str, StepRecord] = field(default_factory=dict)
    cursor: int = 0
    error: str = ""
    halt_reason: str = ""
    compensations: list[CompensationRecord] = field(default_factory=list)
    approvals: list[str] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def new(
        cls,
        cycle_id: str,
        name: str,
        plan: list[str],
        params: dict[str, object],
        strategy: str,
    ) -> "CycleRecord":
        stamp = now_iso()
        return cls(
            cycle_id=cycle_id,
            name=name,
            status=CycleStatus.RUNNING.value,
            strategy=strategy,
            params=dict(params),
            plan=list(plan),
            steps={step: StepRecord(name=step) for step in plan},
            created_at=stamp,
            updated_at=stamp,
        )

    def checkpoint(self) -> dict[str, object]:
        """Where the cycle would resume: cursor position and next step."""

        next_step = self.plan[self.cursor] if self.cursor < len(self.plan) else ""
        return {"cursor": self.cursor, "total": len(self.plan), "next_step": next_step}

    def as_dict(self) -> dict[str, object]:
        """Operator facing shape: steps in plan order, checkpoint computed."""

        return {
            "cycle_id": self.cycle_id,
            "name": self.name,
            "status": self.status,
            "strategy": self.strategy,
            "params": self.params,
            "plan": self.plan,
            "steps": [self.steps[name].as_dict() for name in self.plan if name in self.steps],
            "checkpoint": self.checkpoint(),
            "error": self.error,
            "halt_reason": self.halt_reason,
            "compensations": [record.as_dict() for record in self.compensations],
            "approvals": list(self.approvals),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def to_storage(self) -> dict[str, object]:
        """Persisted shape: steps keyed by name and the cursor kept explicit."""

        return {
            "cycle_id": self.cycle_id,
            "name": self.name,
            "status": self.status,
            "strategy": self.strategy,
            "params": self.params,
            "plan": self.plan,
            "steps": {
                name: self.steps[name].as_dict() for name in self.plan if name in self.steps
            },
            "cursor": self.cursor,
            "error": self.error,
            "halt_reason": self.halt_reason,
            "compensations": [record.as_dict() for record in self.compensations],
            "approvals": list(self.approvals),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def summary(self) -> dict[str, object]:
        """Compact form used by the cycle listing endpoint."""

        return {
            "cycle_id": self.cycle_id,
            "name": self.name,
            "status": self.status,
            "strategy": self.strategy,
            "checkpoint": self.checkpoint(),
            "error": self.error,
            "halt_reason": self.halt_reason,
            "compensations": len(self.compensations),
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "CycleRecord":
        plan = [str(step) for step in payload.get("plan", []) if str(step)]
        raw_steps = payload.get("steps", {})
        steps: dict[str, StepRecord] = {}
        if isinstance(raw_steps, dict):
            for name, raw in raw_steps.items():
                if isinstance(raw, dict):
                    steps[str(name)] = StepRecord.from_dict(raw)
        compensations = [
            CompensationRecord.from_dict(raw)
            for raw in payload.get("compensations", [])
            if isinstance(raw, dict)
        ]
        params = payload.get("params", {})
        return cls(
            cycle_id=str(payload.get("cycle_id", "")),
            name=str(payload.get("name", "")),
            status=str(payload.get("status", CycleStatus.RUNNING.value)),
            strategy=str(payload.get("strategy", "hold")),
            params=dict(params) if isinstance(params, dict) else {},
            plan=plan,
            steps=steps,
            cursor=int(payload.get("cursor", 0) or 0),
            error=str(payload.get("error", "")),
            halt_reason=str(payload.get("halt_reason", "")),
            compensations=compensations,
            approvals=[str(item) for item in payload.get("approvals", [])],
            created_at=str(payload.get("created_at", "")),
            updated_at=str(payload.get("updated_at", "")),
        )
