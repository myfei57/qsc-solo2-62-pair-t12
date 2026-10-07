"""State model for orchestrated control cycles.

A cycle walks the canonical control line one step at a time. Every
transition is journaled so a failed cycle resumes from its checkpoint
instead of starting over, and a repeated entry with the same cycle id is
a replay rather than a doubled effect.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

# Step lifecycle: pending -> running -> done | failed | skipped.
# "compensated" marks a step whose effect was undone by a restart.
PENDING = "pending"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
SKIPPED = "skipped"
STEP_COMPENSATED = "compensated"

# Cycle lifecycle.
CYCLE_RUNNING = "running"
CYCLE_PAUSED = "paused"
CYCLE_FAILED = "failed"
CYCLE_DONE = "done"
CYCLE_COMPENSATING = "compensating"
CYCLE_COMPENSATED = "compensated"
CYCLE_ABORTED = "aborted"

# Compensation execution modes declared by each step.
MODE_AUTO = "auto"
MODE_MANUAL = "manual"

# Cycle compensation policies chosen when the cycle starts.
POLICY_AUTO = "auto"
POLICY_MANUAL = "manual"
POLICY_NONE = "none"
POLICIES = (POLICY_AUTO, POLICY_MANUAL, POLICY_NONE)

# Compensation record statuses.
COMP_PENDING = "pending"
COMP_DONE = "done"
COMP_FAILED = "failed"
COMP_SKIPPED = "skipped"

# Why a compensation record exists.
PURPOSE_ROLLBACK = "rollback"
PURPOSE_RESTART = "restart"


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def new_id() -> str:
    return uuid.uuid4().hex


@dataclass
class StepRecord:
    """Journal entry for one stage of the control line."""

    stage: str
    status: str = PENDING
    attempts: int = 0
    memo: dict = field(default_factory=dict)
    detail: dict = field(default_factory=dict)
    error: str = ""
    started_at: str = ""
    finished_at: str = ""

    def as_dict(self) -> dict:
        return {
            "stage": self.stage,
            "status": self.status,
            "attempts": self.attempts,
            "memo": dict(self.memo),
            "detail": dict(self.detail),
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "StepRecord":
        return cls(
            stage=str(payload.get("stage", "")),
            status=str(payload.get("status", PENDING)),
            attempts=int(payload.get("attempts", 0)),
            memo=dict(payload.get("memo", {}) or {}),
            detail=dict(payload.get("detail", {}) or {}),
            error=str(payload.get("error", "")),
            started_at=str(payload.get("started_at", "")),
            finished_at=str(payload.get("finished_at", "")),
        )


@dataclass
class CycleRecord:
    """Journal entry for one orchestrated pass over the control line."""

    id: str
    params: dict = field(default_factory=dict)
    compensation: str = POLICY_AUTO
    status: str = CYCLE_RUNNING
    cursor: int = 0
    steps: list[StepRecord] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def new(
        cls, cycle_id: str, params: dict, compensation: str, stages: list[str]
    ) -> "CycleRecord":
        at = now_iso()
        return cls(
            id=cycle_id,
            params=dict(params),
            compensation=compensation,
            status=CYCLE_RUNNING,
            cursor=0,
            steps=[StepRecord(stage=stage) for stage in stages],
            created_at=at,
            updated_at=at,
        )

    def step(self, stage: str) -> StepRecord | None:
        for step in self.steps:
            if step.stage == stage:
                return step
        return None

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "status": self.status,
            "cursor": self.cursor,
            "compensation": self.compensation,
            "params": dict(self.params),
            "steps": [step.as_dict() for step in self.steps],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "CycleRecord":
        return cls(
            id=str(payload.get("id", "")),
            params=dict(payload.get("params", {}) or {}),
            compensation=str(payload.get("compensation", POLICY_AUTO)),
            status=str(payload.get("status", CYCLE_RUNNING)),
            cursor=int(payload.get("cursor", 0)),
            steps=[
                StepRecord.from_dict(item)
                for item in payload.get("steps", [])
                if isinstance(item, dict)
            ],
            created_at=str(payload.get("created_at", "")),
            updated_at=str(payload.get("updated_at", "")),
        )


@dataclass
class CompensationRecord:
    """One compensation action planned or executed for a cycle step."""

    id: str
    cycle_id: str
    stage: str
    action: str
    mode: str
    purpose: str
    status: str = COMP_PENDING
    detail: str = ""
    error: str = ""
    created_at: str = ""
    finished_at: str = ""

    @classmethod
    def new(
        cls, cycle_id: str, stage: str, action: str, mode: str, purpose: str
    ) -> "CompensationRecord":
        return cls(
            id=new_id(),
            cycle_id=cycle_id,
            stage=stage,
            action=action,
            mode=mode,
            purpose=purpose,
            created_at=now_iso(),
        )

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "cycle_id": self.cycle_id,
            "stage": self.stage,
            "action": self.action,
            "mode": self.mode,
            "purpose": self.purpose,
            "status": self.status,
            "detail": self.detail,
            "error": self.error,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "CompensationRecord":
        return cls(
            id=str(payload.get("id", "")),
            cycle_id=str(payload.get("cycle_id", "")),
            stage=str(payload.get("stage", "")),
            action=str(payload.get("action", "")),
            mode=str(payload.get("mode", MODE_AUTO)),
            purpose=str(payload.get("purpose", PURPOSE_RESTART)),
            status=str(payload.get("status", COMP_PENDING)),
            detail=str(payload.get("detail", "")),
            error=str(payload.get("error", "")),
            created_at=str(payload.get("created_at", "")),
            finished_at=str(payload.get("finished_at", "")),
        )
