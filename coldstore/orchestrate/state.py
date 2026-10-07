"""Statuses and control signals for orchestrated control cycles."""

from __future__ import annotations

from enum import Enum


class StepStatus(str, Enum):
    """Lifecycle of one step inside a cycle."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    SKIPPED = "skipped"
    FAILED = "failed"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"

    def __str__(self) -> str:
        return self.value


class CycleStatus(str, Enum):
    """Lifecycle of a whole control cycle."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    HALTED = "halted"
    AWAITING_APPROVAL = "awaiting_approval"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"

    def __str__(self) -> str:
        return self.value


# Statuses after which a cycle no longer accepts forward work: re-entering
# with the same cycle id simply reports the recorded outcome.
TERMINAL_STATUSES = (CycleStatus.COMPLETED, CycleStatus.COMPENSATED)

# Statuses from which the compensation walk may start or continue.
COMPENSABLE_STATUSES = (
    CycleStatus.FAILED,
    CycleStatus.HALTED,
    CycleStatus.AWAITING_APPROVAL,
    CycleStatus.COMPENSATING,
    CycleStatus.COMPENSATION_FAILED,
)


class FailureStrategy(str, Enum):
    """What the cycle does when a step exhausts its attempts.

    HOLD parks the cycle at the failing checkpoint until an operator resumes
    or compensates it. COMPENSATE immediately walks the completed steps
    backwards, still pausing for approval on steps declared manual.
    """

    HOLD = "hold"
    COMPENSATE = "compensate"

    def __str__(self) -> str:
        return self.value


class CompensationMode(str, Enum):
    """Whether a step's compensation runs by itself or waits for a human."""

    AUTO = "auto"
    MANUAL = "manual"

    def __str__(self) -> str:
        return self.value


class SkipStep(Exception):
    """Raised by a step body to mark itself not applicable for this run."""


class HaltCycle(Exception):
    """Raised by a gate step to pause the cycle without failing it.

    The raising step has not done its work, so it stays pending at the
    checkpoint; resuming re-evaluates the gate. A safety trip uses this so
    the cycle continues only after an operator clears the latch.
    """
