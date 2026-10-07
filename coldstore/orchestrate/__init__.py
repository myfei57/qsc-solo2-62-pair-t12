"""Cycle orchestration: resumable, idempotent, compensatable control cycles.

A control cycle runs a dozen steps against the store. This package replaces
the one-shot walk with a persisted breakpoint per cycle:

* every step has an explicit state (``state.StepStatus``) and the cycle has
  one too (``state.CycleStatus``); both are written to the store after every
  transition, so a crashed console resumes where it stopped;
* re-entering an existing cycle id resumes it from the checkpoint instead of
  starting over, and non-idempotent effects run through a persisted effect
  log so a duplicate submission never applies them twice;
* a failing step is retried in place up to its declared attempts, then the
  cycle strategy decides: ``hold`` parks the cycle for an operator,
  ``compensate`` undoes the completed steps in reverse;
* compensations declared ``manual`` park the cycle in ``awaiting_approval``
  until an operator approves them; ``auto`` compensations run as soon as the
  walk reaches them.
"""

from .engine import Orchestrator
from .record import CompensationRecord, CycleRecord, StepRecord
from .state import (
    CompensationMode,
    CycleStatus,
    FailureStrategy,
    HaltCycle,
    SkipStep,
    StepStatus,
)
from .steps import Step, StepContext
from .store import OrchestrationStore

__all__ = [
    "CompensationMode",
    "CompensationRecord",
    "CycleRecord",
    "CycleStatus",
    "FailureStrategy",
    "HaltCycle",
    "OrchestrationStore",
    "Orchestrator",
    "SkipStep",
    "Step",
    "StepContext",
    "StepRecord",
    "StepStatus",
]
