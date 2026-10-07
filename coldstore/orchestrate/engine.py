"""The orchestration engine: forward passes, breakpoints and compensation.

The rules every cycle obeys:

* A cycle is addressed by its id. Re-submitting an existing id resumes the
  recorded cycle from its checkpoint; it never starts the plan over.
* A failing step is retried in place up to its ``max_attempts``. When those
  are exhausted the failure strategy decides: ``hold`` parks the cycle as
  ``failed`` at the checkpoint, ``compensate`` starts undoing completed
  steps in reverse order. The plan is never silently restarted from zero.
* Compensation runs per step in reverse. Steps declared ``manual`` park the
  cycle in ``awaiting_approval`` until an operator approves them; ``auto``
  steps compensate as soon as they are reached.
* Compensation is itself resumable: a failed compensation keeps the cycle in
  ``compensation_failed`` and the next compensate call retries that step,
  skipping steps already compensated.
"""

from __future__ import annotations

import threading

from coldstore.store.store import Store

from .record import CompensationRecord, CycleRecord, now_iso
from .state import (
    COMPENSABLE_STATUSES,
    TERMINAL_STATUSES,
    CompensationMode,
    CycleStatus,
    FailureStrategy,
    HaltCycle,
    SkipStep,
    StepStatus,
)
from .steps import Step, StepContext
from .store import OrchestrationStore

# The console serves requests from a thread pool, so all record mutations are
# serialised through one lock; a cycle record is read and written as a whole.
_LOCK = threading.RLock()

# Step statuses that still have a live effect worth compensating.
_UNDONE_STATUSES = (StepStatus.DONE.value, StepStatus.COMPENSATION_FAILED.value)


class Orchestrator:
    """Drives cycle records through their lifecycle over a shared store."""

    def __init__(self, store: Store) -> None:
        self._data = OrchestrationStore(store)

    def run(
        self,
        cycle_id: str,
        name: str,
        steps: list[Step],
        params: dict[str, object],
        strategy: FailureStrategy = FailureStrategy.HOLD,
    ) -> CycleRecord:
        """Start a cycle, or resume the recorded one from its checkpoint."""

        with _LOCK:
            record = self._data.load(cycle_id)
            if record is None:
                record = CycleRecord.new(
                    cycle_id, name, [step.name for step in steps], params, str(strategy)
                )
                self._data.register(record)
            else:
                self._check_compatible(record, steps, params, strategy)
                if record.status in [str(status) for status in TERMINAL_STATUSES]:
                    return record
                if record.status in (
                    CycleStatus.AWAITING_APPROVAL.value,
                    CycleStatus.COMPENSATING.value,
                    CycleStatus.COMPENSATION_FAILED.value,
                ):
                    # Compensation owns the cycle now; forward work waits for it.
                    return record
                self._reopen(record)
            self._forward(record, steps)
            return record

    def resume(self, cycle_id: str, steps: list[Step]) -> CycleRecord:
        """Resume a recorded cycle with the plan and params it was created with."""

        with _LOCK:
            record = self._require(cycle_id)
            return self.run(record.cycle_id, record.name, steps, record.params, record.strategy)

    def compensate(self, cycle_id: str, steps: list[Step]) -> CycleRecord:
        """Undo the completed steps of a failed or halted cycle, in reverse."""

        with _LOCK:
            record = self._require(cycle_id)
            if record.status not in [str(status) for status in COMPENSABLE_STATUSES]:
                raise ValueError(
                    f"cycle {cycle_id} is {record.status}; only a failed or halted "
                    "cycle can be compensated"
                )
            self._check_plan(record, steps)
            self._compensate(record, steps)
            return record

    def approve(self, cycle_id: str, steps: list[Step], step_name: str = "") -> CycleRecord:
        """Approve pending manual compensations and continue the walk."""

        with _LOCK:
            record = self._require(cycle_id)
            if record.status != CycleStatus.AWAITING_APPROVAL.value:
                raise ValueError(f"cycle {cycle_id} has no compensation awaiting approval")
            self._check_plan(record, steps)
            pending = self._pending_manual(record, steps)
            targets = [step_name] if step_name else pending
            for target in targets:
                if target not in pending:
                    raise ValueError(f"step {target} is not awaiting approval")
                if target not in record.approvals:
                    record.approvals.append(target)
            self._compensate(record, steps)
            return record

    def get(self, cycle_id: str) -> CycleRecord | None:
        return self._data.load(cycle_id)

    def cycles(self) -> list[CycleRecord]:
        return self._data.cycles()

    def _forward(self, record: CycleRecord, steps: list[Step]) -> None:
        record.status = CycleStatus.RUNNING.value
        self._save(record)
        while record.cursor < len(steps):
            step = steps[record.cursor]
            step_record = record.steps[step.name]
            if step_record.status in (StepStatus.DONE.value, StepStatus.SKIPPED.value):
                record.cursor += 1
                continue
            if not self._attempt(record, steps, step, step_record):
                return
            record.cursor += 1
            self._save(record)
        record.status = CycleStatus.COMPLETED.value
        record.error = ""
        record.halt_reason = ""
        self._save(record)

    def _attempt(self, record: CycleRecord, steps: list[Step], step: Step, step_record) -> bool:
        """Run one step to a resting state; False means the pass stops here."""

        while True:
            step_record.status = StepStatus.RUNNING.value
            step_record.attempts += 1
            step_record.started_at = now_iso()
            self._save(record)
            try:
                result = step.run(self._context(record, step.name))
            except SkipStep as skip:
                step_record.status = StepStatus.SKIPPED.value
                step_record.note = str(skip)
                step_record.finished_at = now_iso()
                self._save(record)
                return True
            except HaltCycle as halt:
                # The gate did not let the cycle through; the step keeps its
                # place so a later resume re-evaluates the gate.
                step_record.status = StepStatus.PENDING.value
                record.status = CycleStatus.HALTED.value
                record.halt_reason = str(halt)
                self._save(record)
                return False
            except Exception as exc:  # noqa: BLE001 - any step failure parks the cycle
                step_record.status = StepStatus.FAILED.value
                step_record.error = str(exc)
                step_record.finished_at = now_iso()
                record.error = f"{step.name}: {exc}"
                if step_record.attempts < step.max_attempts:
                    self._save(record)
                    continue
                self._on_failure(record, steps)
                return False
            step_record.status = StepStatus.DONE.value
            step_record.error = ""
            step_record.result = result
            step_record.finished_at = now_iso()
            self._save(record)
            return True

    def _on_failure(self, record: CycleRecord, steps: list[Step]) -> None:
        if record.strategy == FailureStrategy.COMPENSATE.value:
            self._compensate(record, steps)
            return
        record.status = CycleStatus.FAILED.value
        self._save(record)

    def _compensate(self, record: CycleRecord, steps: list[Step]) -> None:
        record.status = CycleStatus.COMPENSATING.value
        self._save(record)
        for step in reversed(steps):
            step_record = record.steps[step.name]
            if step_record.status not in _UNDONE_STATUSES:
                continue
            if step.compensate is None:
                step_record.status = StepStatus.COMPENSATED.value
                record.compensations.append(
                    CompensationRecord(
                        step=step.name, mode="none", status="done", approved=True, at=now_iso()
                    )
                )
                self._save(record)
                continue
            mode = step.compensation_mode
            if mode == CompensationMode.MANUAL and step.name not in record.approvals:
                record.status = CycleStatus.AWAITING_APPROVAL.value
                self._save(record)
                return
            try:
                step.compensate(self._context(record, step.name))
            except Exception as exc:  # noqa: BLE001 - a failed undo must not lose the rest
                step_record.status = StepStatus.COMPENSATION_FAILED.value
                record.compensations.append(
                    CompensationRecord(
                        step=step.name,
                        mode=str(mode),
                        status="failed",
                        approved=True,
                        error=str(exc),
                        at=now_iso(),
                    )
                )
                record.status = CycleStatus.COMPENSATION_FAILED.value
                self._save(record)
                return
            step_record.status = StepStatus.COMPENSATED.value
            record.compensations.append(
                CompensationRecord(
                    step=step.name,
                    mode=str(mode),
                    status="done",
                    approved=mode != CompensationMode.MANUAL or step.name in record.approvals,
                    at=now_iso(),
                )
            )
            self._save(record)
        record.status = CycleStatus.COMPENSATED.value
        self._save(record)

    def _reopen(self, record: CycleRecord) -> None:
        """Ready a recorded cycle for another forward pass.

        A step left ``running`` belongs to a crashed pass and goes back to
        pending; a failed step gets a fresh set of attempts because the
        operator re-entering the cycle is the retry decision.
        """

        for step_record in record.steps.values():
            if step_record.status == StepStatus.RUNNING.value:
                step_record.status = StepStatus.PENDING.value
            elif step_record.status == StepStatus.FAILED.value:
                step_record.status = StepStatus.PENDING.value
                step_record.attempts = 0
        record.error = ""
        record.halt_reason = ""
        self._save(record)

    def _pending_manual(self, record: CycleRecord, steps: list[Step]) -> list[str]:
        pending: list[str] = []
        for step in reversed(steps):
            step_record = record.steps[step.name]
            if (
                step_record.status in _UNDONE_STATUSES
                and step.compensate is not None
                and step.compensation_mode == CompensationMode.MANUAL
                and step.name not in record.approvals
            ):
                pending.append(step.name)
        return pending

    def _context(self, record: CycleRecord, step_name: str) -> StepContext:
        return StepContext(self._data, record.cycle_id, step_name, record.params)

    def _require(self, cycle_id: str) -> CycleRecord:
        record = self._data.load(cycle_id)
        if record is None:
            raise ValueError(f"unknown cycle {cycle_id}")
        return record

    def _check_compatible(
        self,
        record: CycleRecord,
        steps: list[Step],
        params: dict[str, object],
        strategy: FailureStrategy,
    ) -> None:
        self._check_plan(record, steps)
        if record.params != dict(params):
            raise ValueError(
                f"cycle {record.cycle_id} was created with different params; "
                "use a new cycle_id for new input"
            )
        if record.strategy != str(strategy):
            raise ValueError(f"cycle {record.cycle_id} was created with strategy {record.strategy}")

    def _check_plan(self, record: CycleRecord, steps: list[Step]) -> None:
        if record.plan != [step.name for step in steps]:
            raise ValueError(f"cycle {record.cycle_id} was created with a different step plan")

    def _save(self, record: CycleRecord) -> None:
        record.updated_at = now_iso()
        self._data.save(record)
