"""Checkpointed execution of the control line with saga compensation.

The rules of the line:

* a failing step stops the cycle; the failure is journaled and nothing is
  retried or rolled back implicitly;
* re-entering with the same cycle id, or an explicit resume, continues
  from the journaled cursor;
* rolling back one step and restarting the whole cycle are explicit
  operator commands, never a side effect of a failure;
* compensation runs automatically for steps declared reversible and waits
  for operator approval where safety latches are involved.
"""

from __future__ import annotations

import threading

from coldstore.store import Store

from . import journal, model
from .model import CompensationRecord, CycleRecord
from .steps import CycleContext, StepSpec, ordered_specs, spec_for


class Orchestrator:
    """Runs control cycles and keeps the journal in the shared store."""

    def __init__(self, store: Store) -> None:
        self._store = store
        self._lock = threading.RLock()

    # -- running cycles ----------------------------------------------------

    def start(
        self,
        rt: object,
        params: dict,
        cycle_id: str = "",
        compensation: str = model.POLICY_AUTO,
    ) -> tuple[CycleRecord, bool]:
        """Start a cycle, or re-enter the one this id already belongs to.

        The cycle id is the idempotency key: a finished cycle replays its
        recorded state, an unfinished one resumes from its checkpoint, and
        the same id with different params is rejected as a client error.
        """

        with self._lock:
            if compensation not in model.POLICIES:
                raise ValueError(
                    f"compensation must be one of {', '.join(model.POLICIES)}"
                )
            if cycle_id:
                existing = journal.load_cycle(self._store, cycle_id)
                if existing is not None:
                    if existing.params != dict(params):
                        raise ValueError(
                            f"cycle {cycle_id} already exists with different params"
                        )
                    if existing.status in (
                        model.CYCLE_FAILED,
                        model.CYCLE_PAUSED,
                        model.CYCLE_RUNNING,
                    ):
                        return self._run(rt, existing), False
                    return existing, True
            record = CycleRecord.new(
                cycle_id or model.new_id(),
                params,
                compensation,
                [spec.stage.value for spec in ordered_specs()],
            )
            self._save(record)
            journal.index_cycle(self._store, record.id)
            return self._run(rt, record), False

    def resume(self, rt: object, cycle_id: str) -> CycleRecord:
        """Continue a failed, paused or interrupted cycle from its cursor."""

        with self._lock:
            record = self._require(cycle_id)
            if record.status not in (
                model.CYCLE_FAILED,
                model.CYCLE_PAUSED,
                model.CYCLE_RUNNING,
            ):
                raise ValueError(f"cycle {cycle_id} is {record.status}; nothing to resume")
            return self._run(rt, record)

    def _run(self, rt: object, record: CycleRecord) -> CycleRecord:
        record.status = model.CYCLE_RUNNING
        self._save(record)
        while record.cursor < len(record.steps):
            step = record.steps[record.cursor]
            if step.status in (model.DONE, model.SKIPPED):
                record.cursor += 1
                continue
            spec = spec_for(step.stage)
            ctx = self._context(record)
            if step.status != model.RUNNING:
                # a fresh attempt prepares a new idempotency memo; a step left
                # "running" by a crash keeps the memo it was journaled with
                step.memo = spec.prepare(rt, ctx) if spec.prepare else {}
                step.attempts += 1
                step.status = model.RUNNING
                step.error = ""
                step.started_at = model.now_iso()
                self._save(record)
            try:
                outcome = spec.run(rt, ctx, dict(step.memo))
            except Exception as exc:  # noqa: BLE001 - the failure becomes cycle state
                step.status = model.FAILED
                step.error = str(exc)
                step.finished_at = model.now_iso()
                record.status = model.CYCLE_FAILED
                self._save(record)
                return record
            step.detail = dict(outcome.detail)
            step.status = outcome.status
            step.finished_at = model.now_iso()
            record.cursor += 1
            self._save(record)
        record.status = model.CYCLE_DONE
        self._save(record)
        return record

    # -- rollback and restart ----------------------------------------------

    def rollback(self, rt: object, cycle_id: str) -> CycleRecord:
        """Compensate the last executed step and move the cursor back to it."""

        with self._lock:
            record = self._require(cycle_id)
            if record.status not in (model.CYCLE_FAILED, model.CYCLE_PAUSED):
                raise ValueError(
                    f"cycle {cycle_id} is {record.status}; rollback needs failed or paused"
                )
            index = record.cursor - 1
            while index >= 0 and record.steps[index].status not in (
                model.DONE,
                model.SKIPPED,
            ):
                index -= 1
            if index < 0:
                raise ValueError(f"cycle {cycle_id} has nothing to roll back")
            step = record.steps[index]
            spec = spec_for(step.stage)
            if step.status == model.DONE and spec.compensate is not None:
                comp = CompensationRecord.new(
                    record.id,
                    spec.stage.value,
                    spec.action,
                    self._mode_for(record, spec),
                    model.PURPOSE_ROLLBACK,
                )
                comps = journal.load_compensations(self._store, record.id)
                comps.append(comp)
                if comp.mode == model.MODE_AUTO:
                    self._execute(rt, record, comp, spec, step)
                journal.save_compensations(self._store, record.id, comps)
            else:
                self._reset_step(record, step, index)
            record.status = model.CYCLE_PAUSED
            self._save(record)
            return record

    def restart(
        self, rt: object, cycle_id: str, rerun: bool = False
    ) -> tuple[CycleRecord, CycleRecord | None]:
        """Compensate every completed step in reverse, then retire the cycle.

        Returns the retired record and, when a rerun was asked for and no
        manual compensation is still pending, the fresh cycle replacing it.
        """

        with self._lock:
            record = self._require(cycle_id)
            if record.status not in (
                model.CYCLE_FAILED,
                model.CYCLE_PAUSED,
                model.CYCLE_DONE,
            ):
                raise ValueError(
                    f"cycle {cycle_id} is {record.status}; restart needs failed, paused or done"
                )
            if record.compensation == model.POLICY_NONE:
                record.status = model.CYCLE_ABORTED
            else:
                comps = journal.load_compensations(self._store, record.id)
                for step in reversed(record.steps):
                    if step.status != model.DONE:
                        continue
                    spec = spec_for(step.stage)
                    if spec.compensate is None:
                        continue
                    comp = CompensationRecord.new(
                        record.id,
                        spec.stage.value,
                        spec.action,
                        self._mode_for(record, spec),
                        model.PURPOSE_RESTART,
                    )
                    comps.append(comp)
                    if comp.mode == model.MODE_AUTO:
                        self._execute(rt, record, comp, spec, step)
                journal.save_compensations(self._store, record.id, comps)
                record.status = self._resting_status(comps)
            self._save(record)
            follow = None
            if rerun and record.status in (model.CYCLE_COMPENSATED, model.CYCLE_ABORTED):
                follow = self._rerun(rt, record)
            return record, follow

    def rerun(self, rt: object, cycle_id: str) -> CycleRecord:
        """Start a fresh cycle with the same params as a finished one."""

        with self._lock:
            record = self._require(cycle_id)
            if record.status not in (
                model.CYCLE_DONE,
                model.CYCLE_COMPENSATED,
                model.CYCLE_ABORTED,
            ):
                raise ValueError(
                    f"cycle {cycle_id} is {record.status}; rerun needs a finished cycle"
                )
            return self._rerun(rt, record)

    def _rerun(self, rt: object, record: CycleRecord) -> CycleRecord:
        taken = set(journal.list_cycle_ids(self._store))
        attempt = 1
        new_id = f"{record.id}-r{attempt}"
        while new_id in taken:
            attempt += 1
            new_id = f"{record.id}-r{attempt}"
        follow = CycleRecord.new(
            new_id,
            record.params,
            record.compensation,
            [spec.stage.value for spec in ordered_specs()],
        )
        self._save(follow)
        journal.index_cycle(self._store, follow.id)
        return self._run(rt, follow)

    # -- compensation approval ----------------------------------------------

    def approve(
        self, rt: object, cycle_id: str, compensation_id: str = ""
    ) -> tuple[CycleRecord, list[CompensationRecord]]:
        """Execute pending compensations, and retry failed ones."""

        with self._lock:
            record = self._require(cycle_id)
            comps = journal.load_compensations(self._store, record.id)
            executed = False
            for comp in comps:
                if comp.status not in (model.COMP_PENDING, model.COMP_FAILED):
                    continue
                if compensation_id and comp.id != compensation_id:
                    continue
                spec = spec_for(comp.stage)
                step = record.step(comp.stage)
                if step is None:
                    continue
                comp.status = model.COMP_PENDING
                comp.error = ""
                self._execute(rt, record, comp, spec, step)
                executed = True
            if not executed:
                raise ValueError(f"cycle {cycle_id} has no compensations to run")
            journal.save_compensations(self._store, record.id, comps)
            if record.status == model.CYCLE_COMPENSATING:
                record.status = self._resting_status(comps)
            self._save(record)
            return record, comps

    def _execute(
        self,
        rt: object,
        record: CycleRecord,
        comp: CompensationRecord,
        spec: StepSpec,
        step: model.StepRecord,
    ) -> None:
        try:
            note = spec.compensate(rt, self._context(record), dict(step.detail))
        except Exception as exc:  # noqa: BLE001 - compensation failure is journaled
            comp.status = model.COMP_FAILED
            comp.error = str(exc)
        else:
            if note is None:
                comp.status = model.COMP_SKIPPED
                comp.detail = "nothing to undo"
            else:
                comp.status = model.COMP_DONE
                comp.detail = note
        comp.finished_at = model.now_iso()
        self._after_compensation(record, comp)

    def _after_compensation(self, record: CycleRecord, comp: CompensationRecord) -> None:
        if comp.status not in (model.COMP_DONE, model.COMP_SKIPPED):
            return
        index = self._step_index(record, comp.stage)
        if index is None:
            return
        step = record.steps[index]
        if comp.purpose == model.PURPOSE_ROLLBACK:
            self._reset_step(record, step, index)
        else:
            step.status = model.STEP_COMPENSATED

    def _reset_step(self, record: CycleRecord, step: model.StepRecord, index: int) -> None:
        step.status = model.PENDING
        step.memo = {}
        step.detail = {}
        step.error = ""
        record.cursor = min(record.cursor, index)

    @staticmethod
    def _resting_status(comps: list[CompensationRecord]) -> str:
        restart_comps = [comp for comp in comps if comp.purpose == model.PURPOSE_RESTART]
        if any(
            comp.status in (model.COMP_PENDING, model.COMP_FAILED)
            for comp in restart_comps
        ):
            return model.CYCLE_COMPENSATING
        if restart_comps:
            return model.CYCLE_COMPENSATED
        return model.CYCLE_ABORTED

    # -- reads ----------------------------------------------------------------

    def status(self, cycle_id: str) -> CycleRecord:
        return self._require(cycle_id)

    def list_cycles(self) -> list[CycleRecord]:
        return journal.list_cycles(self._store)

    def compensations(self, cycle_id: str) -> list[CompensationRecord]:
        self._require(cycle_id)
        return journal.load_compensations(self._store, cycle_id)

    def summary(self) -> dict[str, int]:
        records = self.list_cycles()
        pending = 0
        failed = 0
        for record in records:
            for comp in journal.load_compensations(self._store, record.id):
                if comp.status == model.COMP_PENDING:
                    pending += 1
                elif comp.status == model.COMP_FAILED:
                    failed += 1
        return {
            "cycles": len(records),
            "cycles_running": sum(
                1 for record in records if record.status == model.CYCLE_RUNNING
            ),
            "cycles_failed": sum(
                1 for record in records if record.status == model.CYCLE_FAILED
            ),
            "cycles_compensating": sum(
                1 for record in records if record.status == model.CYCLE_COMPENSATING
            ),
            "compensations_pending": pending,
            "compensations_failed": failed,
        }

    # -- helpers ---------------------------------------------------------------

    def _require(self, cycle_id: str) -> CycleRecord:
        record = journal.load_cycle(self._store, cycle_id)
        if record is None:
            raise LookupError(f"no cycle {cycle_id}")
        return record

    def _save(self, record: CycleRecord) -> None:
        record.updated_at = model.now_iso()
        journal.save_cycle(self._store, record)

    def _context(self, record: CycleRecord) -> CycleContext:
        details = {
            step.stage: dict(step.detail)
            for step in record.steps
            if step.status in (model.DONE, model.SKIPPED)
        }
        return CycleContext(
            cycle_id=record.id, params=dict(record.params), details=details
        )

    @staticmethod
    def _mode_for(record: CycleRecord, spec: StepSpec) -> str:
        if record.compensation == model.POLICY_MANUAL:
            return model.MODE_MANUAL
        return spec.mode or model.MODE_AUTO

    @staticmethod
    def _step_index(record: CycleRecord, stage: str) -> int | None:
        for index, step in enumerate(record.steps):
            if step.stage == stage:
                return index
        return None
