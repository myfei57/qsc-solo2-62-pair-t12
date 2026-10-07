"""Step definitions and the context a step body runs against."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .state import CompensationMode
from .store import OrchestrationStore


class StepContext:
    """What a step body sees: the cycle parameters and the effect log.

    Steps must reach the outside world only through what is persisted: the
    frozen cycle parameters and the components themselves. Anything passed
    around in memory would be gone after a crash and break resume.
    """

    def __init__(
        self,
        data: OrchestrationStore,
        cycle_id: str,
        step_name: str,
        params: dict[str, object],
    ) -> None:
        self._data = data
        self.cycle_id = cycle_id
        self.step_name = step_name
        self.params = params

    def effect_once(self, key: str, apply: Callable[[], object]) -> object:
        """Run a non-idempotent effect once per cycle, however often re-entered.

        The first call persists the effect's result under the step and key;
        later passes (resume, retry, duplicate submission) get the stored
        result back without applying the effect again.
        """

        marker = f"{self.step_name}:{key}"
        result, present = self._data.effect(self.cycle_id, marker)
        if present:
            return result
        result = apply()
        self._data.record_effect(self.cycle_id, marker, result)
        return result

    def effect_result(self, key: str) -> tuple[object, bool]:
        """Read what an earlier ``effect_once`` recorded, without re-running it.

        Compensations use this to recover values captured when the forward
        effect ran, such as the compressor stage to restore.
        """

        return self._data.effect(self.cycle_id, f"{self.step_name}:{key}")


@dataclass(frozen=True)
class Step:
    """One named step of a cycle plan.

    ``run`` performs the work, ``compensate`` undoes it for a cycle that is
    being rolled back. ``max_attempts`` bounds in-place retries before the
    failure strategy takes over. ``compensation_mode`` decides whether the
    compensation executes as soon as it is reached or parks the cycle until
    an operator approves it.
    """

    name: str
    run: Callable[[StepContext], object]
    compensate: Callable[[StepContext], object] | None = None
    max_attempts: int = 1
    compensation_mode: CompensationMode = CompensationMode.AUTO
