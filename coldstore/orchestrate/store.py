"""Persistence for cycle records and the step effect log.

Two key families live next to the other component keys in the shared store:

``orch:cycle:<id>``           the cycle record (state, checkpoint, compensations)
``orch:effect:<id>:<marker>`` a marker proving one non-idempotent effect ran

The effect log is what makes re-entry idempotent: a step that crashed after
applying its effect but before the record was saved finds the marker on the
next pass and does not apply the effect twice.
"""

from __future__ import annotations

import json

from coldstore.store.store import Store

from .record import CycleRecord

CYCLE_KEY_PREFIX = "orch:cycle:"
EFFECT_KEY_PREFIX = "orch:effect:"
INDEX_KEY = "orch:cycles"


class OrchestrationStore:
    """Reads and writes cycle records inside the shared key/value store."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def save(self, record: CycleRecord) -> None:
        self._store.put(
            f"{CYCLE_KEY_PREFIX}{record.cycle_id}",
            json.dumps(record.to_storage(), ensure_ascii=False),
        )

    def load(self, cycle_id: str) -> CycleRecord | None:
        raw, present = self._store.get(f"{CYCLE_KEY_PREFIX}{cycle_id}")
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        return CycleRecord.from_dict(payload)

    def register(self, record: CycleRecord) -> None:
        """Persist a new cycle and add it to the listing index."""

        self.save(record)
        ids = self.cycle_ids()
        if record.cycle_id not in ids:
            ids.append(record.cycle_id)
            self._store.put(INDEX_KEY, json.dumps(ids, ensure_ascii=False))

    def cycle_ids(self) -> list[str]:
        raw, present = self._store.get(INDEX_KEY)
        if not present:
            return []
        try:
            ids = json.loads(raw)
        except ValueError:
            return []
        if not isinstance(ids, list):
            return []
        return [str(item) for item in ids]

    def cycles(self) -> list[CycleRecord]:
        """Every loadable cycle record, in registration order."""

        records: list[CycleRecord] = []
        for cycle_id in self.cycle_ids():
            record = self.load(cycle_id)
            if record is not None:
                records.append(record)
        return records

    def effect(self, cycle_id: str, marker: str) -> tuple[object, bool]:
        """Read a stored effect result, reporting whether it was recorded."""

        raw, present = self._store.get(f"{EFFECT_KEY_PREFIX}{cycle_id}:{marker}")
        if not present:
            return None, False
        try:
            payload = json.loads(raw)
        except ValueError:
            return None, False
        if not isinstance(payload, dict) or "result" not in payload:
            return None, False
        return payload["result"], True

    def record_effect(self, cycle_id: str, marker: str, result: object) -> None:
        """Persist the result of a non-idempotent effect exactly once."""

        self._store.put(
            f"{EFFECT_KEY_PREFIX}{cycle_id}:{marker}",
            json.dumps({"result": result}, ensure_ascii=False),
        )
