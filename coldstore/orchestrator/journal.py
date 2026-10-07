"""Persistence for cycle records and compensation records.

Everything lives in the shared document store: one key per cycle record,
one key per cycle's compensation list, and an append-only index of cycle
identifiers. Writes go through the store's atomic replace so a crash
cannot leave half a journal entry behind.
"""

from __future__ import annotations

import json

from coldstore.store import Store, append_command, load_commands

from .model import CompensationRecord, CycleRecord

RECORD_PREFIX = "cycle:record:"
INDEX_KEY = "cycle:index"
COMP_PREFIX = "cycle:comp:"


def save_cycle(store: Store, record: CycleRecord) -> None:
    store.put(f"{RECORD_PREFIX}{record.id}", json.dumps(record.as_dict(), ensure_ascii=False))


def load_cycle(store: Store, cycle_id: str) -> CycleRecord | None:
    raw, present = store.get(f"{RECORD_PREFIX}{cycle_id}")
    if not present:
        return None
    try:
        payload = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    return CycleRecord.from_dict(payload)


def index_cycle(store: Store, cycle_id: str) -> None:
    append_command(store, INDEX_KEY, cycle_id)


def list_cycle_ids(store: Store) -> list[str]:
    return load_commands(store, INDEX_KEY)


def list_cycles(store: Store) -> list[CycleRecord]:
    records: list[CycleRecord] = []
    for cycle_id in list_cycle_ids(store):
        record = load_cycle(store, cycle_id)
        if record is not None:
            records.append(record)
    return records


def save_compensations(store: Store, cycle_id: str, records: list[CompensationRecord]) -> None:
    store.put(
        f"{COMP_PREFIX}{cycle_id}",
        json.dumps([record.as_dict() for record in records], ensure_ascii=False),
    )


def load_compensations(store: Store, cycle_id: str) -> list[CompensationRecord]:
    raw, present = store.get(f"{COMP_PREFIX}{cycle_id}")
    if not present:
        return []
    try:
        payload = json.loads(raw)
    except ValueError:
        return []
    if not isinstance(payload, list):
        return []
    return [CompensationRecord.from_dict(item) for item in payload if isinstance(item, dict)]
