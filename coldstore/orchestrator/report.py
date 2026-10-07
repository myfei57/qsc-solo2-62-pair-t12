"""Projections of the orchestration journal for the operator endpoints."""

from __future__ import annotations

from .model import DONE, SKIPPED, CycleRecord


def cycle_summary(record: CycleRecord) -> dict:
    """One row of the cycle list: id, state and progress."""

    finished = sum(1 for step in record.steps if step.status in (DONE, SKIPPED))
    return {
        "id": record.id,
        "status": record.status,
        "cursor": record.cursor,
        "steps": len(record.steps),
        "finished": finished,
        "compensation": record.compensation,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }
