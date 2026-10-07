"""Operator counters and store reset."""

from __future__ import annotations

from dataclasses import dataclass

from coldstore.orchestrate.state import CycleStatus
from coldstore.orchestrate.store import OrchestrationStore

from .runtime import Runtime

_ATTENTION_STATUSES = (
    str(CycleStatus.FAILED),
    str(CycleStatus.AWAITING_APPROVAL),
    str(CycleStatus.COMPENSATION_FAILED),
)


@dataclass(frozen=True)
class OpsCounters:
    """Small set of counters shown on the operator page."""

    store_keys: int
    rooms: int
    compressors: int
    running: int
    supply_open: int
    audit_entries: int
    cycles: int
    cycles_attention: int

    def as_dict(self) -> dict[str, object]:
        return {
            "store_keys": self.store_keys,
            "rooms": self.rooms,
            "compressors": self.compressors,
            "running": self.running,
            "supply_open": self.supply_open,
            "audit_entries": self.audit_entries,
            "cycles": self.cycles,
            "cycles_attention": self.cycles_attention,
        }


def collect(rt: Runtime) -> OpsCounters:
    cycles = OrchestrationStore(rt.store).cycles()
    return OpsCounters(
        store_keys=rt.store.count(),
        rooms=rt.rooms.count(),
        compressors=rt.compressors.count(),
        running=rt.compressors.running_count(),
        supply_open=len(rt.supply.opened_ids()),
        audit_entries=rt.auditor.count(),
        cycles=len(cycles),
        cycles_attention=sum(1 for record in cycles if record.status in _ATTENTION_STATUSES),
    )


def reset_store(rt: Runtime) -> dict[str, object]:
    """Clear every persisted key, as a factory reset would."""

    rt.store.clear()
    return {"reset": True, "keys": rt.store.count()}
