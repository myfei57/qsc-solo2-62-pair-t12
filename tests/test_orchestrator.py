"""End to end tests for the orchestrated control cycle."""

from __future__ import annotations

import json
import tempfile
import unittest

from coldstore.console.runtime import Runtime
from coldstore.console.seed import seed_defaults
from coldstore.console.server import Server
from coldstore.orchestrator import Orchestrator, journal, model
from coldstore.orchestrator.model import CycleRecord
from coldstore.orchestrator.steps import ordered_specs
from coldstore.store import Store


class OrchestratorHttpCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        seed_defaults(self.store)
        self.server = Server(self.store)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def call(self, method: str, path: str, payload: dict | None = None):
        response = self.server.dispatch(method, path, payload)
        body = response.body.decode("utf-8")
        parsed = json.loads(body) if response.content_type.startswith("application/json") else body
        return response.status, parsed

    def add_room(self, room_id: str = "r1", zone: int = 1, temp: float = -20.0) -> None:
        status, _ = self.call("POST", "/room/add", {"id": room_id, "zone": zone, "temp": temp})
        self.assertEqual(status, 200)

    def add_comp(self, comp_id: str = "c1", stages: int = 4) -> None:
        status, _ = self.call("POST", "/comp/add", {"id": comp_id, "stages": stages})
        self.assertEqual(status, 200)

    def run_params(self, **overrides) -> dict:
        params = {
            "room_id": "r1",
            "comp_id": "c1",
            "temp": -5.0,
            "ppm": 1.0,
            "amount": 2.0,
            "defrost": 0,
        }
        params.update(overrides)
        return params

    def step_of(self, payload: dict, stage: str) -> dict:
        for step in payload["steps"]:
            if step["stage"] == stage:
                return step
        raise AssertionError(f"no step {stage}")

    def test_run_completes_and_persists(self) -> None:
        self.add_room()
        self.add_comp()
        status, body = self.call("POST", "/cycle/run", dict(self.run_params(), id="cy1"))
        self.assertEqual(status, 200)
        self.assertEqual(body["cycle_id"], "cy1")
        self.assertEqual(body["cycle_status"], "done")
        self.assertFalse(body["replayed"])
        self.assertEqual(len(body["steps"]), 9)
        self.assertEqual(self.step_of(body, "interlock")["status"], "skipped")
        self.assertEqual(self.step_of(body, "quota")["status"], "done")
        self.assertEqual(body["stage"], 1)
        self.assertTrue(body["supply_open"])

        status, state = self.call("GET", "/cycle/status", {"id": "cy1"})
        self.assertEqual(status, 200)
        self.assertEqual(state["status"], "done")
        self.assertEqual(state["cursor"], 9)

        # the journal survives a restart of the console
        self.store.close()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.server = Server(self.store)
        status, state = self.call("GET", "/cycle/status", {"id": "cy1"})
        self.assertEqual(status, 200)
        self.assertEqual(state["status"], "done")
        self.assertEqual(self.step_of(state, "quota")["status"], "done")

        status, listing = self.call("GET", "/cycle/list")
        self.assertEqual(status, 200)
        self.assertEqual(listing["count"], 1)
        self.assertEqual(listing["cycles"][0]["id"], "cy1")
        self.assertEqual(listing["cycles"][0]["finished"], 9)

    def test_idempotent_reentry_does_not_double_effects(self) -> None:
        self.add_room()
        self.add_comp()
        params = dict(self.run_params(), id="cy2")
        status, first = self.call("POST", "/cycle/run", params)
        self.assertEqual(first["quota"], 2.0)
        self.assertEqual(first["audit_count"], 1)

        status, second = self.call("POST", "/cycle/run", params)
        self.assertEqual(status, 200)
        self.assertTrue(second["replayed"])
        self.assertEqual(second["cycle_status"], "done")
        self.assertEqual(second["quota"], 2.0)
        self.assertEqual(second["audit_count"], 1)

        status, readings = self.call("GET", "/temp/readings")
        self.assertEqual(readings["samples"], 1)

        # a reused id carrying different params is a client error, not a new cycle
        status, error = self.call("POST", "/cycle/run", dict(params, temp=-6.0))
        self.assertEqual(status, 400)
        self.assertIn("different params", error["error"])

    def test_failure_resumes_from_checkpoint(self) -> None:
        self.add_room()
        params = self.run_params(comp_id="ghost", id="cy3")
        status, body = self.call("POST", "/cycle/run", params)
        self.assertEqual(status, 200)
        self.assertEqual(body["cycle_status"], "failed")
        self.assertEqual(self.step_of(body, "temp")["status"], "done")
        self.assertEqual(self.step_of(body, "ammonia")["status"], "done")
        self.assertEqual(self.step_of(body, "comp")["status"], "failed")
        self.assertIn("not found", self.step_of(body, "comp")["error"])
        self.assertEqual(self.step_of(body, "quota")["status"], "pending")

        # nothing past the failure ran
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 0.0)

        # fix the world, then resume from the checkpoint
        self.add_comp("ghost")
        status, body = self.call("POST", "/cycle/resume", {"id": "cy3"})
        self.assertEqual(status, 200)
        self.assertEqual(body["cycle_status"], "done")
        self.assertEqual(self.step_of(body, "comp")["attempts"], 2)

        # the steps before the failure did not run again
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 2.0)
        status, readings = self.call("GET", "/temp/readings")
        self.assertEqual(readings["samples"], 1)
        self.assertEqual(body["audit_count"], 1)

    def test_rollback_steps_back_one_step(self) -> None:
        self.add_comp()
        params = self.run_params(room_id="ghost-room", defrost=1, id="cy4")
        status, body = self.call("POST", "/cycle/run", params)
        self.assertEqual(body["cycle_status"], "failed")
        self.assertEqual(self.step_of(body, "defrost")["status"], "failed")
        self.assertTrue(body["supply_open"])

        status, body = self.call("POST", "/cycle/rollback", {"id": "cy4"})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "paused")
        self.assertEqual(self.step_of(body, "valve")["status"], "pending")
        self.assertEqual(body["cursor"], 3)

        # the rollback compensated the valve step automatically
        status, comps = self.call("GET", "/cycle/compensations", {"id": "cy4"})
        self.assertEqual(comps["pending"], 0)
        self.assertEqual(comps["compensations"][0]["stage"], "valve")
        self.assertEqual(comps["compensations"][0]["purpose"], "rollback")
        self.assertEqual(comps["compensations"][0]["status"], "done")
        status, valve_state = self.call("GET", "/valve")
        self.assertEqual(valve_state["opened"], [])

        # resume re-runs the valve step and fails on defrost again
        status, body = self.call("POST", "/cycle/resume", {"id": "cy4"})
        self.assertEqual(body["cycle_status"], "failed")
        self.assertTrue(body["supply_open"])

        # fix the room and finish the cycle
        self.add_room("ghost-room", 2, -10.0)
        status, body = self.call("POST", "/cycle/resume", {"id": "cy4"})
        self.assertEqual(body["cycle_status"], "done")

    def test_restart_compensates_completed_steps(self) -> None:
        self.add_room()
        self.add_comp()
        params = dict(self.run_params(defrost=1), id="cy5")
        status, body = self.call("POST", "/cycle/run", params)
        self.assertEqual(body["cycle_status"], "done")
        self.assertEqual(body["quota"], 2.0)
        self.assertTrue(body["supply_open"])
        self.assertEqual(body["defrosting"], ["r1"])

        status, body = self.call("POST", "/cycle/restart", {"id": "cy5"})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "compensated")
        by_stage = {comp["stage"]: comp for comp in body["compensations"]}
        self.assertEqual(set(by_stage), {"comp", "valve", "defrost", "quota"})
        self.assertTrue(all(comp["status"] == "done" for comp in body["compensations"]))
        self.assertTrue(
            all(step["status"] == "compensated" for step in body["steps"]
                if step["stage"] in by_stage)
        )

        # every effect is undone
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 0.0)
        status, valve_state = self.call("GET", "/valve")
        self.assertEqual(valve_state["opened"], [])
        status, comp_state = self.call("GET", "/comp")
        self.assertEqual(comp_state["total_stage"], 0)
        status, defrost_state = self.call("GET", "/defrost")
        self.assertEqual(defrost_state["defrosting"], [])

    def test_safety_compensation_waits_for_approval(self) -> None:
        self.add_room()
        self.add_comp()
        params = dict(self.run_params(ppm=120.0), id="cy6")
        status, body = self.call("POST", "/cycle/run", params)
        self.assertEqual(body["cycle_status"], "done")
        self.assertTrue(body["tripped"])
        status, interlock = self.call("GET", "/interlock")
        self.assertTrue(interlock["tripped"])

        status, body = self.call("POST", "/cycle/restart", {"id": "cy6"})
        self.assertEqual(body["status"], "compensating")
        pending = [comp for comp in body["compensations"] if comp["status"] == "pending"]
        self.assertEqual([comp["stage"] for comp in pending], ["interlock"])
        self.assertEqual(pending[0]["mode"], "manual")

        # the safety latch is still held while approval is outstanding
        status, interlock = self.call("GET", "/interlock")
        self.assertTrue(interlock["tripped"])

        # quota was returned automatically even on the tripped path
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 0.0)

        status, body = self.call("POST", "/cycle/compensate", {"id": "cy6"})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "compensated")
        status, interlock = self.call("GET", "/interlock")
        self.assertFalse(interlock["tripped"])

    def test_restart_with_rerun_starts_a_fresh_cycle(self) -> None:
        self.add_room()
        self.add_comp()
        self.call("POST", "/cycle/run", dict(self.run_params(), id="cy7"))
        status, body = self.call("POST", "/cycle/restart", {"id": "cy7", "rerun": True})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "compensated")
        self.assertFalse(body["rerun_pending"])
        self.assertIsNotNone(body["rerun"])
        self.assertEqual(body["rerun"]["id"], "cy7-r1")
        self.assertEqual(body["rerun"]["status"], "done")

        # quota: +2 by cy7, -2 by its compensation, +2 by the rerun
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 2.0)

        status, listing = self.call("GET", "/cycle/list")
        self.assertEqual(listing["count"], 2)

    def test_restart_without_compensation_aborts(self) -> None:
        self.add_room()
        self.add_comp()
        params = dict(self.run_params(), id="cy8", compensation="none")
        self.call("POST", "/cycle/run", params)
        status, body = self.call("POST", "/cycle/restart", {"id": "cy8"})
        self.assertEqual(body["status"], "aborted")
        self.assertEqual(body["compensations"], [])
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 2.0)

    def test_manual_policy_holds_every_compensation(self) -> None:
        self.add_room()
        self.add_comp()
        params = dict(self.run_params(), id="cy9", compensation="manual")
        self.call("POST", "/cycle/run", params)
        status, body = self.call("POST", "/cycle/restart", {"id": "cy9"})
        self.assertEqual(body["status"], "compensating")
        pending = [comp for comp in body["compensations"] if comp["status"] == "pending"]
        self.assertEqual({comp["stage"] for comp in pending}, {"comp", "valve", "defrost", "quota"})

        # nothing was undone before approval
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 2.0)

        status, body = self.call("POST", "/cycle/compensate", {"id": "cy9"})
        self.assertEqual(body["status"], "compensated")
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 0.0)

    def test_rerun_endpoint_and_error_paths(self) -> None:
        self.add_room()
        self.add_comp()
        self.call("POST", "/cycle/run", dict(self.run_params(), id="cy10"))

        # rerun straight from done
        status, body = self.call("POST", "/cycle/rerun", {"id": "cy10"})
        self.assertEqual(status, 200)
        self.assertEqual(body["cycle_id"], "cy10-r1")
        self.assertEqual(body["cycle_status"], "done")

        status, error = self.call("GET", "/cycle/status", {"id": "ghost"})
        self.assertEqual(status, 404)
        status, error = self.call("POST", "/cycle/resume", {"id": "cy10"})
        self.assertEqual(status, 400)
        self.assertIn("nothing to resume", error["error"])
        status, error = self.call("POST", "/cycle/rollback", {"id": "cy10"})
        self.assertEqual(status, 400)
        status, error = self.call("POST", "/cycle/compensate", {"id": "cy10"})
        self.assertEqual(status, 400)
        status, error = self.call(
            "POST", "/cycle/run", dict(self.run_params(), id="cy11", compensation="maybe")
        )
        self.assertEqual(status, 400)
        self.assertIn("compensation", error["error"])

    def test_legacy_cycle_endpoint_is_orchestrated(self) -> None:
        self.add_room()
        self.add_comp()
        status, body = self.call("POST", "/cycle", self.run_params())
        self.assertEqual(status, 200)
        self.assertEqual(body["cycle_status"], "done")
        self.assertTrue(body["cycle_id"])
        self.assertEqual(body["quota"], 2.0)
        self.assertEqual(len(body["steps"]), 9)

    def test_ops_counters_include_cycles(self) -> None:
        self.add_room()
        self.add_comp()
        self.call("POST", "/cycle/run", dict(self.run_params(), id="cy12"))
        self.call("POST", "/cycle/run", dict(self.run_params(comp_id="ghost"), id="cy13"))
        status, ops = self.call("GET", "/ops")
        self.assertEqual(status, 200)
        self.assertEqual(ops["cycles"], 2)
        self.assertEqual(ops["cycles_failed"], 1)
        self.assertEqual(ops["compensations_pending"], 0)


class CrashWindowCase(unittest.TestCase):
    """A step interrupted between its effect and the journal write must not
    double-apply when the cycle resumes."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        seed_defaults(self.store)
        self.rt = Runtime(self.store)
        self.orchestrator = Orchestrator(self.store)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def craft(self, stage: str, memo: dict) -> CycleRecord:
        """A cycle left behind by a crash just after ``stage`` took effect."""

        params = {
            "room_id": "",
            "comp_id": "",
            "temp": -5.0,
            "ppm": 1.0,
            "amount": 2.0,
            "defrost": 0,
        }
        stages = [spec.stage.value for spec in ordered_specs()]
        record = CycleRecord.new("crafted", params, "auto", stages)
        for index, step in enumerate(record.steps):
            if step.stage == stage:
                step.status = model.RUNNING
                step.memo = memo
                step.attempts = 1
                record.cursor = index
                break
            step.status = model.DONE
        journal.save_cycle(self.store, record)
        journal.index_cycle(self.store, record.id)
        return record

    def test_quota_reentry_applies_only_the_missing_delta(self) -> None:
        self.craft("quota", {"baseline": 0.0, "target": 2.0})
        # the effect landed but the journal write did not
        self.rt.accumulator.add(2.0)

        record = self.orchestrator.resume(self.rt, "crafted")
        self.assertEqual(record.status, "done")
        self.assertEqual(self.rt.accumulator.value(), 2.0)
        self.assertFalse(record.step("quota").detail["applied"])

    def test_temp_reentry_does_not_duplicate_the_reading(self) -> None:
        self.craft("temp", {"at": 1_700_000_000})
        # the effect landed but the journal write did not
        self.rt.temp.record(-5.0, at=1_700_000_000)

        record = self.orchestrator.resume(self.rt, "crafted")
        self.assertEqual(record.status, "done")
        self.assertEqual(len(self.rt.temp.readings()), 1)
        self.assertFalse(record.step("temp").detail["recorded"])


if __name__ == "__main__":
    unittest.main()
