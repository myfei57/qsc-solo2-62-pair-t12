"""Tests for the orchestration engine and its console endpoints."""

from __future__ import annotations

import json
import tempfile
import unittest

from coldstore.console.seed import seed_defaults
from coldstore.console.server import Server
from coldstore.orchestrate import (
    CompensationMode,
    FailureStrategy,
    HaltCycle,
    Orchestrator,
    SkipStep,
    Step,
)
from coldstore.store import Store


def counting_step(name, calls, **kwargs):
    def run(ctx):
        calls.append(name)
        return name

    return Step(name, run, **kwargs)


class EngineCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.orchestrator = Orchestrator(self.store)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def test_run_completes_and_persists_every_transition(self) -> None:
        calls = []
        steps = [counting_step("a", calls), counting_step("b", calls)]
        record = self.orchestrator.run("c1", "test", steps, {"x": 1})
        self.assertEqual(record.status, "completed")
        self.assertEqual([step["status"] for step in record.as_dict()["steps"]], ["done", "done"])

        reloaded = Orchestrator(self.store).get("c1")
        self.assertIsNotNone(reloaded)
        self.assertEqual(reloaded.status, "completed")
        self.assertEqual(reloaded.checkpoint(), {"cursor": 2, "total": 2, "next_step": ""})

    def test_failure_parks_at_checkpoint_and_resume_continues_from_it(self) -> None:
        calls = []
        state = {"fail": True}

        def flaky(ctx):
            calls.append("b")
            if state["fail"]:
                raise RuntimeError("boom")
            return "b"

        steps = [counting_step("a", calls), Step("b", flaky), counting_step("c", calls)]
        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "failed")
        self.assertEqual(record.checkpoint()["next_step"], "b")
        self.assertEqual(record.error, "b: boom")

        state["fail"] = False
        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "completed")
        # Step a ran once: the resume continued from the checkpoint.
        self.assertEqual(calls, ["a", "b", "b", "c"])

    def test_reentry_after_completion_applies_nothing_twice(self) -> None:
        calls = []
        steps = [counting_step("a", calls), counting_step("b", calls)]
        self.orchestrator.run("c1", "test", steps, {})
        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "completed")
        self.assertEqual(calls, ["a", "b"])

    def test_effect_log_guards_the_crash_window(self) -> None:
        applied = []
        state = {"fail": True}

        def flaky(ctx):
            ctx.effect_once("effect", lambda: applied.append("x") or "done")
            if state["fail"]:
                raise RuntimeError("boom")
            return "ok"

        steps = [Step("risky", flaky)]
        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "failed")
        self.assertEqual(applied, ["x"])

        state["fail"] = False
        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "completed")
        # The effect was recorded before the failure, so it did not run again.
        self.assertEqual(applied, ["x"])

    def test_hold_strategy_waits_for_an_operator_to_compensate(self) -> None:
        undone = []

        def fail(ctx):
            raise RuntimeError("boom")

        steps = [
            Step("a", lambda ctx: "a", compensate=lambda ctx: undone.append("a")),
            Step("b", fail),
        ]
        record = self.orchestrator.run("c1", "test", steps, {}, FailureStrategy.HOLD)
        self.assertEqual(record.status, "failed")
        self.assertEqual(undone, [])

        record = self.orchestrator.compensate("c1", steps)
        self.assertEqual(record.status, "compensated")
        self.assertEqual(undone, ["a"])
        compensations = record.as_dict()["compensations"]
        self.assertEqual(compensations[-1]["step"], "a")
        self.assertEqual(compensations[-1]["mode"], "auto")

    def test_compensate_strategy_walks_completed_steps_backwards(self) -> None:
        undone = []

        def fail(ctx):
            raise RuntimeError("boom")

        steps = [
            Step("a", lambda ctx: "a", compensate=lambda ctx: undone.append("a")),
            Step("b", lambda ctx: "b", compensate=lambda ctx: undone.append("b")),
            Step("c", fail),
        ]
        record = self.orchestrator.run("c1", "test", steps, {}, FailureStrategy.COMPENSATE)
        self.assertEqual(record.status, "compensated")
        self.assertEqual(undone, ["b", "a"])

    def test_manual_compensation_parks_until_approved(self) -> None:
        undone = []

        def fail(ctx):
            raise RuntimeError("boom")

        steps = [
            Step(
                "a",
                lambda ctx: "a",
                compensate=lambda ctx: undone.append("a"),
                compensation_mode=CompensationMode.MANUAL,
            ),
            Step("b", fail),
        ]
        record = self.orchestrator.run("c1", "test", steps, {}, FailureStrategy.COMPENSATE)
        self.assertEqual(record.status, "awaiting_approval")
        self.assertEqual(undone, [])

        with self.assertRaises(ValueError):
            self.orchestrator.approve("c1", steps, step_name="b")

        record = self.orchestrator.approve("c1", steps)
        self.assertEqual(record.status, "compensated")
        self.assertEqual(undone, ["a"])
        self.assertEqual(record.approvals, ["a"])

    def test_failed_compensation_is_resumable(self) -> None:
        undone = []
        state = {"fail": True}

        def compensate_a(ctx):
            if state["fail"]:
                raise RuntimeError("undo boom")
            undone.append("a")

        def fail(ctx):
            raise RuntimeError("boom")

        steps = [Step("a", lambda ctx: "a", compensate=compensate_a), Step("b", fail)]
        record = self.orchestrator.run("c1", "test", steps, {}, FailureStrategy.COMPENSATE)
        self.assertEqual(record.status, "compensation_failed")

        state["fail"] = False
        record = self.orchestrator.compensate("c1", steps)
        self.assertEqual(record.status, "compensated")
        self.assertEqual(undone, ["a"])

    def test_params_and_plan_are_frozen_at_creation(self) -> None:
        steps = [Step("a", lambda ctx: "a")]
        self.orchestrator.run("c1", "test", steps, {"x": 1})
        with self.assertRaises(ValueError):
            self.orchestrator.run("c1", "test", steps, {"x": 2})
        with self.assertRaises(ValueError):
            self.orchestrator.run("c1", "test", [Step("b", lambda ctx: "b")], {"x": 1})

    def test_halt_pauses_the_cycle_and_resume_reevaluates_the_gate(self) -> None:
        calls = []
        state = {"latched": True}

        def gate(ctx):
            calls.append("gate")
            if state["latched"]:
                raise HaltCycle("latched")
            return "clear"

        steps = [counting_step("a", calls), Step("gate", gate), counting_step("c", calls)]
        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "halted")
        self.assertEqual(record.halt_reason, "latched")
        self.assertEqual(record.checkpoint()["next_step"], "gate")

        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "halted")
        self.assertEqual(calls, ["a", "gate", "gate"])

        state["latched"] = False
        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "completed")
        self.assertEqual(calls, ["a", "gate", "gate", "gate", "c"])

    def test_skip_step_marks_the_step_skipped(self) -> None:
        def not_applicable(ctx):
            raise SkipStep("nothing to do")

        steps = [Step("a", not_applicable), Step("b", lambda ctx: "b")]
        record = self.orchestrator.run("c1", "test", steps, {})
        self.assertEqual(record.status, "completed")
        statuses = {step["name"]: step["status"] for step in record.as_dict()["steps"]}
        self.assertEqual(statuses, {"a": "skipped", "b": "done"})

    def test_compensate_rejects_a_completed_cycle(self) -> None:
        steps = [Step("a", lambda ctx: "a")]
        self.orchestrator.run("c1", "test", steps, {})
        with self.assertRaises(ValueError):
            self.orchestrator.compensate("c1", steps)


class ConsoleOrchestrationCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = f"{self._tmp.name}/state.json"
        self.store = Store.open(self.path)
        seed_defaults(self.store)
        self.server = Server(self.store)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def restart_console(self) -> None:
        """Simulate a process restart: reopen the same store file fresh."""

        self.store.close()
        self.store = Store.open(self.path)
        self.server = Server(self.store)

    def call(self, method: str, path: str, payload: dict | None = None):
        response = self.server.dispatch(method, path, payload)
        body = response.body.decode("utf-8")
        parsed = json.loads(body) if response.content_type.startswith("application/json") else body
        return response.status, parsed

    def run_cycle(self, cycle_id: str, **params):
        payload = {"cycle_id": cycle_id, "temp": -5.0, "ppm": 0.0, "amount": 0.0}
        payload.update(params)
        return self.call("POST", "/orchestration/run", payload)

    def test_run_completes_and_is_visible_on_ops_endpoints(self) -> None:
        self.call("POST", "/room/add", {"id": "r1", "zone": 1, "temp": -5.0})
        self.call("POST", "/comp/add", {"id": "c1", "stages": 3})

        status, cycle = self.run_cycle("cy1", room_id="r1", comp_id="c1", amount=2.5)
        self.assertEqual(status, 200)
        self.assertEqual(cycle["status"], "completed")
        self.assertEqual(len(cycle["steps"]), 11)
        self.assertEqual(cycle["checkpoint"], {"cursor": 11, "total": 11, "next_step": ""})
        self.assertTrue(all(step["status"] == "done" for step in cycle["steps"]))

        status, listing = self.call("GET", "/orchestration")
        self.assertEqual(status, 200)
        self.assertEqual(listing["count"], 1)
        self.assertEqual(listing["attention"], [])

        status, lookup = self.call("POST", "/orchestration/lookup", {"cycle_id": "cy1"})
        self.assertEqual(status, 200)
        self.assertEqual(lookup["cycle_id"], "cy1")

        status, ops = self.call("GET", "/ops")
        self.assertEqual(status, 200)
        self.assertEqual(ops["cycles"], 1)
        self.assertEqual(ops["cycles_attention"], 0)

    def test_reentry_does_not_meter_quota_or_audit_twice(self) -> None:
        status, cycle = self.run_cycle("cy1", amount=5.0)
        self.assertEqual(cycle["status"], "completed")
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 5.0)
        status, audit = self.call("GET", "/audit")
        audit_count = len(audit["entries"])

        status, cycle = self.run_cycle("cy1", amount=5.0)
        self.assertEqual(cycle["status"], "completed")
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 5.0)
        status, audit = self.call("GET", "/audit")
        self.assertEqual(len(audit["entries"]), audit_count)

    def test_ammonia_trip_halts_and_reset_plus_resume_completes(self) -> None:
        self.call("POST", "/comp/add", {"id": "c1", "stages": 3})
        status, cycle = self.run_cycle("cy1", comp_id="c1", ppm=30.0, amount=1.0)
        self.assertEqual(cycle["status"], "halted")
        self.assertEqual(cycle["checkpoint"]["next_step"], "comp.stage")
        status, interlock = self.call("GET", "/interlock")
        self.assertTrue(interlock["tripped"])
        # The safety response stopped the compressor and latched the interlock.
        status, comp = self.call("POST", "/comp/lookup", {"id": "c1"})
        self.assertFalse(comp["compressor"]["running"])

        # Resuming while the latch is held halts at the same gate again.
        status, cycle = self.call("POST", "/orchestration/resume", {"cycle_id": "cy1"})
        self.assertEqual(cycle["status"], "halted")

        self.call("POST", "/interlock/reset")
        status, cycle = self.call("POST", "/orchestration/resume", {"cycle_id": "cy1"})
        self.assertEqual(cycle["status"], "completed")
        # The trip was recorded once even though the cycle was entered twice.
        status, trips = self.call("GET", "/interlock/trips")
        self.assertEqual(trips["count"], 1)

    def test_failure_resume_continues_from_checkpoint_without_repeating_steps(self) -> None:
        # comp.stage fails because the compressor does not exist yet.
        status, cycle = self.run_cycle("cy1", comp_id="ghost", amount=3.0)
        self.assertEqual(cycle["status"], "failed")
        self.assertEqual(cycle["checkpoint"]["next_step"], "comp.stage")
        self.assertIn("comp.stage", cycle["error"])

        status, ops = self.call("GET", "/ops")
        self.assertEqual(ops["cycles_attention"], 1)
        status, listing = self.call("GET", "/orchestration")
        self.assertEqual(listing["attention"], ["cy1"])

        self.call("POST", "/comp/add", {"id": "ghost", "stages": 2})
        status, cycle = self.call("POST", "/orchestration/resume", {"cycle_id": "cy1"})
        self.assertEqual(cycle["status"], "completed")

        # temp.record ran before the failure and was not repeated on resume.
        status, readings = self.call("GET", "/temp/readings")
        self.assertEqual(readings["samples"], 1)
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 3.0)

    def test_halted_cycle_compensates_manual_steps_only_after_approval(self) -> None:
        self.call("POST", "/room/add", {"id": "r1", "zone": 1, "temp": -5.0})
        self.call("POST", "/interlock/trip", {"reason": "manual"})
        # The interlock gate halts the cycle after defrost has started.
        status, cycle = self.run_cycle("cy1", room_id="r1", defrost=1)
        self.assertEqual(cycle["status"], "halted")
        status, defrost = self.call("GET", "/defrost")
        self.assertEqual(defrost["defrosting"], ["r1"])

        status, cycle = self.call("POST", "/orchestration/compensate", {"cycle_id": "cy1"})
        self.assertEqual(cycle["status"], "awaiting_approval")
        # Reverting a defrost is a manual decision: the room is still warming.
        status, defrost = self.call("GET", "/defrost")
        self.assertEqual(defrost["defrosting"], ["r1"])

        status, cycle = self.call("POST", "/orchestration/approve", {"cycle_id": "cy1"})
        self.assertEqual(cycle["status"], "compensated")
        status, defrost = self.call("GET", "/defrost")
        self.assertEqual(defrost["defrosting"], [])
        manual = [c for c in cycle["compensations"] if c["mode"] == "manual"]
        self.assertEqual([c["step"] for c in manual], ["defrost.toggle"])
        self.assertTrue(manual[0]["approved"])

    def test_failed_cycle_compensates_auto_steps_and_refunds_quota(self) -> None:
        self.call("POST", "/comp/add", {"id": "c1", "stages": 3})

        def broken_record(kind, detail):
            raise RuntimeError("audit store down")

        # The quota is metered and then the final step fails, so the
        # compensate strategy walks the completed chain backwards.
        self.server.runtime.auditor.record = broken_record
        status, cycle = self.run_cycle("cy1", comp_id="c1", amount=4.0, strategy="compensate")
        self.assertEqual(cycle["status"], "compensated")
        compensated = {c["step"]: c["mode"] for c in cycle["compensations"]}
        self.assertEqual(compensated["quota.meter"], "auto")
        self.assertEqual(compensated["valve.supply"], "auto")
        self.assertEqual(compensated["comp.stage"], "auto")

        # The metered quota was refunded and the compressor chain undone.
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 0.0)
        status, comp = self.call("POST", "/comp/lookup", {"id": "c1"})
        self.assertFalse(comp["compressor"]["running"])
        status, valve = self.call("GET", "/valve")
        self.assertEqual(valve["opened"], [])

    def test_invalid_input_fails_the_cycle_not_the_request(self) -> None:
        status, cycle = self.run_cycle("cy1", temp=500.0)
        self.assertEqual(status, 200)
        self.assertEqual(cycle["status"], "failed")
        self.assertEqual(cycle["checkpoint"]["next_step"], "validate")

    def test_cycle_survives_a_console_restart(self) -> None:
        # The cycle fails midway and the console goes down with it.
        status, cycle = self.run_cycle("cy1", comp_id="ghost", amount=3.0)
        self.assertEqual(cycle["status"], "failed")
        self.restart_console()

        # The breakpoint is still on disk after the restart.
        status, cycle = self.call("POST", "/orchestration/lookup", {"cycle_id": "cy1"})
        self.assertEqual(cycle["status"], "failed")
        self.assertEqual(cycle["checkpoint"]["next_step"], "comp.stage")

        self.call("POST", "/comp/add", {"id": "ghost", "stages": 2})
        status, cycle = self.call("POST", "/orchestration/resume", {"cycle_id": "cy1"})
        self.assertEqual(cycle["status"], "completed")
        status, readings = self.call("GET", "/temp/readings")
        self.assertEqual(readings["samples"], 1)
        status, quota = self.call("GET", "/quota")
        self.assertEqual(quota["value"], 3.0)

    def test_unknown_cycle_and_bad_strategy_are_rejected(self) -> None:
        status, error = self.call("POST", "/orchestration/resume", {"cycle_id": "nope"})
        self.assertEqual(status, 404)
        status, error = self.call("POST", "/orchestration/lookup", {"cycle_id": "nope"})
        self.assertEqual(status, 404)
        status, error = self.call("POST", "/orchestration/compensate", {"cycle_id": "nope"})
        self.assertEqual(status, 404)
        status, error = self.call("POST", "/orchestration/approve", {"cycle_id": "nope"})
        self.assertEqual(status, 404)
        status, error = self.call("POST", "/orchestration/run", {"strategy": "wild"})
        self.assertEqual(status, 400)

    def test_approve_requires_a_cycle_waiting_for_approval(self) -> None:
        status, cycle = self.run_cycle("cy1")
        self.assertEqual(cycle["status"], "completed")
        status, error = self.call("POST", "/orchestration/approve", {"cycle_id": "cy1"})
        self.assertEqual(status, 400)


if __name__ == "__main__":
    unittest.main()
