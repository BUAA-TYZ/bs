import copy
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.sar_selector_pilot import PilotChoice, candidate, choose_label
from test_task_gate import gate_state


class SelectorPilotTests(unittest.TestCase):
    def test_noop_hook_matches_frozen_choice(self):
        state = gate_state()
        state.config["image_size_mb"] = 1
        original = candidate.ThreePlanChoice(8)
        instrumented = PilotChoice(8)

        class Noop:
            def intervene(self, task, state, subset, options, quotes, details, trials, winner, prior, calendar):
                return winner, calendar

        instrumented.controller = Noop()
        self.assertEqual(original.select_actions(copy.deepcopy(state)), instrumented.select_actions(copy.deepcopy(state)))
        self.assertEqual(original.decisions, instrumented.decisions)
        self.assertEqual(original.raw_tasks, instrumented.raw_tasks)
        self.assertEqual(original.stats, instrumented.stats)

    def test_force_unquoted_ground_preserves_original_calendar_and_raw_semantics(self):
        state = gate_state()
        state.config["image_size_mb"] = 1
        policy = PilotChoice(8)

        class Force:
            def intervene(self, task, state, subset, options, quotes, details, trials, winner, prior, calendar):
                self.unquoted = quotes["ground"] is None
                return "ground", prior

        policy.controller = Force()
        with patch.object(candidate.Calendar, "quote", side_effect=[None, None, None]):
            actions = policy.select_actions(state)
        self.assertTrue(policy.controller.unquoted)
        self.assertTrue(all(isinstance(a, candidate.RawImageAction) for a in actions))
        self.assertEqual(policy.raw_tasks, {"task0"})
        self.assertEqual(policy.decisions[0]["selected"], "ground")

    def test_censoring_and_tie_order(self):
        def row(finish, censor=7200):
            return {"status": "PASS", "completion_seconds": finish, "censor_seconds": censor}

        self.assertEqual(choose_label({"local": row(400), "cgs": row(None), "ground": row(500)})["label"], "local")
        tied = choose_label({"local": row(400), "cgs": row(400), "ground": row(None)})
        self.assertEqual(tied["label"], "local")
        self.assertEqual(tied["ties"], ["local", "cgs"])
        self.assertEqual(choose_label({p: row(None) for p in ("local", "cgs", "ground")})["status"], "INDETERMINATE")
        self.assertEqual(choose_label({"local": row(400), "cgs": row(None, 300), "ground": row(500)})["status"], "INDETERMINATE")

    def test_execution_failure_is_not_indeterminate_or_zero_score(self):
        branches = {p: {"status": "PASS", "completion_seconds": 100, "censor_seconds": 7200}
                    for p in ("local", "cgs", "ground")}
        branches["ground"]["status"] = "FAILED"
        self.assertEqual(choose_label(branches)["status"], "ERROR")


if __name__ == "__main__":
    unittest.main()
