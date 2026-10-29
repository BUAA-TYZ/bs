from copy import deepcopy
import unittest
from unittest.mock import patch

from sim.entities import ActionType
from sim.scheduler.contact_graph import arrival, Contact, quantize
from sim.scheduler.heft import schedule
from sim.scheduler.literature import CGSComputeAdapted, HEFTAdapted
from sim.scheduler.predictive import PredictivePolicy
from sim.scheduler.reservation import _Ledger
from test_overlap_planners import planner_state
from test_reservation import contention_state


def service_state(mode="work_conserving", ground=False):
    state = contention_state(False)
    state.config.update(dt=10, compute_service_mode=mode)
    state.links = {}
    state.neighbors = {sid: [] for sid in state.satellites}
    for sid, satellite in state.satellites.items():
        satellite["compute_rate"] = 10
        satellite["next_gs_windows"] = {"gs": (0, 1000) if sid == 0 else None}
    state.ground_stations["gs"]["compute_rate"] = 10
    state.ground_options[0] = [dict(gs_id="gs", bandwidth_mbps=8)] if ground else []
    for tile in state.tiles.values():
        tile["location"] = 0
    return state


class ComputeServicePlannerTests(unittest.TestCase):
    def test_calendar_accumulates_actual_compute_and_keeps_result_barrier(self):
        for mode, expected in (("step", [10, 20]), ("work_conserving", [0.1, 0.2])):
            state = service_state(mode)
            ledger = _Ledger(state)
            for index, (tid, tile) in enumerate(state.tiles.items()):
                quote = next(q for q in ledger.options(tid, tile) if q["sat"] == 0 and not q["ground"])
                self.assertAlmostEqual(quote["ready"], expected[index])
                self.assertGreaterEqual(quote["down"][1], quantize(quote["ready"], 10))
                self.assertGreaterEqual(quote["finish"], 20)
                ledger.commit(tile, quote)
                self.assertAlmostEqual(ledger.compute[0], expected[index])

    def test_ground_calendar_preserves_compute_budget_but_rounds_delivery(self):
        state = service_state(ground=True)
        ledger = _Ledger(state)
        for index, (tid, tile) in enumerate(state.tiles.items()):
            quote = next(q for q in ledger.options(tid, tile) if q["ground"])
            self.assertAlmostEqual(quote["ready"], 10 + (index + 1) / 10)
            self.assertEqual(quote["finish"], 20)
            ledger.commit(tile, quote)
        state.tiles["t0"]["deadline_remaining_s"] = 15
        self.assertFalse(any(q["ground"] for q in _Ledger(state).options("t0", state.tiles["t0"])))

    def test_predictive_groups_actual_work_and_cannot_cross_short_contact(self):
        state = planner_state()
        state.satellites[1]["next_gs_windows"] = {"gs": (0, 5)}
        self.assertIsNone(PredictivePolicy().select_actions(state)[0].target_sat_id)
        state.config["compute_service_mode"] = "work_conserving"
        self.assertEqual(PredictivePolicy().select_actions(state)[0].target_sat_id, 1)
        state.links["0-1"]["contact_windows"] = [(0, 1)]
        self.assertIsNone(PredictivePolicy().select_actions(state)[0].target_sat_id)
        self.assertEqual(arrival(Contact(0, 1, 0, 100, 1000, 10000), 0.1, 0.1, dt=10), 20)

    def test_cgs_quotes_both_fast_packets_at_same_barrier_without_rounding_busy(self):
        state = service_state()
        opportunities = []

        def capture(rows, plan, ground, size, **kwargs):
            opportunities.append(rows)
            return 20, 0, ()

        with patch("sim.scheduler.literature.choose_opportunity", side_effect=capture):
            actions = CGSComputeAdapted().select_actions(state)
        self.assertEqual([rows[0][0] for rows in opportunities], [10, 10])
        self.assertTrue(all(action.action_type == ActionType.LOCAL for action in actions))
        state.config["compute_service_mode"] = "step"
        opportunities.clear()
        with patch("sim.scheduler.literature.choose_opportunity", side_effect=capture):
            CGSComputeAdapted().select_actions(state)
        self.assertEqual([rows[0][0] for rows in opportunities], [10, 20])

    def test_heft_keeps_fractional_compute_and_defers_sink_until_step_end(self):
        state = service_state()
        observed = {}

        def capture(costs, successors, communication, average, **kwargs):
            observed["costs"] = deepcopy(costs)
            observed["hook"] = kwargs["arrival_time"]
            result = schedule(costs, successors, communication, average, **kwargs)
            observed["placements"] = result[0]
            return result

        with patch("sim.scheduler.literature.schedule", side_effect=capture):
            HEFTAdapted().select_actions(state)
        self.assertEqual(observed["costs"]["t0"][0], 0.1)
        self.assertAlmostEqual(observed["placements"]["t1"].end, 0.2)
        for tid, tile in state.tiles.items():
            sink = observed["placements"]["sink", tile["parent_task_id"]]
            self.assertEqual(sink.start, 20)
        self.assertEqual(observed["hook"]("t0", ("sink", "task0"), 0, ("ground", "gs"), 0.1), 20)
        state.config["compute_service_mode"] = "step"
        with patch("sim.scheduler.literature.schedule", side_effect=capture):
            HEFTAdapted().select_actions(state)
        self.assertIsNone(observed["hook"])
        self.assertEqual(observed["costs"]["t0"][0], 10)


if __name__ == "__main__":
    unittest.main()
