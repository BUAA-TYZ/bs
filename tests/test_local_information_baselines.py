import copy
import unittest

from analysis.local_information_baselines import LocalInformationPlane, LocalMinLoad, reachable_helpers
from analysis.sar_three_plan import ThreePlanChoice
from test_task_gate import gate_state


def observation():
    state = gate_state()
    state.config.update(mem_capacity_gb=1., task_source_sat_ids=[0, 1], sar_source_isolation=True,
                        image_size_mb=1, ground_input_relay=True)
    for t in state.tiles.values():
        t.update(source_sat_id=0, unique_input_size_mb=.125)
    for row in state.satellites.values():
        row['vram_remaining_gb'] = 1.
    for row in state.ground_stations.values():
        row['min_elevation_deg'] = 10
    for row in state.links.values():
        row['latency_ms'] = 0
    return state


def deliver(plane, state, sid=2, generated=0., received=0., hops=1):
    plane.delivered[0][sid] = {'satellite': copy.deepcopy(state.satellites[sid]),
                             'generated_s': generated, 'received_s': received, 'hops': hops}


class LocalInformationTests(unittest.TestCase):
    def test_hidden_dynamic_mutation_cannot_change_view_or_actions(self):
        state = observation()
        plane = LocalInformationPlane([0, 1])
        a = plane.view(state, 0, 1)
        changed = copy.deepcopy(state)
        for sid in (1, 2, 3):
            changed.satellites[sid].update(committed_compute_s=99999, queue_len=999,
                mem_remaining_gb=0, vram_remaining_gb=0, isl_backlog_mb={0: 9999},
                ground_backlog_mb={'gs': 9999}, committed_result_mb=9999, unrouted_result_mb=9999)
        changed.ground_stations['gs'].update(committed_compute_s=99999, queue_len=999, running=1)
        b = LocalInformationPlane([0, 1]).view(changed, 0, 1)
        self.assertEqual(a, b)
        self.assertEqual(ThreePlanChoice(8).select_actions(a), ThreePlanChoice(8).select_actions(b))

    def test_full_static_plan_and_ground_quote_survive(self):
        state = observation()
        before = copy.deepcopy(state)
        view = LocalInformationPlane([0, 1]).view(state, 0, 1)
        self.assertEqual(view.links, state.links)
        self.assertEqual(set(view.satellites), set(state.satellites))
        self.assertEqual(view.satellites[3]['next_gs_windows'], state.satellites[3]['next_gs_windows'])
        policy = ThreePlanChoice(8)
        policy.select_actions(view)
        self.assertIsNotNone(policy.decisions[0]['quotes_seconds']['ground'])
        self.assertEqual(state, before)

    def test_received_state_is_delayed_and_radius_is_current(self):
        state = observation(); plane = LocalInformationPlane([0, 1])
        state.satellites[2]['committed_compute_s'] = 17
        deliver(plane, state)
        state.satellites[2]['committed_compute_s'] = 99
        self.assertEqual(plane.view(state, 0, 1).satellites[2]['committed_compute_s'], 17)
        state.neighbors[0].remove(2)
        self.assertFalse(plane.view(state, 0, 1).satellites[2]['dynamic_known'])

    def test_history_only_uses_delivered_legal_samples_once(self):
        state = observation(); plane = LocalInformationPlane([0, 1]); plane.estimator = 'history'
        state.satellites[2]['committed_compute_s'] = 20
        deliver(plane, state)
        plane.enqueue(state)
        state.time = 50
        plane.enqueue(state)
        self.assertEqual(plane.history[0][2][1]['committed_compute_s'], 1)
        state.time = 200
        state.satellites[2]['committed_compute_s'] = 99999
        view = plane.view(state, 0, 1)
        self.assertFalse(view.satellites[2]['dynamic_known'])
        self.assertEqual(view.satellites[2]['committed_compute_s'], 20)
        self.assertEqual(view.satellites[3]['committed_compute_s'], 0)
        self.assertEqual(view.ground_stations['gs']['committed_compute_s'], 0)

    def test_not_yet_received_or_excess_hop_advertisement_is_unknown(self):
        state = observation(); plane = LocalInformationPlane([0, 1])
        deliver(plane, state, received=1)
        self.assertNotIn(2, plane.view(state, 0, 1).config['known_dynamic_nodes'])
        deliver(plane, state, hops=2)
        self.assertNotIn(2, plane.view(state, 0, 1).config['known_dynamic_nodes'])

    def test_other_source_is_not_a_relay_in_neighborhood(self):
        state = observation()
        state.neighbors = {0: [1], 1: [0, 2], 2: [1, 3], 3: [2]}
        self.assertEqual(reachable_helpers(state, 0, 2, {0, 1}), set())

    def test_minload_chooses_known_lowest_work_and_waits_if_unknown(self):
        state = observation(); plane = LocalInformationPlane([0, 1])
        state.satellites[2]['committed_compute_s'] = 40
        state.satellites[3]['committed_compute_s'] = 5
        deliver(plane, state, 2); deliver(plane, state, 3)
        view = plane.view(state, 0, 1)
        self.assertEqual(LocalMinLoad().select_actions(view)[0].target_sat_id, 3)
        empty = LocalInformationPlane([0, 1]).view(state, 0, 1)
        self.assertEqual(LocalMinLoad().select_actions(empty), [])


if __name__ == '__main__':
    unittest.main()
