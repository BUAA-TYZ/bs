import copy
import random
import unittest

from sim.entities import Action, ActionType
from sim.scheduler.reservation import _Ledger, _ForecastLedger, ForecastReservationPolicy
from test_reservation import contention_state
from test_simulation import make_env


def chain_state():
    state = contention_state()
    state.config.update(result_relay=True, relay_max_hops=3, contact_scan_s=1,
                        isl_bandwidth_floor_mbps=8)
    for sat in state.satellites.values():
        sat["next_gs_windows"] = {"gs": None}
    state.satellites[3]["next_gs_windows"] = {"gs": (40, 20)}
    state.neighbors = {0: [1], 1: [0], 2: [], 3: []}
    state.links = {"0-1": {"up": True, "bandwidth_mbps": 8, "contact_windows": [(0, 10)]},
                   "1-2": {"up": False, "bandwidth_mbps": 8, "contact_windows": [(20, 10)]},
                   "2-3": {"up": False, "bandwidth_mbps": 8, "contact_windows": [(35, 20)]}}
    return state


class ForecastReservationTests(unittest.TestCase):
    def test_pruned_quotes_match_unpruned_search_after_reservations(self):
        rng = random.Random(20261012)
        for case in range(40):
            state = contention_state()
            state.config.update(result_relay=True, relay_max_hops=3, contact_scan_s=1,
                                isl_bandwidth_floor_mbps=4)
            state.neighbors = {sid: [] for sid in state.satellites}
            state.links = {}
            for sid, sat in state.satellites.items():
                sat["committed_compute_s"] = rng.randrange(8)
                sat["next_gs_windows"] = {"gs": (rng.randrange(20), 10) if rng.random() < 0.6 else None}
                for dst in range(sid + 1, len(state.satellites)):
                    up = rng.random() < 0.5
                    start = 0 if up else rng.randrange(1, 10)
                    state.links[f"{sid}-{dst}"] = {"up": up, "bandwidth_mbps": rng.randrange(4, 17),
                                                    "contact_windows": [(start, 10), (20, 10)]}
                    if up:
                        state.neighbors[sid].append(dst)
                        state.neighbors[dst].append(sid)
            full = _ForecastLedger(state, guard=bool(case % 2), multihop=True, defer=True, prune=False)
            fast = _ForecastLedger(state, guard=bool(case % 2), multihop=True, defer=True)
            for tid, tile in state.tiles.items():
                expected, actual = full.options(tid, tile), fast.options(tid, tile)
                self.assertEqual(actual[:1], expected[:1], f"case={case}, tile={tid}")
                if expected:
                    full.commit(tile, expected[0])
                    fast.commit(tile, actual[0])

    def test_waits_for_fast_future_neighbor_then_dispatches_legally(self):
        state = contention_state(False)
        state.tiles = {"t0": state.tiles["t0"]}
        state.config.update(result_relay=True, relay_max_hops=3, contact_scan_s=1,
                            isl_bandwidth_floor_mbps=8)
        state.satellites[0]["compute_rate"] = 0.01
        state.satellites[0]["next_gs_windows"]["gs"] = (0, 200)
        state.satellites[2]["next_gs_windows"]["gs"] = (20, 200)
        state.neighbors[0] = []
        state.neighbors[2] = [1]
        state.links["0-2"].update(up=False, contact_windows=[(10, 100)])
        immediate = ForecastReservationPolicy(multihop=True)
        self.assertEqual(immediate.select_actions(state)[0].action_type, ActionType.LOCAL)
        policy = ForecastReservationPolicy(multihop=True, defer=True)
        before = copy.deepcopy(state)
        self.assertEqual(policy.select_actions(state)[0].action_type, ActionType.WAIT)
        self.assertEqual(policy.stats["wait_better_contact"], 1)
        self.assertEqual(policy.stats["accepted"], 0)
        self.assertEqual(state, before)
        state.time = 10
        state.neighbors[0] = [2]
        state.neighbors[2] = [0, 1]
        state.links["0-2"].update(up=True, contact_windows=[(0, 100)])
        state.satellites[2]["next_gs_windows"]["gs"] = (10, 200)
        action = policy.select_actions(state)[0]
        self.assertEqual(action.action_type, ActionType.OFFLOAD)
        self.assertEqual(action.target_sat_id, 2)

    def test_waiting_for_next_window_cannot_emit_current_offload(self):
        state = contention_state(False)
        state.config.update(result_relay=True, relay_max_hops=3, contact_scan_s=1,
                            isl_bandwidth_floor_mbps=8)
        state.links["0-2"]["contact_windows"] = [(0, 1), (20, 100)]
        ledger = _ForecastLedger(state, guard=True, multihop=True, defer=True)
        slot = ledger.input_transfer(0, 2, 0, 1)
        self.assertEqual(slot, (21, 20, 0))
        self.assertTrue(ledger.should_defer(0, 2, slot))
        state.tiles["t0"]["deadline_remaining_s"] = 5
        self.assertEqual(ledger.options("t0", state.tiles["t0"]), [])

    def test_guard_rejects_optimistic_short_contact(self):
        state = contention_state()
        state.config.update(dt=10, contact_scan_s=30, isl_bandwidth_floor_mbps=6.15)
        state.links["0-2"].update(bandwidth_mbps=11.6, contact_windows=[(0, 50)])
        self.assertIsNotNone(_Ledger(state).transfer(0, 2, 0, 16))
        self.assertIsNone(_ForecastLedger(state, guard=True).transfer(0, 2, 0, 16))
        state.links["0-2"]["contact_windows"] = [(0, 100)]
        self.assertEqual(_ForecastLedger(state, guard=True).transfer(0, 2, 0, 16), (30, 0, 0))

    def test_future_multihop_plan_respects_hops_memory_and_time(self):
        state = chain_state()
        ledger = _ForecastLedger(state, guard=True, multihop=True)
        self.assertEqual(_Ledger(state).result_options(0, 0, 1), [])
        quotes = ledger.result_options(0, 0, 1)
        self.assertEqual(len(quotes), 1)
        quote = quotes[0]
        self.assertEqual(quote["finish"], 41)
        self.assertEqual([(s, d, slot[1]) for s, d, slot in quote["relays"]],
                         [(0, 1, 0), (1, 2, 20), (2, 3, 35)])
        self.assertIsNone(ledger.transfer(1, 2, 0, 1))
        state.config["relay_max_hops"] = 2
        self.assertEqual(ledger.result_options(0, 0, 1), [])
        state.config["relay_max_hops"] = 3
        ledger.memory[2] = 0
        self.assertEqual(ledger.result_options(0, 0, 1), [])

    def test_path_bookings_are_isolated_then_consume_each_hop(self):
        state = chain_state()
        ledger = _ForecastLedger(state, guard=True, multihop=True)
        quote = ledger.result_options(0, 0, 1)[0]
        quote.update(sat=0, ready=0, incoming=None, ground=False)
        child = ledger.fork()
        child.commit(state.tiles["t0"], quote)
        for src, dst, slot in quote["relays"]:
            self.assertEqual(ledger.links[src, dst].channels, [[]])
            self.assertEqual(child.links[src, dst].channels, [[(slot[1], slot[0])]])
        self.assertGreater(child.result_options(0, 0, 1)[0]["finish"], quote["finish"])

    def test_backlog_is_not_served_during_disconnection(self):
        state = chain_state()
        state.links["0-1"]["contact_windows"] = [(0, 10), (100, 20)]
        state.satellites[0]["isl_backlog_mb"][1] = 15
        ledger = _ForecastLedger(state, multihop=True)
        self.assertEqual(ledger.links[0, 1].channels, [[(0, 10), (100, 105)]])
        self.assertEqual(ledger.contact_transfer(0, 1, 20, 1, future=True), (106, 105, 0))

    def test_no_relay_and_no_guard_bypass_in_fallback(self):
        state = chain_state()
        state.config["result_relay"] = False
        for multihop in (False, True):
            self.assertEqual(_ForecastLedger(state, guard=True, multihop=multihop).result_options(0, 0, 1), [])
        state.links["0-1"]["contact_windows"] = [(0, 1)]
        policy = ForecastReservationPolicy(guard=True)
        ledger = policy.make_ledger(state)
        action = policy.fallback_action(ledger, state.tiles["t0"], Action("t0", ActionType.OFFLOAD, target_sat_id=1))
        self.assertEqual(action.action_type, ActionType.WAIT)
        self.assertEqual(ledger.memory[1], 1)

    def test_environment_exports_physical_floor_without_advancing_rng(self):
        env = make_env()
        self.addCleanup(env.close)
        state = env.export_state()
        cfg = env.topology.cfg
        expected = cfg.bandwidth_mbps_min / (1 + cfg.max_range_km / cfg.bandwidth_distance_scale_km) if cfg.bandwidth_distance_scale_km else cfg.bandwidth_mbps_min
        self.assertEqual(state.config["isl_bandwidth_floor_mbps"], expected)
        self.assertEqual(env.export_state(), copy.deepcopy(state))


if __name__ == "__main__":
    unittest.main()
