import copy
from types import SimpleNamespace
import unittest

from sim.entities import Action, ActionType, EnvState, Link
from sim.scheduler.reservation import JointReservationPolicy, ReservationPolicy, ServiceCalendar, _Ledger
from sim.marl.decision import decision_state
from test_simulation import make_env, add_task


def contention_state(alternative=True):
    satellites = {
        sid: {"compute_rate": 0.1 if sid < 2 else 1.0,
              "mem_remaining_gb": 1.0, "queue_len": int(sid < 2),
              "committed_compute_s": 0.0, "committed_result_mb": 0.0,
              "unrouted_result_mb": 0.0, "isl_backlog_mb": {}, "ground_backlog_mb": {},
              "next_gs_windows": {"gs": (0, 3 if sid == 2 else 10) if sid >= 2 else None}}
        for sid in range(4 if alternative else 3)}
    links, neighbors = {}, {sid: [] for sid in satellites}
    for src in (0, 1):
        for dst in range(2, len(satellites)):
            links[f"{src}-{dst}"] = {"up": True, "bandwidth_mbps": 8,
                                      "contact_windows": [(0, 100)]}
            neighbors[src].append(dst)
            neighbors[dst].append(src)
    tiles = {f"t{src}": {"location": src, "state": "QUEUED", "in_transfer": False,
                          "data_size_mb": 0.125, "data_size_gb": 0.125 / 1024,
                          "compute_cost": 1, "vram_req_gb": 0.1, "result_size_mb": 0.125,
                          "waiting_time": 0, "parent_task_id": f"task{src}",
                          "task_progress": {"failed": 0}, "deadline_remaining_s": None}
             for src in (0, 1)}
    return EnvState(0, satellites,
                    {"gs": {"compute_rate": 8, "queue_len": 0, "running": 0,
                            "committed_compute_s": 0, "bandwidth_mbps": 8, "receive_channels": 1}},
                    {sid: [] for sid in satellites}, neighbors, links, tiles,
                    {"dt": 1, "vram_capacity_gb": 1})


class ReservationTests(unittest.TestCase):
    def test_joint_rejects_partial_image_without_mutating_ledger(self):
        state = contention_state(False)
        for tile in state.tiles.values():
            tile["parent_task_id"] = "image"
        ledger = _Ledger(state)
        before = copy.deepcopy(ledger.__dict__)
        self.assertIsNone(JointReservationPolicy().plan(ledger, state.tiles))
        self.assertEqual(ledger.compute, before["compute"])
        self.assertEqual(ledger.memory, before["memory"])
        for name in ("links", "radios", "stations"):
            self.assertEqual({k: v.channels for k, v in getattr(ledger, name).items()},
                             {k: v.channels for k, v in before[name].items()})
        self.assertEqual([a.action_type for a in JointReservationPolicy().select_actions(state)],
                         [ActionType.WAIT, ActionType.WAIT])
        self.assertEqual(ReservationPolicy().select_actions(state)[0].target_sat_id, 2)

    def test_joint_spreads_siblings_and_preserves_ready_work(self):
        state = contention_state()
        for tile in state.tiles.values():
            tile["parent_task_id"] = "image"
        self.assertEqual([a.target_sat_id for a in JointReservationPolicy().select_actions(state)], [2, 3])
        state.tiles["t0"]["state"] = "READY"
        state.pending_task_tiles = {"image": {"t1": state.tiles["t1"]}}
        actions = {a.tile_id: a for a in JointReservationPolicy().select_actions(state)}
        self.assertEqual(actions["t0"].action_type, ActionType.LOCAL)
        self.assertEqual(actions["t1"].target_sat_id, 2)

    def test_joint_preflights_hidden_siblings_with_public_action_limit(self):
        state = contention_state()
        for sat in state.satellites.values():
            sat["next_gs_windows"]["gs"] = (0, 100)
        tile = state.tiles["t0"]
        state.tiles = {f"tile{i}": {**tile, "parent_task_id": "image"} for i in range(8)}
        state.pending_task_tiles = {"image": dict(state.tiles)}
        limited = decision_state(state)
        policy = JointReservationPolicy()
        actions = policy.select_actions(limited)
        self.assertEqual(len(actions), 4)
        self.assertEqual(policy.stats["planned_tile_rows"], 8)
        self.assertTrue(all(a.action_type != ActionType.WAIT for a in actions))
        # 后半图不可接收时，不能只对可见的前四片接单。
        state.tiles["tile7"]["vram_req_gb"] = 2
        self.assertTrue(all(a.action_type == ActionType.WAIT for a in policy.select_actions(limited)))

    def test_export_keeps_pending_siblings_after_candidate_filter(self):
        env = make_env(execution_commitment=True)
        self.addCleanup(env.close)
        add_task(env, count=8)
        state = decision_state(env.export_state())
        self.assertEqual(len(state.tiles), 4)
        self.assertEqual(sum(map(len, state.pending_task_tiles.values())), 8)

    def test_calendar_preserves_gaps_channels_and_deadline(self):
        calendar = ServiceCalendar(2)
        calendar.reserve((4, 2, 0))
        calendar.reserve((5, 0, 1))
        self.assertEqual(calendar.quote(0, 2, 2), (2, 0, 0))
        self.assertEqual(calendar.quote(1, 2, 6), (6, 4, 0))
        self.assertIsNone(calendar.quote(1, 2, 5))
        self.assertEqual(calendar.channels, [[(2, 4)], [(0, 5)]])

    def test_simultaneous_sources_are_redirected_before_admission(self):
        state = contention_state()
        before = copy.deepcopy(state)
        independent = ReservationPolicy(False).select_actions(state)
        coordinated = ReservationPolicy().select_actions(state)
        self.assertEqual([a.target_sat_id for a in independent], [2, 2])
        self.assertEqual([a.target_sat_id for a in coordinated], [2, 3])
        self.assertEqual(state, before)

    def test_full_window_defers_without_failing_task_and_releases_next_round(self):
        state = contention_state(False)
        policy = ReservationPolicy()
        actions = policy.select_actions(state)
        self.assertEqual(actions[0].target_sat_id, 2)
        self.assertEqual(actions[1].action_type, ActionType.WAIT)
        del state.tiles["t0"]
        self.assertEqual(policy.select_actions(state)[0].target_sat_id, 2)

    def test_ground_capacity_is_shared_across_different_receivers(self):
        state = contention_state()
        for sid in (2, 3):
            state.satellites[sid]["next_gs_windows"]["gs"] = (0, 3)
        for tile in state.tiles.values():
            tile["result_size_mb"] = 1
        actions = ReservationPolicy().select_actions(state)
        self.assertEqual(actions[0].target_sat_id, 2)
        self.assertEqual(actions[1].action_type, ActionType.WAIT)

    def test_memory_and_incoming_compute_are_not_overbooked(self):
        state = contention_state()
        state.satellites[2]["mem_remaining_gb"] = state.tiles["t0"]["data_size_gb"]
        state.satellites[2]["next_gs_windows"]["gs"] = (0, 100)
        actions = ReservationPolicy().select_actions(state)
        self.assertEqual([a.target_sat_id for a in actions], [2, 3])
        state.satellites[2]["committed_compute_s"] = 100
        state.satellites[2]["next_gs_windows"]["gs"] = (0, 3)
        self.assertEqual(ReservationPolicy().select_actions(state)[0].target_sat_id, 3)

    def test_export_counts_inflight_work_and_releases_after_failure(self):
        env = make_env(execution_commitment=True)
        self.addCleanup(env.close)
        first = add_task(env, size=4)[0]
        add_task(env)
        self.assertEqual(env.export_state().satellites[0]["committed_compute_s"], 0)
        env.step([Action(first.tile_id, ActionType.OFFLOAD, target_sat_id=1)])
        state = env.export_state()
        self.assertEqual(state.satellites[1]["committed_compute_s"], 1)
        self.assertEqual(state.satellites[1]["committed_result_mb"], 1)
        from sim.entities import FailureReason
        env._fail_tile(first, FailureReason.DEADLINE_MISS)
        self.assertEqual(env.export_state().satellites[1]["committed_compute_s"], 0)

    def test_competing_tasks_deliver_more_with_reservation_in_real_pipeline(self):
        outcomes = []
        for reserve in (False, True):
            env = make_env(num_sats=4, sim_steps=8, execution_commitment=True,
                           result_relay=False, result_size_mb=0.125,
                           satellite_compute_rates=[0.1, 0.1, 1, 1],
                           ground_relay_delay_s=0, aggregation_delay_s=0)
            self.addCleanup(env.close)
            env.topology.links = {f"{src}-{dst}": Link(src, dst, True, 8, 0)
                                  for src in (0, 1) for dst in (2, 3)}
            visibility = env.vis_cache

            def refresh(t, *args):
                visibility.visible_gs_by_sat = {sid: ["gs"] if sid >= 2 and t < (3 if sid == 2 else 10) else []
                                               for sid in range(4)}
                visibility.best_gs_by_sat = {sid: "gs" if options else None
                                            for sid, options in visibility.visible_gs_by_sat.items()}

            visibility.refresh = refresh
            visibility.windows_at = lambda sid, t: {"gs": (0, (3 if sid == 2 else 10) - t)
                                                    if sid >= 2 and t < (3 if sid == 2 else 10) else None}
            visibility.is_visible = lambda sid, gid, stations: gid in visibility.visible_gs_by_sat.get(sid, [])
            env.contact_cache = SimpleNamespace(windows_at=lambda seconds: {
                key: [(0, 100)] for key in env.topology.links})
            for src in (0, 1):
                add_task(env, src=src, size=0.125)
            policy = ReservationPolicy(reserve)
            for _ in range(8):
                env.step(policy.select_actions(env.export_state()))
            outcomes.append(env.metrics.completed_tasks)
            self.assertEqual(env.metrics.failed_tiles, 0)
        self.assertEqual(outcomes, [1, 2])


if __name__ == "__main__":
    unittest.main()
