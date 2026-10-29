from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import torch

from sim.config import load_config
from sim.env import SimulationEnv
from sim.cache.visibility import VisibilityCache
from sim.marl.actor import SatActorCritic
from sim.marl.decision import build_batch
from sim.marl.features import build_features, feature_dim
from sim.scheduler.predictive import PredictivePolicy
from sim.entities import Action, ActionType, TileState
from sim.topology import bounded_edges
from test_simulation import make_env, add_task, FakeTopology, FakeVisibility
import train_marl


class PredictiveTests(unittest.TestCase):
    def test_precomputed_visibility_preserves_sample_grid_and_windows(self):
        cfg = load_config("examples/config_predictive.yaml")
        env = SimulationEnv(cfg)
        self.addCleanup(env.close)
        cached = VisibilityCache(cfg.num_sats, 1, 1800, dt=cfg.dt)
        cached.precompute(env.topology.sat_recs, env.topology.t0, env.ground_station_objs, cfg.sim_steps)
        for t in (0, 1, 2, 5, 35, 79, 127, 211, 500, 719, 1079):
            for cache in (env.vis_cache, cached):
                cache.refresh(t, env.topology.sat_recs, env.topology.t0, env.ground_station_objs, env.ground_stations)
            self.assertEqual(env.vis_cache.visible_gs_by_sat, cached.visible_gs_by_sat)
            self.assertEqual(env.vis_cache.window_cache, cached.window_cache)
            for key in env.vis_cache._geom_cache:
                np.testing.assert_allclose(env.vis_cache._geom_cache[key], cached._geom_cache[key], rtol=1e-9, atol=1e-7)

    def test_sparse_precompute_matches_full_grid_and_skips_unread_steps(self):
        # 细步长下只传播 refresh 步与 30 s 扫描步，取值须与逐步全网格逐位相同。
        cfg = replace(load_config("examples/config_predictive.yaml"), dt=0.5, sim_steps=400,
                      ground_visibility_update_steps=20, task_arrival_end_step=None)
        env = SimulationEnv(cfg)
        self.addCleanup(env.close)
        sparse = VisibilityCache(cfg.num_sats, 20, 1800, dt=0.5)
        sparse.precompute(env.topology.sat_recs, env.topology.t0, env.ground_station_objs, 400, chunk_size=7)
        read = np.flatnonzero(sparse._precomputed_index >= 0)
        self.assertLess(len(read), len(sparse._precomputed_index) // 5)
        from sim.orbit import time_grid
        times = time_grid(env.topology.t0, np.arange(len(sparse._precomputed_index)) * 0.5)
        sat, (gs_id, station) = env.topology.sat_recs[0], next(iter(env.ground_station_objs.items()))
        altitude, _, distance = (sat - station).at(times).altaz()
        rows = sparse._precomputed_index[read]
        np.testing.assert_array_equal(sparse._precomputed[0, gs_id][0][rows], altitude.degrees[read])
        np.testing.assert_array_equal(sparse._precomputed[0, gs_id][1][rows], distance.km[read])
        for t in (0, 19, 20, 133, 399):
            sparse.refresh(t, env.topology.sat_recs, env.topology.t0, env.ground_station_objs, env.ground_stations)

    def test_execution_commitment_blocks_return_hop_even_without_mask(self):
        for committed in (False, True):
            with self.subTest(committed=committed):
                env = make_env(execution_commitment=committed)
                self.addCleanup(env.close)
                tile = add_task(env)[0]
                env.step([Action(tile.tile_id, ActionType.OFFLOAD, target_sat_id=1)])
                self.assertEqual(tile.state, TileState.READY)
                self.assertEqual(tile.tile_id in env.export_state().tiles, not committed)
                env.step([Action(tile.tile_id, ActionType.OFFLOAD, target_sat_id=0)])
                self.assertEqual(tile.location, 1 if committed else 0)
                if committed:
                    self.assertEqual(tile.state, TileState.COMPUTED)
                    env.step([])
                    self.assertEqual(env.metrics.completed_tasks, 1)

    def test_degree_limit_checks_both_ends_and_rejects_invisible(self):
        pairs = [(0, 1), (0, 2), (1, 2), (2, 3)]
        geometry = [(True, 1), (True, 2), (True, 3), (False, 0)]
        self.assertEqual(bounded_edges(pairs, geometry, 1), {(0, 1)})
        self.assertEqual(bounded_edges(pairs, geometry, 0), {(0, 1), (0, 2), (1, 2)})

    def test_contacts_match_geometry_and_do_not_consume_bandwidth_rng(self):
        cfg = load_config("examples/config_predictive.yaml")
        cfg = replace(cfg, num_sats=8, topology={**cfg.topology, "window_lookahead_s": 120})
        env = SimulationEnv(cfg)
        self.addCleanup(env.close)
        before = env.topology.rng.getstate()
        windows = env.contact_cache.windows_at(0)
        self.assertEqual(before, env.topology.rng.getstate())
        links = env.topology.snapshot(0, cfg.dt)
        degree = [0] * cfg.num_sats
        for key, link in links.items():
            predicted = any(start == 0 for start, _ in windows[key])
            self.assertEqual(predicted, link.up)
            if link.up:
                degree[link.i] += 1
                degree[link.j] += 1
        self.assertLessEqual(max(degree), 4)
        self.assertEqual(windows, env.contact_cache.windows_at(0))

    def test_predictive_shape_ablation_and_ready_are_observable(self):
        env = make_env()
        self.addCleanup(env.close)
        add_task(env)
        state = env.export_state()
        state.links["0-1"]["contact_windows"] = [(0, 10), (30, 20)]
        state.satellites[1]["next_gs_windows"] = {"gs": (15, 45)}
        first = build_features(state, 0, "contact_window")
        off = build_features(state, 0, "contact_window", window_features=False)
        state.links["0-1"]["contact_windows"] = [(0, 50)]
        state.satellites[1]["next_gs_windows"] = {"gs": (35, 25)}
        np.testing.assert_array_equal(off, build_features(state, 0, "contact_window", window_features=False))
        self.assertFalse(np.array_equal(first, build_features(state, 0, "contact_window")))
        tid = next(iter(state.tiles))
        state.tiles[tid]["state"] = "READY"
        self.assertEqual(build_features(state, 0, "contact_window")[-4], 1)
        obs, tiles, masks, _ = build_batch(state, feature_set="contact_window")
        model = SatActorCritic(num_gs=1, hidden=16, feature_set="contact_window", encoder="task_attention")
        logits, value = model(torch.tensor(obs))
        self.assertEqual(obs.shape, (2, feature_dim(1, "contact_window")))
        self.assertEqual(logits.shape, (2, 4, 11))
        self.assertTrue(torch.isfinite(logits).all())
        loss = model.get_action_and_value(torch.tensor(obs), torch.tensor(tiles), action_mask=torch.tensor(masks))[1].sum()
        loss.backward()
        self.assertIsNotNone(model.task_actor.neighbor_encoder.weight.grad)

    def test_teacher_rejects_short_contact_and_preserves_ready(self):
        env = make_env()
        self.addCleanup(env.close)
        tile = add_task(env)[0]
        state = env.export_state()
        state.satellites[0]["next_gs_windows"] = {"gs": None}
        state.satellites[1]["next_gs_windows"] = {"gs": (5, 30)}
        state.ground_options[0] = []
        state.links["0-1"]["contact_windows"] = [(0, 0.1)]
        self.assertIsNone(PredictivePolicy().select_actions(state)[0].target_sat_id)
        state.links["0-1"]["contact_windows"] = [(0, 30)]
        self.assertEqual(PredictivePolicy().select_actions(state)[0].target_sat_id, 1)
        state.tiles[tile.tile_id]["state"] = "READY"
        self.assertIsNone(PredictivePolicy().select_actions(state)[0].target_sat_id)

    def test_pooled_critic_is_permutation_equivariant(self):
        obs = torch.randn(5, 7)
        perm = torch.tensor([3, 1, 4, 0, 2])
        torch.testing.assert_close(train_marl.critic_observations(obs, "mappo", pooling=True)[perm],
                                   train_marl.critic_observations(obs[perm], "mappo", pooling=True))

    def test_resumed_training_matches_uninterrupted_parameters(self):
        env = make_env(sim_steps=5, task_arrival_rate=0.7)
        self.addCleanup(env.close)
        with tempfile.TemporaryDirectory() as tmp:
            def args(directory, count, resume=False):
                argv = ["--config", "examples/config.yaml", "--out-dir", str(Path(tmp) / directory),
                        "--algorithm", "mappo", "--critic-pooling", "--episodes", str(count),
                        "--eval-every", "1", "--hidden", "8", "--rollout-steps", "5",
                        "--minibatch", "16", "--ppo-epochs", "1", "--validation-seeds", "10001"]
                return train_marl.parser().parse_args(argv + (["--resume"] if resume else []))
            with patch("train_marl.load_config", return_value=env.cfg), \
                    patch("sim.env.TopologyModel", FakeTopology), patch("sim.env.VisibilityCache", FakeVisibility), \
                    patch("builtins.print"):
                train_marl.train(args("whole", 2))
                train_marl.train(args("resumed", 1))
                train_marl.train(args("resumed", 2, True))
            whole = torch.load(Path(tmp) / "whole/last.pt", weights_only=True)
            resumed = torch.load(Path(tmp) / "resumed/last.pt", weights_only=True)
            for key in whole["model_state"]:
                torch.testing.assert_close(whole["model_state"][key], resumed["model_state"][key], rtol=0, atol=0)


if __name__ == "__main__":
    unittest.main()
