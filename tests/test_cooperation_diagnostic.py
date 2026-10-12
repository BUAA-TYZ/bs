from pathlib import Path
import tempfile
import unittest

from diagnose_cooperation import (compare_runs, inject_synthetic_task, run_diagnostic,
                                 synthetic_config, synthetic_env)
from sim.config import load_config
from analyze_cooperation_results import hop_churn, local_stages


class CooperationDiagnosticTests(unittest.TestCase):
    def test_feasible_cooperation_finishes_whole_task(self):
        cfg = synthetic_config(load_config("examples/config_training.yaml"), 8)
        with tempfile.TemporaryDirectory() as directory:
            runs = [run_diagnostic(cfg, label, Path(directory) / label, synthetic_env, inject_synthetic_task)
                    for label in ("local", "committed_window")]
        self.assertEqual(runs[0]["metrics"]["overall"]["completed_tasks"], 0)
        self.assertEqual(runs[1]["metrics"]["overall"]["completed_tasks"], 1)
        self.assertEqual(runs[1]["computed_unique_tiles"]["remote_satellite"], 2)
        self.assertEqual(runs[1]["metrics"]["network"]["sent_mb"]["isl:0->1"], 4)
        self.assertEqual(compare_runs(runs)[0]["gained_vs_local"], ["witness"])

    def test_insufficient_bandwidth_does_not_create_false_completion(self):
        cfg = synthetic_config(load_config("examples/config_training.yaml"), 0.8)
        with tempfile.TemporaryDirectory() as directory:
            run = run_diagnostic(cfg, "committed_window", Path(directory) / "slow", synthetic_env, inject_synthetic_task)
        self.assertEqual(run["metrics"]["overall"]["completed_tasks"], 0)
        self.assertEqual(run["plans"]["witness"]["target"], 0)
        self.assertEqual(run["completed_tasks_with_finished_isl_hop"], [])

    def test_pending_wait_is_censored_and_failed_tile_excluded(self):
        times = {"created": 0, "start_compute": 1, "end_compute": 3, "end_downlink": None}
        run = {"metrics": {"experiment": {"config": {"dt": 10, "sim_steps": 20}}},
               "tiles": {"pending": {"state": "COMPUTED", "timestamps_steps": times},
                         "failed": {"state": "FAILED", "timestamps_steps": times}}}
        stages = local_stages(run)
        self.assertEqual(stages["pending_postcompute"], [170])
        self.assertEqual(stages["completed_postcompute"], [])

    def test_only_finished_hops_count_as_reversals(self):
        events = [{"event": kind, "tile_id": "x", "sat_from": src, "sat_to": dst}
                  for kind, src, dst in [("tx_end", 0, 1), ("tx_start", 1, 0),
                                         ("tx_end", 1, 2), ("tx_end", 2, 1)]]
        stats = hop_churn(events)
        self.assertEqual(stats["completed_isl_hops"], 3)
        self.assertEqual(stats["immediate_reverse_hops"], 1)
