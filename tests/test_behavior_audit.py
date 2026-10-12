import unittest

from audit_policy_behavior import summarize_events


class BehaviorAuditTests(unittest.TestCase):
    def test_attempts_do_not_count_as_completed_cooperation(self):
        events = [
            {"event": "tx_start", "task_id": "failed", "tile_id": "f", "sat_to": 1},
            {"event": "tx_end", "task_id": "pending", "tile_id": "p", "sat_to": 1},
            {"event": "computed", "task_id": "pending", "tile_id": "p", "sat_to": 1},
            {"event": "computed", "task_id": "local", "tile_id": "l", "sat_to": 0},
            {"event": "tx_end", "task_id": "remote", "tile_id": "r", "sat_to": 1},
            {"event": "computed", "task_id": "remote", "tile_id": "r", "sat_to": 1},
            {"event": "ground_compute_end", "task_id": "ground", "tile_id": "g", "sat_to": "gs"},
        ]
        result = summarize_events(events, {k: 0 for k in ("failed", "pending", "local", "remote", "ground")},
                                  ["local", "remote", "ground"])
        self.assertEqual(result["computed_unique_tiles"],
                         {"source_satellite": 1, "remote_satellite": 2, "ground": 1})
        self.assertEqual(result["completed_tasks_with_finished_isl_hop"], ["remote"])
        self.assertEqual(result["completed_tasks_with_remote_satellite_compute"], ["remote"])
        self.assertEqual(result["completed_tasks_with_ground_compute"], ["ground"])
