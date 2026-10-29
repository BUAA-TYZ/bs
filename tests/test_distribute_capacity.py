import unittest

from analysis.distribute_capacity import portable, validate_assignment


class DistributedCapacityTests(unittest.TestCase):
    def test_assignment_partitions_jobs_without_repeating_completed(self):
        validate_assignment({"a": {}, "b": {}, "c": {}}, {"mac": ["a"], "remote": ["b"]}, {"c": {}})


    def test_assignment_rejects_duplicate_missing_or_reused_jobs(self):
        for assignments, reused in [
            ({"one": ["a"], "two": ["a", "b"]}, {}),
            ({"one": ["a"]}, {}),
            ({"one": ["a", "b"]}, {"a": {}}),
            ({"one": ["a", "b", "unknown"]}, {}),
        ]:
            with self.subTest(assignments=assignments, reused=reused):
                with self.assertRaises(AssertionError):
                    validate_assignment({"a": {}, "b": {}}, assignments, reused)


    def test_portable_hashes_normalize_only_checkout_prefix(self):
        self.assertEqual(portable({"/host/project/analysis/run.py": "abc", "sim/env.py": "def"}), {
            "analysis/run.py": "abc", "sim/env.py": "def"})
        with self.assertRaises(ValueError):
            portable({"/first/analysis/run.py": "a", "/second/analysis/run.py": "a"})
