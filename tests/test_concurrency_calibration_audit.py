import json
from pathlib import Path
import tempfile
import unittest

from analysis.finalize_jetson_concurrency import audit, timed_swap


class ConcurrencyAuditTests(unittest.TestCase):
    def fixture(self, root, *, changed_output=False, slow_round=False, swap=False):
        runs = []
        for mode, size in (("batch1", 1), ("streams2", 2)):
            for repeat in range(1, 4):
                name = f"round{repeat}_{mode}"
                directory = root / name
                directory.mkdir()
                seconds = 0.6 if size == 1 else (1.19 if slow_round and repeat == 3 else 1.0)
                row = {"cohort_s": seconds, "windows": size}
                report = {"status": "passed", "summary": {"windows": 64,
                          "windows_per_s": size / seconds, "full_64_window_service_s": 64 * seconds / size},
                          "cohort_size": size, "rows": [row] * (64 // size),
                          "memory": {"min_available_bytes": 10e9},
                          "cuda_memory": {"peak_reserved_bytes": 6e9},
                          "memory_before_measurement": {"pswpin": 3, "pswpout": 50},
                          "memory_samples": [{"pswpin": 3, "pswpout": 51 if swap and size == 2 else 50}],
                          "output_hashes": {"0": "different" if changed_output and size == 2 else "same"}}
                report.update({key: "same" for key in
                               ("weights_sha256", "fixture_sha256", "raster_sha256", "backend_flags", "versions")})
                (directory / "report.json").write_text(json.dumps(report))
                runs.append({"name": name, "mode": mode, "round": repeat, "returncode": 0})
        (root / "series.json").write_text(json.dumps({"status": "passed", "runs": runs}))

    def test_selects_repeated_gain_and_rejects_single_round_regression_or_swap(self):
        for values, expected in (({}, "streams2"), ({"slow_round": True}, "batch1"),
                                 ({"swap": True}, "batch1")):
            with self.subTest(values=values), tempfile.TemporaryDirectory() as directory:
                self.fixture(Path(directory), **values)
                summary, _ = audit(Path(directory))
                self.assertEqual(summary["selected_mode"], expected)

    def test_changed_results_cannot_be_compared_as_acceleration(self):
        with tempfile.TemporaryDirectory() as directory:
            self.fixture(Path(directory), changed_output=True)
            with self.assertRaisesRegex(ValueError, "output changed"):
                audit(Path(directory))

    def test_startup_swap_is_distinct_from_timed_service_swap(self):
        self.assertEqual(timed_swap({"memory_before_measurement": {"pswpin": 2, "pswpout": 12},
                                    "memory_samples": [{"pswpin": 2, "pswpout": 12}]}),
                         {"pswpin": 0, "pswpout": 0})


if __name__ == "__main__":
    unittest.main()
