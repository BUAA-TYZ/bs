from pathlib import Path
import tempfile
import threading
import time
import unittest

from workload_benchmark.sar_data import (
    BoundedRemoteZip, assigned_split, check_group_isolation,
    parse_annotations, provisional_group, select_candidates, contained_labels,
)


class SarPilotDataTests(unittest.TestCase):
    def test_outside_vertices_exclude_whole_image(self):
        annotations = parse_annotations("-20 10 40 10 40 50 -20 50 unknow 0")
        with self.assertRaises(ValueError):
            contained_labels(annotations, ["unknow"])

    def test_invalid_and_difficult_annotations_rejected(self):
        for row in ("nan 0 1 0 1 1 0 1 x 0", "0 0 1 1 0 1 1 0 x 0", "0 0 1"):
            with self.assertRaises(ValueError):
                parse_annotations(row)
        with self.assertRaises(ValueError):
            contained_labels(parse_annotations("0 0 1 0 1 1 0 1 x 1"), ["x"])

    def test_selection_deterministic_and_group_isolated(self):
        candidates = [{"label_member": f"train_ann/G{i:03}__{j}__.txt",
                       "provisional_group_id": f"G{i:03}",
                       "class_counts": {"unknow" if i % 2 else "Cargo-Ship": 1}}
                      for i in range(40) for j in range(2)]
        first = select_candidates(candidates, ["Cargo-Ship", "unknow"], {"train": 12, "val": 6})
        second = select_candidates(list(reversed(candidates)), ["Cargo-Ship", "unknow"], {"train": 12, "val": 6})
        self.assertEqual(first, second)
        self.assertEqual(len(first), 18)
        self.assertTrue(all(item["split"] == assigned_split(item["provisional_group_id"]) for item in first))
        check_group_isolation(first)
        self.assertEqual(provisional_group("test_png/G001__2__.png"), "G001")
        with self.assertRaises(ValueError):
            check_group_isolation([{"provisional_group_id": "x", "split": "train"},
                                   {"provisional_group_id": "x", "split": "val"}])

    def test_native_unknown_token_retained(self):
        annotations = parse_annotations("0 0 40 0 40 50 0 50 unknow 0")
        text, counts, _ = contained_labels(annotations, ["Cargo-Ship", "unknow"])
        self.assertTrue(text.startswith("1 "))
        self.assertEqual(counts, {"unknow": 1})

    def test_budget_rejected_before_network(self):
        with tempfile.TemporaryDirectory() as temporary:
            reader = object.__new__(BoundedRemoteZip)
            reader.memory, reader.ranges = [], []
            reader.lock = threading.Lock()
            reader.cache_root = Path(temporary)
            reader.reserved, reader.budget, reader.max_request = 0, 10, 10
            reader.deadline = time.monotonic() + 10
            reader.opener = lambda *args, **kwargs: self.fail("Network must not be called")
            with self.assertRaises(ValueError):
                reader.fetch(0, 11)


if __name__ == "__main__":
    unittest.main()
