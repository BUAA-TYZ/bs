import json
import unittest

from workload_benchmark.split_audit import audit_split


def clean_records():
    return [
        {"sample_id": "a-1", "source_scene_id": "scene-a", "split": "train", "sha256": "a" * 64},
        {"sample_id": "a-2", "source_scene_id": "scene-a", "split": "train", "sha256": "b" * 64},
        {"sample_id": "b-1", "source_scene_id": "scene-b", "split": "validation", "sha256": "c" * 64},
        {"sample_id": "c-1", "source_scene_id": "scene-c", "split": "test", "sha256": "d" * 64},
    ]


class WorkloadSplitAuditTests(unittest.TestCase):
    def test_separate_scenes_allow_multiple_crops_within_training(self):
        report = audit_split(clean_records())
        self.assertTrue(report["eligible"])
        self.assertEqual(report["violations"], [])
        self.assertEqual(report["counts"]["splits"]["train"], {"sample_count": 2, "scene_count": 1})
        self.assertEqual(report["counts"]["record_count"], 4)
        self.assertIn("not verified", report["scope"])
        self.assertEqual(json.loads(json.dumps(report, allow_nan=False)), report)

    def test_distinct_overlapping_crops_cannot_cross_scene_split(self):
        records = clean_records()
        records[3]["source_scene_id"] = "scene-a"
        report = audit_split(records)
        self.assertFalse(report["eligible"])
        violation = next(v for v in report["violations"] if v["code"] == "scene_crosses_splits")
        self.assertEqual(violation["record_indices"], [0, 1, 3])
        self.assertEqual(violation["splits"], ["train", "test"])

    def test_identical_content_with_different_scene_names_is_leakage(self):
        records = clean_records()
        records[3]["sha256"] = records[0]["sha256"].upper()
        report = audit_split(records)
        self.assertFalse(report["eligible"])
        violation = next(v for v in report["violations"] if v["code"] == "content_crosses_splits")
        self.assertEqual(violation["record_indices"], [0, 3])
        self.assertNotIn("scene_crosses_splits", [v["code"] for v in report["violations"]])

    def test_missing_provenance_is_not_reconstructed_from_sample_name(self):
        records = clean_records()
        records[0]["sample_id"] = "scene-a_crop_001.png"
        del records[0]["source_scene_id"]
        records[1]["sha256"] = " "
        records[2]["sample_id"] = None
        report = audit_split(records)
        self.assertFalse(report["eligible"])
        invalid_fields = {v["field"] for v in report["violations"] if v["code"] == "invalid_field"}
        self.assertEqual(invalid_fields, {"source_scene_id", "sha256", "sample_id"})

    def test_duplicate_sample_id_rejected_even_with_whitespace(self):
        records = clean_records()
        records[1]["sample_id"] = " a-1 "
        report = audit_split(records)
        self.assertFalse(report["eligible"])
        violation = next(v for v in report["violations"] if v["code"] == "duplicate_sample_id")
        self.assertEqual(violation["record_indices"], [0, 1])

    def test_invalid_split_does_not_substitute_for_validation(self):
        records = clean_records()
        records[2]["split"] = "val"
        report = audit_split(records)
        self.assertFalse(report["eligible"])
        self.assertIn("invalid_split", [v["code"] for v in report["violations"]])
        self.assertEqual([v["split"] for v in report["violations"] if v["code"] == "missing_split"],
                         ["validation"])

    def test_empty_or_unstructured_input_never_becomes_eligible(self):
        for records in ([], None, {"train": []}, [None], ["sample.png"]):
            with self.subTest(records=records):
                report = audit_split(records)
                self.assertFalse(report["eligible"])
                self.assertTrue(report["violations"])
                json.dumps(report, allow_nan=False)

    def test_missing_any_required_split_prevents_readiness(self):
        for split in ("train", "validation", "test"):
            with self.subTest(split=split):
                report = audit_split([r for r in clean_records() if r["split"] != split])
                self.assertFalse(report["eligible"])
                self.assertEqual([v["split"] for v in report["violations"] if v["code"] == "missing_split"],
                                 [split])

    def test_non_sha256_digest_is_rejected(self):
        for digest in ("a" * 63, "z" * 64, "sha256:" + "a" * 64):
            with self.subTest(digest=digest):
                records = clean_records()
                records[0]["sha256"] = digest
                report = audit_split(records)
                self.assertFalse(report["eligible"])
                self.assertIn("invalid_sha256", [v["code"] for v in report["violations"]])


if __name__ == "__main__":
    unittest.main()
