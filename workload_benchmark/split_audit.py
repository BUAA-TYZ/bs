"""Audit explicit sample provenance before offline training or evaluation."""

from collections import defaultdict
from collections.abc import Mapping
import re


SPLITS = ("train", "validation", "test")
FIELDS = ("sample_id", "source_scene_id", "split", "sha256")


def audit_split(records):
    """Return a JSON-safe eligibility report for a list of sample records.

    IDs are trimmed and SHA256 values are case-normalized. Scene IDs must come
    from explicit provenance, never an inferred filename convention. Counts
    include all records with a recognized split, even if other fields fail.
    Passing proves only scene/content separation in the supplied metadata;
    geography, capture time, ship identity and metadata authenticity are unaudited.
    """
    violations = []
    counts = {split: {"sample_count": 0, "scene_count": 0} for split in SPLITS}
    report = {
        "eligible": False,
        "violations": violations,
        "counts": {"record_count": 0, "splits": counts},
        "scope": "Supplied scene/content metadata only; geographical, temporal, "
                 "ship-identity isolation and provenance authenticity are not verified.",
    }
    if not isinstance(records, list):
        violations.append({"code": "invalid_input", "message": "Provide a list of sample records."})
        return report
    report["counts"]["record_count"] = len(records)
    if not records:
        violations.append({"code": "empty_records", "message": "Provide nonempty sample provenance."})

    sample_indices = defaultdict(list)
    scenes = defaultdict(lambda: defaultdict(list))
    contents = defaultdict(lambda: defaultdict(list))
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            violations.append({"code": "invalid_record", "record_indices": [index],
                               "message": "Each sample must be a mapping with explicit provenance."})
            continue
        values = {}
        for field in FIELDS:
            value = record.get(field)
            if not isinstance(value, str) or not value.strip():
                violations.append({"code": "invalid_field", "record_indices": [index],
                                   "field": field, "message": f"Supply a nonempty string for {field}."})
            else:
                values[field] = value.strip()

        sample_id = values.get("sample_id")
        if sample_id is not None:
            sample_indices[sample_id].append(index)
        digest = values.get("sha256")
        if digest is not None:
            if re.fullmatch(r"[0-9a-fA-F]{64}", digest) is None:
                violations.append({"code": "invalid_sha256", "record_indices": [index],
                                   "message": "Supply the 64 hexadecimal characters of the sample SHA256."})
                digest = None
            else:
                digest = digest.lower()

        split = values.get("split")
        if split not in SPLITS:
            if split is not None:
                violations.append({"code": "invalid_split", "record_indices": [index],
                                   "message": "Use exactly train, validation or test as the split."})
            continue
        counts[split]["sample_count"] += 1
        scene = values.get("source_scene_id")
        if scene is not None:
            scenes[scene][split].append(index)
        if digest is not None:
            contents[digest][split].append(index)

    for sample_id, indices in sorted(sample_indices.items()):
        if len(indices) > 1:
            violations.append({"code": "duplicate_sample_id", "sample_id": sample_id,
                               "record_indices": indices,
                               "message": "Assign one unique sample ID to each sample; remove repeated records."})
    for field, groups, code in (
        ("source_scene_id", scenes, "scene_crosses_splits"),
        ("sha256", contents, "content_crosses_splits"),
    ):
        for value, by_split in sorted(groups.items()):
            if len(by_split) > 1:
                violations.append({"code": code, field: value,
                                   "splits": [split for split in SPLITS if split in by_split],
                                   "record_indices": sorted(i for group in by_split.values() for i in group),
                                   "message": "Keep all samples from this scene or identical content in one split."})
    for split in SPLITS:
        counts[split]["scene_count"] = sum(split in by_split for by_split in scenes.values())
        if counts[split]["sample_count"] == 0:
            violations.append({"code": "missing_split", "split": split,
                               "message": f"Provide a nonempty {split} split before training/evaluation readiness."})
    report["eligible"] = not violations
    return report
