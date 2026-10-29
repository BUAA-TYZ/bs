"""Prepare a bounded, group-separated development pilot from the public SAR release."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
from pathlib import Path
import sys
import time
import zipfile

import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from workload_benchmark.sar_data import (
    ARCHIVE_BYTES, CENTRAL_SHA256, EXCLUDED_GROUPS, BoundedRemoteZip,
    check_group_isolation, digest, parse_annotations, provisional_group,
    select_candidates, validate_member, contained_labels,
)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def similarity_audit(records, thumbnails):
    flagged = []
    for mode in ("whole_64", "quadrant_32"):
        matrices, identities = {}, {}
        for split in ("train", "val"):
            vectors, ids = [], []
            for record in records:
                if record["split"] != split:
                    continue
                array = thumbnails[record["image_member"]]
                crops = [array] if mode == "whole_64" else [
                    array[y:y + 32, x:x + 32] for y in (0, 32) for x in (0, 32)]
                for index, crop in enumerate(crops):
                    vector = crop.flatten().astype(np.float32)
                    vector -= vector.mean()
                    norm = np.linalg.norm(vector)
                    if norm <= 1e-8:
                        continue
                    vectors.append(vector / norm)
                    ids.append((record["image_member"], index))
            matrices[split] = np.asarray(vectors)
            identities[split] = ids
        if not all(len(matrices[split]) for split in ("train", "val")):
            continue
        correlations = matrices["train"] @ matrices["val"].T
        for train_index, val_index in np.argwhere(correlations >= 0.995):
            flagged.append({"mode": mode, "train": identities["train"][train_index],
                            "val": identities["val"][val_index],
                            "correlation": float(correlations[train_index, val_index])})
    flagged.sort(key=lambda item: -item["correlation"])
    return {"threshold": 0.995, "flag_count": len(flagged), "top_flags": flagged[:100],
            "interpretation": "Pixel screening only; absence of flags does not prove scene independence."}


def verify_existing(data_root, result_root):
    data_root, result_root = Path(data_root).resolve(), Path(result_root).resolve()
    manifest_path = data_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if digest(Path(manifest["selection_path"]).read_bytes()) != manifest["selection_sha256"]:
        raise ValueError("Frozen selection checksum changed")
    owners = {}
    for record in manifest["records"]:
        for key in ("image", "label", "source_label"):
            if digest((data_root / record[key]).read_bytes()) != record[key + "_sha256"]:
                raise ValueError(f"Manifest checksum mismatch: {record[key]}")
        with Image.open(data_root / record["image"]) as image:
            if image.size != (800, 800):
                raise ValueError("Manifest image geometry changed")
            pixel_hash = digest(np.asarray(image.convert("RGB")).tobytes())
        if "decoded_rgb_sha256" in record and record["decoded_rgb_sha256"] != pixel_hash:
            raise ValueError("Decoded pixel checksum changed")
        if pixel_hash in owners and owners[pixel_hash] != record["split"]:
            raise ValueError("Exact decoded pixels cross development partitions")
        owners[pixel_hash] = record["split"]
        record["decoded_rgb_sha256"] = pixel_hash
    check_group_isolation(manifest["records"])
    manifest["decoded_pixel_geometry"] = [800, 800, 3]
    manifest["decoded_pixel_encoding"] = "uint8 RGB row-major bytes"
    inventory_path = result_root / "annotation_inventory.json"
    if inventory_path.exists():
        manifest["annotation_inventory_path"] = str(inventory_path)
        manifest["annotation_inventory_sha256"] = digest(inventory_path.read_bytes())
    write_json(manifest_path, manifest)
    write_json(result_root / "data_summary.json", {key: value for key, value in manifest.items()
                                                if key not in {"records", "network_requests"}})
    print(f"Verified {len(manifest['records'])} image/label/source hashes and decoded RGB hashes", flush=True)
    return manifest


def prepare(data_root, result_root, audit_cache, tail_path, annotations_only=False):
    started = time.monotonic()
    data_root, result_root = Path(data_root).resolve(), Path(result_root).resolve()
    manifest_path, selection_path = data_root / "manifest.json", result_root / "selection.json"
    if manifest_path.exists() and not annotations_only:
        raise ValueError("Completed pilot exists; do not overwrite its manifest")
    remote = BoundedRemoteZip(data_root / "archive_ranges", audit_cache, tail_path)
    archive = zipfile.ZipFile(remote)
    infos = archive.infolist()
    ordered = sorted(infos, key=lambda item: item.header_offset)
    next_offset = {item.filename: ordered[index + 1].header_offset
                   for index, item in enumerate(ordered[:-1])}
    all_classes, raw_counts, skipped, candidates = set(), Counter(), Counter(), []
    excluded_classes = {}
    label_texts, annotations_by_member = {}, {}
    for source_split in ("train", "test"):
        labels = [item for item in infos if item.filename.startswith(source_split + "_ann/")
                  and item.filename.endswith(".txt")]
        start = min(item.header_offset for item in labels)
        end = max(next_offset[item.filename] for item in labels)
        remote.fetch(start, end - start)
        for info in labels:
            validate_member(info, 256 * 1024)
            label_bytes = archive.read(info)
            try:
                annotations = parse_annotations(label_bytes.decode("utf-8-sig"))
            except (UnicodeError, ValueError):
                skipped["invalid_annotation_images"] += 1
                continue
            all_classes.update(item.class_name for item in annotations)
            raw_counts.update(item.class_name for item in annotations)
            if any(item.difficulty != 0 for item in annotations):
                skipped["difficulty_nonzero_images"] += 1
                excluded_classes.setdefault("difficulty_nonzero_images", Counter()).update(
                    item.class_name for item in annotations)
                continue
            group = provisional_group(info.filename)
            if group in EXCLUDED_GROUPS:
                skipped["previously_inspected_group_images"] += 1
                excluded_classes.setdefault("previously_inspected_group_images", Counter()).update(
                    item.class_name for item in annotations)
                continue
            image_member = source_split + "_png/" + Path(info.filename).stem + ".png"
            image_info = archive.getinfo(image_member)
            validate_member(image_info, 4 * 1024 * 1024)
            label_texts[info.filename] = label_bytes
            annotations_by_member[info.filename] = annotations
            candidates.append({"image_member": image_member, "label_member": info.filename,
                               "provisional_group_id": group,
                               "original_split": source_split,
                               "source_label_sha256": digest(label_bytes)})
    names = sorted(all_classes)
    converted = {}
    usable = []
    for candidate in candidates:
        try:
            labels, counts, stats = contained_labels(annotations_by_member[candidate["label_member"]], names)
        except ValueError:
            skipped["out_of_bounds_annotation_images"] += 1
            excluded_classes.setdefault("out_of_bounds_annotation_images", Counter()).update(
                item.class_name for item in annotations_by_member[candidate["label_member"]])
            continue
        candidate.update(class_counts=counts, conversion_stats=stats)
        converted[candidate["label_member"]] = labels
        usable.append(candidate)
    inventory = {"native_class_counts_all_valid_labels": dict(sorted(raw_counts.items())),
                 "excluded_image_counts": dict(skipped),
                 "excluded_object_counts_by_image_exclusion_reason": {
                     reason: dict(sorted(counts.items())) for reason, counts in excluded_classes.items()},
                 "exclusion_order": ["invalid_annotation_images", "difficulty_nonzero_images",
                                     "previously_inspected_group_images", "out_of_bounds_annotation_images"],
                 "valid_candidate_images": len(usable),
                 "note": "Reasons are mutually exclusive per image in listed order. Classes in invalid rows are not inferred."}
    write_json(result_root / "annotation_inventory.json", inventory)
    if annotations_only:
        if selection_path.exists():
            expected = json.loads(selection_path.read_text())["records"]
            if select_candidates(usable, names) != expected:
                raise ValueError("Frozen selection cannot be reproduced from current source labels")
            print("Frozen selection reproduced from source labels", flush=True)
        print(json.dumps(inventory, indent=2), flush=True)
        return inventory
    selected = select_candidates(usable, names)
    if len(selected) > 192:
        raise ValueError("Pilot image cap exceeded")
    selection = {
        "seed": 1701, "targets": {"train": 128, "val": 64},
        "partition": "SHA256(seed:provisional_group_id) modulo 5; zero is development validation",
        "selection": "Deterministic inverse-frequency uncovered-class coverage, then hash order; one image/group first, at most two",
        "excluded_groups": sorted(EXCLUDED_GROUPS), "names": names,
        "native_class_counts_all_valid_labels": dict(sorted(raw_counts.items())),
        "skipped_images": dict(skipped), "valid_candidate_images": len(usable),
        "source_scene_identity_verified": False, "final_test_created": False,
        "conversion": "Original four vertices normalized by 800; entire image excluded if any target is out of bounds, invalid or difficult. Selection biased to fully contained objects; no border-quality claim.",
        "download_limit_bytes": 300 * 1024 * 1024, "max_images": 192,
        "max_image_bytes": 4 * 1024 * 1024, "records": selected,
    }
    if selection_path.exists():
        if json.loads(selection_path.read_text()) != selection:
            raise ValueError("Existing selection differs; refusing retrospective reselection")
    else:
        write_json(selection_path, selection)
    print(f"Selected {len(selected)} images; native classes={len(names)}; labels read={remote.downloaded} bytes", flush=True)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for candidate in selected:
            info = archive.getinfo(candidate["image_member"])
            end = next_offset.get(info.filename, 27_927_051_260)
            length = end - info.header_offset
            if length > 4 * 1024 * 1024 + 1024:
                raise ValueError("Unexpected PNG archive span")
            futures.append(executor.submit(remote.fetch, info.header_offset, length))
        for index, future in enumerate(as_completed(futures), 1):
            future.result()
            if index % 32 == 0 or index == len(futures):
                print(f"PNG ranges ready {index}/{len(futures)}; network={remote.downloaded / 1024 ** 2:.1f} MiB", flush=True)
    records, thumbnails, owners = [], {}, {}
    for candidate in selected:
        image_data = archive.read(candidate["image_member"])
        image_hash = digest(image_data)
        if image_hash in owners and owners[image_hash] != candidate["split"]:
            raise ValueError("Exact duplicate image bytes cross development partitions")
        owners[image_hash] = candidate["split"]
        with Image.open(io.BytesIO(image_data)) as image:
            image.load()
            if image.size != (800, 800) or image.format != "PNG":
                raise ValueError("Unexpected SAR image geometry or format")
            thumbnails[candidate["image_member"]] = np.asarray(
                image.convert("L").resize((64, 64), Image.Resampling.BILINEAR))
            mode = image.mode
            decoded_rgb_sha256 = digest(np.asarray(image.convert("RGB")).tobytes())
        basename = candidate["original_split"] + "_" + Path(candidate["image_member"]).stem
        image_path = data_root / "images" / candidate["split"] / (basename + ".png")
        label_path = data_root / "labels" / candidate["split"] / (basename + ".txt")
        source_path = data_root / "source_labels" / candidate["split"] / (basename + ".txt")
        for path in (image_path, label_path, source_path):
            path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(image_data)
        label_path.write_text(converted[candidate["label_member"]])
        source_path.write_bytes(label_texts[candidate["label_member"]])
        records.append({**candidate, "image": str(image_path.relative_to(data_root)),
                        "label": str(label_path.relative_to(data_root)),
                        "source_label": str(source_path.relative_to(data_root)),
                        "image_sha256": image_hash, "image_bytes": len(image_data),
                        "label_sha256": digest(label_path.read_bytes()), "image_mode": mode,
                        "decoded_rgb_sha256": decoded_rgb_sha256})
    check_group_isolation(records)
    counts = {split: dict(sum((Counter(item["class_counts"]) for item in records
                              if item["split"] == split), Counter())) for split in ("train", "val")}
    summary = {"image_counts": dict(Counter(item["split"] for item in records)),
               "group_counts": {split: len({item["provisional_group_id"] for item in records
                                            if item["split"] == split}) for split in ("train", "val")},
               "class_counts": counts,
               "missing_classes": {split: sorted(set(names) - set(counts[split])) for split in ("train", "val")},
               "conversion_stats": dict(sum((Counter(item["conversion_stats"]) for item in records), Counter())),
               "cross_split_similarity": similarity_audit(records, thumbnails)}
    dataset = {"path": str(data_root), "train": "images/train", "val": "images/val",
               "names": dict(enumerate(names))}
    write_json(data_root / "dataset.yaml", dataset)
    manifest = {"source_repository": "https://github.com/gisbingxin/SDFSD-v1.0",
                "archive_file_id": "16gKlS9-CLuTIEUJpel3fbslsTI8uCVhG", "archive_bytes": ARCHIVE_BYTES,
                "central_directory_sha256": CENTRAL_SHA256, "source_scene_identity_verified": False,
                "final_test_created": False, "selection_path": str(selection_path),
                "selection_sha256": digest(selection_path.read_bytes()), "names": names,
                "summary": summary, "network_bytes_this_run": remote.downloaded,
                "network_requests": remote.requests, "elapsed_seconds": time.monotonic() - started,
                "records": records}
    write_json(manifest_path, manifest)
    write_json(result_root / "data_summary.json", {key: value for key, value in manifest.items()
                                                if key not in {"records", "network_requests"}})
    print(json.dumps(summary, indent=2), flush=True)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data/workload_assets/sar_pilot_20261015")
    parser.add_argument("--result-root", default="results/sar_pilot_20261015")
    parser.add_argument("--audit-cache", default="/tmp/sdfsd-audit")
    parser.add_argument("--tail-path", default="/tmp/sdfsd-archive-tail.bin")
    parser.add_argument("--annotations-only", action="store_true")
    parser.add_argument("--verify-existing", action="store_true")
    args = parser.parse_args()
    if args.verify_existing:
        verify_existing(args.data_root, args.result_root)
    else:
        prepare(args.data_root, args.result_root, args.audit_cache, args.tail_path, args.annotations_only)


if __name__ == "__main__":
    main()
