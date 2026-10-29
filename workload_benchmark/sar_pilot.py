"""公开 SAR 小样本的离线训练、开发集评估和本机服务时间测量。"""

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import time

import numpy as np
from PIL import Image
import torch
import yaml

from workload_benchmark.postprocess import Detection, serialize_detections


TRAINING = {
    "epochs": 10, "imgsz": 832, "batch": 8, "device": "mps", "workers": 0,
    "seed": 1701, "deterministic": True, "optimizer": "AdamW", "lr0": 0.001,
    "weight_decay": 0.0005, "cache": False, "amp": False, "patience": 0,
    "close_mosaic": 0, "mosaic": 0.0, "mixup": 0.0, "cutmix": 0.0,
    "hsv_h": 0.0, "hsv_s": 0.0, "hsv_v": 0.0, "degrees": 0.0,
    "translate": 0.0, "scale": 0.0, "shear": 0.0, "perspective": 0.0,
    "flipud": 0.0, "fliplr": 0.5, "bgr": 0.0, "copy_paste": 0.0,
    "auto_augment": None, "erasing": 0.0, "multi_scale": False,
    "plots": False, "save": True, "save_period": -1, "val": True,
    "verbose": False, "fraction": 1.0, "compile": False,
}
VALIDATION = {
    "imgsz": 832, "batch": 8, "device": "mps", "workers": 0,
    "conf": 0.001, "iou": 0.7, "max_det": 300, "half": False,
    "plots": False, "save_json": False, "save_txt": False, "verbose": False,
    "split": "val", "augment": False,
}
MAX_TRAIN_SECONDS = 20 * 60


def training_options(epochs):
    if epochs not in (10, 40):
        raise ValueError("Only the fixed 10-epoch pilot or 40-epoch diagnostic is supported")
    return {**TRAINING, "epochs": epochs}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")


def class_predictors(model):
    head = model.model[-1]
    for branch_name in ("cv3", "one2one_cv3"):
        if not hasattr(head, branch_name):
            continue
        for index, branch in enumerate(getattr(head, branch_name)):
            predictor = branch[-1]
            if not isinstance(predictor, torch.nn.Conv2d) or predictor.out_channels != head.nc:
                raise ValueError(f"Unexpected classification predictor: {branch_name}.{index}")
            yield f"{branch_name}.{index}", predictor, float(head.stride[index])


def reset_class_predictors(model):
    """类别编号语义已改变；只重置分类末层，保留定位、角度和骨干参数。"""
    names = []
    with torch.no_grad():
        for name, predictor, stride in class_predictors(model):
            predictor.reset_parameters()
            predictor.bias.fill_(math.log(5 / predictor.out_channels / (640 / stride) ** 2))
            names.append(name)
    if not names:
        raise ValueError("No classification predictors found")
    return names


def class_parameter_hash(model):
    digest = hashlib.sha256()
    for name, predictor, _ in class_predictors(model):
        digest.update(name.encode("ascii"))
        for parameter in predictor.parameters():
            digest.update(parameter.detach().cpu().float().contiguous().numpy().tobytes())
    return digest.hexdigest()


def export_metrics(metrics, names, supports, *, scope="development validation only; no independent final test"):
    """不使用 maps 属性：该属性会把缺失类别填成总体 mAP。"""
    box = metrics.box
    indices = np.asarray(box.ap_class_index, dtype=int)
    ap = np.asarray(box.all_ap, dtype=float)
    rows = {int(class_id): ap[index] for index, class_id in enumerate(indices)}
    per_class = []
    for class_id, name in enumerate(names):
        count = int(supports.get(class_id, 0))
        values = rows.get(class_id)
        # 全部预测均未命中时，Ultralytics 不填 AP 数组；有 GT 的类别记零。
        ap50 = float(values[0]) if count and values is not None else (0.0 if count else None)
        mean_ap = float(np.mean(values)) if count and values is not None else (0.0 if count else None)
        per_class.append({"id": class_id, "name": name, "ground_truth_instances": count,
                          "ap50": ap50, "ap50_95": mean_ap})
    evaluated = [row for row in per_class if row["ground_truth_instances"]]
    return {
        "metric": "Ultralytics 8.3.221 OBB AP using batch_probiou, not polygon-IoU AP",
        "scope": scope,
        "mAP50_supported_classes": float(np.mean([row["ap50"] for row in evaluated])) if evaluated else None,
        "mAP50_95_supported_classes": float(np.mean([row["ap50_95"] for row in evaluated])) if evaluated else None,
        "supported_class_count": len(evaluated), "total_class_count": len(names),
        "per_class": per_class,
    }


def normalize_names(raw_names):
    if isinstance(raw_names, dict):
        normalized = {int(key): value for key, value in raw_names.items()}
        if len(normalized) != len(raw_names) or set(normalized) != set(range(len(normalized))):
            raise ValueError("Class names must use unique contiguous indices starting at zero")
        names = [normalized[index] for index in range(len(normalized))]
    elif isinstance(raw_names, list):
        names = raw_names
    else:
        raise ValueError("Class names must be a list or integer-indexed mapping")
    if not names or any(not isinstance(name, str) or not name for name in names) or len(names) != len(set(names)):
        raise ValueError("Class names must be nonempty unique strings")
    return names


def dataset_inventory(data_path):
    config = yaml.safe_load(data_path.read_text())
    if "download" in config:
        raise ValueError("Pilot dataset must already exist locally; download hooks are forbidden")
    names = normalize_names(config["names"])
    root = Path(config.get("path", data_path.parent)).expanduser()
    if not root.is_absolute():
        root = (data_path.parent / root).resolve()
    images, supports = {}, {}
    for split, expected in (("train", 128), ("val", 64)):
        directory = root / config[split]
        if not directory.is_dir():
            raise ValueError(f"Expected a local {split} image directory: {directory}")
        images[split] = sorted(path for path in directory.iterdir()
                               if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".tif", ".tiff"})
        if len(images[split]) != expected:
            raise ValueError(f"Fixed pilot requires {expected} {split} images, got {len(images[split])}")
        counts = {index: 0 for index in range(len(names))}
        for image in images[split]:
            if "images" not in image.parts:
                raise ValueError("Expected Ultralytics images/<split> layout")
            parts = list(image.parts)
            parts[len(parts) - 1 - parts[::-1].index("images")] = "labels"
            label = Path(*parts).with_suffix(".txt")
            for line in label.read_text().splitlines():
                fields = line.split()
                if len(fields) != 9:
                    raise ValueError(f"Expected normalized OBB class + 8 coordinates: {label}")
                class_id = int(fields[0])
                xy = np.asarray(fields[1:], dtype=float)
                if class_id not in counts or not np.all(np.isfinite(xy)) or np.any((xy < 0) | (xy > 1)):
                    raise ValueError(f"Invalid OBB annotation: {label}")
                counts[class_id] += 1
        supports[split] = counts
    return names, images, supports


def validate_manifest(manifest_path, names, images):
    manifest = json.loads(manifest_path.read_text())
    if manifest["names"] != names or manifest.get("final_test_created") is not False:
        raise ValueError("Dataset manifest class order or development-only scope mismatch")
    audit = manifest["summary"]["cross_split_similarity"]
    if audit["flag_count"] != 0 or audit.get("top_flags"):
        raise ValueError("Cross-split image-similarity audit is flagged; training is forbidden")
    root = manifest_path.parent.resolve()
    records = manifest["records"]
    if len(records) != sum(len(value) for value in images.values()):
        raise ValueError("Manifest record count does not match the fixed dataset")
    expected = {(split, path.resolve()) for split, paths in images.items() for path in paths}
    observed, group_owners, byte_owners, pixel_owners = set(), {}, {}, {}
    for record in records:
        split = record["split"]
        paths = {}
        for field in ("image", "label", "source_label"):
            path = (root / record[field]).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise ValueError(f"Manifest contains missing or escaping {field} path")
            expected_digest = record[{"image": "image_sha256", "label": "label_sha256",
                                      "source_label": "source_label_sha256"}[field]]
            if sha256(path) != expected_digest:
                raise ValueError(f"Manifest {field} digest mismatch: {path}")
            paths[field] = path
        key = (split, paths["image"])
        if key in observed:
            raise ValueError("Duplicate image record in manifest")
        observed.add(key)
        relative_image = paths["image"].relative_to(root)
        if len(relative_image.parts) != 3 or relative_image.parts[:2] != ("images", split):
            raise ValueError("Manifest image is assigned to the wrong partition")
        expected_label = root / "labels" / split / paths["image"].with_suffix(".txt").name
        if paths["label"] != expected_label:
            raise ValueError("Manifest image/label pairing does not match the loader")
        with Image.open(paths["image"]) as image:
            if image.size != (800, 800):
                raise ValueError("Pilot image geometry changed")
            decoded_digest = hashlib.sha256(np.asarray(image.convert("RGB")).tobytes()).hexdigest()
        if decoded_digest != record["decoded_rgb_sha256"]:
            raise ValueError("Decoded image pixel digest mismatch")
        for owners, identity, reason in (
            (group_owners, record["provisional_group_id"], "provisional source group"),
            (byte_owners, record["image_sha256"], "image byte duplicate"),
            (pixel_owners, decoded_digest, "decoded pixel duplicate"),
        ):
            if identity in owners and owners[identity] != split:
                raise ValueError(f"Cross-split {reason} detected")
            owners[identity] = split
    if observed != expected:
        raise ValueError("Manifest image paths do not match the dataset directories")
    selection_path = Path(manifest["selection_path"])
    if not selection_path.is_file() or sha256(selection_path) != manifest["selection_sha256"]:
        raise ValueError("Preregistered dataset selection changed or is missing")
    return manifest


class TrainingBudgetExceeded(RuntimeError):
    pass


def statistics(values):
    array = np.asarray(values, dtype=float)
    return {"count": len(array), "mean": float(array.mean()), "p50": float(np.median(array)),
            "p95": float(np.percentile(array, 95))}


def measure_service(weights, images):
    from ultralytics import YOLO

    samples = []
    for path in images[:8]:
        with Image.open(path) as image:
            samples.append((path.name, np.asarray(image.convert("RGB"))))
    result = {}
    for device in ("cpu", "mps"):
        model = YOLO(str(weights))

        def sync():
            if device == "mps":
                torch.mps.synchronize()

        def predict(rgb, window_id):
            bgr = np.ascontiguousarray(rgb[:, :, ::-1])
            output = model.predict(bgr, device=device, imgsz=832, conf=0.25, iou=0.7,
                                   max_det=300, half=False, verbose=False, save=False)[0]
            obb = output.obb.cpu()
            xy = obb.xyxyxyxy.reshape(-1, 8).numpy()
            scores, classes = obb.conf.numpy(), obb.cls.numpy()
            detections = [Detection(window_id, index, int(classes[index]), float(scores[index]),
                                    tuple(float(value) for value in vertices))
                          for index, vertices in enumerate(xy)]
            height, width = rgb.shape[:2]
            payload = serialize_detections(detections, window_id, (0, 0, width, height))
            return len(payload), len(detections)

        # select_device() 会重设 CPU 线程数，须先建立 predictor 再固定测量线程。
        model.predictor = model._smart_load("predictor")(
            overrides={**model.overrides, "device": device, "imgsz": 832, "conf": 0.25,
                       "iou": 0.7, "max_det": 300, "half": False, "verbose": False, "save": False},
            _callbacks=model.callbacks,
        )
        model.predictor.setup_model(model=model.model, verbose=False)
        torch.set_num_threads(4)
        for index in range(3):
            predict(samples[index % len(samples)][1], index)
        sync()
        records = []
        for repeat in range(2):
            if torch.get_num_threads() != 4:
                raise RuntimeError("CPU thread count changed during service timing")
            for index, (name, rgb) in enumerate(samples):
                sync()
                started = time.perf_counter()
                size, count = predict(rgb, index)
                sync()
                records.append({"image": name, "repeat": repeat, "seconds": time.perf_counter() - started,
                                "serialized_bytes": size, "detections": count})
        result[device] = {"seconds": statistics([row["seconds"] for row in records]),
                          "serialized_bytes": statistics([row["serialized_bytes"] for row in records]),
                          "cpu_threads_observed": torch.get_num_threads(), "records": records}
        del model
        if device == "mps":
            torch.mps.empty_cache()
    return {
        "scope": "local CPU/MPS proxy only; neither flight hardware nor ground server calibration",
        "input": "first 8 development images, decoded RGB, original patch size; network letterbox 832",
        "boundary": "decoded RGB -> BGR -> preprocess/inference/OBB NMS -> CPU polygons -> OBB1 serialization",
        "excluded": ["image decoding", "full-scene overlap extraction", "cross-window merge", "transport"],
        "warmup_calls_per_device": 3, "repeats_per_image": 2, "cpu_threads": 4,
        "confidence": 0.25, "nms_iou": 0.7, "max_det": 300, "devices": result,
    }


def run(args):
    training = training_options(getattr(args, "epochs", 10))
    reference_value = getattr(args, "reference_checkpoint", None)
    reference = Path(reference_value).resolve() if reference_value else None
    if reference is not None and (training["epochs"] != 40 or not reference.is_file()):
        raise ValueError("Reference checkpoint must exist locally and is only supported for the fixed 40-epoch diagnostic")
    data_path, output, project, weights = [Path(value).resolve() for value in
                                          (args.data, args.output, args.project, args.weights)]
    if not weights.is_file():
        raise FileNotFoundError(f"Pretrained weights must already be local: {weights}")
    if any((output / name).exists() for name in ("protocol.json", "summary.json")) or project.exists():
        raise FileExistsError("Pilot artifacts already exist; this runner never overwrites an experiment")
    manifest = data_path.parent / "manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError("Prepared dataset manifest is required")
    names, images, supports = dataset_inventory(data_path)
    validate_manifest(manifest, names, images)
    if not torch.backends.mps.is_available():
        raise RuntimeError("Frozen pilot requires MPS; no silent CPU fallback")
    output.mkdir(parents=True, exist_ok=True)
    project.mkdir(parents=True, exist_ok=False)
    os.environ.update({"YOLO_CONFIG_DIR": str(project / "ultralytics_config"),
                       "YOLO_AUTOINSTALL": "false", "YOLO_OFFLINE": "true",
                       "WANDB_MODE": "disabled", "COMET_MODE": "DISABLED"})
    from ultralytics import YOLO, __version__, settings
    from ultralytics.models.yolo.obb.train import OBBTrainer
    from ultralytics.utils import callbacks

    if __version__ != "8.3.221":
        raise RuntimeError("This runner's model/metric contract was verified against Ultralytics 8.3.221")
    settings.update({key: False for key in ("sync", "wandb", "comet", "clearml", "dvc", "hub",
                                            "mlflow", "neptune", "raytune", "tensorboard")})
    # 本预实验只留本地回调，禁用第三方追踪器及云端集成。
    callbacks.add_integration_callbacks = lambda instance: None
    torch.set_num_threads(4)
    torch.set_num_interop_threads(1)
    random.seed(1701)
    np.random.seed(1701)
    torch.manual_seed(1701)
    started = time.perf_counter()
    model = YOLO(str(weights))
    if model.model.model[-1].nc != len(names):
        raise ValueError("This pilot expects 15 DOTA outputs replaced by 15 SAR class outputs")
    reset_names = reset_class_predictors(model.model)
    model.model.names = dict(enumerate(names))
    epoch0 = project / "epoch0.pt"
    model.save(str(epoch0))
    # save() 使用 FP16 存储；以实际重载后的参数哈希验证训练器没有覆盖分类末层。
    model = YOLO(str(epoch0))
    expected_head_hash = class_parameter_hash(model.model)
    protocol = {
        "created_utc": datetime.now(timezone.utc).isoformat(), "seed": 1701,
        "scope": "one bounded offline development run; no final test or checkpoint selection",
        "data": str(data_path), "data_yaml_sha256": sha256(data_path),
        "manifest_sha256": sha256(manifest), "classes": names, "supports": supports,
        "source_weights": str(weights), "source_weights_sha256": sha256(weights),
        "reference_checkpoint": {"path": str(reference), "sha256": sha256(reference)} if reference else None,
        "initialization": {"reset_predictors": reset_names, "method": "Conv2d.reset_parameters then cls bias log(5/nc/(640/stride)^2)",
                           "preserved": "backbone, neck, box regressors, angle regressors and intermediate class branches",
                           "epoch0_sha256": sha256(epoch0), "epoch0_class_parameter_hash": expected_head_hash},
        "training": training, "validation": VALIDATION, "optional_albumentations_disabled": True,
        "optimizer_schedule": {"warmup_epochs": 3.0, "warmup_minimum_batch_index": 100,
                               "warmup_rule": "ni <= max(round(warmup_epochs * batches_per_epoch), 100)",
                               "nominal_batch_size_nbs": 64, "post_warmup_accumulation": 8,
                               "lr_schedule": "linear decay with the requested epoch horizon; lrf=0.01",
                               "instrumentation": "count completed optimizer_step calls; no behavior change"},
        "max_training_seconds": MAX_TRAIN_SECONDS,
        "checkpoint_rule": "last epoch EMA, never best development checkpoint; wall-budget interruption reported explicitly",
        "host": {"platform": platform.platform(), "machine": platform.machine(),
                 "torch": torch.__version__, "ultralytics": __version__},
    }
    write_json(output / "protocol.json", protocol)
    before = export_metrics(model.val(data=str(data_path), project=str(project), name="epoch0_val",
                                     exist_ok=False, **VALIDATION), names, supports["val"])
    write_json(output / "epoch0_metrics.json", before)
    # val() 会融合卷积；训练必须重新读取未融合的 epoch0 权重。
    model = YOLO(str(epoch0))
    state = {"train_start": None, "batches": 0, "epochs_completed": 0, "deadline_exceeded": False,
             "optimizer_steps": 0, "optimizer_steps_during_warmup": 0, "optimizer_steps_by_epoch": {}}

    def on_train_start(trainer):
        actual = class_parameter_hash(trainer.model)
        if actual != expected_head_hash:
            raise RuntimeError("Trainer changed the reinitialized classification head before training")
        state["trainer_class_hash_verified"] = actual
        state["train_start"] = time.perf_counter()
        state["warmup_last_batch_index"] = max(round(trainer.args.warmup_epochs * len(trainer.train_loader)), 100)
        state["nominal_batch_size_nbs"] = trainer.args.nbs

    def check_budget(trainer):
        if state["train_start"] is not None and time.perf_counter() - state["train_start"] >= MAX_TRAIN_SECONDS:
            trainer.stop = True
            state["deadline_exceeded"] = True
            raise TrainingBudgetExceeded("20-minute training budget exhausted at a batch boundary")

    def on_batch_end(trainer):
        state["batches"] += 1
        check_budget(trainer)

    def on_epoch_end(trainer):
        state["epochs_completed"] = trainer.epoch + 1

    model.add_callback("on_train_start", on_train_start)
    model.add_callback("on_train_batch_start", check_budget)
    model.add_callback("on_train_batch_end", on_batch_end)
    model.add_callback("on_train_epoch_end", on_epoch_end)

    class FixedLastOBBTrainer(OBBTrainer):
        def optimizer_step(self):
            super().optimizer_step()
            epoch = self.epoch + 1
            during_warmup = state["batches"] <= state["warmup_last_batch_index"]
            state["optimizer_steps"] += 1
            state["optimizer_steps_during_warmup"] += int(during_warmup)
            counts = state["optimizer_steps_by_epoch"].setdefault(
                epoch, {"epoch": epoch, "total": 0, "during_warmup": 0, "after_warmup": 0})
            counts["total"] += 1
            counts["during_warmup" if during_warmup else "after_warmup"] += 1

        def build_dataset(self, img_path, mode="train", batch=None):
            from ultralytics.data.augment import Albumentations

            dataset = super().build_dataset(img_path, mode=mode, batch=batch)
            for transform in dataset.transforms.transforms:
                if isinstance(transform, Albumentations):
                    transform.p = 0.0
            return dataset

        def final_eval(self):
            # 库默认额外评估 best.pt；这里仅在外部评估固定最后一轮。
            return None

    status = "completed"
    try:
        model.train(trainer=FixedLastOBBTrainer, data=str(data_path), project=str(project),
                    name="train", exist_ok=False, **training)
    except (TrainingBudgetExceeded, KeyboardInterrupt) as error:
        status = "interrupted"
        state["interruption_reason"] = str(error) or "keyboard interrupt"
        trainer = model.trainer
        if trainer is None or not hasattr(trainer, "ema"):
            raise
        trainer.ema.update_attr(trainer.model, include=["yaml", "nc", "args", "names", "stride"])
        trainer.fitness = getattr(trainer, "fitness", None)
        trainer.save_model()
    train_seconds = time.perf_counter() - state["train_start"] if state["train_start"] else None
    if state["epochs_completed"] != training["epochs"] and status == "completed":
        status = "interrupted"
        state["interruption_reason"] = "Trainer exited before the fixed epoch count"
    last = project / "train" / "weights" / "last.pt"
    if not last.is_file():
        raise FileNotFoundError("Training did not produce last.pt")
    final_model = YOLO(str(last))
    after = export_metrics(final_model.val(data=str(data_path), project=str(project), name="last_val",
                                          exist_ok=False, **VALIDATION), names, supports["val"])
    write_json(output / "last_metrics.json", after)
    training_set_diagnostic = None
    if reference is not None:
        train_validation = {**VALIDATION, "split": "train"}
        diagnostic_scope = "training-set fit diagnostic only; not validation, independent test, or checkpoint selection"
        current_train = export_metrics(
            final_model.val(data=str(data_path), project=str(project), name="current_train_diagnostic",
                            exist_ok=False, **train_validation), names, supports["train"], scope=diagnostic_scope)
        reference_model = YOLO(str(reference))
        if normalize_names(reference_model.names) != names:
            raise ValueError("Reference checkpoint class semantics do not match the pilot")
        reference_train = export_metrics(
            reference_model.val(data=str(data_path), project=str(project), name="reference_train_diagnostic",
                                exist_ok=False, **train_validation), names, supports["train"], scope=diagnostic_scope)
        training_set_diagnostic = {
            "scope": diagnostic_scope, "image_count": len(images["train"]),
            "data_yaml_sha256": sha256(data_path), "manifest_sha256": sha256(manifest),
            "reference": {"path": str(reference), "sha256": sha256(reference), "metrics": reference_train},
            "current": {"path": str(last), "sha256": sha256(last), "metrics": current_train},
        }
        write_json(output / "training_set_diagnostic.json", training_set_diagnostic)
        del reference_model
    del model
    torch.mps.empty_cache()
    service = measure_service(last, images["val"])
    write_json(output / "service_timing.json", service)
    summary = {
        "status": status, "epochs_requested": training["epochs"], "epochs_completed": state["epochs_completed"],
        "training_batches_completed": state["batches"], "training_seconds": train_seconds,
        "optimizer_updates": {"total": state["optimizer_steps"],
                              "during_warmup": state["optimizer_steps_during_warmup"],
                              "after_warmup": state["optimizer_steps"] - state["optimizer_steps_during_warmup"],
                              "by_epoch": list(state["optimizer_steps_by_epoch"].values()),
                              "warmup_last_batch_index": state.get("warmup_last_batch_index"),
                              "nominal_batch_size_nbs": state.get("nominal_batch_size_nbs")},
        "total_seconds": time.perf_counter() - started,
        "training_budget_seconds": MAX_TRAIN_SECONDS, "budget_exceeded": state["deadline_exceeded"],
        "interruption_reason": state.get("interruption_reason"),
        "trainer_class_hash_verified": state.get("trainer_class_hash_verified"),
        "before": before, "after": after, "service_timing": service,
        "training_set_diagnostic": training_set_diagnostic,
        "checkpoint": {"path": str(last), "sha256": sha256(last), "selection": "last EMA"},
        "parameters": sum(parameter.numel() for parameter in YOLO(str(last)).model.parameters()),
        "host": protocol["host"],
        "limitations": ["128 training and 64 development patches only; no independent final test",
                        "provisional source grouping does not establish scene/ship identity isolation",
                        "images with any invalid/out-of-bounds/difficult object excluded; no border-robustness claim",
                        "patch-level fine-grained detection AP is neither binary military/civilian accuracy nor operational reliability",
                        "local CPU/MPS timings are not a flight/ground-server calibration"],
    }
    write_json(output / "summary.json", summary)
    print(json.dumps({"status": status, "summary": str(output / "summary.json"),
                      "epochs_completed": state["epochs_completed"], "after": after}, ensure_ascii=False))
    return summary
