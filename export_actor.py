"""导出仅含 actor 的单星 TorchScript 并测量本机 CPU 推理，包含预处理之外的网络耗时。"""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import time

import numpy as np
import torch
from torch import nn

from sim.scheduler.marl_policy import MARLPolicy


def actor_module(model):
    if model.task_actor is not None:
        return model.task_actor
    return nn.Sequential(model.trunk, model.policy_head, nn.Unflatten(1, (model.num_tiles, model.act_dim)))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()
    torch.set_num_threads(1)
    policy = MARLPolicy(args.checkpoint)
    model = policy.model
    actor = actor_module(model).eval()
    params = sum(p.numel() for p in actor.parameters())
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    sample = torch.zeros(1, model.in_dim)
    with torch.no_grad():
        traced = torch.jit.trace(actor, sample)
        traced.save(str(out / "actor.pt"))
        loaded = torch.jit.load(str(out / "actor.pt"))
        for _ in range(10):
            inputs = torch.randn_like(sample)
            torch.testing.assert_close(loaded(inputs), model(inputs)[0])
        for _ in range(50):
            loaded(sample)
        elapsed = []
        for _ in range(500):
            start = time.perf_counter_ns()
            loaded(sample)
            elapsed.append((time.perf_counter_ns() - start) / 1e6)
    report = {"checkpoint": args.checkpoint, "episode": policy.metadata["episode"],
              "checkpoint_sha256": hashlib.sha256(Path(args.checkpoint).read_bytes()).hexdigest(),
              "actor_parameters": params, "fp32_parameter_bytes": params * 4,
              "artifact_bytes": (out / "actor.pt").stat().st_size,
              "under_one_million_parameters": params <= 1_000_000,
              "inference_batch_size": 1, "input_shape": [1, model.in_dim],
              "output_shape": [1, model.num_tiles, model.act_dim],
              "cpu_threads": 1, "machine": platform.machine(), "platform": platform.platform(),
              "median_ms": float(np.median(elapsed)), "p95_ms": float(np.percentile(elapsed, 95)),
              "feature_options": model.feature_options,
              "scope": "CPU network only; excludes orbit prediction, features, masks, runtime memory; not flight hardware"}
    (out / "profile.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
