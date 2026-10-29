"""从当前冻结场景派生公共temporal中继面板，只更改中继决策组件。"""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_sar_frozen import sha256_bytes
from sim.config import config_from_dict


def derive(base, simple_path_budget=False):
    cfg = deepcopy(base)
    cfg["relay_routing"] = "temporal"
    if simple_path_budget:
        # 执行器禁止回访；N-1覆盖所有无环中继路径，不额外施加8跳截断。
        cfg["relay_max_hops"] = max(1, cfg["num_sats"] - 1)
    config_from_dict(cfg)
    return cfg


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--source", type=Path, default=Path("results/sar_dispatch_20261016/main_config.json"))
    parser.add_argument("--simple-path-budget", action="store_true")
    args = parser.parse_args()
    if args.out.exists():
        raise ValueError("Use a new study directory")
    source = args.source
    manifest_path = source.with_name("manifest.json")
    original = source.read_bytes()
    manifest = json.loads(manifest_path.read_bytes())
    if manifest["config_sha256"][source.name] != sha256_bytes(original):
        raise ValueError("Source manifest mismatch")
    base = json.loads(original)
    cfg = derive(base, args.simple_path_budget)
    data = json.dumps(cfg, indent=2, allow_nan=False).encode()
    args.out.mkdir(parents=True)
    (args.out / "main_config.json").write_bytes(data)
    study = {"scope": "shared routing protocol; physics, workload and scoring unchanged",
             "changed_fields": {key: {"before": base.get(key), "after": cfg[key]}
                                for key in cfg if cfg[key] != base.get(key)},
             "hop_budget_rule": "max(1, num_sats - 1), no revisits" if args.simple_path_budget else "inherited",
             "source": str(source),
             "source_sha256": sha256_bytes(original), "inputs": manifest["inputs"],
             "source_manifest_sha256": sha256_bytes(manifest_path.read_bytes()),
             "config_sha256": {"main_config.json": sha256_bytes(data)},
             "generator_sha256": sha256_bytes(Path(__file__).read_bytes())}
    (args.out / "manifest.json").write_text(json.dumps(study, indent=2))
    print(json.dumps({"out": str(args.out), "config_sha256": study["config_sha256"]}))


if __name__ == "__main__":
    main()
