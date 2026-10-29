"""只扩展共同输入路径能力，保留已冻结的任务、轨道、资源和确认样本。"""

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path


def derive(source, target, hops, ground_relay=False):
    if hops < 1:
        raise ValueError("Input hop bound must be positive")
    original = json.loads((source / "run.json").read_text())
    target.mkdir(parents=True, exist_ok=False)
    revision = {"input_relay_max_hops": hops, "ground_input_relay": ground_relay,
                "base_manifest_sha256": hashlib.sha256((source / "run.json").read_bytes()).hexdigest(),
                "base_prepared_dir": str(source),
                "scope": "common store-and-forward input capability for every method; not an algorithm-only gain",
                "preparer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    frozen = deepcopy(original)
    frozen["input_protocol_revision"] = revision
    frozen["provenance"]["input_protocol_revision"] = revision
    for info in frozen["case_info"]:
        path = source / f"{info['id']}_config.json"
        cfg = json.loads(path.read_text())
        revision.setdefault("base_config_sha256", {})[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        cfg["input_relay_max_hops"] = hops
        if ground_relay:
            cfg["ground_input_relay"] = True
        (target / path.name).write_text(json.dumps(cfg, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    if (source / "selection.json").exists():
        raw = (source / "selection.json").read_bytes()
        if hashlib.sha256(raw).hexdigest() != original["selection_sha256"]:
            raise ValueError("Original confirmation selection hash differs")
        selection = json.loads(raw)
        selection["common_input_protocol"] = revision
        (target / "selection.json").write_text(json.dumps(selection, indent=2, ensure_ascii=False) + "\n")
        frozen["selection_sha256"] = hashlib.sha256((target / "selection.json").read_bytes()).hexdigest()
    if (source / "acquisition.json").exists():
        (target / "acquisition.json").write_bytes((source / "acquisition.json").read_bytes())
    (target / "run.json").write_text(json.dumps(frozen, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    return revision


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--hops", type=int, default=2)
    parser.add_argument("--ground-input-relay", action="store_true")
    args = parser.parse_args()
    print(json.dumps(derive(args.prepared_dir, args.out_dir, args.hops, args.ground_input_relay),
                     indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
