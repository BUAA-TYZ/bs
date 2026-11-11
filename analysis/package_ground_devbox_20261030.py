"""仅打包本次计算所需的源码和登记输入，不携带凭据或工作区其他文件。"""
import hashlib
import json
from pathlib import Path
import tarfile

BASE = Path(__file__).resolve().parents[1]
ROOT = Path('results/redesign_20261021/main_scenario/ground_scaling_20261029')
DIST = ROOT / 'distributed_20261030'


def main():
    paths = set()
    pending = []

    def add(value):
        p = Path(value)
        p = p if p.is_absolute() else BASE / p
        p = p.resolve()
        p.relative_to(BASE)
        if p not in paths and p.is_file():
            paths.add(p)
            if p.suffix == '.json':
                pending.append(p)

    for folder in ('analysis', 'sim', 'tests'):
        for p in (BASE / folder).rglob('*.py'):
            add(p)
    for name in ('train_marl.py', 'requirements.txt', 'requirements-analysis.txt'):
        add(name)
    for name in ('manifest.json', 'PREGEN_REGISTRATION.json', 'TEST_OPENED.json', 'run_manifest.json'):
        add(ROOT / name)
    for stage in ('L_single_completion', 'L_scaled', 'M_scaled'):
        add(ROOT / stage / 'run_manifest.json')
        for p in (BASE / ROOT / stage / 'data').rglob('protocol.json'):
            add(p)
    add('results/redesign_20261021/main_scenario/local_comparison_20261027/manifest.json')
    add('results/redesign_20261021/main_scenario/bc_stage2_20261022/data/2026-10-22/six_hours/protocol.json')
    # 递归补齐所有已登记哈希的依赖；不扫描任意未登记结果目录。
    def walk(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value):
                    p = Path(key)
                    if p.is_file():
                        add(p)
                if isinstance(value, str):
                    p = Path(value)
                    if len(value) < 400 and p.suffix in ('.json', '.pt', '.npy', '.npz', '.csv', '.tle') and p.is_file():
                        add(p)
                elif isinstance(value, (dict, list)):
                    walk(value)
        elif isinstance(obj, list):
            for value in obj:
                if isinstance(value, str) and len(value) < 400 and Path(value).is_file():
                    add(value)
                elif isinstance(value, (dict, list)):
                    walk(value)
    while pending:
        walk(json.loads(pending.pop().read_text()))
    DIST.mkdir(exist_ok=True)
    index = {str(p.relative_to(BASE)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
    index_path = DIST / 'BUNDLE_HASHES.json'
    index_path.write_text(json.dumps(index, indent=2) + '\n')
    archive = DIST / 'input_bundle.tar.gz'
    with tarfile.open(archive, 'w:gz') as tf:
        for p in sorted(paths):
            tf.add(p, arcname=str(p.relative_to(BASE)), recursive=False)
        tf.add(index_path, arcname=str(index_path), recursive=False)
    print(json.dumps(dict(files=len(paths), raw_bytes=sum(p.stat().st_size for p in paths),
                          archive=str(archive), archive_bytes=archive.stat().st_size)))


if __name__ == '__main__':
    main()
