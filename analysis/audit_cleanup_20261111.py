"""清理清单与快照恢复检查；只删除已登记、无引用且与快照逐字节一致的文件。"""
import argparse
import ast
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'results/nonuniform_20261111/cleanup'
TAG = 'pre-cleanup-20261111'
PAPER = Path('/Users/bytedance/Downloads/中期_content/content')
PROTECTED = ('sim', 'tests', 'analysis', 'data', '.venv',
    'results/redesign_20261021/main_scenario', 'results/jetson_concurrency_20261016',
    'results/method_cost_20261028', 'results/mechanism_diagnostic_20261029',
    'results/nonuniform_20261111', 'results/ground_scaling_preflight_20261029')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write('\n')


def label(path):
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def files(path):
    return sorted(p for p in path.rglob('*') if p.is_file()) if path.is_dir() else [path]


def plan():
    assert not (OUT / 'PLAN.json').exists(), 'Preserve existing cleanup plan'
    commit = subprocess.check_output(['git', 'rev-parse', TAG + '^{commit}'], cwd=ROOT).decode().strip()
    tree = subprocess.check_output(['git', 'ls-tree', '-r', '-z', TAG], cwd=ROOT)
    tracked = {r.split(b'\t', 1)[1].decode(): r.split()[2].decode() for r in tree.split(b'\0') if r}
    candidates = [p for p in (ROOT / 'results').iterdir() if p.name not in
                  {'redesign_20261021', 'nonuniform_20261111', 'jetson_concurrency_20261016',
                   'method_cost_20261028', 'mechanism_diagnostic_20261029', 'ground_scaling_preflight_20261029'}]
    candidates += [ROOT / 'results/redesign_20261021' / name for name in
                   ('smoke_v1', 'smoke_v2', 'M_day_20261015', 'M_day_20261015_v2', 'M_2026-10-15_k16_86400s')]
    candidates += list((ROOT / 'checkpoints').iterdir())
    candidates += list(ROOT.glob('*.py'))
    candidates += [ROOT / name for name in ('tmp', 'output', 'metrics.json', '.playwright-cli')]
    host = ROOT / 'results/redesign_20261021/calibration/capacity_20261021/host_probe'
    candidates += [p for p in host.iterdir() if 'wheel' in p.name or '.tar' in p.name]
    for base in ('sim', 'tests', 'analysis', '.'):
        caches = (ROOT / base).glob('__pycache__') if base == '.' else (ROOT / base).rglob('__pycache__')
        candidates += list(caches)
    candidates = sorted(set(p for p in candidates if p.exists()))
    # 保守地将所有analysis源码视为仍可能使用；额外记录静态import图和显式入口。
    scope = set((ROOT / 'sim').rglob('*.py')) | set((ROOT / 'analysis').rglob('*.py')) | set((ROOT / 'tests').rglob('*.py'))
    scope |= set((ROOT / 'docs/thesis').glob('*.md')) | set(PAPER.glob('*.tex'))
    scope |= set((ROOT / 'results/redesign_20261021').rglob('REPORT.md')) | set((ROOT / 'results/redesign_20261021').rglob('HANDOFF.md'))
    scope |= set(ROOT.glob('*.py')) | {ROOT / 'README.md'}
    for base in ('examples', 'workbench', 'czml_tools', 'workload_benchmark'):
        scope |= {p for p in (ROOT / base).rglob('*') if p.is_file() and p.suffix in ('.py', '.sh', '.yaml', '.yml', '.md')}
    # 冻结输入清单可能通过哈希键引用外部旧结果，纳入保护闭包。
    for base in ('results/redesign_20261021', 'results/jetson_concurrency_20261016', 'results/method_cost_20261028'):
        for p in (ROOT / base).rglob('*.json'):
            if any(word in p.name.lower() for word in ('manifest', 'protocol', 'registration', 'hashes', 'freeze')):
                scope.add(p)
    scope.discard(Path(__file__).resolve())
    text = {p: p.read_text(errors='replace').replace('\\_', '_') for p in sorted(scope)}
    imports, fragments = {}, {}
    for p, body in text.items():
        if p.suffix != '.py':
            continue
        try:
            tree_ast = ast.parse(body)
        except SyntaxError:
            continue
        imports[label(p)] = sorted({n.module for n in ast.walk(tree_ast) if isinstance(n, ast.ImportFrom) and n.module}
            | {alias.name for n in ast.walk(tree_ast) if isinstance(n, ast.Import) for alias in n.names})
        fragments[p] = {n.value for n in ast.walk(tree_ast) if isinstance(n, ast.Constant) and isinstance(n.value, str)
                        and 8 <= len(n.value) < 150 and '\n' not in n.value and '_' in n.value}
    rows = []
    for p in candidates:
        rel = label(p)
        refs = []
        for source, body in text.items():
            if source == p or source.is_relative_to(p):
                continue
            if p.name in body or rel in body or (p.suffix == '.py' and p.stem in imports.get(label(source), [])):
                refs.append({'source': label(source), 'kind': 'path/name/import'})
            elif any(p.name.startswith(s) and len(s) < len(p.name) for s in fragments.get(source, ())):
                refs.append({'source': label(source), 'kind': 'constructed_prefix'})
        listing = files(p)
        untracked = [label(f) for f in listing if label(f) not in tracked]
        protected = any(rel == s or rel.startswith(s + '/') for s in PROTECTED)
        uncertain = p.is_symlink() or p.name in {'tmp', 'output', 'metrics.json', '.playwright-cli'}
        if protected:
            decision, reason = 'KEEP', '强制保留范围；不改现有流水线或缓存'
        elif refs:
            decision, reason = 'KEEP', '发现当前代码、测试、文档、论文或冻结输入引用'
        elif untracked or uncertain:
            decision, reason = 'PENDING', '保留待定：未全部进入快照或通用输出路径的动态引用不能排除'
        else:
            decision, reason = 'DELETE', '旧阶段孤立产物/脚本/模型；指定范围及保守扩展引用检查无命中'
        rows.append(dict(path=rel, decision=decision, reason=reason,
            bytes=sum(f.stat().st_size for f in listing), file_count=len(listing),
            allocated_bytes=sum(f.stat().st_blocks * 512 for f in listing), references=refs,
            not_in_snapshot=untracked, files=[dict(path=label(f), bytes=f.stat().st_size,
                sha256=sha(f), git_blob=tracked.get(label(f))) for f in listing] if decision == 'DELETE' else []))
    result = dict(stage='0.5', created_at=datetime.now(timezone.utc).isoformat(), snapshot_tag=TAG,
        snapshot_commit=commit, strategy='Conservative superset: all analysis/sim/tests code, imports, entrypoints, thesis docs, redesign reports/handoffs, paper TeX including comments, frozen input manifests, root and auxiliary entrypoints.',
        protected_prefixes=PROTECTED, reference_files={label(p): sha(p) for p in scope}, imports=imports,
        candidates=rows, counts=dict(Counter(r['decision'] for r in rows)))
    write(OUT / 'PLAN.json', result)
    lines = ['# 仓库清理清单（2026-11-11）', '', f'快照：`{TAG}` -> `{commit}`。',
        '先冻结本清单再删除。DELETE为计划删除；完成情况以results/nonuniform_20261111/cleanup/EXECUTION.json为准。',
        '完整引用证据、每文件恢复blob和SHA256见同目录PLAN.json。所有analysis脚本保守纳入引用范围，不以未匹配import为由删除动态入口。',
        '未进标签的忽略缓存、安装包保留待定；sim/tests/analysis/data/.venv及用户指定结果强制保留。论文扫描包含注释，不改论文。', '',
        '|路径|字节数|文件数|决定|理由与引用检查|', '|---|---:|---:|---|---|']
    for row in rows:
        refs = '; '.join(x['source'] + ' [' + x['kind'] + ']' for x in row['references'][:3])
        detail = row['reason'] + (f"；引用{len(row['references'])}处：{refs}" if refs else '；引用0处')
        if row['not_in_snapshot']:
            detail += f"；未入快照{len(row['not_in_snapshot'])}文件"
        lines.append(f"|`{row['path']}`|{row['bytes']}|{row['file_count']}|{row['decision']}|{detail}|")
    with (ROOT / 'docs/CLEANUP_20261111.md').open('x') as stream:
        stream.write('\n'.join(lines) + '\n')
    print(json.dumps(dict(counts=result['counts'], reference_files=len(scope),
        deletion_files=sum(r['file_count'] for r in rows if r['decision'] == 'DELETE'),
        deletion_bytes=sum(r['bytes'] for r in rows if r['decision'] == 'DELETE')), ensure_ascii=False))


def apply():
    plan_path = OUT / 'PLAN.json'
    value = json.loads(plan_path.read_text())
    assert not (OUT / 'EXECUTION.json').exists()
    assert subprocess.check_output(['git', 'rev-parse', TAG + '^{commit}'], cwd=ROOT).decode().strip() == value['snapshot_commit']
    for p, h in value['reference_files'].items():
        path = Path(p) if Path(p).is_absolute() else ROOT / p
        assert sha(path) == h, p
    targets = {f['path']: f for r in value['candidates'] if r['decision'] == 'DELETE' for f in r['files']}
    proc = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    for name, row in targets.items():
        path = ROOT / name
        assert path.resolve().is_relative_to(ROOT) and not path.is_symlink()
        assert sha(path) == row['sha256'], name
        proc.stdin.write((row['git_blob'] + '\n').encode()); proc.stdin.flush()
        size = int(proc.stdout.readline().split()[2]); data = proc.stdout.read(size); proc.stdout.read(1)
        assert hashlib.sha256(data).hexdigest() == row['sha256'], ('Snapshot bytes differ or LFS pointer', name)
    proc.stdin.close(); assert proc.wait() == 0
    started = datetime.now(timezone.utc).isoformat()
    for name in targets:
        (ROOT / name).unlink()
    for row in value['candidates']:
        if row['decision'] == 'DELETE':
            path = ROOT / row['path']
            if path.is_dir():
                for folder in sorted((p for p in path.rglob('*') if p.is_dir()), key=lambda p:len(p.parts), reverse=True):
                    folder.rmdir()
                path.rmdir()
    record = dict(status='DELETED_AS_REGISTERED', started_at=started,
        finished_at=datetime.now(timezone.utc).isoformat(), plan_sha256=sha(plan_path),
        snapshot_tag=TAG, snapshot_commit=value['snapshot_commit'],
        deleted_files=len(targets), logical_bytes=sum(f['bytes'] for f in targets.values()),
        allocated_bytes=sum(r['allocated_bytes'] for r in value['candidates'] if r['decision'] == 'DELETE'),
        note='Working-tree file bytes removed; Git history intentionally retains recoverable data, so this is not a claim of equal repository or physical-volume shrinkage.')
    write(OUT / 'EXECUTION.json', record)
    print(json.dumps(record))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('plan', 'apply'))
    args = parser.parse_args()
    plan() if args.mode == 'plan' else apply()
