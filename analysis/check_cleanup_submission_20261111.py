"""清理提交的独立暂存扫描；仅保存路径与统计，不保存匹配到的凭据。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'results/nonuniform_20261111/cleanup'


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT)


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write('\n')


def independent(name):
    paths = [p.decode() for p in git('diff','--cached','--name-only','--diff-filter=ACMR','-z').split(b'\0') if p]
    large = []
    for offset in range(0,len(paths),200):
        batch = paths[offset:offset+200]
        large += subprocess.check_output(['find',*batch,'-type','f','-size','+50M','-print'],cwd=ROOT).decode().splitlines()
    inventory, matches = {}, []
    token_path = Path.home()/'.local/state/bs-evaluation-queue/token'
    known = token_path.read_bytes().strip() if token_path.exists() else None
    patterns = {
        'private_key': rb'-----BEGIN (?:RSA |OPENSSH |EC |DSA |ENCRYPTED )?PRIVATE KEY-----',
        'provider_token': rb'(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}|AKIA[0-9A-Z]{16}|sk-(?:proj-)?[A-Za-z0-9_-]{24,}|xox[baprs]-[A-Za-z0-9-]{20,})',
        'jwt': rb'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}',
        'literal_bearer': rb'Bearer[ \t]+[A-Za-z0-9._~+/-]{24,}',
        'literal_assignment': rb'''(?i)(?:password|passwd|api_key|secret|access_token|auth_token)["']?\s*[:=]\s*["']([^"'\n]{12,})["']''',
    }
    for path in paths:
        data = git('show', ':'+path)
        inventory[path] = dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
        if known and known in data:
            matches.append(dict(path=path,rule='exact_queue_credential'))
        for rule, pattern in patterns.items():
            for match in re.finditer(pattern,data):
                matches.append(dict(path=path,rule=rule,line=data[:match.start()].count(b'\n')+1))
    deleted = {p.decode() for p in git('diff','--cached','--name-only','--diff-filter=D','-z').split(b'\0') if p}
    plan = json.loads((OUT/'PLAN.json').read_text())
    registered = {f['path'] for row in plan['candidates'] if row['decision']=='DELETE' for f in row['files']}
    assert deleted == registered
    for path in large:
        attr = git('check-attr','--cached','filter','--',path).decode().strip()
        assert attr.endswith(': lfs'), ('Large ordinary staged file',path)
        assert git('show',':'+path).startswith(b'version https://git-lfs.github.com/spec/v1\n')
    ignored = git('check-ignore','runs/','workbench/settings.json').decode().splitlines()
    assert set(ignored) == {'runs/','workbench/settings.json'}
    value = dict(at=datetime.now(timezone.utc).isoformat(),staged_added_modified_files=len(paths),
        staged_bytes=sum(v['bytes'] for v in inventory.values()),deleted_files=len(deleted),
        inventory=inventory,over_50_MiB=large,large_file_command='find <all staged ACMR paths in batches> -type f -size +50M -print',
        matches=matches,known_queue_credential_checked=known is not None,ignored_paths=ignored,
        limitation='Pattern and exact-known-value checks do not prove absence of every possible unknown credential encoding.')
    if name != '-':
        save(OUT/name,value)
    print(json.dumps({k:v for k,v in value.items() if k!='inventory'}))
    assert not matches, 'Review matching paths without printing matched content'


def builtin(name):
    binary = Path.home()/'.bytesec/commit_hook/ide_hardcode_mac_amd64'
    cache = Path.home()/'.cache/bs-cleanup-20261111'
    cache.mkdir(parents=True,exist_ok=True)
    log_path = cache/(Path(name).stem.lower()+'.log')
    started = time.monotonic()
    with log_path.open('xb') as log:
        os.chmod(log_path,0o600)
        result = subprocess.run([str(binary),'-input',str(ROOT),'-commit','-taskFrom','cli-pre-commit'],
            cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
    data = log_path.read_bytes()
    value = dict(at=datetime.now(timezone.utc).isoformat(),exit_code=result.returncode,
        wall_seconds=time.monotonic()-started,output_bytes=len(data),output_sha256=hashlib.sha256(data).hexdigest(),
        external_private_log=str(log_path),explicit_timeout_seconds=None,
        invocation='Same installed scanner and arguments as the Git hook, directly awaited without its two-second watchdog.',
        status='PASS' if result.returncode==0 else 'NEEDS_REVIEW',
        hook_remains_enabled=True)
    save(OUT/name,value)
    print(json.dumps(value))


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=('independent','builtin'))
    parser.add_argument('--output')
    args=parser.parse_args()
    builtin(args.output or 'BUILTIN_SCAN.json') if args.mode=='builtin' else independent(args.output or 'INDEPENDENT_SUBMISSION_SCAN.json')
