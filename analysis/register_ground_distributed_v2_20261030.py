"""只登记用户批准的计算位置变更，不生成数据或启动仿真。"""
import importlib.metadata
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from analysis import run_ground_distributed_v2_20261030 as d


def main():
    assert not d.REG.exists()
    audit=d.r.read(d.PREVIOUS/'PREFLIGHT_AUDIT.json')
    assert audit['status']=='PASS'
    failures=list((d.r.ROOT/'M_scaled/validation').glob('*/*.progress.json'))
    assert len(failures)==18 and all(d.r.read(p)['status']=='FAILED' for p in failures)
    for host in d.HOSTS:
        path_check=d.r.read(d.DIST/f'PATH_PREFLIGHT_{host}.json')
        assert path_check['status']=='PASS' and path_check['no_simulation_environment_created']
    packages=('torch','numpy','networkx','skyfield','sgp4','PyYAML','tqdm','matplotlib',
        'jplephem','certifi','python-dateutil','six','sympy','mpmath','filelock','fsspec',
        'jinja2','markupsafe','typing-extensions','shapely','scipy','charset-normalizer','setuptools')
    code=('import json,os,platform,importlib.metadata as m; '
        f'print(json.dumps(dict(host=platform.node(),platform=platform.platform(),cpu_count=os.cpu_count(),'
        f'packages={{p:m.version(p) for p in {packages!r}}})))')
    environments={}
    for host in d.HOSTS:
        command=f'cd {d.REMOTE_ROOT} && .venv/bin/python -c {shlex.quote(code)}'
        row=json.loads(subprocess.check_output(['ssh','-o','BatchMode=yes',host,command],text=True,timeout=30))
        assert row['cpu_count']>=8
        for pkg,version in row['packages'].items():
            assert version.split('+')[0]==importlib.metadata.version(pkg).split('+')[0],(host,pkg,version)
        environments[host]=row
    d.r.write_json(d.DIST/'RUNTIME_ENVIRONMENTS.json',environments)
    env=dict(os.environ,PYTHONPATH='tests:.')
    test=subprocess.run([sys.executable,'-m','unittest','test_ground_distributed_v2_20261030','-v'],
        env=env,capture_output=True,text=True)
    d.r.write_json(d.DIST/'DISPATCH_TESTS.json',dict(at=d.now(),returncode=test.returncode,
        stdout=test.stdout,stderr=test.stderr))
    assert test.returncode==0
    files=[Path('analysis')/name for name in ('preflight_ground_devbox_20261030.py',
        'verify_ground_devbox_preflight_20261030.py','run_ground_distributed_v2_20261030.py',
        'register_ground_distributed_v2_20261030.py','check_ground_deployment_path_20261030.py')]
    files += [Path('tests/test_ground_distributed_v2_20261030.py'),Path('examples/config.yaml'),
        Path('data/tle/global_spread_inc53_66.tle')]
    files += [d.PREVIOUS/name for name in ('REGISTRATION.json','EXECUTION_PLACEMENT_AMENDMENT.md','BUNDLE_HASHES.json',
        'PREFLIGHT_AUDIT.json',
        'preflight_mac.json','preflight_devbox1.json','preflight_devbox2.json','preflight_devbox3.json')]
    files += [d.DIST/name for name in ('PATH_CORRECTION_AMENDMENT.md','RUNTIME_ENVIRONMENTS.json','DISPATCH_TESTS.json')]
    files += [d.DIST/f'PATH_PREFLIGHT_{host}.json' for host in d.HOSTS]
    files += [d.PREVIOUS/host/'final_status.json' for host in d.HOSTS]
    files += [d.PREVIOUS/'local/launch.json']
    run=d.r.read(d.r.ROOT/'M_scaled/run_manifest.json')
    old=d.r.read(d.r.ROOT/'launch.json')
    value=dict(at=d.now(),status='FROZEN_BEFORE_REMOTE_TESTS',manifest_sha256=d.r.sha(d.r.ROOT/'manifest.json'),
        protocol_sha256=d.r.sha(d.r.PROTOCOL),opening_sha256=d.r.sha(d.r.ROOT/'TEST_OPENED.json'),
        hashes={str(p):d.r.sha(p) for p in files},assignments=d.make_assignments(run),
        old_launch=old,remote_workers=6,local_workers=9,torch_threads=1,
        deadline=d.r.END.isoformat(),preflight_hashes={str(p):d.r.sha(p) for p in files if 'preflight_' in p.name},
        input_bundle_sha256=d.r.sha(d.PREVIOUS/'input_bundle.tar.gz'),
        runtime_archive_sha256=d.r.sha('/tmp/ground_scaling_runtime_linux_20261030.tar.gz'),
        startup_failures={str(p):d.r.sha(p) for p in failures},
        notes=['User explicitly authorized deployment-path correction and restarting 18 pre-simulation failures on 2026-10-30.',
            '60 M_scaled jobs, 20 per host, batch size 6, no prefetch/retry; local L batch size 9.',
            'Previous coordinator drains existing L batch then exits; v2 resumes only unstarted L. Failure records are archived before output sync.',
            'Remote real root is /Users/bytedance/Projects/bs, not a symlink; full frozen verification is proven in spawn before restart.',
            'Linux x86_64 CPU and Mac ARM CPU are heterogeneous. Validation smoke equality does not prove all trajectories bitwise identical.',
            'No data regeneration, new test opening, model selection, training, tuning or paper edits.',
            'Original simulation, physics, model, observation, metrics and report source hashes remain frozen.'])
    d.exclusive(d.REG,value)
    d.registration()
    print(json.dumps(dict(status='PASS',registration_sha256=d.r.sha(d.REG),assignments={h:len(j) for h,j in value['assignments'].items()})))


if __name__=='__main__':main()
