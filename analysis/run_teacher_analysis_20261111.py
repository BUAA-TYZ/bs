"""验证日教师消融的独立登记与HTTP队列适配；旧入口只在子进程绑定。"""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import time
import traceback

from analysis import run_nonuniform_gate1_20261111 as queue

ROOT = Path('results/nonuniform_20261111/teacher_analysis')
DOC = Path('docs/thesis/TEACHER_ANALYSIS_PREREGISTRATION_20261111.md')
EXPERIMENT = 'teacher_ablation_20261111'
DATES = queue.DATES
read, write, sha, now = queue.read, queue.write, queue.sha, queue.now


def verify_manifest():
    m = read(ROOT/'manifest.json')
    assert sha(ROOT/'manifest.json') == read(ROOT/'REGISTRATION.json')['manifest_sha256']
    for path, expected in m['hashes'].items():
        assert sha(path) == expected, path
    return m


def prepare():
    from analysis import run_dynamic_heuristics_v2 as old
    from analysis.run_load_pressure_short import verify
    from analysis.redesign_scenario import digest
    assert read(ROOT/'TESTS_PASS.json')['status'] == 'PASS'
    files = set(Path('sim').rglob('*.py')) | set(Path('analysis').rglob('*.py')) | set(Path('tests').rglob('*.py'))
    files.update([DOC, ROOT/'TESTS_PASS.json', ROOT/'tests.log', Path('main.py')])
    hashes = {str(p): sha(p) for p in files}
    protocols, jobs, configs = [], [], {}
    for date in DATES:
        path = str(old.BC/'data'/date/'six_hours/protocol.json')
        p = read(path); verify(p); protocols.append(path); hashes[path] = sha(path)
        hashes.update(p['input_hashes']); hashes.update(p['frozen_protocol']['source_hashes'])
        hashes.update(p['frozen_protocol']['implementation_hashes'])
        configs[date] = {'v3_dynamic': digest(old.configuration(p, 'v3_dynamic'))}
        baseline = Path('results/nonuniform_20261111/gate1/results')/f'{date}_baseline.json'
        hashes[str(baseline)] = sha(baseline)
        for seed in (1701, 1702, 1703):
            q = 'q2' if seed == 1701 else 'q1'
            result = old.MAIN/'mappo_20261022/validation'/q/str(seed)/f'{date}.json'
            hashes[str(result)] = sha(result)
        jobs.append(dict(id=f'{date}_heft_cooperative', job=['TEACHER_ABLATION',path,'heft_cooperative'],
                         output=str(ROOT/'results'/f'{date}_heft_cooperative.json'), legacy_host='local', hosts=['local']))
    frozen = old.MAIN/'fixed_mappo_frozen_20261025/manifest.json'
    hashes[str(frozen)] = sha(frozen)
    hashes.update(read(frozen)['frozen_files'])
    for path, expected in hashes.items(): assert sha(path) == expected, path
    write(ROOT/'manifest.json', dict(hashes=hashes, protocols=protocols, config_hashes=configs, jobs=jobs))
    write(ROOT/'REGISTRATION.json', dict(at=now(), manifest_sha256=sha(ROOT/'manifest.json'),
        spec=dict(id=EXPERIMENT, deadline=time.time()+7*86400, hosts={'local':3}, jobs=jobs),
        no_retry=True, final_tests_sealed=True, estimated_run_hours=[1,2]))
    print({'registration_sha256':sha(ROOT/'REGISTRATION.json'), 'files':len(hashes), 'tasks':3})


def execute(job, host):
    started = time.time()
    try:
        from analysis import run_dynamic_heuristics_v2 as old
        from analysis.teacher_ablation_20261111 import HeftCooperativeThreePlan
        m = verify_manifest(); assert job in m['jobs'] and host == 'local'
        old.ROOT = ROOT/'heft_cooperative'
        old.verify_manifest = verify_manifest
        old.DynamicThreePlan = HeftCooperativeThreePlan
        short = old.job((job['job'][1], 'v3_dynamic'))
        if short['status'] != 'PASS': return dict(status='FAILED', legacy=short, job=job['id'])
        result = read(short['path'])
        result.update(stage=job['job'][0], method=job['job'][2], host=host, job_id=job['id'],
            runtime=queue.runtime(), registration_sha256=sha(ROOT/'REGISTRATION.json'),
            manifest_sha256=sha(ROOT/'manifest.json'),
            started_utc=datetime.fromtimestamp(started, timezone.utc).isoformat(),
            finished_utc=now(), adapter_wall_seconds=time.time()-started)
        verify_manifest(); write(job['output'], result)
        return dict(status='PASS', path=job['output'], sha256=sha(job['output']))
    except Exception:
        failure = dict(status='FAILED', job=job['id'], traceback=traceback.format_exc())
        write(ROOT/'failures'/f"{job['id']}.json", failure)
        return failure


def configure():
    queue.ROOT = ROOT
    queue.EXPERIMENT = EXPERIMENT
    queue.HOSTS = {'local':3}
    queue.verify_manifest = verify_manifest
    queue.execute = execute


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare','publish','worker','status','verify'])
    args = parser.parse_args(); configure()
    if args.mode == 'prepare': prepare()
    elif args.mode == 'publish': queue.publish()
    elif args.mode == 'worker': queue.worker('local')
    elif args.mode == 'verify': print({'files':len(verify_manifest()['hashes'])})
    else: print(queue.api('/status', exp=EXPERIMENT))
