"""清理后的验证日冒烟；独立登记、HTTP领取，原科学入口保持不变。"""
import argparse
import base64
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

ROOT = Path('results/nonuniform_20261111/cleanup/smoke')
PARENT = Path('results/redesign_20261021/main_scenario/bc_stage2_20261022/data/2026-10-22/six_hours/protocol.json')
EXPERIMENT = 'cleanup_smoke_20261111'
URL = 'http://10.251.250.34:18765'
METHOD = 'v3_dynamic'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path, obj):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open('x') as stream:
        json.dump(obj, stream, ensure_ascii=False, indent=2)
        stream.write('\n')


def api(route, **body):
    token = (Path.home() / '.local/state/bs-evaluation-queue/token').read_text().strip()
    request = urllib.request.Request(URL + route, data=json.dumps(body).encode(),
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=20) as response:
        return json.load(response)


def prepare():
    from analysis import run_dynamic_heuristics_v2 as runner
    from analysis.run_load_pressure_short import verify
    from analysis.redesign_scenario import digest
    assert not ROOT.exists()
    original = read(PARENT)
    verify(original)
    protocol = deepcopy(original)
    start = original['score_start_s']
    end = start + 3600
    duration = end + 600
    config = protocol['config']
    end_step = round(end / config['dt'])
    arrivals = config['scheduled_task_arrivals']
    assert len(arrivals) == len(protocol['events'])
    retained = [(a, e) for a, e in zip(arrivals, protocol['events']) if a['release_step'] < end_step]
    assert retained == list(zip(arrivals, protocol['events']))[:len(retained)]
    config['scheduled_task_arrivals'] = [a for a, _ in retained]
    config['sim_steps'] = round(duration / config['dt'])
    config['task_arrival_end_step'] = end_step
    protocol.update(score_end_s=end, sim_duration_s=duration, followup_s=600,
        events=[e for _, e in retained], scored_task_ids=[e['task_id'] for _, e in retained if e['scored']],
        config_sha256=digest(config), parent_protocol=str(PARENT), parent_sha256=sha(PARENT),
        peak={'scope': 'First hour of the already registered validation six-hour window; no new peak selection.'})
    protocol['input_hashes'][str(PARENT)] = sha(PARENT)
    verify(protocol)
    hashes = {str(p): sha(p) for p in [PARENT, Path(__file__), Path('analysis/run_dynamic_heuristics_v2.py'),
        Path('analysis/evaluation_queue.py'), Path('docs/CLEANUP_20261111.md'),
        Path('docs/thesis/NONUNIFORM_AMENDMENT_20261111.md')]}
    hashes.update(original['frozen_protocol']['source_hashes'])
    hashes.update(original['frozen_protocol']['implementation_hashes'])
    protocol_path = ROOT / 'protocol.json'
    write(protocol_path, protocol)
    hashes[str(protocol_path)] = sha(protocol_path)
    manifest = dict(hashes=hashes, config_hashes={original['date']:{METHOD:digest(runner.configuration(protocol, METHOD))}})
    write(ROOT / 'manifest.json', manifest)
    output = ROOT / 'QUEUE_RESULT.json'
    spec = dict(id=EXPERIMENT, deadline=time.time()+7200, hosts={'local':1,'devbox1':6,'devbox2':6,'devbox3':6},
        jobs=[dict(id='M_validation_2026-10-22_v3_dynamic_1h', job=['M_cleanup', str(protocol_path), METHOD],
            output=str(output), legacy_host='local', hosts=['local'])])
    write(ROOT / 'REGISTRATION.json', dict(at=datetime.now(timezone.utc).isoformat(),
        purpose='Post-cleanup dependency smoke, not Gate 1 and not a scientific comparison.',
        date=original['date'], tasks=1, score_window_s=3600, warmup_s=start, followup_s=600,
        scored_images=len(protocol['scored_task_ids']), estimated_wall_minutes=[5,30],
        allocation='One Mac slot through the existing devbox3 HTTP queue; other hosts idle because only one local-cleanup smoke is needed.',
        no_retry=True, new_final_test_dates_opened=False, manifest_sha256=sha(ROOT/'manifest.json'), spec=spec))
    print(json.dumps({'status':'REGISTERED','images':len(protocol['scored_task_ids']), 'sha256':sha(ROOT/'REGISTRATION.json')}))


def execute():
    from analysis import run_dynamic_heuristics_v2 as runner
    # 只重定向输出和清单；原job的冻结输入、守恒和随访检查原样执行。
    runner.ROOT = ROOT
    short = runner.job((str(ROOT/'protocol.json'), METHOD))
    write(ROOT/'JOB_STATUS.json', short)
    if short['status'] != 'PASS':
        raise RuntimeError('Smoke failed; preserved JOB_STATUS and progress, no retry')
    result = read(short['path'])
    result['stage'] = 'M_cleanup'
    write(ROOT/'QUEUE_RESULT.json', result)


def run():
    registration = read(ROOT/'REGISTRATION.json')
    assert sha(ROOT/'manifest.json') == registration['manifest_sha256']
    for path, expected in read(ROOT/'manifest.json')['hashes'].items():
        assert sha(path) == expected, path
    assert not (ROOT/'LAUNCH.json').exists()
    started = time.time()
    owner = f'local:cleanup:{os.getpid()}'
    write(ROOT/'LAUNCH.json', dict(owner=owner, pid=os.getpid(), started_at=datetime.now(timezone.utc).isoformat()))
    published = api('/experiments', spec=registration['spec'])
    released = api('/release', exp=EXPERIMENT, host='local', completed={}, evidence={'registration_sha256':sha(ROOT/'REGISTRATION.json')})
    claim = api('/claim', exp=EXPERIMENT, host='local', owner=owner, capacity=1, request='cleanup-smoke-once')
    assert len(claim['jobs']) == 1
    write(ROOT/'CLAIM.json', dict(published=published,released=released,claim=claim))
    job = claim['jobs'][0]
    with (ROOT/'worker.log').open('x') as log:
        child = subprocess.Popen([sys.executable,'-B','-m','analysis.smoke_cleanup_20261111','execute'],
            stdout=log, stderr=subprocess.STDOUT, env={**os.environ, 'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'})
        while child.poll() is None:
            if time.time() >= registration['spec']['deadline']:
                child.terminate()
                child.wait()
                break
            try:
                api('/heartbeat', exp=EXPERIMENT, owner=owner)
            except Exception as exc:
                print('Heartbeat unavailable:', type(exc).__name__, flush=True)
            time.sleep(10)
    output = Path(job['output'])
    if child.returncode == 0 and output.exists():
        short = dict(status='PASS',path=str(output),sha256=sha(output))
        blob = base64.b64encode(output.read_bytes()).decode()
    else:
        short = dict(status='FAILED',returncode=child.returncode,log=str(ROOT/'worker.log'))
        blob = None
    saved = api('/finish', exp=EXPERIMENT, jid=job['id'], owner=owner, short=short, blob=blob)
    status = api('/status', exp=EXPERIMENT)
    write(ROOT/'QUEUE_AUDIT.json', dict(saved=saved,status=status,wall_seconds=time.time()-started))
    print(json.dumps({'status':short['status'],'wall_seconds':time.time()-started}))
    assert status['complete'] and status['counts'] == {'PASS':1}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode',choices=('prepare','run','execute'))
    args = parser.parse_args()
    {'prepare':prepare,'run':run,'execute':execute}[args.mode]()
