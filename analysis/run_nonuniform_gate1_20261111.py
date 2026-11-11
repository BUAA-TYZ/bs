"""关卡1六项同版本配对评估；不改旧入口，只适配候选、输出和队列身份。"""
import argparse
import base64
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import sys
import time
import traceback
import urllib.error
import urllib.request
import uuid

ROOT = Path('results/nonuniform_20261111/gate1')
EXPERIMENT = 'nonuniform_gate1_20261111'
DOC = Path('docs/thesis/NONUNIFORM_GATE1_IMPLEMENTATION_20261111.md')
DATES = ('2026-10-22','2026-10-23','2026-10-24')
ARMS = ('baseline','nu')
HOSTS = {'local':6}
ASSIGN = {(d,a):'local' for d in DATES for a in ARMS}
URL = 'http://10.251.250.34:18765'


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path,value,exclusive=True):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x' if exclusive else 'w') as f:
        json.dump(value,f,ensure_ascii=False,indent=2,allow_nan=False);f.write('\n')


def now():
    return datetime.now(timezone.utc).isoformat()


def runtime():
    import numpy, torch
    return dict(python=sys.version,platform=platform.platform(),machine=platform.machine(),
        numpy=numpy.__version__,torch=torch.__version__,cpu_count=os.cpu_count())


def api(route,retry=False,**body):
    token=(Path.home()/'.local/state/bs-evaluation-queue/token').read_text().strip()
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while True:
        try:
            request=urllib.request.Request(URL+route,data=json.dumps(body).encode(),
                headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
            with opener.open(request,timeout=30) as response:return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code<500 or not retry:raise
        except (urllib.error.URLError,TimeoutError,OSError):
            if not retry:raise
        # 只重取相同请求；不重放仿真、不释放所有权。
        time.sleep(10)


def verify_manifest():
    registration=read(ROOT/'REGISTRATION.json');manifest=read(ROOT/'manifest.json')
    assert sha(ROOT/'manifest.json')==registration['manifest_sha256']
    for path,expected in manifest['hashes'].items():assert sha(path)==expected,path
    return manifest


def prepare():
    from analysis import run_dynamic_heuristics_v2 as old
    from analysis.run_load_pressure_short import verify
    from analysis.redesign_scenario import digest
    assert not (ROOT/'REGISTRATION.json').exists()
    tests=read(ROOT/'TESTS_PASS.json');assert tests['status']=='PASS'
    paths=[str(old.BC/'data'/d/'six_hours/protocol.json') for d in DATES]
    files=set(Path('analysis').rglob('*.py')) | set(Path('sim').rglob('*.py')) | set(Path('tests').rglob('*.py'))
    files.update([Path('main.py'),DOC,ROOT/'TESTS_PASS.json',ROOT/'tests_final.log',ROOT/'HOST_PREFLIGHT.json',
        Path('docs/thesis/NONUNIFORM_PREREGISTRATION_20261111.md'),Path('docs/thesis/NONUNIFORM_AMENDMENT_20261111.md')])
    hashes={str(p):sha(p) for p in files}
    configs={}
    for path in paths:
        p=read(path);verify(p);hashes[path]=sha(path)
        hashes.update(p['input_hashes']);hashes.update(p['frozen_protocol']['source_hashes'])
        hashes.update(p['frozen_protocol']['implementation_hashes'])
        configs[p['date']]={'v3_dynamic':digest(old.configuration(p,'v3_dynamic'))}
    for path,expected in hashes.items():assert sha(path)==expected,path
    jobs=[]
    for date,path in zip(DATES,paths):
        for arm in ARMS:
            jid=f'{date}_{arm}'
            jobs.append(dict(id=jid,job=['GATE1',path,arm],output=str(ROOT/'results'/f'{jid}.json'),
                legacy_host='local',hosts=[ASSIGN[date,arm]]))
    write(ROOT/'manifest.json',dict(hashes=hashes,protocols=paths,config_hashes=configs,jobs=jobs))
    # 队列要求有限领取期限；七天仅是调度保护，不用于选择或截断科学结果。
    spec=dict(id=EXPERIMENT,deadline=time.time()+7*86400,hosts=HOSTS,jobs=jobs)
    write(ROOT/'REGISTRATION.json',dict(at=now(),manifest_sha256=sha(ROOT/'manifest.json'),spec=spec,
        tasks=6,no_retry=True,final_tests_sealed=True,estimated_run_hours=[2,5],
        implementation_start_utc='2026-11-11T09:17:14Z',scope='Same-code paired validation, no training'))
    print(json.dumps({'status':'REGISTERED','sha256':sha(ROOT/'REGISTRATION.json'),'files':len(hashes)}))


def collect_blocks(env,decisions):
    rows=[]
    for d in decisions:
        if d['selected']!='c_nu':continue
        for tid in env.tasks[d['task_id']].tile_ids:
            tile=env.tiles[tid];end=tile.timestamps.end_downlink
            actual=None if end is None else end*env.cfg.dt-d['time_seconds']
            rows.append(dict(task_id=d['task_id'],tile_id=tid,windows=list(tile.windows_xyxy),
                input_mb=tile.data_size_mb,compute_cost=tile.compute_cost,
                result_mb=tile.actual_result_size_mb,state=tile.state.name,
                decision_s=d['time_seconds'],actual_finish_after_decision_s=actual,
                allocation_prediction_s=d['nu']['allocation_prediction_s'][tid],
                calendar_prediction_s=d['nu']['calendar_prediction_s'][tid]))
    return rows


def execute(job,host):
    started=time.time()
    try:
        from analysis import run_dynamic_heuristics_v2 as old
        from analysis.nonuniform_granularity_20261111 import NonuniformEnv,NonuniformThreePlan
        m=verify_manifest();assert job in m['jobs'] and host in job['hosts']
        _,path,arm=job['job'];captured=[]
        def make_env(cfg):
            env=NonuniformEnv(cfg);captured.append(env);return env
        old.ROOT=ROOT/arm
        old.verify_manifest=verify_manifest
        old.GranularityEnv=make_env
        old.DynamicThreePlan=lambda env:NonuniformThreePlan(env,enabled=arm=='nu')
        short=old.job((path,'v3_dynamic'))
        if short['status']!='PASS':return dict(status='FAILED',legacy=short,job=job['id'])
        result=read(short['path']);env=captured[0]
        result.update(stage='GATE1',method=arm,host=host,job_id=job['id'],runtime=runtime(),
            registration_sha256=sha(ROOT/'REGISTRATION.json'),manifest_sha256=sha(ROOT/'manifest.json'),
            blocks=collect_blocks(env,result['decisions']),started_utc=datetime.fromtimestamp(started,timezone.utc).isoformat(),
            finished_utc=now(),adapter_wall_seconds=time.time()-started)
        verify_manifest();write(job['output'],result)
        return dict(status='PASS',path=job['output'],sha256=sha(job['output']))
    except Exception:
        failure=dict(status='FAILED',job=job['id'],traceback=traceback.format_exc())
        write(ROOT/'failures'/f"{job['id']}.json",failure)
        return failure


def publish():
    verify_manifest();r=read(ROOT/'REGISTRATION.json')
    write(ROOT/'VALIDATION_OPENED.json',dict(at=now(),registration_sha256=sha(ROOT/'REGISTRATION.json')))
    created=api('/experiments',spec=r['spec'])
    released=api('/release',exp=EXPERIMENT,host='local',completed={},evidence={'registration_sha256':sha(ROOT/'REGISTRATION.json')})
    write(ROOT/'PUBLICATION.json',dict(created=created,released=released))


def worker(host):
    verify_manifest();owner=f'{host}:gate1:{os.getpid()}'
    folder=ROOT/'workers'/host
    write(folder/'launch.json',dict(at=now(),pid=os.getpid(),owner=owner,command=sys.argv,runtime=runtime()))
    active={};completed=[];blocked=False
    with ProcessPoolExecutor(max_workers=HOSTS[host],mp_context=multiprocessing.get_context('spawn'),max_tasks_per_child=1) as pool:
        while True:
            api('/heartbeat',retry=True,exp=EXPERIMENT,owner=owner)
            for future,job in list(active.items()):
                if not future.done():continue
                try:short=future.result()
                except Exception:short=dict(status='FAILED',traceback=traceback.format_exc(),job=job['id'])
                blob=base64.b64encode(Path(short['path']).read_bytes()).decode() if short['status']=='PASS' else None
                api('/finish',retry=True,exp=EXPERIMENT,jid=job['id'],owner=owner,short=short,blob=blob)
                blocked |= short['status']!='PASS';completed.append(short);del active[future]
            if not blocked and len(active)<HOSTS[host]:
                claimed=api('/claim',retry=True,exp=EXPERIMENT,host=host,owner=owner,
                    capacity=HOSTS[host]-len(active),request=str(uuid.uuid4()))
                blocked |= bool(claimed.get('blocked'))
                for job in claimed['jobs']:active[pool.submit(execute,job,host)]=job
            status=api('/status',retry=True,exp=EXPERIMENT)
            own=[j for j in status['jobs'] if host in j['spec']['hosts']]
            write(folder/'status.json',dict(at=now(),owner=owner,active=[j['id'] for j in active.values()],
                completed=completed,counts=status['counts'],blocked=status['blocked']),exclusive=False)
            if not active and (blocked or all(j['state'] in ('PASS','FAILED') for j in own)):break
            time.sleep(10)
    write(folder/'final_status.json',dict(at=now(),status='BLOCKED' if blocked else 'COMPLETE',completed=completed))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=('prepare','publish','worker','status','verify'))
    parser.add_argument('--host',choices=HOSTS);args=parser.parse_args()
    if args.mode=='prepare':prepare()
    elif args.mode=='publish':publish()
    elif args.mode=='worker':worker(args.host)
    elif args.mode=='verify':print(json.dumps({'status':'PASS','files':len(verify_manifest()['hashes']),'runtime':runtime()}))
    else:print(json.dumps(api('/status',exp=EXPERIMENT)))
