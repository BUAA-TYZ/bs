"""CGS差距登记、同版本重放和独立报价队列，不开启最终测试。"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import random
import time
import traceback

from analysis import run_nonuniform_gate1_20261111 as queue

ROOT=Path('results/nonuniform_20261111/cgs_gap')
DOC=Path('docs/thesis/CGS_GAP_PREREGISTRATION_20261111.md')
DATES=queue.DATES
read,write,sha,now=queue.read,queue.write,queue.sha,queue.now


def folder(kind):
    return ROOT/kind


def experiment(kind):
    return f'cgs_gap_{kind}_20261111'


def verify_manifest(kind='replay'):
    root=folder(kind);m=read(root/'manifest.json')
    assert sha(root/'manifest.json')==read(root/'REGISTRATION.json')['manifest_sha256']
    for path,expected in m['hashes'].items():assert sha(path)==expected,path
    return m


def prepare_replay():
    from analysis import run_dynamic_heuristics_v2 as old
    from analysis.run_load_pressure_short import verify
    from analysis.redesign_scenario import digest
    assert read(ROOT/'TESTS_PASS.json')['status']=='PASS'
    rng=random.Random(20261111);samples=[];population=[];protocols=[];jobs=[];configs={}
    files=set(Path('sim').rglob('*.py'))|set(Path('analysis').rglob('*.py'))|set(Path('tests').rglob('*.py'))
    files.update([DOC,ROOT/'TESTS_PASS.json',ROOT/'tests.log',Path('main.py')])
    hashes={str(path):sha(path) for path in files}
    for date in DATES:
        protocol=str(old.BC/'data'/date/'six_hours/protocol.json');p=read(protocol);verify(p)
        protocols.append(protocol);hashes[protocol]=sha(protocol)
        hashes.update(p['input_hashes']);hashes.update(p['frozen_protocol']['source_hashes'])
        hashes.update(p['frozen_protocol']['implementation_hashes'])
        baseline=str(Path('results/nonuniform_20261111/gate1/results')/f'{date}_baseline.json')
        r=read(baseline);hashes[baseline]=sha(baseline)
        strata=defaultdict(list);wanted=set(p['scored_task_ids'])
        for decision in r['decisions']:
            if decision['task_id'] in wanted and decision['selected'] in ('cgs_4','cgs_16'):
                strata[decision['windows']].append(dict(date=date,**decision))
        for windows in (9,64,100):
            rows=sorted(strata[windows],key=lambda d:int(d['task_id'].split('_')[-1]))
            assert len(rows)>=11
            population.append(dict(date=date,windows=windows,count=len(rows)))
            for row in rng.sample(rows,11):
                row['id']=f"{date}_{row['task_id']}";samples.append(row)
        configs[date]={'v3_dynamic':digest(old.configuration(p,'v3_dynamic'))}
        jobs.append(dict(id=f'{date}_capture',job=['CGS_GAP_REPLAY',protocol,'capture'],
            output=str(ROOT/'replay/results'/f'{date}.json'),legacy_host='local',hosts=['local']))
    write(ROOT/'SAMPLES.json',dict(seed=20261111,stratum_order='date then 9,64,100; numeric task IDs',
        population=population,per_stratum=11,samples=samples))
    hashes[str(ROOT/'SAMPLES.json')]=sha(ROOT/'SAMPLES.json')
    write(folder('replay')/'manifest.json',dict(hashes=hashes,protocols=protocols,config_hashes=configs,jobs=jobs))
    write(folder('replay')/'REGISTRATION.json',dict(at=now(),manifest_sha256=sha(folder('replay')/'manifest.json'),
        spec=dict(id=experiment('replay'),deadline=time.time()+7*86400,hosts={'local':3},jobs=jobs),
        final_tests_sealed=True,no_retry=True,estimated_replay_hours=[1,1.5],
        enumeration_estimate='May exceed two hours; estimate from frozen snapshot quote timing before publication'))
    print(dict(samples=len(samples),population=sum(r['count'] for r in population),
               registration_sha256=sha(folder('replay')/'REGISTRATION.json')))


def capture(job,host):
    from analysis import run_dynamic_heuristics_v2 as old
    from analysis.nonuniform_granularity_20261111 import NonuniformEnv
    from analysis.cgs_gap_capture_20261111 import capturing_policy,save_snapshot
    path=job['job'][1];date=read(path)['date'];saved=[]
    samples=[s for s in read(ROOT/'SAMPLES.json')['samples'] if s['date']==date]
    def save(snapshot):
        output=ROOT/'snapshots'/f"{snapshot['sample']['id']}.pkl.gz"
        saved.append(save_snapshot(output,snapshot))
        write(ROOT/'capture_progress'/f'{date}.json',dict(at=now(),captured=saved),exclusive=False)
    old.ROOT=ROOT/'replay/full_run';old.verify_manifest=lambda:verify_manifest('replay')
    old.GranularityEnv=NonuniformEnv
    old.DynamicThreePlan=lambda env:capturing_policy(env,samples,save)
    short=old.job((path,'v3_dynamic'))
    assert short['status']=='PASS',short
    result=read(short['path']);reference=read(Path('results/nonuniform_20261111/gate1/results')/f'{date}_baseline.json')
    keys=('action_trace_sha256','decisions','outcomes','resources','granularity','R600','config_sha256')
    for key in keys:assert result[key]==reference[key],('deterministic_replay_mismatch',date,key)
    assert len(saved)==len(samples)==33
    assert {s['sample']['id'] for s in saved}=={s['id'] for s in samples}
    return dict(replay_checks={key:True for key in keys},snapshots=saved,
                original_record_sha256=sha(short['path']),full_result_path=short['path'])


def evaluate(job,host):
    from analysis.cgs_gap_20261111 import load_snapshot,enumerate_snapshot,local_search
    entry=job['entry'];snapshot=load_snapshot(entry['snapshot']['path'],entry['snapshot']['sha256'])
    raw=ROOT/'enumeration/raw'/f"{job['id']}.jsonl.gz";raw.parent.mkdir(parents=True,exist_ok=True)
    def progress(value):write(ROOT/'enumeration/progress'/f"{job['id']}.json",dict(at=now(),host=host,**value),exclusive=False)
    function=enumerate_snapshot if entry['plan']['status']=='ELIGIBLE' else local_search
    result=function(snapshot,entry['plan'],raw,progress)
    return dict(**result,sample=entry['snapshot']['sample'],plan=entry['plan'],
                raw_path=str(raw),raw_sha256=sha(raw),snapshot_sha256=entry['snapshot']['sha256'])


def execute(job,host):
    started=time.time();kind='replay' if job['job'][0]=='CGS_GAP_REPLAY' else 'enumeration'
    try:
        m=verify_manifest(kind);assert job in m['jobs'] and host in job['hosts']
        result=capture(job,host) if kind=='replay' else evaluate(job,host)
        verify_manifest(kind)
        record=dict(status='PASS',stage=job['job'][0],protocol=job['job'][1],method=job['job'][2],
            host=host,job_id=job['id'],runtime=queue.runtime(),
            registration_sha256=sha(folder(kind)/'REGISTRATION.json'),
            manifest_sha256=sha(folder(kind)/'manifest.json'),
            started_utc=datetime.fromtimestamp(started,timezone.utc).isoformat(),finished_utc=now(),
            wall_seconds=time.time()-started,**result)
        write(job['output'],record)
        return dict(status='PASS',path=job['output'],sha256=sha(job['output']))
    except Exception:
        failure=dict(status='FAILED',job=job['id'],traceback=traceback.format_exc())
        write(folder(kind)/'failures'/f"{job['id']}.json",failure)
        return failure


def configure(kind):
    queue.ROOT=folder(kind);queue.EXPERIMENT=experiment(kind)
    queue.HOSTS=read(folder(kind)/'REGISTRATION.json')['spec']['hosts']
    queue.verify_manifest=lambda:verify_manifest(kind);queue.execute=execute


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['prepare','publish','worker','status','verify'])
    p.add_argument('--kind',choices=['replay','enumeration'],default='replay');p.add_argument('--host',default='local')
    args=p.parse_args()
    if args.mode=='prepare':
        assert args.kind=='replay';prepare_replay()
    else:
        configure(args.kind)
        if args.mode=='publish':
            verify_manifest(args.kind)
            spec=read(folder(args.kind)/'REGISTRATION.json')['spec']
            created=queue.api('/experiments',spec=spec)
            released={h:queue.api('/release',exp=experiment(args.kind),host=h,completed={},evidence={'registration_sha256':sha(folder(args.kind)/'REGISTRATION.json')}) for h in spec['hosts']}
            write(folder(args.kind)/'PUBLICATION.json',dict(at=now(),created=created,released=released))
        elif args.mode=='worker':queue.worker(args.host)
        elif args.mode=='verify':print(dict(files=len(verify_manifest(args.kind)['hashes'])))
        else:print(queue.api('/status',exp=experiment(args.kind)))
