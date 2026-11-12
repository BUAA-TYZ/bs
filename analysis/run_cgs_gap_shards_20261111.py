"""把完整枚举索引区间分片派发；最终按图合并，不改变枚举空间。"""
import argparse
import base64
from concurrent.futures import ProcessPoolExecutor
import gzip
import itertools
import json
import math
import multiprocessing
from pathlib import Path
import time
import traceback

from analysis import run_cgs_gap_20261111 as r
from analysis import cgs_gap_20261111 as core

CHUNK=5000
DOC=Path('docs/thesis/CGS_GAP_SHARDING_20261111.md')


def ranges(total, size=CHUNK):
    return [(start,min(start+size,total)) for start in range(0,total,size)]


def plan_entry(row):
    return dict(snapshot=row,plan=core.candidates(core.load_snapshot(row['path'],row['sha256'])))


def quote_range(snapshot,plan,start,end,raw_path,progress=lambda _:None):
    assert 0<=start<end<=plan['planned_quotes']
    started=time.monotonic()
    positions=[tuple(p) if isinstance(p,list) else p for p in plan['positions']]
    best=None;best_assignment=None;finite=count=0
    source=itertools.islice(core.assignments(plan['blocks'],len(positions)),start,end)
    with gzip.open(raw_path,'xt',encoding='utf8') as output:
        for index,assignment in enumerate(source,start):
            value=core.quote(snapshot,[positions[i] for i in assignment],plan['ordered'])
            output.write(json.dumps([index,value],allow_nan=False,separators=(',',':'))+'\n')
            count+=1
            if value is not None:
                finite+=1
                if best is None or value<best:
                    best,best_assignment=value,assignment
            if count%500==0:progress(dict(evaluated=count,total=end-start,wall_seconds=time.monotonic()-started))
    assert count==end-start
    if best is not None:
        assert abs(core.quote(snapshot,[positions[i] for i in best_assignment],plan['ordered'])-best)<1e-8
    return dict(start=start,end=end,evaluated=count,finite=finite,canonical_best_quote=best,
        canonical_best_assignment=best_assignment,wall_seconds=time.monotonic()-started)


def prepare():
    assert r.read(r.ROOT/'SHARD_TESTS_PASS.json')['status']=='PASS'
    assert r.read(r.ROOT/'FINAL_ENUM_TESTS_PASS.json')['status']=='PASS'
    preflights=[r.ROOT/'deployment'/f'HOST_PREFLIGHT_{host}.json' for host in ('devbox1','devbox2','devbox3')]
    for path in preflights:assert r.read(path)['status']=='PASS'
    base=r.verify_manifest('replay');final=r.queue.api('/status',exp=r.experiment('replay'))
    assert final['complete'] and final['counts']=={'PASS':3} and not final['blocked']
    assert r.read(r.folder('replay')/'workers/local/final_status.json')['status']=='COMPLETE'
    snapshots=[]
    for job in base['jobs']:
        result=r.read(job['output']);assert all(result['replay_checks'].values())
        queue_job=next(j for j in final['jobs'] if j['id']==job['id'])
        assert r.sha(job['output'])==queue_job['result']['sha256']
        snapshots.extend(result['snapshots'])
    assert len(snapshots)==99 and len({s['sample']['id'] for s in snapshots})==99
    with ProcessPoolExecutor(max_workers=3,mp_context=multiprocessing.get_context('spawn')) as pool:
        entries=list(pool.map(plan_entry,sorted(snapshots,key=lambda s:s['sample']['id'])))
    over=sum(e['plan']['status']=='OVER_CAP' for e in entries);search=over/99>0.1
    r.write(r.ROOT/'CANDIDATES.json',dict(at=r.now(),samples=entries,
        over_cap_fraction=over/99,local_search_triggered=search,local_search_iteration_limit=20))
    hosts=dict(local=6,devbox1=6,devbox2=6,devbox3=6);jobs=[]
    for entry in entries:
        sample=entry['snapshot']['sample'];plan=entry['plan']
        protocol=base['protocols'][r.DATES.index(sample['date'])]
        if plan['status']=='ELIGIBLE':
            chunks=[('enumerate',start,end) for start,end in ranges(plan['planned_quotes'])]
        elif search and plan['status']=='OVER_CAP':
            chunks=[('local_search',0,20*(plan['blocks']*(len(plan['local_search_positions'])-1)+plan['blocks']*(plan['blocks']-1)//2))]
        else:chunks=[]
        for method,start,end in chunks:
            jid=f"{sample['id']}_{method}_{start:06d}_{end:06d}"
            jobs.append(dict(id=jid,job=['CGS_GAP_ENUMERATION',protocol,method],
                output=str(r.ROOT/'enumeration/results'/f'{jid}.json'),sample_id=sample['id'],
                start=start,end=end,hosts=list(hosts),quote_budget_upper=end-start))
    jobs.sort(key=lambda j:(-j['quote_budget_upper'],j['id']))
    assert len(jobs)>=4
    for index,job in enumerate(jobs):job['legacy_host']=list(hosts)[index%len(hosts)]
    hashes=dict(base['hashes'])
    for path in set(Path('analysis').rglob('*.py'))|set(Path('tests').rglob('*.py')):
        hashes[str(path)]=r.sha(path)
    extra=[DOC,r.ROOT/'CANDIDATES.json',r.ROOT/'PILOT_REGISTRATION.json',r.ROOT/'SHARD_TESTS_PASS.json',r.ROOT/'shard_tests.log']
    extra+=[r.ROOT/'FINAL_ENUM_TESTS_PASS.json',*preflights]
    extra+=list((r.ROOT/'timing').glob('*.json'))
    extra+=[Path(row['path']) for row in snapshots]
    extra+=[Path(j['output']) for j in base['jobs']]
    extra+=[r.folder('replay')/'REGISTRATION.json',r.folder('replay')/'manifest.json']
    for path in extra:hashes[str(path)]=r.sha(path)
    r.write(r.folder('enumeration')/'manifest.json',dict(hashes=hashes,jobs=jobs,protocols=base['protocols']))
    r.write(r.folder('enumeration')/'REGISTRATION.json',dict(at=r.now(),
        manifest_sha256=r.sha(r.folder('enumeration')/'manifest.json'),
        spec=dict(id=r.experiment('enumeration'),deadline=time.time()+7*86400,hosts=hosts,jobs=jobs),
        chunk_size=CHUNK,quote_budget_upper=sum(j['quote_budget_upper'] for j in jobs),
        no_retry=True,final_tests_sealed=True,
        ordering='Descending quote budget then job ID; all hosts share all jobs'))
    r.write(r.folder('replay')/'QUEUE_FINAL.json',final)
    print(dict(samples=99,over_cap=over,local_search=search,tasks=len(jobs),
        quotes_upper=sum(j['quote_budget_upper'] for j in jobs),
        registration_sha256=r.sha(r.folder('enumeration')/'REGISTRATION.json')))


def execute(job,host):
    started=time.time()
    try:
        m=r.verify_manifest('enumeration');assert job in m['jobs'] and host in job['hosts']
        entry=next(e for e in r.read(r.ROOT/'CANDIDATES.json')['samples'] if e['snapshot']['sample']['id']==job['sample_id'])
        snapshot=core.load_snapshot(entry['snapshot']['path'],entry['snapshot']['sha256'])
        plan=entry['plan'];cgs=[tuple(p) if isinstance(p,list) else p for p in plan['cgs']]
        assert abs(core.quote(snapshot,cgs,plan['ordered'])-plan['cgs_quote'])<1e-8
        raw=r.ROOT/'enumeration/raw'/f"{job['id']}.jsonl.gz";raw.parent.mkdir(parents=True,exist_ok=True)
        def progress(value):r.write(r.ROOT/'enumeration/progress'/f"{job['id']}.json",dict(at=r.now(),host=host,**value),exclusive=False)
        if job['job'][2]=='enumerate':
            result=quote_range(snapshot,plan,job['start'],job['end'],raw,progress)
        else:result=core.local_search(snapshot,plan,raw,progress)
        r.verify_manifest('enumeration')
        value=dict(status='PASS',stage=job['job'][0],protocol=job['job'][1],method=job['job'][2],
            sample_id=job['sample_id'],host=host,job_id=job['id'],runtime=r.queue.runtime(),
            raw_path=str(raw),raw_sha256=r.sha(raw),raw_gzip_b64=base64.b64encode(raw.read_bytes()).decode(),
            snapshot_sha256=entry['snapshot']['sha256'],
            registration_sha256=r.sha(r.folder('enumeration')/'REGISTRATION.json'),
            manifest_sha256=r.sha(r.folder('enumeration')/'manifest.json'),
            finished_utc=r.now(),total_wall_seconds=time.time()-started,**result)
        r.write(job['output'],value)
        return dict(status='PASS',path=job['output'],sha256=r.sha(job['output']))
    except Exception:
        failure=dict(status='FAILED',job=job['id'],traceback=traceback.format_exc())
        r.write(r.folder('enumeration')/'failures'/f"{job['id']}.json",failure)
        return failure


def configure():
    r.configure('enumeration');r.queue.execute=execute


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['prepare','publish','worker','verify','status']);p.add_argument('--host',default='local');args=p.parse_args()
    if args.mode=='prepare':prepare()
    else:
        configure()
        if args.mode=='publish':
            r.verify_manifest('enumeration');spec=r.read(r.folder('enumeration')/'REGISTRATION.json')['spec']
            created=r.queue.api('/experiments',spec=spec)
            released={h:r.queue.api('/release',exp=r.experiment('enumeration'),host=h,completed={},
                evidence={'registration_sha256':r.sha(r.folder('enumeration')/'REGISTRATION.json')}) for h in spec['hosts']}
            r.write(r.folder('enumeration')/'PUBLICATION.json',dict(at=r.now(),created=created,released=released))
        elif args.mode=='worker':r.queue.worker(args.host)
        elif args.mode=='verify':print(dict(files=len(r.verify_manifest('enumeration')['hashes'])))
        else:print(r.queue.api('/status',exp=r.experiment('enumeration')))
