"""仅根据冻结样本及报价计时登记枚举作业，不按差距挑样本。"""
import argparse
from collections import Counter
import itertools
import math
from pathlib import Path
import time

from analysis import run_cgs_gap_20261111 as r
from analysis.cgs_gap_20261111 import candidates,load_snapshot,assignments,quote,neighbors


def snapshots():
    saved=[]
    for date in r.DATES:
        p=r.ROOT/'capture_progress'/f'{date}.json'
        if p.exists():saved.extend(r.read(p)['captured'])
    return sorted(saved,key=lambda row:row['sample']['id'])


def pilot_one(row):
    output=r.ROOT/'timing'/f"{row['sample']['id']}.json"
    if output.exists():return r.read(output)
    s=load_snapshot(row['path'],row['sha256']);started=time.monotonic();plan=candidates(s)
    candidate_seconds=time.monotonic()-started
    if plan['status']=='NO_FINITE_CGS_QUOTE':
        result=dict(snapshot=row,plan=plan,seconds_per_quote=None,candidate_seconds=candidate_seconds)
    else:
        positions=plan['positions'] if plan['status']=='ELIGIBLE' else plan['local_search_positions']
        if plan['status']=='ELIGIBLE':
            generator=assignments(plan['blocks'],len(positions));count=plan['planned_quotes']
        else:
            original=tuple(positions.index(p) for p in plan['cgs'])
            generator=iter(neighbors(original,len(positions)));count=len(neighbors(original,len(positions)))
        wanted={i*(count-1)//63 for i in range(64)} if count>=64 else set(range(count))
        records=[];elapsed=0.
        for index,assignment in enumerate(generator):
            if index not in wanted:continue
            start=time.perf_counter();value=quote(s,[positions[i] for i in assignment],plan['ordered'])
            seconds=time.perf_counter()-start;elapsed+=seconds
            records.append(dict(index=index,quote=value,seconds=seconds))
        result=dict(snapshot=row,plan=plan,seconds_per_quote=elapsed/len(records),
            candidate_seconds=candidate_seconds,records=records,total_space=count,
            extrapolated_quote_seconds=elapsed/len(records)*count,
            selection='64 evenly spaced vector indices; timing only, never changes scientific search')
    result.update(at=r.now(),source_sha256=r.sha(__file__))
    r.write(output,result)
    return result


def pilot():
    registration=r.ROOT/'PILOT_REGISTRATION.json'
    if not registration.exists():
        r.write(registration,dict(at=r.now(),source_sha256=r.sha(__file__),
            rule='Earliest sampled decision in each date/window stratum, once available; 64 evenly spaced vector indices'))
    assert r.sha(__file__)==r.read(registration)['source_sha256']
    samples=r.read(r.ROOT/'SAMPLES.json')['samples']
    earliest={min((s for s in samples if s['date']==d and s['windows']==w),key=lambda s:(s['time_seconds'],s['id']))['id'] for d in r.DATES for w in (9,64,100)}
    for row in snapshots():
        if row['sample']['id'] in earliest:
            value=pilot_one(row)
            print(dict(id=row['sample']['id'],status=value['plan']['status'],
                       blocks=value['plan']['blocks'],positions=len(value['plan'].get('positions',[])),
                       seconds_per_quote=value['seconds_per_quote'],planned=value['plan'].get('planned_quotes')))


def prepare_enumeration():
    base=r.verify_manifest('replay')
    final=r.queue.api('/status',exp=r.experiment('replay'))
    assert final['complete'] and final['counts']=={'PASS':3} and not final['blocked']
    assert r.read(r.folder('replay')/'workers/local/final_status.json')['status']=='COMPLETE'
    saved=[]
    for job in base['jobs']:
        result=r.read(job['output']);assert all(result['replay_checks'].values())
        q=next(q for q in final['jobs'] if q['id']==job['id'])
        assert r.sha(job['output'])==q['result']['sha256']
        saved.extend(result['snapshots'])
    assert len(saved)==99 and len({s['sample']['id'] for s in saved})==99
    plans=[]
    for row in sorted(saved,key=lambda s:s['sample']['id']):
        plan=candidates(load_snapshot(row['path'],row['sha256']))
        plans.append(dict(snapshot=row,plan=plan))
    over=sum(entry['plan']['status']=='OVER_CAP' for entry in plans)
    search=over/99>0.1
    r.write(r.ROOT/'CANDIDATES.json',dict(at=r.now(),samples=plans,counts=dict(Counter(e['plan']['status'] for e in plans)),
        over_cap_fraction=over/99,local_search_triggered=search,local_search_iteration_limit=20))
    chosen=[entry for entry in plans if entry['plan']['status']=='ELIGIBLE' or (search and entry['plan']['status']=='OVER_CAP')]
    def work(entry):
        p=entry['plan']
        return p['planned_quotes'] if p['status']=='ELIGIBLE' else 20*(p['blocks']*(len(p['local_search_positions'])-1)+p['blocks']*(p['blocks']-1)//2)
    chosen.sort(key=lambda e:(-work(e),e['snapshot']['sample']['id']))
    hosts=dict(local=6,devbox1=6,devbox2=6,devbox3=6);jobs=[]
    assert len(chosen)>=len(hosts), 'Insufficient valid jobs; report eligibility before publishing'
    for index,entry in enumerate(chosen):
        sample=entry['snapshot']['sample'];method='enumerate' if entry['plan']['status']=='ELIGIBLE' else 'local_search'
        jid=f"{sample['id']}_{method}"
        protocol=base['protocols'][r.DATES.index(sample['date'])]
        jobs.append(dict(id=jid,job=['CGS_GAP_ENUMERATION',protocol,method],
            output=str(r.ROOT/'enumeration/results'/f'{jid}.json'),entry=entry,
            legacy_host=list(hosts)[index%len(hosts)],hosts=list(hosts),quote_budget_upper=work(entry)))
    hashes=dict(base['hashes'])
    for path in set(Path('analysis').rglob('*.py'))|set(Path('tests').rglob('*.py')):
        hashes[str(path)]=r.sha(path)
    paths=[r.ROOT/'CANDIDATES.json',r.folder('replay')/'REGISTRATION.json',r.folder('replay')/'manifest.json',r.ROOT/'PILOT_REGISTRATION.json']
    paths+=list((r.ROOT/'timing').glob('*.json'))
    paths+=[Path(row['path']) for row in saved]
    paths+=[Path(j['output']) for j in base['jobs']]
    for path in paths:hashes[str(path)]=r.sha(path)
    r.write(r.folder('enumeration')/'manifest.json',dict(hashes=hashes,jobs=jobs,protocols=base['protocols']))
    r.write(r.folder('enumeration')/'REGISTRATION.json',dict(at=r.now(),
        manifest_sha256=r.sha(r.folder('enumeration')/'manifest.json'),
        spec=dict(id=r.experiment('enumeration'),deadline=time.time()+7*86400,hosts=hosts,jobs=jobs),
        quote_budget_upper=sum(work(e) for e in chosen),no_retry=True,final_tests_sealed=True,
        ordering='Largest registered quote budget first, then sample ID; all workers may claim all jobs'))
    r.write(r.folder('replay')/'QUEUE_FINAL.json',final)
    print(dict(tasks=len(jobs),over_cap=over,local_search=search,quote_budget_upper=sum(work(e) for e in chosen),
               registration_sha256=r.sha(r.folder('enumeration')/'REGISTRATION.json')))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['pilot','prepare']);args=p.parse_args()
    pilot() if args.mode=='pilot' else prepare_enumeration()
