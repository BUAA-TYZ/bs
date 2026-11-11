"""独立核验原始记录、时间图见证、逐图支配与报告数值；不重跑策略。"""
from collections import defaultdict
import gzip
import json
import math
from pathlib import Path

from analysis import run_teacher_analysis_20261111 as a
from analysis import run_teacher_bounds_20261111 as b

ROOT=a.ROOT


def node(value):
    return tuple(value) if isinstance(value,list) else value


def assert_close(left,right):
    assert math.isclose(left,right,rel_tol=1e-10,abs_tol=1e-8),(left,right)


def independent_metrics(latencies,actual):
    points={str(d):len([v for v in latencies if v is not None and v<=d])/len(latencies) for d in (60,120,600)}
    total=0.
    for value in latencies:total+=600 if value is None or value>600 else value
    return {**{f'P{k}':v for k,v in points.items()},'R600':total/len(latencies),
            'unfinished':len([x for x in latencies if x is None]) if actual else None}


def audit_witness(result,protocol):
    assert a.sha(result['contacts_path'])==result['contacts_sha256']
    contacts=json.loads(gzip.decompress(Path(result['contacts_path']).read_bytes()))
    intervals=defaultdict(list)
    for source,target,start,end in contacts:
        assert 0<=start<=end<=protocol['sim_duration_s']+1e-8
        intervals[node(source),node(target)].append((start,end))
    ids=set(protocol['scored_task_ids']);rows=result['lower']
    assert len(rows)==len(ids) and {r['task_id'] for r in rows}==ids
    count=0
    cfg=protocol['config'];ground={('ground',g['id']) for g in cfg['ground_stations']}
    for row in rows:
        event=cfg['scheduled_task_arrivals'][int(row['task_id'].split('_')[-1])]
        released=event['release_step']*cfg['dt'];assert_close(released,row['release_seconds'])
        assert released+600<=protocol['sim_duration_s']+1e-8
        service=min(*cfg['onboard_compute_profile']['service_s'].values(),
                    *(event['image_tiling']['compute_cost_per_window']/g['compute_rate'] for g in cfg['ground_stations']))
        fixed=cfg['ground_relay_delay_s']+event.get('aggregation_delay_s',cfg['aggregation_delay_s'])
        assert_close(service,row['service_floor_s']);assert_close(fixed,row['fixed_merge_s'])
        if row['latency_seconds'] is None:
            assert not row['witness'];assert row['R600_contribution']==600;continue
        current=event['source_sat_id'];ready=released;computed=0
        for edge in row['witness']:
            source,target=node(edge['source']),node(edge['target'])
            assert source==current;assert_close(edge['start'],ready)
            assert edge['end']>=ready
            if edge['kind']=='compute':
                assert source==target;assert_close(edge['end']-ready,service);computed+=1
            else:
                assert edge['kind']=='contact'
                assert any(start-1e-8<=edge['end']<=end+1e-8 for start,end in intervals[source,target])
            current,ready=target,edge['end'];count+=1
        assert computed==1 and current in ground
        assert_close(ready-released+fixed,row['latency_seconds'])
        assert_close(min(row['latency_seconds'],600),row['R600_contribution'])
        assert_close(row['latency_seconds'],min(v for v in row['relaxed_modes'].values() if v is not None))
    return dict(contact_intervals=len(contacts),images=len(rows),witness_edges=count,
                full_600_coverage=True,positive_service_and_fixed_terms=True)


def main():
    manifests=[a.verify_manifest(),b.verify_manifest()];summary=a.read(ROOT/'report/summary.json')
    curves=a.read(ROOT/'report/curves.json');audits=[];queue_rows=[];result_hashes={}
    for runner,manifest in zip((a,b),manifests):
        final=a.read(runner.ROOT/'QUEUE_FINAL.json');jobs={row['id']:row for row in final['jobs']}
        assert final['counts']=={'PASS':3} and final['complete'] and not final['blocked']
        for spec in manifest['jobs']:
            q=jobs[spec['id']];r=a.read(spec['output'])
            assert q['spec']==spec and q['state']=='PASS' and q['owner'].startswith('local:')
            assert q['result']['sha256']==a.sha(spec['output'])
            assert [r['stage'],r['protocol'],r['method']]==spec['job']
            assert r['registration_sha256']==a.sha(runner.ROOT/'REGISTRATION.json')
            assert r['manifest_sha256']==a.sha(runner.ROOT/'manifest.json')
            result_hashes[spec['output']]=a.sha(spec['output'])
            queue_rows.append(dict(job=spec['id'],owner=q['owner'],sha256=q['result']['sha256']))
    source_by_day={};paired_count=0;dominance_count=0
    for date,path in zip(a.DATES,manifests[0]['protocols']):
        p=a.read(path);ids=p['scored_task_ids'];source_by_day[date]={}
        bound=a.read(ROOT/'bounds'/f'{date}.json');audits.append(dict(date=date,**audit_witness(bound,p)))
        lower={r['task_id']:r['latency_seconds'] for r in bound['lower']}
        for name in ('teacher','heft','F_1701','F_1702','F_1703','reference','lower'):
            if name in ('reference','lower'):
                rows={r['task_id']:r for r in bound[name]};vals=[rows[tid]['latency_seconds'] for tid in ids]
                if name=='reference':
                    for row in rows.values():
                        finite=[v for v in row['quotes'].values() if v is not None]
                        assert row['latency_seconds']==(min(finite) if finite else None)
                        for count,blocks in row['blocks'].items():
                            expected=max(x['quote_s'] for x in blocks) if all(x['quote_s'] is not None for x in blocks) else None
                            assert row['quotes'][f'cooperative_{count}']==expected
            else:
                if name=='teacher':source=Path('results/nonuniform_20261111/gate1/results')/f'{date}_baseline.json'
                elif name=='heft':source=ROOT/'results'/f'{date}_heft_cooperative.json'
                else:source=Path('results/redesign_20261021/main_scenario/mappo_20261022/validation')/('q2' if name=='F_1701' else 'q1')/name[2:]/f'{date}.json'
                r=a.read(source);assert r['checks']['passed']
                indexed={o['task_id']:o for o in r['outcomes']};assert len(indexed)==len(r['outcomes'])
                assert all(indexed[tid]['observed_age_seconds']>=600 for tid in ids)
                vals=[indexed[tid]['latency_seconds'] if indexed[tid]['status']=='completed' else None for tid in ids]
                for tid,actual in zip(ids,vals):
                    optimistic=lower[tid]
                    cap_lower=600 if optimistic is None else min(600,optimistic)
                    cap_actual=600 if actual is None else min(600,actual)
                    assert cap_lower<=cap_actual+1e-8,(date,tid,name,cap_lower,cap_actual)
                    dominance_count+=1
                if name=='heft':
                    for key in ('total_transmitted_bytes','control_bytes','raw_isl_bytes','raw_ground_bytes','result_isl_bytes','result_ground_bytes'):
                        assert_close(r['resources']['whole'].get(key,0),sum(r['resources'][part].get(key,0) for part in ('warmup','main','followup')))
            source_by_day[date][name]=vals
            metrics=independent_metrics(vals,name not in ('reference','lower'))
            for key,value in metrics.items():
                saved=summary['daily'][date][name][key]
                if value is None:assert saved is None
                else:assert_close(value,saved)
            paired_count+=len(vals)
        for key in ('P60','P120','P600','R600','unfinished'):
            assert_close(summary['daily'][date]['F'][key],sum(summary['daily'][date][f'F_{s}'][key] for s in (1701,1702,1703))/3)
    for name,ys in curves['P'].items():
        assert len(ys)==len(curves['D_seconds']) and all(x<=y+1e-12 for x,y in zip(ys,ys[1:]))
        arms=[f'F_{s}' for s in (1701,1702,1703)] if name=='F' else [name]
        for d,actual in zip(curves['D_seconds'],ys):
            expected=sum(sum(v is not None and v<=d for v in source_by_day[date][arm])/len(source_by_day[date][arm])
                         for date in a.DATES for arm in arms)/(3*len(arms))
            assert_close(actual,expected)
    for name,values in summary['day_equal'].items():
        for key,value in values.items():
            rows=[summary['daily'][d][name][key] for d in a.DATES]
            if value is None:assert all(x is None for x in rows)
            else:assert_close(value,sum(rows) if key in ('unfinished','unquoted_or_no_path') else sum(rows)/3)
    for path,expected in summary['input_hashes'].items():assert a.sha(path)==expected,path
    import numpy as np
    indices=np.random.default_rng(20261111).integers(0,3,size=(10000,3))
    interval_checks=0
    for label,entries in summary['comparisons'].items():
        left,right=label.split('_minus_')
        for metric,entry in entries.items():
            delta=np.array([summary['daily'][date][left][metric]-summary['daily'][date][right][metric] for date in a.DATES])
            assert np.allclose(delta,entry['daily'],rtol=0,atol=1e-10)
            expected=np.percentile(delta[indices].mean(axis=1),[2.5,97.5])
            assert np.allclose(expected,entry['paired_day_mean_95'],rtol=0,atol=1e-10)
            assert_close(entry['difference'],summary['day_equal'][left][metric]-summary['day_equal'][right][metric])
            interval_checks+=1
    a.write(ROOT/'DISTRIBUTED_AUDIT.json',dict(status='PASS',tasks=6,unique_ids=len({j['job'] for j in queue_rows}),jobs=queue_rows))
    a.write(ROOT/'INDEPENDENT_AUDIT.json',dict(status='PASS',at=a.now(),
        frozen_hashes_checked=[len(m['hashes']) for m in manifests],paired_records_checked=paired_count,
        per_image_capped_dominance_checks=dominance_count,geometry=audits,
        curve_points_checked=sum(len(v) for v in curves['P'].values()),result_hashes=result_hashes,
        paired_day_intervals_checked=interval_checks,
        proof_scope='Registered simulator, zero serialization, independent windows; only D<=600 and R600',
        reference_is_not_bound=True,reference_unavailable_is_not_actual_unfinished=True))
    files=[ROOT/'REPORT.md',ROOT/'INDEPENDENT_AUDIT.json',ROOT/'DISTRIBUTED_AUDIT.json',
           *sorted((ROOT/'report').glob('*'))]
    hashes={str(path):a.sha(path) for path in files}
    a.write(ROOT/'REPORT_HASHES.json',hashes)
    a.write(ROOT/'REPORT_COMPLETE.json',dict(status='PASS',at=a.now(),hashes_sha256=a.sha(ROOT/'REPORT_HASHES.json'),
                                            audit_sha256=a.sha(ROOT/'INDEPENDENT_AUDIT.json')))
    print({'status':'PASS','dominance_checks':dominance_count,'curve_points':sum(len(v) for v in curves['P'].values())})


if __name__=='__main__':main()
