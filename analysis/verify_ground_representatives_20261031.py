"""独立由outcomes重算主表、代表包络和配对区间。"""
import numpy as np
from pathlib import Path
from analysis import run_ground_representatives_20261031 as r
from analysis.verify_s_formal_report_20261029 import raw_metrics, close
from analysis.verify_ground_scaling_20261029 import check_pair

KEYS=('P60','P600','mean_completed_s','R600','unfinished')
LOCAL=('ground_16','local_16','minload_2_4','local_heft_16','local_cgs_16','zero_2_16')


def mean(xs):
    return None if any(x is None for x in xs) else sum(xs)/len(xs)


def main():
    r.verify();out=r.ROOT/'report';a=r.read(out/'FINAL_AUDIT.json')
    hashes=r.read(out/'HASHES.json')
    for path,h in {**hashes,**a['provenance']}.items():assert r.sha(path)==h,path
    ownership=r.read(r.ROOT/'DISTRIBUTED_AUDIT.json')['records']
    assert len(ownership)==45 and len({v['job_id'] for v in ownership.values()})==45
    for item in r.read(r.SPEC)['jobs']:
        row=r.read(item['output']);proof=ownership[item['output']]
        assert proof['job_id']==item['id'] and proof['sha256']==r.sha(item['output'])
        assert proof['owner'].split(':')[0] in r.queue.HOSTS
        assert row['registration_sha256']==r.sha(r.REG)
        assert row['supplement_adapter_sha256']==r.sha(r.__file__)
        assert [row['stage'],row['protocol'],row['method']]==item['job']
    days={};values={};seen=set();logical=0
    for scale in 'SML':
        days[scale]={};values[scale]={}
        for setting in ('single','scaled'):
            days[scale][setting]={};values[scale][setting]={}
            bydate=a['record_paths'][scale][setting]
            expected=set(r.original.METHODS)|(set(r.METHODS) if scale!='S' else set())
            assert all(set(bydate[d])==expected for d in r.original.DATES)
            for method in expected:
                dvals={}
                for date in r.original.DATES:
                    path=bydate[date][method];seen.add(path);logical+=1
                    row=r.read(path);p=r.read(row['protocol']);ids=set(p['scored_task_ids'])
                    rows=[o for o in row['outcomes'] if o['task_id'] in ids]
                    assert row['checks']['passed'] and row['status']=='PASS'
                    assert len(rows)==len(ids) and {o['task_id'] for o in rows}==ids
                    assert all(o['observed_age_seconds']>=600 for o in rows)
                    assert row['config_sha256']==r.original.digest(r.original.configuration(p,method))
                    ref=r.read(r.read(bydate[date]['zero_2_16'])['protocol'])
                    assert all(p[k]==ref[k] for k in ('config','scored_task_ids','score_start_s','score_end_s','sim_duration_s','date'))
                    dvals[date]=raw_metrics(rows)
                    for k,v in dvals[date].items():close(v,a['daily'][scale][setting][method][date][k])
                    for seg,v in row['resources'].items():
                        helpers=set(range(p['config']['num_sats']))-set(p['config']['task_source_sat_ids'])
                        close(sum(v['helper_busy_by_node'].get(str(s),0) for s in helpers),v['helper_busy_s'])
                        close(v['helper_busy_s']/(len(helpers)*v['duration_s']),v['helper_utilization'])
                        close(sum(v.get(k,0) for k in ('control_bytes','raw_isl_bytes','result_isl_bytes','raw_ground_bytes','result_ground_bytes')),v['total_transmitted_bytes'])
                        if 'stations' in v:
                            close(sum(x['bytes'] for x in v['stations'].values()),v['ground_bytes'])
                            for g in p['config']['ground_stations']:
                                station=v['stations'][g['id']]
                                close(station['bytes']/(g['bandwidth_mbps']/8*1e6*g.get('receive_channels',1)*v['duration_s']),station['utilization'])
                                assert 0<=station['utilization']<=1.000001
                        if 'ground_pool_busy_s' in v:assert 0<=v['ground_pool_busy_s']<=v['duration_s']+1e-6
                    for k in ('ground_bytes','helper_busy_s','total_transmitted_bytes'):
                        close(sum(row['resources'][s][k] for s in ('warmup','main','followup')),row['resources']['whole'][k])
                days[scale][setting][method]=dvals
                values[scale][setting][method]={k:sum(dvals[d][k] for d in r.original.DATES) if k in ('unfinished','n')
                    else mean([dvals[d][k] for d in r.original.DATES]) for k in next(iter(dvals.values()))}
                for k,v in values[scale][setting][method].items():close(v,a['values'][scale][setting][method][k])
            for family in ('fixed','dynamic'):
                ms=[f'{family}_{s}' for s in (1701,1702,1703)]
                values[scale][setting][family]={k:mean([values[scale][setting][m][k] for m in ms]) for k in KEYS}
                days[scale][setting][family]={d:{k:mean([days[scale][setting][m][d][k] for m in ms]) for k in KEYS} for d in r.original.DATES}
                for k,v in values[scale][setting][family].items():close(v,a['values'][scale][setting][family][k])
    assert len(seen)==a['unique_records']==372 and logical==a['logical_records']==432
    assert a['record_paths']['S']['single']==a['record_paths']['S']['scaled']
    gaps=r.read(out/'REPRESENTATIVE_GAPS.json');changes=r.read(out/'SCENARIO_PAIRED.json');pairs=r.read(out/'MODEL_PAIRS.json')
    indices=np.random.default_rng(20261027).integers(0,3,size=(10000,3));count=0
    for scale in 'SML':
        for method in days[scale]['single']:
            for k in KEYS:
                check_pair(changes[scale][method][k]['paired'],[days[scale]['scaled'][method][d][k] for d in r.original.DATES],
                           [days[scale]['single'][method][d][k] for d in r.original.DATES],indices);count+=1
                x,y=values[scale]['scaled'][method][k],values[scale]['single'][method][k]
                close(None if x is None or y is None else x-y,changes[scale][method][k]['aggregate_difference'])
        for setting in ('single','scaled'):
            for model,bs in pairs[scale][setting].items():
                for b,pair in bs.items():
                    for k in KEYS:
                        check_pair(pair[k],[days[scale][setting][model][d][k] for d in r.original.DATES],
                                   [days[scale][setting][b][d][k] for d in r.original.DATES],indices);count+=1
        for family in ('fixed','dynamic'):
            g=gaps[scale][family];assert g['scope']==list(LOCAL)
            for k in ('P60','R600'):
                if scale=='S':
                    assert g['single'][k] is g['scaled'][k] is g['change'][k] is None
                    continue
                expected=[]
                for setting in ('single','scaled'):
                    v=values[scale][setting];best=(max if k=='P60' else min)(v[b][k] for b in LOCAL)
                    close(g[setting][k]['best'],best)
                    assert g[setting][k]['sources']==[b for b in LOCAL if v[b][k]==best]
                    expected.append(v[family][k]-best);close(g[setting][k]['difference'],expected[-1])
                close(g['change'][k],expected[1]-expected[0])
    assert count==2310,count
    r.write(out/'INDEPENDENT_AUDIT.json',dict(status='PASS',unique_records=372,logical_records=432,new_records=45,
        intervals=count,provenance_hashes=len(a['provenance']),report_hashes=len(hashes),report_hashes_sha256=r.sha(out/'HASHES.json')))
    print('REPRESENTATIVE INDEPENDENT AUDIT PASS',count)


if __name__=='__main__':main()
