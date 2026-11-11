"""独立从逐图记录复算扩站主表、配对区间、包络和资源分母。"""
import math
import numpy as np
from PIL import Image
from analysis import run_ground_scaling_20261029 as run
from analysis.run_dynamic_heuristics_v2 import read, sha
from analysis.run_dynamic_learning import write_json
from analysis.verify_s_formal_report_20261029 import raw_metrics, close, inequality

ROOT = run.ROOT
KEYS = ('P60','mean_completed_s','P600','R600','unfinished')


def check_pair(saved, a, b, indices):
    if any(v is None for v in a+b):
        assert all(saved[k] is None for k in ('daily','mean','ci95'))
        return
    delta = np.asarray(a)-np.asarray(b)
    assert len(saved['daily']) == 3
    for v,w in zip(delta,saved['daily']):close(float(v),w)
    close(float(delta.mean()),saved['mean'])
    ci = np.quantile(delta[indices].mean(axis=1),[.025,.975])
    for v,w in zip(ci,saved['ci95']):close(float(v),w)


def main():
    out = ROOT/'report'; hashes = read(out/'REPORT_HASHES.json')
    for path,expected in hashes.items():assert sha(path)==expected,path
    m=run.verify_manifest();a=read(out/'FINAL_AUDIT.json');p=read(out/'PAIRWISE.json')
    changes=read(out/'SCENARIO_PAIRED.json');curves=read(out/'full_curves.json');best=read(out/'BEST_LOCAL.json')
    scales=a['included_scales']
    assert a['status']=='PASS' and a['logical_records']==120*len(scales)
    assert a['unique_records']==sum(60 if s=='S' else 120 for s in scales)
    assert set(scales)|set(a['missing'])=={'S','M','L'}
    for path,expected in a['provenance'].items():assert sha(path)==expected,path
    daily={};mean={};rows_by={};count=0
    for scale in scales:
        daily[scale]={};mean[scale]={};rows_by[scale]={}
        for setting in ('single','scaled'):
            daily[scale][setting]={};mean[scale][setting]={};rows_by[scale][setting]={}
            for method in run.METHODS:
                days={};rows_by[scale][setting][method]={}
                for date in run.DATES:
                    path=a['record_paths'][scale][setting][date][method];r=read(path);protocol=read(r['protocol'])
                    ids=set(protocol['scored_task_ids']);rows=[o for o in r['outcomes'] if o['task_id'] in ids]
                    assert len(rows)==len(ids) and {o['task_id'] for o in rows}==ids
                    assert all(o['observed_age_seconds']>=600 for o in rows)
                    values=raw_metrics(rows);days[date]=values;rows_by[scale][setting][method][date]=rows
                    for key,value in values.items():close(value,a['daily'][scale][setting][method][date][key])
                    for seg,resource in r['resources'].items():
                        close(sum(resource['helper_busy_by_node'].get(str(sid),0.) for sid in
                            set(range(protocol['config']['num_sats']))-set(protocol['config']['task_source_sat_ids'])),resource['helper_busy_s'])
                        stations=resource.get('stations')
                        if stations:
                            close(sum(v['bytes'] for v in stations.values()),resource['ground_bytes'])
                            for g in protocol['config']['ground_stations']:
                                v=stations[g['id']];close(v['bytes']/(g['bandwidth_mbps']*1e6/8*g.get('receive_channels',1)*resource['duration_s']),v['utilization'])
                        if 'ground_pool_busy_s' in resource:
                            assert resource['ground_pool_busy_s']<=resource['duration_s']+1e-6
                daily[scale][setting][method]=days
                table={key:None if any(days[d][key] is None for d in run.DATES) else
                    sum(days[d][key] for d in run.DATES)/(1 if key in ('unfinished','n') else 3)
                    for key in next(iter(days.values()))}
                mean[scale][setting][method]=table
                for k,v in table.items():close(v,a['day_equal'][scale][setting][method][k])
                xs=sorted({60.,600.,*[r['latency_seconds'] for rows in rows_by[scale][setting][method].values() for r in rows
                    if r['status']=='completed' and 60<=r['latency_seconds']<=600]})
                c=curves[scale][setting][method];assert c['D']==xs
                for x,y in zip(xs,c['P'],strict=True):
                    close(sum(sum(r['status']=='completed' and r['latency_seconds']<=x for r in rows)/len(rows)
                        for rows in rows_by[scale][setting][method].values())/3,y)
            for scope,baselines in run.SCOPES.items():
                saved=a['scopes'][scale][setting][scope];assert saved['baselines']==baselines
                family='fixed' if scope.startswith('fixed') else 'dynamic'
                for key in KEYS:
                    vals=[mean[scale][setting][b][key] for b in baselines]
                    value=None if any(v is None for v in vals) else (max if key in ('P60','P600') else min)(vals)
                    item=best[scale][setting][family][key]
                    close(item['value'],value)
                    assert item['baselines']==([] if value is None else [b for b in baselines if mean[scale][setting][b][key]==value])
                    if key!='unfinished':close(value,saved['envelope'][key])
                    for model in run.MAPPO:
                        if not model.startswith(family+'_'):continue
                        v=mean[scale][setting][model][key]
                        close(None if v is None or value is None else v-value,item['model_differences'][model])
                        if key!='unfinished':assert saved['criteria'][model][key]==inequality(v,value,key)
                for model,checks in saved['criteria'].items():
                    assert saved['all_pass'][model]==(None if any(v is None for v in checks.values()) else all(checks.values()))
    indices=np.random.default_rng(20261027).integers(0,3,size=(10000,3))
    for scale in scales:
        for method in run.METHODS:
            if scale=='S':
                assert a['record_paths']['S']['single']==a['record_paths']['S']['scaled']
            for k in KEYS:
                x=[daily[scale]['scaled'][method][d][k] for d in run.DATES]
                y=[daily[scale]['single'][method][d][k] for d in run.DATES]
                check_pair(changes[scale][method][k]['paired'],x,y,indices)
                v,w=mean[scale]['scaled'][method][k],mean[scale]['single'][method][k]
                close(None if v is None or w is None else v-w,changes[scale][method][k]['aggregate_difference']);count+=1
        for setting in ('single','scaled'):
            for model in run.MAPPO:
                assert set(p[scale][setting][model])==set(run.METHODS)-set(run.MAPPO)
                for baseline,record in p[scale][setting][model].items():
                    for k in KEYS:
                        check_pair(record['paired'][k],[daily[scale][setting][model][d][k] for d in run.DATES],
                            [daily[scale][setting][baseline][d][k] for d in run.DATES],indices);count+=1
                        if k!='unfinished':
                            x,y=mean[scale][setting][model][k],mean[scale][setting][baseline][k]
                            assert record['criteria'][k]==inequality(x,y,k)
                            expected='NA' if x is None or y is None else 'tie' if x==y else 'win' if ((x>y) if k in ('P60','P600') else x<y) else 'loss'
                            assert record['wins'][k]==expected
    image=np.asarray(Image.open(out/'curves.png').convert('RGB'));assert image.std()>10 and min(image.shape[:2])>500
    assert count==940*len(scales)
    write_json(out/'INDEPENDENT_AUDIT.json',dict(status='PASS',logical_records=a['logical_records'],unique_records=a['unique_records'],
        intervals=count,report_hashes=len(hashes),source_hashes=len(a['provenance']),plot_nonblank=True,
        report_hashes_sha256=sha(out/'REPORT_HASHES.json')))
    print('GROUND SCALING INDEPENDENT AUDIT PASS',count,'intervals')


if __name__=='__main__':main()
