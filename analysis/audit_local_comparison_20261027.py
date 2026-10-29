"""独立复算42份验证记录及固定/动态两套跨指标包络。"""
from collections import Counter
import json
import math
from pathlib import Path

from analysis.audit_dynamic_heuristics_20261026 import read, sha, close, mean, percentile, DEADLINES
from analysis.run_load_pressure_short import verify
from analysis.run_main_local_panel import verify_main

MAIN=Path('results/redesign_20261021/main_scenario')
ROOT=MAIN/'local_comparison_20261027'
OUT=ROOT/'independent_audit_20261027'


def dump(p,v): p.write_text(json.dumps(v,ensure_ascii=False,indent=2,allow_nan=False)+'\n')


def main():
    assert not OUT.exists()
    assert sha(ROOT/'manifest.json')=='fdc69a7b77eb9943d3eba62df344a728f2bea5a4f1116cae5d0948207160066a'
    m=read(ROOT/'manifest.json'); f=read(ROOT/'final_status.json'); old=read(ROOT/'summary.json')
    assert f['status']=='COMPLETE' and len(f['results'])==21 and f['reused_records']==21
    verify_main()
    for p,h in {**m['hashes'],**read(ROOT/'FINAL_AUDIT.json')['hashes']}.items(): assert sha(p)==h,p
    for r in f['results']: assert r['status']=='PASS' and sha(r['path'])==r['sha256']
    protocols=[read(p) for p in m['protocols']]
    for p in protocols: verify(p)
    daily={}; aggregates={}; curves={}; provenance={}; tail={}
    for method in m['baselines']+m['extras']:
        daily[method]=[]; sets=[]; provenance[method]=[]
        for p in protocols:
            date=p['date']; path=m['reused'][date].get(method,str(ROOT/'validation'/date/f'{method}.json'))
            r=read(path); assert r['status']=='PASS' and r['checks']['passed']
            provenance[method].append(dict(path=path,sha256=sha(path),reused=method in m['reused'][date]))
            ids=set(p['scored_task_ids']); rows=[o for o in r['outcomes'] if o['task_id'] in ids]
            assert len(rows)==len(ids) and {o['task_id'] for o in rows}==ids
            assert p['sim_duration_s']-p['score_end_s']>=600
            assert all(o['observed_age_seconds']>=600 and p['score_start_s']<=o['release_seconds']<p['score_end_s'] for o in rows)
            done=[o['latency_seconds'] for o in rows if o['status']=='completed']
            a=dict(date=date,n=len(rows),P=[sum(o['status']=='completed' and o['latency_seconds']<=d for o in rows)/len(rows) for d in DEADLINES],
                R600=mean([min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in rows]),
                mean_completed_s=mean(done),P50_completed_s=percentile(done,.5),P90_completed_s=percentile(done,.9),unfinished=len(rows)-len(done))
            for d,v,c in zip(DEADLINES,a['P'],r['curve'],strict=True):
                close(v,c['P_lower']); assert c['D_seconds']==d and c['unknown_count']==0
                assert set(c['on_time_ids'])=={o['task_id'] for o in rows if o['status']=='completed' and o['latency_seconds']<=d}
            close(a['R600'],r['R600'])
            for k in ('mean_completed_s','P50_completed_s','P90_completed_s','unfinished'): close(a[k],r['latency'][k])
            a['resources']=None
            if 'resources' in r:
                cfg=p['config']; helpers=set(range(cfg['num_sats']))-set(cfg['task_source_sat_ids'])
                capacity=sum(g['bandwidth_mbps']/8*g.get('receive_channels',1) for g in cfg['ground_stations'])*1e6
                for z in r['resources'].values():
                    busy=math.fsum(v for sid,v in z['helper_busy_by_node'].items() if int(sid) in helpers)
                    close(busy,z['helper_busy_s']); close(busy/(len(helpers)*z['duration_s']),z['helper_utilization'])
                    close(z.get('raw_ground_bytes',0)+z.get('result_ground_bytes',0),z['ground_bytes'])
                    close(z['ground_bytes']/(capacity*z['duration_s']),z['ground_channel_utilization'])
                    close(math.fsum(z.get(k,0) for k in ('control_bytes','raw_isl_bytes','result_isl_bytes','raw_ground_bytes','result_ground_bytes')),z['total_transmitted_bytes'])
                for k in ('duration_s','total_transmitted_bytes','control_bytes','helper_busy_s','ground_bytes'):
                    close(sum(r['resources'][seg][k] for seg in ('warmup','main','followup')),r['resources']['whole'][k])
                a['resources']=r['resources']['main']
                assert set(r['granularity'])==ids
                assert all(g['requested']==int(method.rsplit('_',1)[1]) and g['actual']<=g['requested'] and g['packet_mb']>=g['unique_mb']-1e-9 for g in r['granularity'].values())
                a['actual_block_counts']=dict(Counter(g['actual'] for g in r['granularity'].values()))
            if method in m['extras']:
                assert r['information']==dict(hops=2,ttl_s=15,estimator='zero',public_contact_plan=True)
            tail[f'{method}_{date}']=[o for o in rows if o['status']!='completed' or o['latency_seconds']>300]
            daily[method].append(a); sets.append(rows)
        ds=daily[method]; a={k:mean([r[k] for r in ds]) for k in ('R600','mean_completed_s','P50_completed_s','P90_completed_s')}
        a.update(P=[mean([r['P'][i] for r in ds]) for i in range(7)],unfinished=sum(r['unfinished'] for r in ds))
        for k,v in old['day_equal'][method].items():
            if k=='P':
                for x,y in zip(a[k],v,strict=True):close(x,y)
            else:close(a[k],v)
        a['resources']=({k:mean([r['resources'][k] for r in ds]) for k in ('total_transmitted_bytes','helper_utilization','ground_channel_utilization')}
            if all(r['resources'] is not None for r in ds) else None)
        aggregates[method]=a
        xs=sorted({60.,600.,*[o['latency_seconds'] for rows in sets for o in rows if o['status']=='completed' and 60<=o['latency_seconds']<=600]})
        ys=[mean([sum(o['status']=='completed' and o['latency_seconds']<=d for o in rows)/len(rows) for rows in sets]) for d in xs]
        curves[method]=dict(D=xs,P=ys); original=read(ROOT/'full_curves.json')[method]
        assert xs==original['D']
        for x,y in zip(ys,original['P'],strict=True):close(x,y)
    envelopes={}; comparisons={}
    for scope,methods in m['comparison_scopes'].items():
        assert len(methods)==(6 if scope=='fixed_six_local_16' else 12)
        env={}
        for metric in ('P60','mean_completed_s','P600','R600'):
            values={method:(aggregates[method]['P'][0 if metric=='P60' else -1] if metric in ('P60','P600') else aggregates[method][metric]) for method in methods}
            extreme=(max if metric in ('P60','P600') else min)(values.values())
            env[metric]=dict(value=extreme,methods=[k for k,v in values.items() if v==extreme])
            close(extreme,old['envelopes'][scope][metric]['value']);assert env[metric]['methods']==old['envelopes'][scope][metric]['methods']
        envelopes[scope]=env
        source=MAIN/'fixed_mappo_frozen_20261025/manifest.json' if scope=='fixed_six_local_16' else MAIN/'dynamic_learning_20261026/BEST_CHECKPOINTS.json'
        selected=read(source);selected=selected['selected'] if scope=='fixed_six_local_16' else selected
        comparisons[scope]={}
        for seed,s in selected.items():
            assert sha(s['checkpoint'])==s['checkpoint_sha256']
            a=s['metrics']; tests=dict(P60=a['P'][0]>env['P60']['value'],mean_completed_s=a['mean_completed_s']<env['mean_completed_s']['value'],
                P600=a['P'][-1]>=env['P600']['value'],R600=a['R600']<env['R600']['value'])
            comparisons[scope][seed]=dict(episodes=s['episodes'],metrics=a,criteria=tests,all_success=all(tests.values()),
                source=str(source),source_sha256=sha(source),checkpoint_sha256=s['checkpoint_sha256'])
    OUT.mkdir(); result=dict(status='PASS',manifest_sha256=sha(ROOT/'manifest.json'),daily=daily,day_equal=aggregates,
        envelopes=envelopes,comparisons=comparisons,provenance=provenance,tests_sealed=True,
        limits=['Old 16 rule resource counters unavailable, not zero','Resource sums from passive counters, not packet replay','Orin four-block peak memory not measured'])
    dump(OUT/'AUDIT.json',result);dump(OUT/'full_curves.json',curves);dump(OUT/'TAIL_RECORDS.json',tail)
    lines=['# 局部规则粒度与CGS/HEFT独立审计','',
        '21项新增任务全PASS，21份复用记录核验通过，42份三日验证完整。冻结输入/复用映射/配置/评分ID/600随访、P阶梯曲线、R600/交付时延及新记录资源计数器通过独立复算。此为验证结果，正式测试继续封存。','',
        '|方法|P60/90/120/180/240/300/600 (%)|已交付均值/P50/P90 s|R600 s|未完成|传输GB|计算星/地面利用率|',
        '|---|---|---|---:|---:|---:|---|']
    for method,a in aggregates.items():
        z=a['resources']; traffic=f"{z['total_transmitted_bytes']/1e9:.3f}" if z else 'N/A'
        util=f"{z['helper_utilization']:.3%}/{z['ground_channel_utilization']:.3%}" if z else 'N/A'
        lines.append(f'|{method}|'+ '/'.join(f'{v*100:.3f}' for v in a['P'])+f"|{a['mean_completed_s']:.3f}/{a['P50_completed_s']:.3f}/{a['P90_completed_s']:.3f}|{a['R600']:.3f}|{a['unfinished']}|{traffic}|{util}|")
    for scope,env in envelopes.items():
        lines+=['',f'## {scope}','', '|指标|包络阈值|来源|','|---|---:|---|']
        for metric,v in env.items(): lines.append(f"|{metric}|{v['value']:.9f}|{', '.join(v['methods'])}|")
        lines+=['','|种子|选中片段|P60严格更高|均值严格更低|P600不下降|R600严格更低|四项全部通过|','|---|---:|---|---|---|---|---|']
        for seed,c in comparisons[scope].items():
            flags=['通过' if v else '未通过' for v in c['criteria'].values()]
            lines.append(f"|{seed}|{c['episodes']}|"+'|'.join(flags)+f"|{'是' if c['all_success'] else '否'}|")
    lines+=['','包络先各方法三日等权，再按指标分别取最好值，不假定存在同一条基线同时实现所有阈值。固定口径保持原六项16，动态口径为十二项4/16。不能把固定1703通过旧口径转述为动态1703通过新口径。正式测试将用测试基线重新计算，不沿用这里的阈值。',
        '', '局部CGS/HEFT作为更广同信息参考独立报告，不加入既定六/十二成功包络。它们仅使用2跳通告、15秒TTL、未知先验及公共计划；不修改原排序/调度逻辑。已交付均值可能受600秒后完成的长尾影响，未完成不能当零时延。',
        '', 'P、R600、已交付统计按日等权，分位数是每日分位数等权；未完成为三日合计。资源为主窗口全部传输，含重叠、多跳、控制和结果。旧16规则没有该资源记录，显示N/A；不能解释为0。新保存计数器只核验分类与分段守恒，不宣称独立逐包物理重放。',
        '', '完整阶梯曲线见full_curves.json与curves.png；超过300秒或未完成逐图证据见TAIL_RECORDS.json，未作隔离因果归因。Orin上4块峰值内存仍未实测，仿真执行测试不代表实机内存可行。']
    (OUT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,2,figsize=(14,5),layout='constrained')
    for ax,methods,title in zip(axes,[m['baselines'],m['extras']],['Local rules: 4 / 16','Local CGS / HEFT']):
        for method in methods:
            c=curves[method];ax.step(c['D'],[v*100 for v in c['P']],where='post',label=method,linestyle='--' if method.endswith('_4') else '-')
        ax.set(title=title,xlabel='Deadline (s)',ylabel='On-time (%)',xlim=(60,600),ylim=(20,100));ax.grid(alpha=.2);ax.legend(fontsize=8)
    fig.savefig(OUT/'curves.png',dpi=150);plt.close(fig)
    dump(OUT/'HASHES.json',{str(p):sha(p) for p in [Path(__file__),*OUT.iterdir()]})
    print(json.dumps(dict(status='PASS',envelopes=envelopes,criteria={s:{k:v['criteria'] for k,v in rows.items()} for s,rows in comparisons.items()})))


if __name__=='__main__':main()
