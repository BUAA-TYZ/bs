"""仅复核冻结记录；不执行环境、不覆盖原面板汇总。"""
from collections import Counter
import hashlib
import json
import math
from pathlib import Path

from analysis.run_load_pressure_short import verify
from analysis.run_main_local_panel import verify_main

ROOT = Path('results/redesign_20261021/main_scenario/dynamic_heuristics_v2_20261025')
OUT = ROOT/'independent_audit_20261026'
BC = ROOT.parent/'bc_stage2_20261022'
DEADLINES = (60,90,120,180,240,300,600)


def read(p):return json.loads(Path(p).read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def close(a,b):assert math.isclose(a,b,rel_tol=1e-9,abs_tol=1e-7),(a,b)
def mean(a):return math.fsum(a)/len(a)


def percentile(xs,q):
    v=sorted(xs);at=(len(v)-1)*q;lo=int(at);hi=math.ceil(at)
    return v[lo]+(v[hi]-v[lo])*(at-lo)


def main():
    assert not OUT.exists(), 'Preserve audit evidence'
    assert sha(ROOT/'manifest.json')=='256333abd75099a264a217745621e042a4d5fc2739a66fc4224f9cc05e78278a'
    m=read(ROOT/'manifest.json');final=read(ROOT/'final_status.json')
    assert final['status']=='COMPLETE' and len(final['results'])==21
    verify_main()
    for path,h in {**m['hashes'],**read(ROOT/'FINAL_AUDIT.json')['hashes']}.items():assert sha(path)==h,path
    for r in final['results']:assert r['status']=='PASS' and sha(r['path'])==r['sha256']
    protocols=[read(p) for p in m['protocols']]
    for p in protocols:verify(p)
    summary=read(ROOT/'summary.json');curves=read(ROOT/'full_curves.json')
    daily={};aggregates={};selected=Counter();actual=Counter();byday=[]
    for method in m['methods']:
        daily[method]=[];sets=[]
        for p in protocols:
            r=read(ROOT/'validation'/p['date']/f'{method}.json')
            assert r['status']=='PASS' and r['checks']['passed']
            ids=set(p['scored_task_ids']);rows=[o for o in r['outcomes'] if o['task_id'] in ids]
            assert len(rows)==len(ids) and {o['task_id'] for o in rows}==ids
            assert p['sim_duration_s']-p['score_end_s']>=600
            assert all(p['score_start_s']<=o['release_seconds']<p['score_end_s'] and o['observed_age_seconds']>=600 for o in rows)
            done=[o['latency_seconds'] for o in rows if o['status']=='completed']
            ps=[sum(o['status']=='completed' and o['latency_seconds']<=d for o in rows)/len(rows) for d in DEADLINES]
            r600=mean([min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in rows])
            for d,v,old in zip(DEADLINES,ps,r['curve']):
                close(v,old['P_lower']);assert old['D_seconds']==d and old['unknown_count']==0
                assert set(old['on_time_ids'])=={o['task_id'] for o in rows if o['status']=='completed' and o['latency_seconds']<=d}
            close(r600,r['R600']);close(mean(done),r['latency']['mean_completed_s'])
            close(percentile(done,.5),r['latency']['P50_completed_s']);close(percentile(done,.9),r['latency']['P90_completed_s'])
            assert len(rows)-len(done)==r['latency']['unfinished']
            sources=set(p['config']['task_source_sat_ids']);helpers=set(range(p['config']['num_sats']))-sources
            assert len(helpers)==144
            for name,z in r['resources'].items():
                duration=z['duration_s']
                busy=math.fsum(v for sid,v in z['helper_busy_by_node'].items() if int(sid) in helpers)
                close(busy,z['helper_busy_s']);close(busy/(144*duration),z['helper_utilization'])
                close(z.get('raw_ground_bytes',0)+z.get('result_ground_bytes',0),z['ground_bytes'])
                close(z['ground_bytes']/(50e6*duration),z['ground_channel_utilization'])
                parts=[z.get(k,0) for k in ('control_bytes','raw_isl_bytes','result_isl_bytes','raw_ground_bytes','result_ground_bytes')]
                close(math.fsum(parts),z['total_transmitted_bytes'])
            for key in ('duration_s','total_transmitted_bytes','control_bytes','helper_busy_s','ground_bytes'):
                close(sum(r['resources'][seg][key] for seg in ('warmup','main','followup')),r['resources']['whole'][key])
            assert set(r['granularity'])==ids
            if method=='v3_16':
                original=read(BC/'validation'/p['date']/'v3.json')
                assert original['outcomes']==r['outcomes'] and original['action_trace_sha256']==r['action_trace_sha256']
            if method=='v3_dynamic':
                decisions=[d for d in r['decisions'] if d['task_id'] in ids]
                assert len(decisions)==len(ids) and len({d['task_id'] for d in decisions})==len(ids)
                counts=Counter(d['selected'] for d in decisions);assert dict(counts)==r['choices']
                selected.update(counts)
                local_actual=Counter('raw_ground' if d['selected']=='ground' else f"onboard_{d['actual']}" for d in decisions)
                actual.update(local_actual);byday.append(dict(date=p['date'],n=len(ids),requested=dict(counts),actual=dict(local_actual)))
                order=('local_4','local_16','cgs_4','cgs_16','ground')
                for d in decisions:
                    q=d['quotes_seconds'];winner=min(order,key=lambda k:q[k] if q[k] is not None else math.inf)
                    if q[winner] is None:winner='cgs_16'
                    assert winner==d['selected']
                    g=r['granularity'][d['task_id']]
                    assert g['requested']==d['requested'] and g['actual']==d['actual']
                    close(g['packet_mb'],d['packet_mb']);close(g['packet_mb']-g['unique_mb'],d['overlap_mb'])
            z=r['resources']['main']
            daily[method].append(dict(date=p['date'],n=len(rows),P=ps,R600=r600,completed_mean_s=mean(done),
                P50_s=percentile(done,.5),P90_s=percentile(done,.9),unfinished=len(rows)-len(done),
                total_GB=z['total_transmitted_bytes']/1e9,helper_utilization=z['helper_utilization'],
                ground_utilization=z['ground_channel_utilization']))
            sets.append(rows)
        a=daily[method]
        aggregate={k:mean([v[k] for v in a]) for k in ('R600','completed_mean_s','P50_s','P90_s','total_GB','helper_utilization','ground_utilization')}
        aggregate.update(P=[mean([v['P'][i] for v in a]) for i in range(7)],unfinished=sum(v['unfinished'] for v in a))
        old=summary['day_equal'][method]
        for i,v in enumerate(aggregate['P']):close(v,old['P'][i])
        for k in ('R600','helper_utilization'):close(aggregate[k],old[k])
        close(aggregate['total_GB']*1e9,old['main_total_bytes']);close(aggregate['ground_utilization'],old['ground_utilization'])
        for d,y in zip(curves[method]['deadlines'],curves[method]['P'],strict=True):
            close(y,mean([sum(o['status']=='completed' and o['latency_seconds']<=d for o in oo)/len(oo) for oo in sets]))
        aggregates[method]=aggregate
    total=sum(selected.values());assert total==1014
    requested={'onboard_4':selected['local_4']+selected['cgs_4'],'onboard_16':selected['local_16']+selected['cgs_16'],'raw_ground':selected['ground']}
    result=dict(status='PASS',source_manifest_sha256=sha(ROOT/'manifest.json'),frozen_input_files_checked=len(m['hashes']),
        daily=daily,day_equal=aggregates,requested_counts=requested,actual_counts=dict(actual),choice_details=dict(selected),
        requested_fraction_pooled={k:v/total for k,v in requested.items()},actual_fraction_pooled={k:v/total for k,v in actual.items()},
        choice_by_date=byday,tests_sealed=True,
        resource_audit_limit='Independent sums from saved passive counters; no packet-level event log or new simulation replay',
        orin_four_block_peak_memory_verified=False)
    OUT.mkdir()
    (OUT/'AUDIT.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    lines=['# 启发式动态粒度：独立复核报告','','21/21 PASS；三日固定16与原v3完整动作hash、逐图outcomes精确一致。输入哈希、评分ID、600秒随访、全阶梯曲线及保存的资源计数器复核通过。测试继续封存。','',
        '|方法|P60/90/120/180/240/300/600 (%)|R600 s|已交付均值/P50/P90 s|未完成|传输GB|计算星/地面利用率|',
        '|---|---|---:|---|---:|---:|---|']
    for method,a in aggregates.items():
        lines.append('|'+method+'|'+'/'.join(f'{v*100:.2f}' for v in a['P'])+f"|{a['R600']:.2f}|{a['completed_mean_s']:.2f}/{a['P50_s']:.2f}/{a['P90_s']:.2f}|{a['unfinished']}|{a['total_GB']:.3f}|{a['helper_utilization']:.3%}/{a['ground_utilization']:.3%}|")
    lines+=['','P、R600、已交付统计与资源均三日等权；P50/P90为每日分位数的等权平均；未完成数为三日总数。资源是21600秒主窗口全部流量的平均，含多跳、包间重叠、结果与控制。',
        '',f'动态请求粒度（1014图）：{requested}；实际调度包数：{dict(actual)}。原图地面单独计，不算星上4/16。小图在请求16时可能只有4个非空包，不能把请求比例当实际比例。',
        '', '4块在最小负载/强地面下改善尾部，但固定4块v3的P60下降；动态报价略改善v3的P60/R600，并非所有指标全面最优。不能从当前验证效果修改已登记奖励或动作。',
        '', '本地4/16也参与报价，因为结果按包完成后回传，粒度影响就绪流水线；推理窗口与总计算量不变。4块包含包内去重和窗口结果合并，包间冗余仍计入传输。Orin端到端4块峰值内存未实测。',
        '', '资源独立复算依赖已保存的被动计数器，核对分类守恒、分段相加及节点忙时；未保存逐包事件流，因此不宣称完成独立逐包物理重放。完整每日表和选择比例见AUDIT.json，完整阶梯曲线见../full_curves.json。']
    (OUT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(10,6),layout='constrained')
    for method,c in curves.items():ax.step(c['deadlines'],[v*100 for v in c['P']],where='post',label=method)
    ax.set(xlabel='Deadline (s)',ylabel='On-time delivery (%)',xlim=(60,600),ylim=(0,100));ax.legend();ax.grid(alpha=.2)
    fig.savefig(OUT/'curves.png',dpi=150);plt.close(fig)
    (OUT/'HASHES.json').write_text(json.dumps({str(p):sha(p) for p in (Path(__file__),OUT/'AUDIT.json',OUT/'REPORT.md',OUT/'curves.png')},indent=2)+'\n')
    print(json.dumps(dict(status='PASS',day_equal=aggregates,requested=requested,actual=dict(actual)),indent=2))


if __name__=='__main__':main()
