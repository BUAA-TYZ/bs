"""只读六项原始结果并独立复算指标、分组不变量、误差和关卡；不启动仿真。"""
from collections import Counter, defaultdict
import math
from pathlib import Path
import time

import numpy as np

from analysis.run_nonuniform_gate1_20261111 import ROOT,DATES,ARMS,verify_manifest,read,write,sha,now


def scored(record,protocol):
    ids=set(protocol['scored_task_ids'])
    rows=[o for o in record['outcomes'] if o['task_id'] in ids]
    assert len(rows)==len(ids)==len({o['task_id'] for o in rows})
    assert all(o['observed_age_seconds']>=600 for o in rows)
    return rows


def metrics(record,protocol):
    rows=scored(record,protocol);n=len(rows)
    completed=[o['latency_seconds'] for o in rows if o['status']=='completed']
    return {**{f'P{d}':sum(x<=d for x in completed)/n for d in (60,120,600)},
        'R600':math.fsum(min(o['latency_seconds'],600) if o['status']=='completed' else 600 for o in rows)/n,
        'unfinished':n-len(completed),'mean_completed_s':float(np.mean(completed)) if completed else None,
        'main_bytes':record['resources']['main']['total_transmitted_bytes'],
        'whole_bytes':record['resources']['whole']['total_transmitted_bytes']}


def distribution(values):
    return dict(n=len(values),median=float(np.median(values)) if values else None,
        P90=float(np.percentile(values,90)) if values else None,
        mean=float(np.mean(values)) if values else None,max=max(values) if values else None)


def diagnostics(records,protocols):
    attempts=[];selected=[];blocks=[];spreads=[]
    for r,p in zip(records,protocols):
        ids=set(p['scored_task_ids']);decisions=[d for d in r['decisions'] if d['task_id'] in ids]
        attempts.extend(d['nu'] for d in decisions)
        chosen={d['task_id']:d for d in decisions if d['selected']=='c_nu'}
        selected.extend(d['nu'] for d in chosen.values())
        grouped=defaultdict(list)
        for b in r['blocks']:
            if b['task_id'] not in chosen:continue
            blocks.append(b);grouped[b['task_id']].append(b)
        for tid,d in chosen.items():
            bb=grouped[tid];assert len(bb)==len(d['nu']['counts'])
            actual=[b['actual_finish_after_decision_s'] for b in bb]
            if all(a is not None for a in actual):spreads.append(max(actual)-min(actual))
    def mapping_summary(rows):
        return dict(n=len(rows),counts=dict(Counter(m['mapping'] for m in rows)),
            fractions={name:sum(m['mapping']==name for m in rows)/len(rows) if rows else None
                for name in ('row_snake','column_snake','long_axis_stripes')},
            selected_bytes=math.fsum(m['selected_bytes'] for m in rows),
            row_snake_bytes=math.fsum(m['row_snake_bytes'] for m in rows),
            difference_bytes=math.fsum(m['selected_bytes']-m['row_snake_bytes'] for m in rows))
    finished=[b for b in blocks if b['actual_finish_after_decision_s'] is not None]
    errors={}
    for key in ('allocation_prediction_s','calendar_prediction_s'):
        values=[b[key]-b['actual_finish_after_decision_s'] for b in finished]
        errors[key]=dict(signed_predicted_minus_actual=distribution(values),absolute=distribution([abs(v) for v in values]))
    total=sum(len(p['scored_task_ids']) for p in protocols)
    return dict(scored_images=total,attempts=len(attempts),selected=len(selected),selection_fraction=len(selected)/total,
        candidate_status=dict(Counter(m['status'] for m in attempts)),blocks_histogram=dict(Counter(len(m['counts']) for m in selected)),
        feasible_mappings=mapping_summary([m for m in attempts if m['status']=='FEASIBLE']),
        selected_mappings=mapping_summary(selected),errors=errors,actual_spread_s=distribution(spreads),
        selected_blocks=len(blocks),completed_blocks=len(finished),censored_blocks=len(blocks)-len(finished),
        spread_complete_images=len(spreads),spread_censored_images=len(selected)-len(spreads))


def audit_group_geometry(record,protocol):
    from sim.workload import ImageTiling,window_starts
    count=0
    by_task=defaultdict(list)
    for b in record['blocks']:by_task[b['task_id']].append(b)
    decisions={d['task_id']:d for d in record['decisions'] if d['selected']=='c_nu'}
    assert set(by_task)==set(decisions)
    for tid,d in decisions.items():
        event=protocol['config']['scheduled_task_arrivals'][int(tid.split('_')[-1])]
        w=ImageTiling.from_dict(event['image_tiling']);bb=by_task[tid]
        boxes=[(x,y,x+w.window_px,y+w.window_px) for y in window_starts(w.height_px,w.window_px,w.stride_px)
            for x in window_starts(w.width_px,w.window_px,w.stride_px)]
        assert Counter(tuple(box) for b in bb for box in b['windows'])==Counter(boxes)
        assert 1<=len(bb)<=16 and all(b['windows'] for b in bb)
        assert math.isclose(sum(b['compute_cost'] for b in bb),len(boxes)*w.compute_cost_per_window)
        truth=dict(zip(boxes,event['window_result_bytes'],strict=True))
        for b in bb:
            assert math.isclose(b['result_mb'],sum(truth[tuple(box)] for box in b['windows'])/1e6,abs_tol=1e-12)
            # 独立坐标压缩小格求并集，不调用实现的扫描线函数。
            expected=0
            for c in w.transport_components:
                rects=[(a//c.downsample,y//c.downsample,math.ceil(x/c.downsample),math.ceil(z/c.downsample)) for a,y,x,z in b['windows']]
                xs=sorted({x for a,y,x,z in rects for x in (a,x)})
                ys=sorted({y for a,y,x,z in rects for y in (y,z)})
                area=sum((x1-x0)*(y1-y0) for x0,x1 in zip(xs,xs[1:]) for y0,y1 in zip(ys,ys[1:])
                    if any(a<=x0 and y<=y0 and x>=x1 and z>=y1 for a,y,x,z in rects))
                expected+=math.ceil(area*c.bands*c.bits_per_sample/8)
            assert w.transport_components
            assert math.isclose(b['input_mb'],expected/w.transport_compression_ratio/1e6,abs_tol=1e-9)
            assert b['actual_finish_after_decision_s'] is None or b['actual_finish_after_decision_s']>=0
            count+=1
        assert sum(d['nu']['counts'])==len(boxes)
        assert d['nu']['selected_bytes']<=d['nu']['row_snake_bytes']+1e-7
    return count


def main():
    started=time.time();m=verify_manifest();queue=read(ROOT/'QUEUE_FINAL.json')
    assert queue['complete'] and queue['counts']=={'PASS':6} and not queue['blocked']
    assert len({j['id'] for j in queue['jobs']})==6
    by_id={j['id']:j for j in queue['jobs']};daily={};records={a:[] for a in ARMS};hashes={};audit=[]
    protocols=[read(p) for p in m['protocols']]
    for job in m['jobs']:
        q=by_id[job['id']];r=read(job['output']);path=job['job'][1];p=read(path)
        assert q['spec']==job and q['state']=='PASS' and q['owner'].split(':')[0] in job['hosts']
        assert sha(job['output'])==q['result']['sha256'];hashes[job['output']]=sha(job['output'])
        assert r['status']=='PASS' and r['checks']['passed']
        assert [r['stage'],r['protocol'],r['method']]==job['job']
        assert r['registration_sha256']==sha(ROOT/'REGISTRATION.json') and r['manifest_sha256']==sha(ROOT/'manifest.json')
        v=metrics(r,p);assert math.isclose(v['R600'],r['R600'],abs_tol=1e-10)
        for d in (60,120,600):
            point=next(x for x in r['curve'] if x['D_seconds']==d)
            assert math.isclose(point['P_lower'],v[f'P{d}'],abs_tol=1e-12)
        whole=r['resources']['whole']
        for key in ('total_transmitted_bytes','control_bytes','raw_isl_bytes','raw_ground_bytes','result_isl_bytes','result_ground_bytes'):
            expected=sum(r['resources'][part].get(key,0) for part in ('warmup','main','followup'))
            assert math.isclose(whole.get(key,0),expected,rel_tol=1e-9,abs_tol=.1)
        daily[job['id']]=v;records[r['method']].append(r)
        audit.append(dict(job=job['id'],owner=q['owner'],sha256=hashes[job['output']],geometry_blocks=audit_group_geometry(r,p)))
    averages={a:{key:(sum(daily[f'{d}_{a}'][key] for d in DATES) if key=='unfinished' else float(np.mean([daily[f'{d}_{a}'][key] for d in DATES])))
        for key in daily[f'{DATES[0]}_{a}']} for a in ARMS}
    differences={d:{k:daily[f'{d}_nu'][k]-daily[f'{d}_baseline'][k] for k in daily[f'{d}_nu']} for d in DATES}
    rng=np.random.default_rng(20261111);indices=rng.integers(0,3,size=(10000,3))
    intervals={k:np.percentile(np.array([differences[d][k] for d in DATES])[indices].mean(axis=1),[2.5,97.5]).tolist() for k in differences[DATES[0]]}
    delta={k:averages['nu'][k]-averages['baseline'][k] for k in averages['nu']}
    passed=(delta['R600']<=-2 or delta['P60']>=.01) and delta['P600']>=-.005
    historical={d:read(Path('results/redesign_20261021/main_scenario/dynamic_heuristics_v2_20261025/validation')/d/'v3_dynamic.json') for d in DATES}
    history={d:metrics(historical[d],p) for d,p in zip(DATES,protocols)}
    diag=diagnostics(records['nu'],protocols)
    summary=dict(day_equal=averages,daily=daily,paired_nu_minus_baseline=differences,aggregate_delta=delta,
        paired_day_bootstrap_95=intervals,historical=history,
        baseline_minus_historical_R600=float(np.mean([daily[f'{d}_baseline']['R600']-history[d]['R600'] for d in DATES])),
        baseline_minus_displayed_70_64_s=averages['baseline']['R600']-70.64,
        gate_pass=passed,diagnostics=diag)
    write(ROOT/'summary.json',summary)
    lines=['# 关卡1：同版本配对验证报告','',f"关卡判定：{'通过' if passed else '未通过'}。只使用本次六项验证结果；未开启最终测试。",'',
        '|方法|P60 %|P120 %|P600 %|R600 s|未完成（三日合计）|主窗传输 GB（日均）|',
        '|---|---:|---:|---:|---:|---:|---:|']
    for a in ARMS:
        v=averages[a];lines.append(f"|{a}|{v['P60']*100:.4f}|{v['P120']*100:.4f}|{v['P600']*100:.4f}|{v['R600']:.6f}|{v['unfinished']}|{v['main_bytes']/1e9:.6f}|")
    lines+=['',f"NU减对照：R600 {delta['R600']:.6f} s；P60 {delta['P60']*100:.4f} 个百分点；P600 {delta['P600']*100:.4f} 个百分点。",
        f"本次对照减历史原始记录日等权R600：{summary['baseline_minus_historical_R600']:.6f} s；减历史显示值70.64 s：{summary['baseline_minus_displayed_70_64_s']:.6f} s。",
        '',f"C_nu选中 {diag['selected']}/{diag['scored_images']} ({diag['selection_fraction']:.2%})；块数分布：{diag['blocks_histogram']}。",
        '候选可行/不可行计数：'+str(diag['candidate_status']),
        '评分图可行候选映射：'+str(diag['feasible_mappings']),
        '评分图最终选中映射：'+str(diag['selected_mappings']),
        '', '## 份额预测误差', '',
        '时间终点是各块结果到达地面，起点是方案决策；不含整图后处理。符号为预测减实际，负值表示过于乐观。下表按评分图已完成块合并统计，不把块当独立日期。',
        '|预测|块数|有符号中位数 s|有符号P90 s|绝对误差中位数 s|绝对误差P90 s|',
        '|---|---:|---:|---:|---:|---:|']
    for key,values in diag['errors'].items():
        s,a=values['signed_predicted_minus_actual'],values['absolute']
        lines.append(f"|{key}|{s['n']}|{s['median']}|{s['P90']}|{a['median']}|{a['P90']}|")
    lines += ['', '各图最慢块减最快块（只计所有块完成图）：'+str(diag['actual_spread_s']),
        f"未完成块 {diag['censored_blocks']}；无法计算完整跨度的选中图 {diag['spread_censored_images']}，均不填零。",
        '', '## 边界与复核','',
        '三验证日均为M单站6小时，600秒暖机回看及随访；10-22既有协议实际暖机650秒沿用。两组同源码、同配置、同评分ID，无训练、无选模。',
        '独立份额使用行蛇形前缀估计输入量、固定可行路径及单调上包络；最终几何映射可改变输入量。共享Calendar仍为原FIFO报价近似，不能把共同T解释为实际同时完成或全局最优。',
        '共享接触容量、窗口级实际执行、后续图像竞争、结果代理与真实结果量差异均可能造成误差；本报告未做因果消融，不能仅据差值判定单一原因。',
        '每块显式接收配额沿M的mem_capacity_gb；地面使用同数值作为候选准入上限，不代表地面物理RAM测量。',
        '按用户发布前修订，六项均由Mac六并发执行，三日期两组同机配对；devbox3只提供HTTP队列。',
        '10000次paired-day bootstrap，seed20261111；仅三日描述性区间，不校正开发选择偏差。未完成区间用每日计数，主表用三日合计。',
        '资源审计检查保存计数器与分段守恒，不是逐包重放。逐日指标、区间、传输分项、选择及逐块记录见summary.json与results/*.json。',
        '', '## 墙钟','', '运行前估计：实现与核验1–2小时，六任务并行2–5小时。实际单任务墙钟：']
    for a in ARMS:
        for r in records[a]:lines.append(f"- {r['date']} {a}，{r['host']}：{r['adapter_wall_seconds']:.1f} s。")
    lines+=['','阶段实际总用时与提交扫描见DELIVERY.md；报告生成前不预填完成时间。']
    (ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(8,5))
    xs=np.arange(0,601)
    for a in ARMS:
        yy=[]
        for r,p in zip(records[a],protocols):
            rr=scored(r,p);yy.append([sum(o['status']=='completed' and o['latency_seconds']<=x for o in rr)/len(rr) for x in xs])
        ax.plot(xs,np.mean(yy,axis=0)*100,label=a)
    ax.set(xlabel='Deadline (s)',ylabel='Delivered (%)',title='M validation, equal weight across three days',ylim=(0,100));ax.legend();ax.grid(alpha=.2)
    fig.tight_layout();fig.savefig(ROOT/'curves.png',dpi=150);plt.close(fig)
    outputs=[ROOT/'summary.json',ROOT/'REPORT.md',ROOT/'curves.png']
    write(ROOT/'INDEPENDENT_AUDIT.json',dict(status='PASS',at=now(),records=audit,input_hashes_verified=len(m['hashes']),
        output_hashes={str(p):sha(p) for p in outputs},audit_wall_seconds=time.time()-started))
    write(ROOT/'REPORT_COMPLETE.json',dict(status='PASS',at=now(),audit_sha256=sha(ROOT/'INDEPENDENT_AUDIT.json'),gate_pass=passed))


if __name__=='__main__':main()
