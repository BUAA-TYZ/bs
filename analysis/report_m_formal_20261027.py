"""正式测试逐图独立复算、两套口径、配对差值及权衡图。"""
from pathlib import Path
import json
import math
import numpy as np

from analysis.run_dynamic_heuristics_v2 import read, sha, configuration, digest
from analysis.run_dynamic_learning import write_json
from analysis.run_load_pressure_short import verify
from analysis.audit_dynamic_heuristics_20261026 import close

DEADLINES = (60, 90, 120, 180, 240, 300, 600)
METRICS = ('P60', 'mean_completed_s', 'P600', 'R600')
HIGH = {'P60', 'P600'}


def metrics(rows):
    if not rows:
        raise ValueError('Empty score set: undefined metrics, never silently discard a date')
    completed = [r['latency_seconds'] for r in rows if r['status'] == 'completed']
    value = {f'P{d}':sum(r['status']=='completed' and r['latency_seconds']<=d for r in rows)/len(rows) for d in DEADLINES}
    value.update(R600=math.fsum(min(r['latency_seconds'],600) if r['status']=='completed' else 600 for r in rows)/len(rows),
        mean_completed_s=float(np.mean(completed)) if completed else None,
        P50_completed_s=float(np.quantile(completed,.5)) if completed else None,
        P90_completed_s=float(np.quantile(completed,.9)) if completed else None,
        unfinished=len(rows)-len(completed), n=len(rows))
    return value


def criteria(model, baseline):
    return {k: None if model[k] is None or baseline[k] is None else
        (model[k]>=baseline[k] if k=='P600' else model[k]>baseline[k] if k=='P60' else model[k]<baseline[k]) for k in METRICS}


def wins(model, baseline):
    out = {}
    for k in METRICS:
        a,b = model[k],baseline[k]
        out[k] = 'NA' if a is None or b is None else 'tie' if a==b else 'win' if ((a>b) if k in HIGH else (a<b)) else 'loss'
    return out


def pareto(values, x):
    result = []
    for name,a in values.items():
        if a[x] is None or a['P60'] is None:
            continue
        ax = a[x] if x in HIGH else -a[x]
        dominated = False
        for other,b in values.items():
            if other == name or b[x] is None or b['P60'] is None:
                continue
            bx = b[x] if x in HIGH else -b[x]
            if bx>=ax and b['P60']>=a['P60'] and (bx>ax or b['P60']>a['P60']):
                dominated = True
                break
        if not dominated:
            result.append(name)
    return sorted(result, key=lambda n:(values[n][x],n))


def paired_bootstrap(a,b,indices):
    if any(x is None for x in [*a,*b]):
        return dict(daily=None, mean=None, ci95=None)
    delta = np.asarray(a)-np.asarray(b)
    draws = delta[indices].mean(axis=1)
    return dict(daily=delta.tolist(), mean=float(delta.mean()), ci95=np.quantile(draws,[.025,.975]).tolist())


def label_points(ax, points):
    """只移动编号标注，保留重合数据点的真实位置。"""
    ax.figure.canvas.draw()
    occupied=[];bounds=ax.get_window_extent()
    if ax.get_legend():
        box=ax.get_legend().get_window_extent()
        occupied.append((box.x0,box.y0,box.x1,box.y1))
    for label,x,y in points:
        px,py=ax.transData.transform((x,y))
        offsets=sorted(((dx,dy) for dx in range(-280,281,24) for dy in range(-280,281,18)),
                       key=lambda v:v[0]**2+v[1]**2)
        for dx,dy in offsets:
            cx,cy=px+dx,py+dy;box=(cx-10,cy-8,cx+10,cy+8)
            if not (bounds.x0+4<=box[0] and box[2]<=bounds.x1-4 and bounds.y0+4<=box[1] and box[3]<=bounds.y1-4):
                continue
            if any(box[0]<b[2] and box[2]>b[0] and box[1]<b[3] and box[3]>b[1] for b in occupied):
                continue
            occupied.append(box)
            position=ax.transAxes.inverted().transform((cx,cy))
            ax.annotate(label,(x,y),xytext=position,textcoords='axes fraction',ha='center',va='center',fontsize=7,
                bbox=dict(facecolor='white',edgecolor='none',alpha=.8,pad=.2),
                arrowprops=dict(arrowstyle='-',color='#888888',linewidth=.4))
            break


def report(root):
    from analysis.run_m_formal_20261027 import MAPPO, GLOBAL, OPEN
    root = Path(root)
    m=read(root/'manifest.json'); run=read(root/'run_manifest.json'); state=read(root/'simulation_status.json')
    assert state['status']=='COMPLETE' and len(state['results'])==len(m['methods'])*3
    for path,h in {**m['hashes'],**run['hashes']}.items():
        assert sha(path)==h, path
    for r in state['results']:
        assert r['status']=='PASS' and sha(r['path'])==r['sha256']
    protocols = [read(path) for path in run['protocols']]
    for p in protocols:
        verify(p)
    daily={}; aggregate={}; all_rows={}; curves={}; tails={}; provenance={}
    for method in m['methods']:
        days=[]; sets=[]
        for path,p in zip(run['protocols'], protocols, strict=True):
            source=root/'validation'/p['date']/f'{method}.json'; r=read(source)
            assert r['status']=='PASS' and r['checks']['passed'] and r['date']==p['date'] and r['method']==method
            assert r['protocol']==path and r['config_sha256']==digest(configuration(p,method))
            assert r['opening_sha256']==sha(OPEN) and not r['tests_sealed'] and r['l_tests_sealed']
            if method in m['models']:
                assert r['checkpoint']==m['models'][method] and r['checkpoint_sha256']==sha(m['models'][method])
            ids=set(p['scored_task_ids']); rows=[o for o in r['outcomes'] if o['task_id'] in ids]
            assert len(rows)==len(ids) and {o['task_id'] for o in rows}==ids
            assert p['sim_duration_s']-p['score_end_s']>=600
            assert all(o['observed_age_seconds']>=600 and p['score_start_s']<=o['release_seconds']<p['score_end_s'] for o in rows)
            a=metrics(rows)
            for d,c in zip(DEADLINES,r['curve'],strict=True):
                close(a[f'P{d}'],c['P_lower']); assert c['D_seconds']==d and c['unknown_count']==0
                assert set(c['on_time_ids'])=={o['task_id'] for o in rows if o['status']=='completed' and o['latency_seconds']<=d}
            close(a['R600'],r['R600'])
            for k in ('mean_completed_s','P50_completed_s','P90_completed_s','unfinished'):
                if a[k] is not None:
                    close(a[k],r['latency'][k])
            cfg=p['config']; helpers=set(range(cfg['num_sats']))-set(cfg['task_source_sat_ids'])
            rate=sum(g['bandwidth_mbps']/8*g.get('receive_channels',1) for g in cfg['ground_stations'])*1e6
            for z in r['resources'].values():
                busy=math.fsum(v for sid,v in z['helper_busy_by_node'].items() if int(sid) in helpers)
                close(busy,z['helper_busy_s']);close(busy/(len(helpers)*z['duration_s']),z['helper_utilization'])
                close(z.get('raw_ground_bytes',0)+z.get('result_ground_bytes',0),z['ground_bytes'])
                close(z['ground_bytes']/(rate*z['duration_s']),z['ground_channel_utilization'])
                close(math.fsum(z.get(k,0) for k in ('control_bytes','raw_isl_bytes','result_isl_bytes','raw_ground_bytes','result_ground_bytes')),z['total_transmitted_bytes'])
            for k in ('duration_s','total_transmitted_bytes','control_bytes','helper_busy_s','ground_bytes'):
                close(sum(r['resources'][seg][k] for seg in ('warmup','main','followup')),r['resources']['whole'][k])
            assert set(r['granularity'])==ids
            assert all(g['packet_mb']>=g['unique_mb']-1e-9 for g in r['granularity'].values())
            a.update(date=p['date'],resources=r['resources'])
            days.append(a);sets.append(rows)
            tails[f'{method}_{p["date"]}']=[o for o in rows if o['status']!='completed' or o['latency_seconds']>300]
            provenance[str(source)]=sha(source)
        daily[method]=days;all_rows[method]=sets
        keys=[*METRICS,'P90','P120','P180','P240','P300','P50_completed_s','P90_completed_s']
        a={k:float(np.mean([d[k] for d in days])) if all(d[k] is not None for d in days) else None for k in keys}
        a['unfinished']=sum(d['unfinished'] for d in days)
        a['resources']={seg:{k:float(np.mean([d['resources'][seg][k] for d in days])) for k in
            ('total_transmitted_bytes','helper_utilization','ground_channel_utilization')} for seg in ('main','whole')}
        a['total_bytes_all_days']={seg:sum(d['resources'][seg]['total_transmitted_bytes'] for d in days) for seg in ('main','whole')}
        aggregate[method]=a
        xs=sorted({60.,600.,*[o['latency_seconds'] for rows in sets for o in rows if o['status']=='completed' and 60<=o['latency_seconds']<=600]})
        curves[method]=dict(D=xs,P=[float(np.mean([sum(o['status']=='completed' and o['latency_seconds']<=x for o in rows)/len(rows) for rows in sets])) for x in xs])
    scopes={}
    for scope,baselines in m['comparison_scopes'].items():
        assert len(baselines)==(6 if scope=='fixed_six_local_16' else 12)
        envelope={};sources={}
        for k in METRICS:
            vals={b:aggregate[b][k] for b in baselines}
            best=(max if k in HIGH else min)(vals.values()) if all(v is not None for v in vals.values()) else None
            envelope[k]=best;sources[k]=[b for b,v in vals.items() if v==best] if best is not None else []
        family='fixed' if scope.startswith('fixed_') else 'dynamic'
        scopes[scope]=dict(envelope=envelope,sources=sources,criteria={n:criteria(aggregate[n],envelope) for n in MAPPO if n.startswith(family+'_')})
    baselines=[n for n in m['methods'] if n not in MAPPO]
    indices=np.random.default_rng(m['bootstrap']['seed']).integers(0,3,size=(m['bootstrap']['draws'],3))
    pairs={n:{b:dict(group='global_reference' if b in GLOBAL else 'same_information', wins=wins(aggregate[n],aggregate[b]),
        criteria=criteria(aggregate[n],aggregate[b]),paired={k:paired_bootstrap([d[k] for d in daily[n]],[d[k] for d in daily[b]],indices) for k in METRICS})
        for b in baselines} for n in MAPPO}
    local_baselines=[b for b in baselines if b not in GLOBAL]
    strongest={}
    for k in METRICS:
        vals={b:aggregate[b][k] for b in local_baselines if aggregate[b][k] is not None}
        best=(max if k in HIGH else min)(vals.values())
        strongest[k]=[b for b,v in vals.items() if v==best]
    local={n:a for n,a in aggregate.items() if n not in GLOBAL}
    fronts={x:pareto(local,x) for x in ('P600','R600')}
    audit=dict(status='PASS',manifest_sha256=sha(root/'manifest.json'),daily=daily,day_equal=aggregate,
        scopes=scopes,strongest_same_information_by_metric=strongest,pareto=fronts,provenance=provenance,
        limitations=['Three paired dates: discrete descriptive intervals, selection bias not corrected',
                    'Orin four-block peak memory not measured', 'Saved resource counters, not independent packet replay'])
    write_json(root/'FINAL_AUDIT.json',audit);write_json(root/'PAIRED_COMPARISONS.json',pairs)
    write_json(root/'full_curves.json',curves);write_json(root/'TAIL_RECORDS.json',tails)
    def fmt(v):
        return 'NA' if v is None else f'{v:.4f}'
    lines=['# M正式测试报告','', '三日等权；模型冻结后一次开启12-21至23。完整方法33项，99条记录。L测试仍封存。', '',
        '|方法|P60/90/120/180/240/300/600 %|交付均值/P50/P90 s|R600 s|未完成合计|三日主窗/全程总传输GB|计算星/地面主窗利用率|',
        '|---|---|---|---:|---:|---|---|']
    for n,a in aggregate.items():
        z=a['resources']['main']
        lines.append(f'|{n}|'+ '/'.join(fmt(a[f'P{d}']*100) for d in DEADLINES)+'|'+
            '/'.join(fmt(a[k]) for k in ('mean_completed_s','P50_completed_s','P90_completed_s'))+
            f"|{fmt(a['R600'])}|{a['unfinished']}|{a['total_bytes_all_days']['main']/1e9:.3f}/{a['total_bytes_all_days']['whole']/1e9:.3f}|{z['helper_utilization']:.3%}/{z['ground_channel_utilization']:.3%}|")
    for scope,v in scopes.items():
        lines+=['',f'## {scope}','','包络与来源：'+json.dumps(dict(values=v['envelope'],sources=v['sources']),ensure_ascii=False),
            '', '|模型|P60严格更高|均值严格更低|P600不下降|R600严格更低|全通过|','|---|---|---|---|---|---|']
        for n,c in v['criteria'].items():
            lines.append('|'+n+'|'+'|'.join('NA' if flag is None else '通过' if flag else '未通过' for flag in c.values())+'|'+str(all(flag is True for flag in c.values()))+'|')
    lines+=['','## 配对差与逐基线比较','', '最强同信息基线按指标分别为：'+json.dumps(strongest,ensure_ascii=False),
        '', '逐基线胜平负、四项判定、逐日配对差与95%日期bootstrap区间见PAIRWISE.md和PAIRED_COMPARISONS.json。差值为MAPPO减基线，P越大越好、时延越小越好。',
        '', '三个日期的日期级配对bootstrap仅为描述性不确定性，区间离散且不校正选择偏差，不据此宣称多重比较显著性。',
        '', '权衡图tradeoff_P600.png、tradeoff_R600.png标出同信息实测方法前沿；固定/动态三种子均值只展示，不替代种子判定。全局参考与均值点不参与局部前沿。',
        '', 'Orin上4块峰值内存未实测。资源审计限保存计数器，不是逐包重放。长尾记录只提供关联证据，不是隔离因果。合成轨道相位留出不代表跨月真实轨道预报。']
    (root/'REPORT.md').write_text('\n'.join(lines)+'\n')
    pl=['# MAPPO逐基线比较','','胜平负按指标方向；P600相等为平且满足不下降。全局参考明确单列组别。']
    for n,rows in pairs.items():
        pl+=['',f'## {n}','','|基线|组别|指标|胜平负|原不等式通过|三日差|均差|95%区间|','|---|---|---|---|---|---|---:|---|']
        for b,v in rows.items():
            for k in METRICS:
                q=v['paired'][k]
                pl.append(f"|{b}|{v['group']}|{k}|{v['wins'][k]}|{v['criteria'][k]}|{q['daily']}|{fmt(q['mean'])}|{q['ci95']}|")
    (root/'PAIRWISE.md').write_text('\n'.join(pl)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    # 点编号对应旁侧清单，避免33个方法名称互相覆盖。
    names=list(aggregate)
    for x in ('P600','R600'):
        fig,(ax,key)=plt.subplots(1,2,figsize=(15,8),gridspec_kw={'width_ratios':[1.3,1]},layout='constrained')
        key.axis('off')
        labels=[]
        for i,n in enumerate(names,1):
            a=aggregate[n];color='#b23a48' if n.startswith('dynamic_') else '#197a6c' if n.startswith('fixed_') else '#777777' if n in GLOBAL else '#315da8'
            ax.scatter(a[x]*(100 if x=='P600' else 1),a['P60']*100,color=color,marker='x' if n in GLOBAL else 'o',s=38)
            labels.append((str(i),a[x]*(100 if x=='P600' else 1),a['P60']*100))
            key.text(0,1-(i-1)*.029,f'{i:02d}  {n}',fontsize=8,color=color,va='top')
        frontier=fronts[x]
        ax.plot([aggregate[n][x]*(100 if x=='P600' else 1) for n in frontier],[aggregate[n]['P60']*100 for n in frontier],color='#222222',linestyle='--',linewidth=1,label='Local Pareto frontier')
        for family,color in (('fixed','#197a6c'),('dynamic','#b23a48')):
            group=[aggregate[n] for n in MAPPO if n.startswith(family+'_')]
            ax.scatter(np.mean([a[x] for a in group])*(100 if x=='P600' else 1),np.mean([a['P60'] for a in group])*100,
                marker='*',s=180,color=color,edgecolors='black',label=family+' MAPPO mean')
        ax.set(xlabel='P600 (%) - higher is better' if x=='P600' else 'R600 (s) - lower is better',ylabel='P60 (%) - higher is better')
        ax.grid(alpha=.2);ax.legend(fontsize=9,loc='best');label_points(ax,labels)
        fig.savefig(root/f'tradeoff_{x}.png',dpi=160);plt.close(fig)
    fig,axes=plt.subplots(1,2,figsize=(15,6),layout='constrained')
    for ax,group in zip(axes,[[n for n in names if n in MAPPO or '_bc_' in n],[n for n in names if n not in MAPPO and '_bc_' not in n]]):
        for n in group:
            c=curves[n];ax.step(c['D'],np.asarray(c['P'])*100,where='post',label=n)
        ax.set(xlabel='Deadline (s)',ylabel='P_D (%)',xlim=(60,600));ax.grid(alpha=.2);ax.legend(fontsize=7)
    fig.savefig(root/'curves.png',dpi=160);plt.close(fig)
    write_json(root/'REPORT_HASHES.json',{str(p):sha(p) for p in [root/'FINAL_AUDIT.json',root/'REPORT.md',root/'PAIRWISE.md',
        root/'PAIRED_COMPARISONS.json',root/'full_curves.json',root/'TAIL_RECORDS.json',root/'tradeoff_P600.png',root/'tradeoff_R600.png',root/'curves.png']})
