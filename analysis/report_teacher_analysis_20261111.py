"""只读验证结果生成两层比较；不运行策略、不修改登记。"""
from collections import Counter
import csv
from datetime import datetime
import math
from pathlib import Path

import numpy as np

from analysis.run_teacher_analysis_20261111 import ROOT,DATES,read,write,sha,now
from analysis.run_dynamic_heuristics_v2 import MAIN

KEYS = ('P60','P120','P600','R600','unfinished')
LABELS = {'teacher':'全局整图复核（CGS协同）','heft':'全局整图复核（HEFT协同）',
          'F':'GTD-GAT-F（三种子均值）','reference':'无争用参考（非理论界）',
          'lower':'独立窗口/无限带宽时延下界'}


def metrics(values, executable):
    n = len(values); assert n
    return {**{f'P{d}':sum(x is not None and x<=d for x in values)/n for d in (60,120,600)},
        'R600':math.fsum(min(x,600) if x is not None else 600 for x in values)/n,
        'unfinished':sum(x is None for x in values) if executable else None,
        'unquoted_or_no_path':sum(x is None for x in values) if not executable else None}


def aggregate(rows):
    return {key:None if any(r[key] is None for r in rows) else
            (sum(r[key] for r in rows) if key in ('unfinished','unquoted_or_no_path') else float(np.mean([r[key] for r in rows])))
            for key in rows[0]}


def outcome_values(record, protocol):
    by_id = {o['task_id']:o for o in record['outcomes']}
    ids = protocol['scored_task_ids']
    assert len(by_id)==len(record['outcomes']) and set(ids)<=set(by_id)
    assert all(by_id[tid]['observed_age_seconds']>=600 for tid in ids)
    return [by_id[tid]['latency_seconds'] if by_id[tid]['status']=='completed' else None for tid in ids]


def main():
    from analysis import run_teacher_analysis_20261111 as a,run_teacher_bounds_20261111 as b
    a.verify_manifest();b.verify_manifest()
    for runner in (a,b):
        s=read(runner.ROOT/'QUEUE_FINAL.json')
        assert s['complete'] and s['counts']=={'PASS':3} and not s['blocked']
    root=ROOT/'report';root.mkdir(exist_ok=True)
    assert not (root/'summary.json').exists(), 'Report already exists; audit it rather than overwrite'
    daily={};values={};choices={};hashes={};fixed=read(MAIN/'fixed_mappo_frozen_20261025/manifest.json')
    for date,path in zip(DATES,read(ROOT/'manifest.json')['protocols']):
        p=read(path); daily[date]={};values[date]={};choices[date]={}
        paths={'teacher':Path('results/nonuniform_20261111/gate1/results')/f'{date}_baseline.json',
               'heft':ROOT/'results'/f'{date}_heft_cooperative.json'}
        for seed in (1701,1702,1703):
            paths[f'F_{seed}']=MAIN/'mappo_20261022/validation'/('q2' if seed==1701 else 'q1')/str(seed)/f'{date}.json'
        baseline_config=read(paths['teacher'])['config_sha256']
        for name,rpath in paths.items():
            r=read(rpath);assert r['status']=='PASS' and r['config_sha256']==baseline_config
            if name.startswith('F_'):
                assert r['job']['checkpoint_sha256']==fixed['selected'][name[2:]]['checkpoint_sha256']
            vv=outcome_values(r,p); values[date][name]=vv;daily[date][name]=metrics(vv,True)
            assert math.isclose(daily[date][name]['R600'],r['R600'],abs_tol=1e-9)
            hashes[str(rpath)]=sha(rpath)
            if name in ('teacher','heft'):
                choices[date][name]=dict(Counter(d['selected'] for d in r['decisions'] if d['task_id'] in p['scored_task_ids']))
        bound_path=ROOT/'bounds'/f'{date}.json';record=read(bound_path);hashes[str(bound_path)]=sha(bound_path)
        hashes[record['contacts_path']]=sha(record['contacts_path'])
        for name in ('reference','lower'):
            by_id={row['task_id']:row for row in record[name]}
            assert len(by_id)==len(p['scored_task_ids']) and set(by_id)==set(p['scored_task_ids'])
            vv=[by_id[tid]['latency_seconds'] for tid in p['scored_task_ids']]
            values[date][name]=vv; daily[date][name]=metrics(vv,False)
        daily[date]['F']={key:(None if daily[date]['F_1701'][key] is None else
                             float(np.mean([daily[date][f'F_{s}'][key] for s in (1701,1702,1703)])))
                          for key in daily[date]['F_1701']}
    means={name:aggregate([daily[d][name] for d in DATES]) for name in daily[DATES[0]]}
    rng=np.random.default_rng(20261111);indices=rng.integers(0,3,size=(10000,3))
    comparisons={}
    for left,right in [('heft','teacher'),('teacher','reference'),('teacher','lower'),('F','reference'),('F','lower')]:
        detail={}
        for key in KEYS:
            if means[left][key] is None or means[right][key] is None:continue
            differences=np.array([daily[d][left][key]-daily[d][right][key] for d in DATES])
            detail[key]=dict(daily=differences.tolist(),difference=means[left][key]-means[right][key],
                paired_day_mean_95=np.percentile(differences[indices].mean(axis=1),[2.5,97.5]).tolist())
        comparisons[f'{left}_minus_{right}']=detail
    all_x=sorted({0.,60.,120.,600.,*(float(x) for d in DATES for vv in values[d].values() for x in vv if x is not None and x<=600)})
    curves={}
    for name in (*LABELS, 'F_1701','F_1702','F_1703'):
        series=[]
        for d in DATES:
            arms=[f'F_{s}' for s in (1701,1702,1703)] if name=='F' else [name]
            series.extend([[sum(x is not None and x<=deadline for x in values[d][arm])/len(values[d][arm])
                           for deadline in all_x] for arm in arms])
        curves[name]=np.mean(series,axis=0).tolist()
    write(root/'curves.json',dict(D_seconds=all_x,P=curves,scope='0 <= D <= 600, equal days; F equal seeds'))
    elapsed={}
    for runner,label in ((a,'ablation'),(b,'bounds')):
        launch=read(runner.ROOT/'LAUNCH.json');final=read(runner.ROOT/'workers/local/final_status.json')
        elapsed[label]=(datetime.fromisoformat(final['at'])-datetime.fromisoformat(launch['at'])).total_seconds()
    summary=dict(at=now(),day_equal=means,daily=daily,comparisons=comparisons,choices=choices,
                 elapsed_seconds=elapsed,input_hashes=hashes,scored_images=sum(len(values[d]['teacher']) for d in DATES))
    write(root/'summary.json',summary)
    with (root/'main_table.csv').open('x',newline='') as output:
        writer=csv.writer(output);writer.writerow(['method',*KEYS,'unquoted_or_no_path'])
        for name,row in means.items():writer.writerow([name,*[row[k] for k in (*KEYS,'unquoted_or_no_path')]])
    lines=['# 教师协同消融与两层无争用分析','',
        '仅M档2026-10-22～24验证日，原每日最忙6小时窗口、相同评分ID及至少600秒随访。新最终测试继续封存。',
        '新增3个完整HEFT协同评估、3个离线分析任务；对照复用本日关卡1同代码的3份已审计baseline。GTD-GAT-F复用原冻结三种子所选检查点的9份验证记录，不重新选模。',
        '', '|方法|P60 %|P120 %|P600 %|R600 s|未完成（3日合计；F再均种子）|',
        '|---|---:|---:|---:|---:|---:|']
    for name in LABELS:
        row=means[name]; unfinished='NA' if row['unfinished'] is None else f"{row['unfinished']:g}"
        lines.append(f"|{LABELS[name]}|{row['P60']*100:.4f}|{row['P120']*100:.4f}|{row['P600']*100:.4f}|{row['R600']:.6f}|{unfinished}|")
    delta=comparisons['heft_minus_teacher']
    lines+=['','## 协同规划器消融','',
        f"HEFT减CGS：P60 {delta['P60']['difference']*100:+.4f} pp，P120 {delta['P120']['difference']*100:+.4f} pp，P600 {delta['P600']['difference']*100:+.4f} pp，R600 {delta['R600']['difference']:+.6f} s，未完成 {delta['unfinished']['difference']:+g} 图。",
        '只替换完整未承诺图像C4/C16的块级候选；整图选择、共享日历报价、整图保护、破同分与顺序承诺保持原实现。残余/已承诺分片仍用旧CGS回退。日志cgs_4/cgs_16键在HEFT组表示HEFT产生的协同候选。',
        '差值只支持这一组合中的规划器比较；不说明HEFT或CGS在任意任务图上普遍占优。',
        '', '## 与两层分析的差距','',
        '|比较（前者减后者）|P60差 pp|P600差 pp|R600差 s|R600差的配对日95%区间 s|',
        '|---|---:|---:|---:|---|']
    for pair in ('teacher_minus_reference','teacher_minus_lower','F_minus_reference','F_minus_lower'):
        c=comparisons[pair];lines.append(f"|{pair}|{c['P60']['difference']*100:+.4f}|{c['P600']['difference']*100:+.4f}|{c['R600']['difference']:+.6f}|{c['R600']['paired_day_mean_95']}|")
    lines+=['', f"参考不可报价图数：{means['reference']['unquoted_or_no_path']}；下界图中无有限路径图数：{means['lower']['unquoted_or_no_path']}。这些不是可执行方法的未完成图数。",
        '无争用参考保留原有限接触计划、4/16分块、输入权限、代理字节、量化及报价近似。它既不是理论界，也不是在执行器中逐图隔离运行的策略成绩；不能将与教师的全部差值归因于争用。',
        '严格层是零序列化/独立窗口的强放宽：无限带宽、无排队/资源竞争/内存限制，删除建链等待，不限跳数，允许所有位置取全局最短单窗服务；保留完整执行网格接触时序和不向上量化的固定合并项。时延下界对应P_D比例上界。与其差距不能当作现有有限带宽下可实现的提升。',
        '完整放宽项、算法及可行域包含证明见 docs/thesis/TEACHER_BOUND_IMPLEMENTATION_20261111.md；只保证登记仿真模型下D<=600和R600，不声称物理世界最优、无限时域完成时间或有限带宽的紧界。',
        '', '## 三种子与逐日结果','', '|种子|P60 %|P120 %|P600 %|R600 s|未完成3日合计|','|---|---:|---:|---:|---:|---:|']
    for seed in (1701,1702,1703):
        r=means[f'F_{seed}'];lines.append(f"|{seed}|{r['P60']*100:.4f}|{r['P120']*100:.4f}|{r['P600']*100:.4f}|{r['R600']:.6f}|{r['unfinished']}|")
    lines+=['','逐日指标、五类配对差、日等权/种子平均、选择分布与输入hash见 report/summary.json；全阶梯见 report/curves.json。',
        '10000次paired-day bootstrap，seed20261111，三天仅为描述性区间，不校正验证开发选择偏差。未完成区间采用每日计数；主表未完成是三日合计。',
        '', '## 审计与墙钟','',
        '发布前全套793测试通过；新增下界12项和消融6项共同18项通过。三个完整评估与三个离线任务均Mac执行，devbox3仅作队列服务；无需跨架构一致性假设。',
        '独立审计检查所有登记hash、六任务唯一归属/原字节、原模型哈希/同配置/评分ID/随访、资源分段守恒、全部曲线与指标、接触见证及逐图截断支配。资源审计是保存计数器核验，不是逐包重放。见INDEPENDENT_AUDIT.json和REPORT_COMPLETE.json。',
        f"运行预估：HEFT1～2小时，两层分析0.5～1小时；实际从worker发布到全部队列确认：HEFT {elapsed['ablation']/60:.2f} 分钟，两层分析 {elapsed['bounds']/60:.2f} 分钟。两批并行，总等待不等于两者之和。",
        '实现/登记和提交耗时见DELIVERY.md。关卡1不通过的既有结论不变，本报告不启动学习或最终评估。',
        '', '![两层P_D曲线](report/curves.png)']
    with (ROOT/'REPORT.md').open('x') as output:output.write('\n'.join(lines)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,2,figsize=(12,4.7))
    plot_labels={'teacher':'Whole-image teacher (CGS)','heft':'Whole-image teacher (HEFT)',
        'F':'GTD-GAT-F (3 seeds)','reference':'No-contention reference (not a bound)',
        'lower':'Certified relaxed delivery ceiling'}
    colors=['#222222','#c44e52','#0072b2','#009e73','#cc79a7']
    for ax in axes:
        for (name,label),color in zip(plot_labels.items(),colors):
            ax.step(all_x,np.asarray(curves[name])*100,where='post',label=label,color=color,
                    linestyle='--' if name in ('reference','lower') else '-')
        ax.set(xlabel='Deadline D (s)',ylabel='Delivered / eligible images (%)',ylim=(0,101))
        ax.grid(alpha=.2)
    axes[0].set(xlim=(0,600),title='M validation: equal weight across 3 days')
    axes[1].set(xlim=(0,120),title='Early delivery (0-120 s)')
    axes[0].legend(fontsize=8,loc='lower right');fig.tight_layout();fig.savefig(root/'curves.png',dpi=160);plt.close(fig)
    print(means)


if __name__=='__main__':main()
