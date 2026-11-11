"""原始逐图记录复算代表方法补充主表，不改原报告。"""
from copy import deepcopy
from pathlib import Path
import numpy as np
from analysis import run_ground_representatives_20261031 as r
from analysis.report_m_formal_20261027 import metrics, paired_bootstrap, criteria
from analysis.report_ground_scaling_20261029 import audit_resources
from analysis.audit_dynamic_heuristics_20261026 import close

KEYS=('P60','P600','mean_completed_s','R600','unfinished')
LOCAL=('ground_16','local_16','minload_2_4','local_heft_16','local_cgs_16','zero_2_16')
NAMES={'fixed':'GTD-GAT-F（三种子均值）','dynamic':'GTD-GAT-D（三种子均值）',
       'ground_16':'原图下传','local_16':'本地处理','minload_2_4':'最小负载 2 跳 4 块',
       'local_heft_16':'局部 HEFT','local_cgs_16':'局部 CGS','zero_2_16':'局部整图复核 2 跳 16 块',
       'global_v3_16':'全局整图复核 F','global_v3_dynamic':'全局整图复核 D',
       'global_heft_16':'全局 HEFT','global_cgs_16':'全局 CGS'}


def average(values):
    return None if any(v is None for v in values) else float(np.mean(values))


def fmt(value,key):
    return 'NA' if value is None else f'{value*(100 if key in ("P60","P600") else 1):.3f}'


def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |',
                      *['| '+' | '.join(map(str,row))+' |' for row in rows]])+'\n'


def paths_and_provenance():
    old=r.read(r.original.ROOT/'report/FINAL_AUDIT.json')
    paths=deepcopy(old['record_paths']);provenance=dict(old['provenance'])
    for scale,root,audit in [('M','m_formal_test_20261027','FINAL_AUDIT.json'),
                            ('L','l_formal_test_20261028','independent_audit_20261028/FINAL_AUDIT.json')]:
        prior=r.read(r.original.MAIN/root/audit)
        provenance.update(prior['provenance'])
        for method in r.METHODS:
            for date in r.original.DATES:
                path=r.original.MAIN/root/'validation'/date/f'{method}.json'
                if path.exists():
                    assert str(path) in prior['provenance']
                    paths[scale]['single'][date][method]=str(path)
    state=r.read(r.ROOT/'final_status.json');assert state['status']=='COMPLETE' and len(state['results'])==45
    for short in state['results']:
        assert short['status']=='PASS' and r.sha(short['path'])==short['sha256']
        row=r.read(short['path']);paths[row['scale']][row['setting']][row['date']][row['method']]=short['path']
        provenance[short['path']]=short['sha256'];provenance[row['protocol']]=r.sha(row['protocol'])
    for scale in 'ML':
        for setting in ('single','scaled'):
            assert all(set(paths[scale][setting][d])==set(r.original.METHODS)|set(r.METHODS) for d in r.original.DATES)
    assert paths['S']['single']==paths['S']['scaled']
    for p,h in provenance.items():assert r.sha(p)==h,p
    return paths,provenance


def main():
    r.verify();out=r.ROOT/'report';paths,provenance=paths_and_provenance()
    daily={};values={};resources={};paired={};gaps={};model_pairs={}
    indices=np.random.default_rng(20261027).integers(0,3,size=(10000,3))
    for scale in 'SML':
        daily[scale]={};values[scale]={};resources[scale]={};paired[scale]={};gaps[scale]={};model_pairs[scale]={}
        for setting in ('single','scaled'):
            daily[scale][setting]={};values[scale][setting]={};resources[scale][setting]={};model_pairs[scale][setting]={}
            for method in paths[scale][setting][r.original.DATES[0]]:
                days={};resource={}
                for date in r.original.DATES:
                    path=paths[scale][setting][date][method];row=r.read(path);protocol=r.read(row['protocol'])
                    assert row['method']==method and row['date']==date and row['status']=='PASS'
                    assert row['config_sha256']==r.original.digest(r.original.configuration(protocol,method))
                    ref=r.read(r.read(paths[scale][setting][date]['zero_2_16'])['protocol'])
                    for k in ('config','scored_task_ids','score_start_s','score_end_s','sim_duration_s','date'):
                        assert protocol[k]==ref[k],(path,k)
                    rows=r.original.check_record(row,protocol);days[date]=metrics(rows)
                    close(days[date]['R600'],row['R600'])
                    for k in ('mean_completed_s','unfinished'):close(days[date][k],row['latency'][k])
                    resource[date]=audit_resources(row,protocol)
                daily[scale][setting][method]=days
                values[scale][setting][method]={k:sum(days[d][k] for d in r.original.DATES) if k in ('unfinished','n')
                    else average([days[d][k] for d in r.original.DATES]) for k in next(iter(days.values()))}
                resources[scale][setting][method]=resource
            for family in ('fixed','dynamic'):
                names=[f'{family}_{s}' for s in (1701,1702,1703)]
                daily[scale][setting][family]={d:{k:average([daily[scale][setting][m][d][k] for m in names])
                    for k in KEYS} for d in r.original.DATES}
                values[scale][setting][family]={k:average([values[scale][setting][m][k] for m in names]) for k in KEYS}
            refs=[m for m in NAMES if m not in ('fixed','dynamic') and m in daily[scale][setting]]
            for model in (*r.original.MAPPO,'fixed','dynamic'):
                model_pairs[scale][setting][model]={b:{k:paired_bootstrap(
                    [daily[scale][setting][model][d][k] for d in r.original.DATES],
                    [daily[scale][setting][b][d][k] for d in r.original.DATES],indices) for k in KEYS} for b in refs}
        for method in daily[scale]['single']:
            paired[scale][method]={k:dict(paired=paired_bootstrap(
                [daily[scale]['scaled'][method][d][k] for d in r.original.DATES],
                [daily[scale]['single'][method][d][k] for d in r.original.DATES],indices),
                aggregate_difference=None if any(values[scale][setting][method][k] is None for setting in ('single','scaled'))
                else values[scale]['scaled'][method][k]-values[scale]['single'][method][k]) for k in KEYS}
        for family in ('fixed','dynamic'):
            entry=dict(scope=list(LOCAL),single={},scaled={},change={},interpretation={})
            for k in ('P60','R600'):
                for setting in ('single','scaled'):
                    val=values[scale][setting]
                    if not all(b in val for b in LOCAL):entry[setting][k]=None;continue
                    best=(max if k=='P60' else min)(val[b][k] for b in LOCAL)
                    entry[setting][k]=dict(best=best,sources=[b for b in LOCAL if val[b][k]==best],difference=val[family][k]-best)
                change=None if any(entry[s][k] is None for s in ('single','scaled')) else entry['scaled'][k]['difference']-entry['single'][k]['difference']
                entry['change'][k]=change
                entry['interpretation'][k]='不可判定：代表集合缺项' if change is None else '保持' if abs(change)<1e-12 else (
                    '相对表现改善' if (change>0 if k=='P60' else change<0) else '相对表现变差')
            gaps[scale][family]=entry
    unique={p for scale in paths.values() for setting in scale.values() for date in setting.values() for p in date.values()}
    logical=sum(len(date) for scale in paths.values() for setting in scale.values() for date in setting.values())
    assert len(unique)==372 and logical==432
    audit=dict(status='PASS',record_paths=paths,provenance=provenance,daily=daily,values=values,unique_records=len(unique),logical_records=logical,
        registration_sha256=r.sha(r.REG),notes='Existing original scientific scopes unchanged; representative envelope is supplemental')
    for name,data in [('FINAL_AUDIT',audit),('RESOURCES',resources),('SCENARIO_PAIRED',paired),('MODEL_PAIRS',model_pairs),('REPRESENTATIVE_GAPS',gaps)]:
        r.write(out/f'{name}.json',data)
    text=['# 地面扩站代表方法补充报告\n',
        '新增45项：M/L扩站各18项，L单站9项。原147项与全部原报告保留；只评估，不训练、微调、选模或调参。\n',
        'S/M每日最忙6小时，L为1小时；12-21至23同日期、同轨道任务场景变体，模型冻结，不是新的独立测试日。600秒暖机及随访。S两栏引用同一记录；S六种新增代表方法不在本次授权补跑范围，缺项NA，不记零。\n',
        'P60/P600为百分比，交付均值/R600为秒。先按日计算再三日等权；F/D再对1701/1702/1703等权。未完成为三日合计，模型再取种子均值。均值只含完成项，R600未完成记600。\n']
    for scale in 'SML':
        text.append(f'## {scale} 档\n')
        text.append(table(['方法','设定','P60 %','P600 %','交付均值 s','R600 s','未完成'],[
            [label,setting,*[fmt(values[scale][setting].get(method,{}).get(k),k) for k in KEYS]]
            for method,label in NAMES.items() for setting in ('single','scaled')]))
    text+=['## 相对最好代表局部方法\n',
           '比较集合固定为原图下传、本地处理、最小负载2跳4块、局部HEFT、局部CGS、局部整图复核2跳16块。按指标分别取三日等权最好值，可能不是同一方法。Δ=模型减最好基线；P60正值、R600负值有利。变化=扩站Δ减单站Δ。此补充比较不替代原六16/十二4+16四项成功标准；点估计改善不代表已领先。\n']
    text.append(table(['档位','模型','指标','单站Δ','扩站Δ','变化','解释'],[
        [scale,family,k,*[fmt(gaps[scale][family][s][k]['difference'] if gaps[scale][family][s][k] else None,k) for s in ('single','scaled')],
         fmt(gaps[scale][family]['change'][k],k),gaps[scale][family]['interpretation'][k]]
        for scale in 'SML' for family in ('fixed','dynamic') for k in ('P60','R600')]))
    text+=['## 原图下传变化\n',table(['档位','指标','扩站减单站','每日配对均值','描述性95%区间'],[
        [scale,k,fmt(paired[scale]['ground_16'][k]['aggregate_difference'],k),fmt(paired[scale]['ground_16'][k]['paired']['mean'],k),
         str(paired[scale]['ground_16'][k]['paired']['ci95'])] for scale in 'ML' for k in KEYS]),
        '区间在JSON中保持原始单位（概率/秒/每日未完成数）；表中P概率点估计展示百分数。未完成合计差与每日配对均值不是同一分母。\n',
        '## 资源和统计边界\n',
        'RESOURCES.json列出逐方法逐日每站主窗/全程利用率、分类通信字节、计算星利用率、共享地面FIFO忙时。MODEL_PAIRS.json包含每个种子及种子均值对全部可用代表方法的配对差；SCENARIO_PAIRED.json含单站/扩站配对。10000次paired-day bootstrap，seed20261027，仅三日期描述性区间，不校正选择偏差。代表最好来源与差值见REPRESENTATIVE_GAPS.json；保存计数器不是逐包重放。\n',
        '## 实现与局限\n',
        '每站400Mbps，M北京+喀什，L再加三亚；共享原总地面算力FIFO，不随站数增加计算能力。结果到任一站即到地面，站间互联额外延迟0。原图下传自动入口在独立补充适配器接入登记的公开几何最早整图到达选站、固定目标和整图到齐屏障；单站仍走原入口。验证日接口及玩具600随访预检不作测试性能证据。\n',
        '全局教师报价继承按接触边预留的近似，未联合预测站级单通道及卫星跨站互斥；实际执行器仍限制这些资源。本次未修复或重跑教师，不能把近似当已证明的唯一原因。Mac ARM与Linux x86全长轨迹逐位一致未证明。原18项启动前路径失败及授权修正、原队列交接记录保留。\n']
    (out/'REPORT.md').write_text('\n'.join(text))
    r.write(out/'HASHES.json',{str(p):r.sha(p) for p in sorted(out.iterdir()) if p.name not in ('HASHES.json','INDEPENDENT_AUDIT.json') and p.is_file()})
    print('REPRESENTATIVE REPORT',len(unique),logical)


if __name__=='__main__':main()
