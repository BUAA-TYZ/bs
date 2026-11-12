"""合并完整分片，独立核验保存报价与最优方案，生成按图统计报告。"""
from collections import Counter,defaultdict
import csv
from datetime import datetime
import gzip
import itertools
import json
import math
from pathlib import Path
import random
import statistics

import numpy as np

from analysis import run_cgs_gap_20261111 as r
from analysis import cgs_gap_20261111 as core


def stats(values):
    if not values:return dict(n=0,mean=None,median=None,p90=None,maximum=None,within_1s=None,within_5s=None)
    values=np.asarray(values,dtype=float)
    return dict(n=len(values),mean=float(values.mean()),median=float(np.median(values)),
        p90=float(np.percentile(values,90)),maximum=float(values.max()),
        within_1s=float(np.mean(values<=1+1e-9)),within_5s=float(np.mean(values<=5+1e-9)))


def verify_stats(values,summary):
    assert summary['n']==len(values)
    if not values:
        assert all(value is None for key,value in summary.items() if key!='n');return
    ordered=sorted(values);position=.9*(len(ordered)-1);left=math.floor(position);right=math.ceil(position)
    expected=dict(mean=math.fsum(values)/len(values),median=statistics.median(values),
        p90=ordered[left]*(right-position)+ordered[right]*(position-left) if right!=left else ordered[left],
        maximum=max(values),within_1s=sum(v<=1+1e-9 for v in values)/len(values),
        within_5s=sum(v<=5+1e-9 for v in values)/len(values))
    for key,value in expected.items():assert math.isclose(summary[key],value,abs_tol=1e-8,rel_tol=1e-10)


def independent_assignments(blocks,k):
    if blocks==4:
        yield from itertools.product(range(k),repeat=4)
    else:
        assert blocks==16
        # 用隔板组合构造计数向量，独立于运行器的递归生成器。
        for chosen in itertools.combinations(range(blocks+k-1),k-1):
            bars=(-1,*chosen,blocks+k-1)
            counts=[bars[i+1]-bars[i]-1 for i in range(k)]
            yield tuple(p for p,count in enumerate(counts) for _ in range(count))


def audit_enumeration(entry,results):
    plan=entry['plan'];n=plan['planned_quotes'];k=len(plan['positions'])
    generator=independent_assignments(plan['blocks'],k)
    expected=0;finite=0;best=None;best_assignment=None;chunk_count=0
    for result in sorted(results,key=lambda v:v['start']):
        assert result['start']==expected and result['end']>expected
        assert r.sha(result['raw_path'])==result['raw_sha256']
        chunk_best=None;chunk_assignment=None;chunk_finite=0;count=0
        with gzip.open(result['raw_path'],'rt') as stream:
            for line in stream:
                index,value=json.loads(line);assignment=next(generator)
                assert index==expected and index<result['end'];expected+=1;count+=1
                if value is not None:
                    assert math.isfinite(value) and value>=0
                    finite+=1;chunk_finite+=1
                    if chunk_best is None or value<chunk_best:chunk_best,chunk_assignment=value,assignment
                    if best is None or value<best:best,best_assignment=value,assignment
        assert count==result['evaluated']==result['end']-result['start']
        assert expected==result['end'] and chunk_finite==result['finite']
        assert chunk_best==result['canonical_best_quote']
        assert (None if chunk_assignment is None else list(chunk_assignment))==result['canonical_best_assignment']
        chunk_count+=1
    assert expected==n and next(generator,None) is None
    snapshot=core.load_snapshot(entry['snapshot']['path'],entry['snapshot']['sha256'])
    positions=[tuple(p) if isinstance(p,list) else p for p in plan['positions']]
    if best is not None:
        recomputed=core.quote(snapshot,[positions[i] for i in best_assignment],plan['ordered'])
        assert recomputed is not None and abs(recomputed-best)<1e-8
    if plan['blocks']==4:assert best is not None and best<=plan['cgs_quote']+1e-8
    incumbent=min(plan['cgs_quote'],best) if best is not None else plan['cgs_quote']
    return dict(evaluated=n,finite=finite,shards=chunk_count,canonical_best_quote=best,
        canonical_best_assignment=best_assignment,best_quote=incumbent,cgs_quote=plan['cgs_quote'],
        improvement_s=plan['cgs_quote']-incumbent,enumeration_minus_cgs_s=incumbent-plan['cgs_quote'],
        incumbent_better_than_canonical=best is None or plan['cgs_quote']<best,
        exact_assignment_space=plan['blocks']==4)


def audit_search(entry,result):
    plan=entry['plan'];positions=[tuple(p) if isinstance(p,list) else p for p in plan['local_search_positions']]
    assignment=tuple(positions.index(tuple(p) if isinstance(p,list) else p) for p in plan['cgs'])
    current=plan['cgs_quote'];accepted=0;evaluated=0;last_iteration=-1;stop='ITERATION_CAP'
    assert r.sha(result['raw_path'])==result['raw_sha256']
    with gzip.open(result['raw_path'],'rt') as stream:
        records=(json.loads(line) for line in stream)
        for iteration,rows in itertools.groupby(records,key=lambda row:row[0]):
            assert iteration==last_iteration+1 and iteration<20
            last_iteration=iteration;expected=core.neighbors(assignment,len(positions))
            best,best_assignment=current,assignment;seen=[]
            for _,candidate,value in rows:
                candidate=tuple(candidate);seen.append(candidate);evaluated+=1
                if value is not None:
                    assert math.isfinite(value) and value>=0
                    if value<best-1e-9:best,best_assignment=value,candidate
            assert seen==expected
            if best_assignment==assignment:
                stop='NO_IMPROVEMENT';assert next(records,None) is None;break
            current,assignment=best,best_assignment;accepted+=1
    assert evaluated==result['evaluated'] and accepted==result['iterations_accepted']
    assert current==result['best_quote'] and list(assignment)==result['best_assignment']
    assert stop==result['stop_reason']
    if stop=='ITERATION_CAP':assert last_iteration==19 and accepted==20
    snapshot=core.load_snapshot(entry['snapshot']['path'],entry['snapshot']['sha256'])
    assert abs(core.quote(snapshot,[positions[i] for i in assignment],plan['ordered'])-current)<1e-8
    return dict(improvement_s=plan['cgs_quote']-current,cgs_quote=plan['cgs_quote'],best_quote=current,
                evaluated=evaluated,iterations_accepted=accepted,stop_reason=stop)


def main():
    base=r.verify_manifest('replay');manifest=r.verify_manifest('enumeration')
    server=r.read(r.ROOT/'SERVER_ARTIFACT_AUDIT.json');assert server['status']=='PASS'
    final=r.read(r.folder('enumeration')/'QUEUE_FINAL.json')
    assert final['complete'] and final['counts']=={'PASS':len(manifest['jobs'])}
    candidates=r.read(r.ROOT/'CANDIDATES.json');entries=candidates['samples']
    sample_doc=r.read(r.ROOT/'SAMPLES.json');assert len(entries)==len(sample_doc['samples'])==99
    assert {e['snapshot']['sample']['id'] for e in entries}=={s['id'] for s in sample_doc['samples']}
    rng=random.Random(20261111);selected=[]
    for date,job in zip(r.DATES,base['jobs']):
        protocol=r.read(job['job'][1]);wanted=set(protocol['scored_task_ids'])
        original=r.read(Path('results/nonuniform_20261111/gate1/results')/f'{date}_baseline.json')
        replay=r.read(job['output']);assert all(replay['replay_checks'].values())
        assert r.sha(replay['full_result_path'])==replay['original_record_sha256']
        full=r.read(replay['full_result_path'])
        for key in replay['replay_checks']:assert full[key]==original[key]
        for windows in (9,64,100):
            pool=sorted((d for d in original['decisions'] if d['task_id'] in wanted and
                d['selected'] in ('cgs_4','cgs_16') and d['windows']==windows),
                key=lambda d:int(d['task_id'].split('_')[-1]))
            selected.extend(f"{date}_{d['task_id']}" for d in rng.sample(pool,11))
    assert selected==[s['id'] for s in sample_doc['samples']]
    grouped=defaultdict(list);input_hashes={};runtime={};total_wall=defaultdict(float)
    queue_by_id={j['id']:j for j in final['jobs']}
    for job in manifest['jobs']:
        result=r.read(job['output']);q=queue_by_id[job['id']]
        assert q['state']=='PASS' and q['spec']==job and q['result']['sha256']==r.sha(job['output'])
        assert result['registration_sha256']==r.sha(r.folder('enumeration')/'REGISTRATION.json')
        assert result['manifest_sha256']==r.sha(r.folder('enumeration')/'manifest.json')
        assert [result['stage'],result['protocol'],result['method']]==job['job']
        if result['method']=='enumerate':assert [result['start'],result['end']]==[job['start'],job['end']]
        grouped[job['sample_id']].append(result);runtime[result['host']]=result['runtime']
        total_wall[result['host']]+=result['total_wall_seconds']
        input_hashes[job['output']]=r.sha(job['output']);input_hashes[result['raw_path']]=r.sha(result['raw_path'])
    rows=[];search_rows=[];over=[];excluded=[]
    for entry in entries:
        sample=entry['snapshot']['sample'];plan=entry['plan'];identifier=sample['id']
        result=grouped[identifier]
        snapshot=core.load_snapshot(entry['snapshot']['path'],entry['snapshot']['sha256'])
        assert snapshot['sample']==sample
        assert sample==next(s for s in sample_doc['samples'] if s['id']==identifier)
        if plan['status']!='NO_FINITE_CGS_QUOTE':
            used={repr(p) for p in plan['cgs']}
            assert len(used)==plan['used_positions']
            assert used<={repr(p) for p in plan['positions']}
            assert (len(used)>8)==(plan['status']=='OVER_CAP')
            if plan['status']=='ELIGIBLE':assert len(plan['positions'])<=8
            cgs=[tuple(p) if isinstance(p,list) else p for p in plan['cgs']]
            assert abs(core.quote(snapshot,cgs,plan['ordered'])-snapshot['calendar_quote'])<1e-8
            assert abs(plan['cgs_quote']-snapshot['calendar_quote'])<1e-8
        else:assert snapshot['calendar_quote'] is None
        common=dict(id=identifier,date=sample['date'],windows=sample['windows'],blocks=sample['actual'],
            requested=sample['requested'],status=plan['status'],used_positions=plan.get('used_positions'),
            candidate_positions=len(plan.get('positions',[])),cross_satellite_blocks=plan.get('cross_satellite_blocks'),
            selected_guard_quote=sample['quotes_seconds'][sample['selected']])
        if plan['status']=='ELIGIBLE':rows.append(dict(**common,**audit_enumeration(entry,result)))
        elif plan['status']=='OVER_CAP':
            over.append(common)
            if candidates['local_search_triggered']:
                assert len(result)==1 and result[0]['method']=='local_search'
                search_rows.append(dict(**common,**audit_search(entry,result[0])))
            else:assert not result
        else:
            assert plan['status']=='NO_FINITE_CGS_QUOTE' and not result;excluded.append(common)
    assert candidates['local_search_triggered']==(len(over)/99>0.1)
    assert len(rows)+len(over)+len(excluded)==99
    summary={}
    groups={'all':rows}
    groups.update({f'windows_{w}':[x for x in rows if x['windows']==w] for w in (9,64,100)})
    groups.update({f'blocks_{b}':[x for x in rows if x['blocks']==b] for b in (4,16)})
    groups.update({f'windows_{w}_blocks_{b}':[x for x in rows if x['windows']==w and x['blocks']==b] for w in (9,64,100) for b in (4,16)})
    for name,values in groups.items():
        improvements=[x['improvement_s'] for x in values];delta=[x['enumeration_minus_cgs_s'] for x in values]
        summary[name]=dict(improvement=stats(improvements),enumeration_minus_cgs=stats(delta))
        verify_stats(improvements,summary[name]['improvement']);verify_stats(delta,summary[name]['enumeration_minus_cgs'])
    local=stats([x['improvement_s'] for x in search_rows]);verify_stats([x['improvement_s'] for x in search_rows],local)
    cap=[dict(windows=w,sampled=sum(s['windows']==w for s in sample_doc['samples']),
        over_cap=sum(x['windows']==w for x in over)) for w in (9,64,100)]
    for x in cap:x['fraction']=x['over_cap']/x['sampled']
    output=r.ROOT/'report';output.mkdir(exist_ok=True)
    for name,value in [('per_image.json',rows),('local_search.json',search_rows),('excluded.json',dict(over_cap=over,nonfinite=excluded)),
        ('summary.json',dict(groups=summary,local_search=local,over_cap=cap,total_over_cap_fraction=len(over)/99,
            pure_canonical_worse_than_cgs=sum(x['incumbent_better_than_canonical'] for x in rows),
            runtime=runtime,worker_wall_seconds=dict(total_wall)))]:r.write(output/name,value)
    if rows:
        keys=['id','date','windows','blocks','requested','candidate_positions','used_positions','cgs_quote',
              'canonical_best_quote','best_quote','improvement_s','enumeration_minus_cgs_s','evaluated','finite','exact_assignment_space']
        with (output/'per_image.csv').open('x',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=keys,extrasaction='ignore');writer.writeheader();writer.writerows(rows)
    lines=['# CGS块级规划报价差距','',
        'M档10-22～24验证日；744幅CGS协同选择中，日期×9/64/100窗口九层各抽11幅，共99幅。seed20261111。新测试日未打开。统计按分层样本逐图等权，不是总体频率加权估计。',
        f"有效枚举{len(rows)}幅；CGS实际位置超8上限{len(over)}幅（{len(over)/99:.2%}）；CGS无有限Calendar报价{len(excluded)}幅。均不替补。",
        '重放完整动作hash、全部决策、结果、资源、粒度及R600与原记录一致。保存的是原报价调用前的状态和已顺序承诺日历，未用空日历替代。',
        '', '## 报价改进量（CGS减枚举最好，非负秒）','',
        '|分组|图数|均值|中位数|P90|最大值|不超过1秒|不超过5秒|',
        '|---|---:|---:|---:|---:|---:|---:|---:|']
    def number(value):return 'NA' if value is None else f'{value:.6f}'
    def percent(value):return 'NA' if value is None else f'{100*value:.2f}%'
    for name in groups:
        s=summary[name]['improvement'];lines.append(f"|{name}|{s['n']}|{number(s['mean'])}|{number(s['median'])}|{number(s['p90'])}|{number(s['maximum'])}|{percent(s['within_1s'])}|{percent(s['within_5s'])}|")
    lines+=['','## 原始符号差值（枚举最好减CGS，非正秒）','',
        '|分组|图数|均值|中位数|P90|最大值|','|---|---:|---:|---:|---:|---:|']
    for name in groups:
        s=summary[name]['enumeration_minus_cgs'];lines.append(f"|{name}|{s['n']}|{number(s['mean'])}|{number(s['median'])}|{number(s['p90'])}|{number(s['maximum'])}|")
    lines+=['','4块是冻结候选集合内全部目标分配的精确枚举；仍受候选截断、固定Calendar输入/计算顺序和路径规则约束，不是全位置全顺序的全局最优。16块只枚举位置块数及固定连续映射，属于近似枚举。',
        f"CGS作为可行incumbent；纯固定映射枚举比CGS差或全部不可行的图数为{sum(x['incumbent_better_than_canonical'] for x in rows)}，每图表单列canonical_best_quote。因此近似枚举中零改进不能证明CGS全局最优，1/5秒比例也仅针对本候选搜索空间。",
        '主比较全部使用同快照Calendar.quote预测，不混入整图选择的额外保护报价，不解释为实际执行改善。原保护报价在每图JSON保留。C家族选择未必每块跨星，cross_satellite_blocks另列；不按结果事后换样本。',
        '', '## 超上限样本','', '|窗口数|预选图数|超上限图数|比例|','|---|---:|---:|---:|']
    for x in cap:lines.append(f"|{x['windows']}|{x['sampled']}|{x['over_cap']}|{x['fraction']:.2%}|")
    lines+=['',f"总体超上限比例{len(over)/99:.2%}；严格超过10%才触发独立局部搜索。",'',
        '## 超上限局部搜索（单独统计）','',
        '|图数|找到改进的均值秒|中位数秒|P90秒|最大值秒|','|---:|---:|---:|---:|---:|',
        f"|{local['n']}|{number(local['mean'])}|{number(local['median'])}|{number(local['p90'])}|{number(local['maximum'])}|",
        '从CGS开始尝试单块移动和两块交换，每轮采用最好严格改进，最多20轮，无改进停止。位置集合为CGS所有实际位置与单块排名前5并集。该改进是同一报价目标下最优性差距的下限，实际差距可能更大；不是执行收益保证。未触发时NA不当0。',
        '', '## 审计与时间','',
        f"重放3项及报价队列{len(manifest['jobs'])}项全部PASS，服务器原字节与唯一领取/完成审计通过。每图所有索引无重叠无缺失；保存报价全部计数、最小值与最佳分配重新报价核验通过。共核验枚举报价{sum(x['evaluated'] for x in rows)}次、局部搜索报价{sum(x['evaluated'] for x in search_rows)}次。",
        '独立隔板组合生成器核验16块索引映射，统计用另一套标准库算法复算。此审计不是第二次重算全部Calendar报价。跨机固定样本报价预检通过，但完整跨架构逐位一致未证明。',
        '原预估重放1～1.5小时；首批报价实测70～84毫秒，单图8位置可能约5小时，因此运行前告知可能超过2小时并登记5000向量分片。四机吞吐预估见RESOURCE_ESTIMATE.json，实际用时与提交记录见DELIVERY.md。',
        '文件：report/per_image.csv、report/per_image.json、report/summary.json、report/local_search.json、report/excluded.json；冻结样本SAMPLES.json，候选CANDIDATES.json，注册replay/REGISTRATION.json与enumeration/REGISTRATION.json。']
    with (r.ROOT/'REPORT.md').open('x') as stream:stream.write('\n'.join(lines)+'\n')
    r.write(r.ROOT/'INDEPENDENT_AUDIT.json',dict(status='PASS',at=r.now(),sample_count=99,
        enumeration_images=len(rows),over_cap_images=len(over),nonfinite_images=len(excluded),local_search_images=len(search_rows),
        enumeration_quotes=sum(x['evaluated'] for x in rows),search_quotes=sum(x['evaluated'] for x in search_rows),
        queue_jobs=len(manifest['jobs']),raw_hashes_and_full_index_coverage=True,independent_best_requote=True,
        independent_statistics=True,source_hashes_checked=len(manifest['hashes']),input_hashes=input_hashes))
    paths=[r.ROOT/'REPORT.md',r.ROOT/'INDEPENDENT_AUDIT.json',r.ROOT/'SERVER_ARTIFACT_AUDIT.json',*sorted(output.glob('*'))]
    r.write(r.ROOT/'REPORT_HASHES.json',{str(path):r.sha(path) for path in paths})
    r.write(r.ROOT/'REPORT_COMPLETE.json',dict(status='PASS',at=r.now(),hashes_sha256=r.sha(r.ROOT/'REPORT_HASHES.json')))
    print(dict(status='PASS',enumeration=len(rows),over_cap=len(over),local_search=len(search_rows),overall=summary['all']['improvement']))


if __name__=='__main__':main()
