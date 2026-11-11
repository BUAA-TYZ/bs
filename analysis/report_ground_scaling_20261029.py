"""扩站补充报告：逐图复算、完整局部包络、同日期配对及逐站资源审计。"""
import math
from pathlib import Path
import numpy as np
from analysis import run_ground_scaling_20261029 as run
from analysis.run_dynamic_heuristics_v2 import read, sha, digest, configuration
from analysis.run_dynamic_learning import write_json
from analysis.run_local_comparison_20261027 import check_record
from analysis.run_load_pressure_short import verify
from analysis.report_m_formal_20261027 import metrics, criteria, wins, paired_bootstrap, METRICS, HIGH, DEADLINES
from analysis.report_s_formal_20261029 import complete_scopes, features
from analysis.audit_dynamic_heuristics_20261026 import close

ROOT = run.ROOT
COMPARE = (*METRICS, 'unfinished')


def audit_resources(row, protocol):
    cfg = protocol['config']; resource = row['resources']
    ids = {g['id'] for g in cfg['ground_stations']}
    durations = dict(whole=protocol['sim_duration_s'], main=protocol['score_end_s']-protocol['score_start_s'],
        warmup=protocol['score_start_s'], followup=protocol['sim_duration_s']-protocol['score_end_s'])
    for segment, duration in durations.items():
        v = resource[segment]; close(v['duration_s'], duration)
        close(v.get('raw_ground_bytes', 0)+v.get('result_ground_bytes', 0), v['ground_bytes'])
        close(sum(v.get(k, 0) for k in ('control_bytes','raw_isl_bytes','result_isl_bytes','raw_ground_bytes','result_ground_bytes')), v['total_transmitted_bytes'])
        helpers = set(range(cfg['num_sats']))-set(cfg['task_source_sat_ids'])
        close(sum(n for sid,n in v['helper_busy_by_node'].items() if int(sid) in helpers), v['helper_busy_s'])
        close(v['helper_busy_s']/(len(helpers)*duration), v['helper_utilization'])
        if 'stations' not in v:
            assert len(ids) == 1, 'Old single-station total is attributable to that sole station only'
            gid = next(iter(ids))
            v['stations'] = {gid:dict(bytes=v['ground_bytes'], utilization=v['ground_channel_utilization'])}
            v['station_provenance'] = 'sole-station aggregate attribution; no new measurement'
        assert set(v['stations']) == ids
        close(sum(s['bytes'] for s in v['stations'].values()), v['ground_bytes'])
        capacity = 0
        for g in cfg['ground_stations']:
            cap = g['bandwidth_mbps']/8*1e6*g.get('receive_channels',1)*duration
            s = v['stations'][g['id']]; close(s['bytes']/cap, s['utilization'])
            assert -.000001 <= s['utilization'] <= 1.000001
            capacity += cap
        close(v['ground_bytes']/capacity, v['ground_channel_utilization'])
        if 'ground_pool_busy_s' in v:
            close(v['ground_pool_busy_s']/duration, v['ground_pool_utilization'])
            assert 0 <= v['ground_pool_utilization'] <= 1.000001
    for key in ('duration_s','ground_bytes','total_transmitted_bytes','control_bytes','raw_isl_bytes','result_isl_bytes',
                'raw_ground_bytes','result_ground_bytes','helper_busy_s'):
        close(sum(resource[s].get(key,0) for s in ('warmup','main','followup')), resource['whole'].get(key,0))
    for gid in ids:
        close(sum(resource[s]['stations'][gid]['bytes'] for s in ('warmup','main','followup')),
            resource['whole']['stations'][gid]['bytes'])
    return resource


def records(manifest, state):
    available = {scale:{'single':{d:dict(v) for d,v in manifest['reused'][scale].items()},
                        'scaled':{d:{} for d in run.DATES}} for scale in ('S','M','L')}
    available['S']['scaled'] = {d:dict(v) for d,v in available['S']['single'].items()}
    for short in state['results']:
        assert short['status'] == 'PASS' and sha(short['path']) == short['sha256']
        row = read(short['path'])
        assert row['stage'] in run.STAGES
        available[row['scale']][row['setting']][row['date']][row['method']] = short['path']
    return available


def aggregate(days):
    assert set(days) == set(run.DATES)
    result = {}
    for key in (*[f'P{d}' for d in DEADLINES], 'R600','mean_completed_s','P50_completed_s','P90_completed_s','unfinished','n'):
        values = [days[d][key] for d in run.DATES]
        result[key] = None if any(v is None for v in values) else sum(values) if key in ('unfinished','n') else float(np.mean(values))
    return result


def format_value(value, key):
    return 'NA' if value is None else f'{value*100:.3f}' if key in HIGH else f'{value:.3f}'


def main():
    out = ROOT/'report'; out.mkdir(exist_ok=False)
    manifest = run.verify_manifest(); state = read(ROOT/'final_status.json')
    assert state['status'] in ('COMPLETE','PARTIAL')
    assert len(state['results']) <= 147
    if state['status'] == 'COMPLETE': assert len(state['results']) == 147
    available = records(manifest, state)
    scales = [s for s in ('S','M','L') if all(set(available[s][setting][date]) == set(run.METHODS)
              for setting in ('single','scaled') for date in run.DATES)]
    missing = {s:{setting:{d:sorted(set(run.METHODS)-set(available[s][setting][d])) for d in run.DATES}
                 for setting in ('single','scaled')} for s in ('S','M','L') if s not in scales}
    available = {s:available[s] for s in scales}
    provenance = dict(manifest['hashes'])
    ranges = read(manifest['ranges'])['ranges']
    daily, means, curves, scopes, pairs, features_saved, tails = {}, {}, {}, {}, {}, {}, {}
    station_rows = []; raw_scored = {}
    indices = np.random.default_rng(20261027).integers(0,3,size=(10000,3))
    for scale in scales:
        daily[scale] = {}; means[scale] = {}; scopes[scale] = {}; pairs[scale] = {}; curves[scale] = {}
        for setting in ('single','scaled'):
            days_by_method = {}; raw_scored[scale,setting] = {}; curves[scale][setting] = {}
            for date in run.DATES:
                assert set(available[scale][setting][date]) == set(run.METHODS), 'Do not shrink the method set'
                original = read(manifest['originals'][scale][date])
                expected = run.derive_protocol(original, scale, setting == 'scaled')
                for method in run.METHODS:
                    path = available[scale][setting][date][method]; row = read(path); p = read(row['protocol'])
                    provenance[path] = sha(path); provenance[row['protocol']] = sha(row['protocol'])
                    verify(p); provenance.update(p['input_hashes'])
                    assert row['method'] == method and row['date'] == date and row['zero_shot']
                    assert p['config'] == expected['config'], 'Scene pair differs beyond registered stations'
                    for key in ('events','scored_task_ids','score_start_s','score_end_s','sim_duration_s','origin_absolute_s'):
                        assert p[key] == original[key], key
                    assert row['config_sha256'] == digest(configuration(p, method))
                    selected = check_record(row,p); a = metrics(selected)
                    close(a['R600'],row['R600'])
                    for key in ('mean_completed_s','P50_completed_s','P90_completed_s','unfinished'):
                        close(a[key], row['latency'][key])
                    if method in manifest['models']:
                        assert row['checkpoint'] == manifest['models'][method]
                        assert row['checkpoint_sha256'] == sha(row['checkpoint'])
                        features_saved[f'{scale}/{setting}/{method}/{date}'] = features(row['feature_audit'], ranges[method],set(p['scored_task_ids']))
                    resource = audit_resources(row,p)
                    a['resources'] = resource
                    days_by_method.setdefault(method,{})[date] = a
                    raw_scored[scale,setting].setdefault(method,{})[date] = selected
                    tails[f'{scale}/{setting}/{method}/{date}'] = [r for r in selected if r['status'] != 'completed' or r['latency_seconds'] > 300]
                    for segment in ('main','whole'):
                        for gid,s in resource[segment]['stations'].items():
                            station_rows.append(dict(scale=scale,setting=setting,method=method,date=date,segment=segment,station=gid,**s))
                    assert set(row['granularity']) == set(p['scored_task_ids'])
                    for g in row['granularity'].values():
                        assert g['actual'] in (4,16) and g['requested'] in (None,4,16)
                        assert g['packet_mb'] >= g['unique_mb']-1e-9
            daily[scale][setting] = days_by_method
            means[scale][setting] = {n:aggregate(days) for n,days in days_by_method.items()}
            scopes[scale][setting] = complete_scopes(means[scale][setting], manifest['comparison_scopes'])
            pairs[scale][setting] = {n:{b:dict(wins=wins(means[scale][setting][n],means[scale][setting][b]),
                criteria=criteria(means[scale][setting][n],means[scale][setting][b]),
                paired={k:paired_bootstrap([days_by_method[n][d][k] for d in run.DATES],
                    [days_by_method[b][d][k] for d in run.DATES],indices) for k in COMPARE})
                for b in run.METHODS if b not in run.MAPPO} for n in run.MAPPO}
            for method,days in raw_scored[scale,setting].items():
                x = sorted({60.,600.,*[r['latency_seconds'] for rows in days.values() for r in rows
                    if r['status']=='completed' and 60<=r['latency_seconds']<=600]})
                curves[scale][setting][method] = dict(D=x,P=[sum(sum(r['status']=='completed' and r['latency_seconds']<=d
                    for r in rows)/len(rows) for rows in days.values())/3 for d in x])
    changes = {}; best = {}
    for scale in scales:
        changes[scale] = {}; best[scale] = {}
        for method in run.METHODS:
            for date in run.DATES:
                a,b = raw_scored[scale,'single'][method][date],raw_scored[scale,'scaled'][method][date]
                assert {r['task_id']:r['release_seconds'] for r in a} == {r['task_id']:r['release_seconds'] for r in b}
            changes[scale][method] = {k:dict(
                paired=paired_bootstrap([daily[scale]['scaled'][method][d][k] for d in run.DATES],
                    [daily[scale]['single'][method][d][k] for d in run.DATES],indices),
                aggregate_difference=None if any(means[scale][s][method][k] is None for s in ('single','scaled')) else
                    means[scale]['scaled'][method][k]-means[scale]['single'][method][k]) for k in COMPARE}
        for setting in ('single','scaled'):
            table = means[scale][setting]; best[scale][setting] = {}
            for family,scope in (('fixed','fixed_six_local_16'),('dynamic','dynamic_twelve_local_4_16')):
                baselines = manifest['comparison_scopes'][scope]
                values = {}
                for k in COMPARE:
                    vals = {n:table[n][k] for n in baselines}
                    value = None if any(v is None for v in vals.values()) else (max if k in HIGH else min)(vals.values())
                    sources = [] if value is None else [n for n,v in vals.items() if v == value]
                    values[k] = dict(value=value, baselines=sources,
                        model_differences={n:None if value is None or table[n][k] is None else table[n][k]-value for n in run.MAPPO if n.startswith(family+'_')})
                best[scale][setting][family] = values
    audit = dict(status='PASS', unique_records=len({p for s in available.values() for d in s.values() for rows in d.values() for p in rows.values()}),
        logical_records=len(scales)*120, new_records=len(state['results']), completion=state['status'],
        included_scales=scales, missing=missing, provenance=provenance, daily=daily, day_equal=means, scopes=scopes,
        record_paths=available, limitations='Saved counters audit, not independent observation or packet replay. Three paired dates give descriptive bootstrap intervals; no selection-bias correction.')
    for name,value in [('FINAL_AUDIT',audit),('PAIRWISE',pairs),('SCENARIO_PAIRED',changes),('BEST_LOCAL',best),
                        ('STATION_UTILIZATION',station_rows),('full_curves',curves),('FEATURE_AUDIT',features_saved),('TAIL_RECORDS',tails)]:
        write_json(out/f'{name}.json',value)
    lines = ['# 地面站随规模扩展：配对补充评估','',
        '同日期、同轨道任务的场景变体，不是新的独立测试日期。全部模型冻结；S/M每日6小时，L每日1小时，三日2026-12-21至23。',
        'S仍为原北京单站，两个设定引用同一批结果，差值为0不构成独立重复证据。M新增喀什，L再新增三亚，每站400 Mbps、1通道。',
        '地面互联传输额外时延假设为0，计算使用原总能力的唯一共享FIFO，不随站数扩容。原单站结果未修改；L缺失9规则的27条单站记录另行补齐。',
        '完整纳入档位：'+','.join(scales)+'；缺项档位：'+(','.join(missing) or '无')+'。缺项为NA，不当0、不缩减包络；原始缺项清单见FINAL_AUDIT.json。',
        'P及交付时延先按日计算再三日等权；未完成为三日合计。未完成的paired-day差值/区间以每日计数为单位，表内总差另列。均值仅包含已交付图像，不能替代R600。',
        '只有3个配对日，10000次paired-day bootstrap、seed20261027，区间为描述性，不校正选择偏差。保存计数器核验不代表逐观测/逐包重放。','',
        '|档位|方法|设定|P60 %|P600 %|交付均值 s|R600 s|未完成合计|','|---|---|---|---:|---:|---:|---:|---:|']
    order = ('P60','P600','mean_completed_s','R600','unfinished')
    for scale in reversed(scales):
        for method in run.METHODS:
            for setting in ('single','scaled'):
                a = means[scale][setting][method]
                lines.append('|'+scale+'|'+method+'|'+setting+'|'+'|'.join(format_value(a[k],k) for k in order)+'|')
            lines.append('|'+scale+'|'+method+'|扩站减单站|'+'|'.join(format_value(changes[scale][method][k]['aggregate_difference'],k) for k in order)+'|')
    lines += ['', '## 完整成功口径', '', '|档位|设定|口径|种子|P60严格高|均值严格低|P600不下降|R600严格低|全部通过|','|---|---|---|---|---|---|---|---|---|']
    for scale in reversed(scales):
        for setting,values in scopes[scale].items():
            for scope,value in values.items():
                for method,checks in value['criteria'].items():
                    lines.append('|'+scale+'|'+setting+'|'+scope+'|'+method+'|'+'|'.join(str(checks[k]) for k in METRICS)+'|'+str(value['all_pass'][method])+'|')
    lines += ['', '## 模型相对基线的差值', '', '逐指标最好局部规则可能不是同一方法；并列来源全部列出。全局参考不进入局部包络。差值为模型减基线；P为百分点，其余为秒/未完成数。','',
        '|档位|设定|模型|指标|基线|差值|','|---|---|---|---|---|---:|']
    for scale in reversed(scales):
        for setting in ('single','scaled'):
            table = means[scale][setting]
            for model in run.MAPPO:
                family = model.split('_')[0]
                for k in COMPARE:
                    item = best[scale][setting][family][k]
                    lines.append(f'|{scale}|{setting}|{model}|{k}|'+','.join(item['baselines'])+'|'+format_value(item['model_differences'][model],k)+'|')
                    for reference in ('global_v3_16','global_v3_dynamic'):
                        delta = None if table[model][k] is None or table[reference][k] is None else table[model][k]-table[reference][k]
                        lines.append(f'|{scale}|{setting}|{model}|{k}|{reference}|'+format_value(delta,k)+'|')
    lines += ['', '## 各站下行利用率', '', '主窗和全程分别以每站400Mbps×1通道×时长为分母；逐日明细在STATION_UTILIZATION.json。','',
        '|档位|设定|方法|分段|站|三日等权利用率 %|','|---|---|---|---|---|---:|']
    groups = {}
    for row in station_rows:
        key = tuple(row[k] for k in ('scale','setting','method','segment','station'))
        groups.setdefault(key,[]).append(row['utilization'])
    for key,values in groups.items():
        assert len(values)==3
        lines.append('|'+ '|'.join(key)+f'|{np.mean(values)*100:.6f}|')
    (out/'REPORT.md').write_text('\n'.join(lines)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes = plt.subplots(3,2,figsize=(14,12),sharex=True,sharey=True)
    for i,scale in enumerate(('S','M','L')):
        for j,setting in enumerate(('single','scaled')):
            ax=axes[i,j]
            if scale not in scales:
                ax.set_title(f'{scale} {setting}: NA (incomplete)');continue
            for method in run.MAPPO+('zero_2_16','zero_2_4','global_v3_dynamic'):
                c=curves[scale][setting][method];ax.step(c['D'],np.asarray(c['P'])*100,where='post',label=method)
            ax.set_title(f'{scale} {setting} ({1 if scale=="L" else 6}h)');ax.set_xlabel('D (s)');ax.set_ylabel('P(D) %');ax.grid(alpha=.2)
    axes[0,0].legend(fontsize=6);fig.tight_layout();fig.savefig(out/'curves.png',dpi=140);plt.close(fig)
    write_json(out/'REPORT_HASHES.json',{str(p):sha(p) for p in sorted(out.iterdir()) if p.is_file()})
    print('GROUND SCALING REPORT PASS',audit['unique_records'])


if __name__ == '__main__':
    main()
