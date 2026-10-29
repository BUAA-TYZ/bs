"""S正式测试独立逐图审计，完整包络及S/M/L已冻结结果对照。"""
from collections import Counter
import json
from analysis.run_local_comparison_20261027 import BASELINES
from analysis.run_m_formal_20261027 import MAPPO
from analysis.report_m_formal_20261027 import HIGH
import math
from pathlib import Path
import numpy as np
from analysis.run_dynamic_heuristics_v2 import read, sha, digest, configuration
from analysis.run_dynamic_learning import write_json
from analysis.run_load_pressure_short import verify
from analysis.audit_dynamic_heuristics_20261026 import close
from analysis.report_m_formal_20261027 import metrics, criteria, wins, paired_bootstrap, DEADLINES, METRICS

ROOT = Path('results/redesign_20261021/main_scenario/s_formal_test_20261029')
OUT = ROOT/'independent_audit_20261029'


def features(a, reference, wanted):
    assert a['tolerance'] == 1e-6
    assert a['decisions'] == len(a['task_ids']) and set(a['task_ids']) <= wanted
    cells = outside = nodes = unknown = 0
    for key, g in a['groups'].items():
        image = key.endswith('/image')
        width, categorical = (8, (2, 5)) if image else (14, (0, 1))
        assert g['continuous_cells'] == g['rows']*(width-2)
        assert g['outside_cells'] == sum(g['outside_by_dimension'])
        for field in ('outside_by_dimension', 'new_categories_by_dimension', 'saturation_by_dimension'):
            assert len(g[field]) == width
            assert all(isinstance(n, int) and 0 <= n <= g['rows'] for n in g[field])
        assert all(g['outside_by_dimension'][i] == 0 for i in categorical)
        assert all(g['new_categories_by_dimension'][i] == 0 for i in range(width) if i not in categorical)
        assert g['missing_reference_rows'] == (0 if key in reference else g['rows'])
        cells += g['continuous_cells']; outside += g['outside_cells']
        if not image:
            nodes += g['rows']
            if key.endswith('/unknown'):
                unknown += g['rows']
    assert cells == a['continuous_cells'] and outside == a['outside_cells']
    for numerator, denominator, recorded in ((outside, cells, a['outside_cell_fraction']),
            (a['outside_decisions'], a['decisions'], a['outside_decision_fraction']),
            (unknown, nodes, a['unknown_node_fraction'])):
        if denominator:
            close(numerator/denominator, recorded)
        else:
            assert recorded is None
    assert 0 <= a['outside_decisions'] <= a['decisions']
    assert 0 <= a['new_category_decisions'] <= a['decisions']
    for name, h in a['candidates'].items():
        samples = [int(v) for v, n in h['histogram'].items() for _ in range(n)]
        assert len(samples) == h['count'] == a['decisions']
        close(np.mean(samples), h['mean'])
        close(np.quantile(samples, .5), h['P50'])
        close(np.quantile(samples, .9), h['P90'])
        assert max(samples) == h['maximum']
    return a



def complete_scopes(aggregate, registered):
    expected = dict(fixed_six_local_16=[n for n in BASELINES if n.endswith('_16')],
                    dynamic_twelve_local_4_16=list(BASELINES))
    assert registered == expected, 'Full original scopes required; no reduced envelope'
    scopes = {}
    for scope, baselines in expected.items():
        assert all(n in aggregate for n in baselines), 'Missing baseline cannot be treated as zero'
        envelope = {}; sources = {}
        for metric in METRICS:
            vals = {n:aggregate[n][metric] for n in baselines}
            best = (max if metric in HIGH else min)(vals.values()) if all(v is not None for v in vals.values()) else None
            envelope[metric] = best
            sources[metric] = [n for n,v in vals.items() if v == best] if best is not None else []
        family = 'fixed' if scope.startswith('fixed_') else 'dynamic'
        checks = {n:criteria(aggregate[n], envelope) for n in MAPPO if n.startswith(family+'_')}
        scopes[scope] = dict(status='DETERMINABLE' if all(v is not None for v in envelope.values()) else 'UNDEFINED_METRIC',
            baselines=baselines, envelope=envelope, sources=sources, criteria=checks,
            all_pass={n:None if any(v is None for v in row.values()) else all(row.values()) for n,row in checks.items()})
    return scopes


def cross_scale(aggregate, manifest, provenance):
    paths = dict(M=ROOT.parent/'m_formal_test_20261027/FINAL_AUDIT.json',
                 L=ROOT.parent/'l_formal_test_20261028/independent_audit_20261028/FINAL_AUDIT.json')
    values = {'S':aggregate}
    source_hashes = {}
    for scope, path in paths.items():
        assert manifest['hashes'][str(path)] == sha(path), 'Cross-scale source changed'
        value = read(path); assert value['status'] == 'PASS'
        values[scope] = value['day_equal']; source_hashes[str(path)] = sha(path)
    methods = sorted(set().union(*(set(v) for v in values.values())))
    rows = []
    for method in methods:
        for scope in ('S', 'M', 'L'):
            a = values[scope].get(method)
            rows.append(dict(scope=scope, method=method, window_hours=1 if scope=='L' else manifest['score_duration_s']/3600 if scope=='S' else 6,
                status='MEASURED' if a is not None else 'NOT_IN_REGISTERED_SET',
                **({k:a[k] for k in ('P60','P600','R600','unfinished')} if a is not None else
                   {k:None for k in ('P60','P600','R600','unfinished')})))
    return dict(rows=rows, source_hashes=source_hashes,
        note='Same dates, different scenarios, frozen models. L uses 1h; S/M use 6h unless S manifest states fallback. Unfinished is three-day sum, not normalized across windows. NA is absent method, not zero.')


def comparison_markdown(comparison):
    lines = ['## S/M/L汇总对照', '', '同日期、不同场景，模型冻结。L为1小时窗口，S/M窗口见表；日期内指标先计算再三日等权，未完成为三日合计。窗口及到达量不同，未完成数不宜直接作规模优劣排名；未登记方法记NA，不当0。', '',
        '|方法|档位|每日窗口h|P60 %|P600 %|R600 s|未完成合计|', '|---|---|---:|---:|---:|---:|---:|']
    for r in comparison['rows']:
        vals = ['NA']*4 if r['status'] != 'MEASURED' else [f"{r['P60']*100:.3f}",f"{r['P600']*100:.3f}",f"{r['R600']:.3f}",str(r['unfinished'])]
        lines.append('|'+r['method']+'|'+r['scope']+f"|{r['window_hours']:g}|"+'|'.join(vals)+'|')
    return '\n'.join(lines)+'\n'


def main():
    formal = True
    OUT.mkdir(exist_ok=False)
    m = read(ROOT/'manifest.json'); run = read(ROOT/'run_manifest.json')
    state = read(ROOT/'final_status.json')
    expected_methods = MAPPO+BASELINES+('global_v3_16', 'global_v3_dynamic')
    assert m['methods'] == list(expected_methods)
    assert state['status'] == 'COMPLETE' and len(state['results']) == 60
    m = {**m, 'protocols':run['protocols']}
    assert len(m['protocols']) == 3 and set(m['dates']) == {'2026-12-21','2026-12-22','2026-12-23'}
    provenance = {**m['hashes'], **run['hashes']}
    for path, expected in provenance.items():
        assert sha(path) == expected, path
    protocols = {read(p)['date']: read(p) for p in m['protocols']}
    for p in protocols.values():
        verify(p)
        assert p['score_end_s']-p['score_start_s'] == m['score_duration_s']
        assert p['score_start_s'] >= 600
        assert p['sim_duration_s']-p['score_end_s'] >= 600
        assert p['config']['num_sats'] == 84 and len(p['config']['task_source_sat_ids']) == 12
        provenance.update(p['input_hashes'])
    ranges = read(m['ranges'])['ranges']
    daily = {}; scored = {}; feature_rows = {}; granularities = {}; tails = {}
    for short in state['results']:
        path = short['path']; assert short['status'] == 'PASS' and sha(path) == short['sha256']
        provenance[path] = short['sha256']
        r = read(path); name, date = r['method'], r['date']; p = protocols[date]
        assert name in m['methods'] and date not in daily.get(name, {})
        assert r['status'] == 'PASS' and r['checks']['passed'] and r['zero_shot']
        assert r['tests_sealed'] is (not formal)
        if formal:
            assert r['opening_sha256'] == sha(ROOT.parent/'S_TEST_OPENED.json')
        assert r['protocol'] in m['protocols'] and r['config_sha256'] == digest(configuration(p, name))
        assert len(r['action_trace_sha256']) == 64
        if name in m['models']:
            assert r['checkpoint'] == m['models'][name] and r['checkpoint_sha256'] == sha(m['models'][name])
        ids = set(p['scored_task_ids']); rows = [o for o in r['outcomes'] if o['task_id'] in ids]
        assert len(rows) == len(ids) and {o['task_id'] for o in rows} == ids
        assert all(o['observed_age_seconds'] >= 600 and p['score_start_s'] <= o['release_seconds'] < p['score_end_s'] for o in rows)
        a = metrics(rows)
        for d, c in zip(DEADLINES, r['curve'], strict=True):
            assert c['D_seconds'] == d and c['unknown_count'] == 0
            close(a[f'P{d}'], c['P_lower'])
            assert set(c['on_time_ids']) == {o['task_id'] for o in rows if o['status'] == 'completed' and o['latency_seconds'] <= d}
        close(a['R600'], r['R600'])
        for k in ('mean_completed_s', 'P50_completed_s', 'P90_completed_s', 'unfinished'):
            if a[k] is None:
                assert r['latency'][k] is None
            else:
                close(a[k], r['latency'][k])
        cfg = p['config']; helpers = set(range(cfg['num_sats']))-set(cfg['task_source_sat_ids'])
        rate = sum(g['bandwidth_mbps']/8*g.get('receive_channels', 1) for g in cfg['ground_stations'])*1e6
        resource = r['resources']
        for seg, duration in {'whole':p['sim_duration_s'], 'warmup':p['score_start_s'],
                'main':p['score_end_s']-p['score_start_s'], 'followup':p['sim_duration_s']-p['score_end_s']}.items():
            close(resource[seg]['duration_s'], duration)
        for z in resource.values():
            busy = math.fsum(v for sid, v in z['helper_busy_by_node'].items() if int(sid) in helpers)
            close(busy, z['helper_busy_s']); close(busy/(len(helpers)*z['duration_s']), z['helper_utilization'])
            close(z.get('raw_ground_bytes', 0)+z.get('result_ground_bytes', 0), z['ground_bytes'])
            close(z['ground_bytes']/(rate*z['duration_s']), z['ground_channel_utilization'])
            close(math.fsum(z.get(k, 0) for k in ('control_bytes', 'raw_isl_bytes', 'result_isl_bytes', 'raw_ground_bytes', 'result_ground_bytes')), z['total_transmitted_bytes'])
        for k in ('duration_s', 'total_transmitted_bytes', 'control_bytes', 'raw_isl_bytes', 'result_isl_bytes', 'raw_ground_bytes', 'result_ground_bytes', 'helper_busy_s', 'ground_bytes'):
            close(sum(resource[seg].get(k, 0) for seg in ('warmup', 'main', 'followup')), resource['whole'].get(k, 0))
        assert set(r['granularity']) == ids
        for task, g in r['granularity'].items():
            assert g['packet_mb'] >= g['unique_mb']-1e-9 and g['actual'] in (4, 16)
            assert g['requested'] in (None, 4, 16)
            if name.startswith('dynamic_') and task in r['choices']:
                assert g['requested'] == (4, 16, 16)[r['choices'][task]]
            elif name != 'global_v3_dynamic' and not name.startswith('dynamic_'):
                assert g['requested'] == (4 if name.endswith('_4') else 16)
        key = f'{name}/{date}'
        granularities[key] = dict(records=r['granularity'], counts=dict(Counter(f"{g['requested']}->{g['actual']}" for g in r['granularity'].values())))
        if name in ranges:
            feature_rows[key] = features(r['feature_audit'], ranges[name], ids)
            assert set(r['unacted_scored_ids']) == ids-set(r['choices'])
        a['resources'] = resource
        daily.setdefault(name, {})[date] = a; scored.setdefault(name, {})[date] = rows
        tails[key] = [o for o in rows if o['status'] != 'completed' or o['latency_seconds'] > 300]
    assert set(daily) == set(m['methods'])
    aggregate = {}; curves = {}
    for name, days in daily.items():
        assert set(days) == set(m['dates'])
        keys = [f'P{d}' for d in DEADLINES]+['R600', 'mean_completed_s', 'P50_completed_s', 'P90_completed_s']
        aggregate[name] = {k:float(np.mean([a[k] for a in days.values()])) if all(a[k] is not None for a in days.values()) else None for k in keys}
        aggregate[name]['scored_images'] = sum(a['n'] for a in days.values())
        aggregate[name]['unfinished'] = sum(a['unfinished'] for a in days.values())
        aggregate[name]['resources'] = {seg:{k:float(np.mean([d['resources'][seg][k] for d in days.values()])) for k in ('total_transmitted_bytes', 'helper_utilization', 'ground_channel_utilization')} for seg in ('main', 'whole')}
        xs = sorted({60., 600., *[o['latency_seconds'] for rows in scored[name].values() for o in rows if o['status'] == 'completed' and 60 <= o['latency_seconds'] <= 600]})
        curves[name] = dict(D=xs, P=[float(np.mean([sum(o['status'] == 'completed' and o['latency_seconds'] <= x for o in rows)/len(rows) for rows in scored[name].values()])) for x in xs])
    diagnostic = None
    if not formal:
        threshold = min(aggregate[n]['R600'] for n in m['finetune_diagnostic_rules'])
        diagnostic = dict(threshold_R600=threshold, sources=[n for n in m['finetune_diagnostic_rules'] if aggregate[n]['R600'] == threshold],
            needs={n:aggregate[n]['R600'] > threshold for n in m['models']}, models_unchanged=True)
    pairs = {n:{b:dict(wins=wins(aggregate[n], aggregate[b]), criteria=criteria(aggregate[n], aggregate[b]),
        group='global_reference' if b.startswith('global_') else 'same_information') for b in m['methods'] if b not in m['models']} for n in m['models']}
    if formal:
        indices = np.random.default_rng(m['bootstrap']['seed']).integers(0, 3, size=(m['bootstrap']['draws'], 3))
        for n, baselines in pairs.items():
            for b, value in baselines.items():
                value['paired'] = {k:paired_bootstrap([daily[n][d][k] for d in m['dates']],
                    [daily[b][d][k] for d in m['dates']], indices) for k in METRICS}
    scopes = complete_scopes(aggregate, m['comparison_scopes'])
    audit = dict(status='PASS', records=60, protocols=3, hashes=len(provenance), provenance=provenance,
        daily=daily, day_equal=aggregate, scopes=scopes, finetune_diagnostic=diagnostic,
        limitation='Saved counters audited; observations/packets not independently replayed; encoded pre-clipping values unavailable')
    for name, value in [('FINAL_AUDIT', audit), ('PAIRWISE', pairs), ('full_curves', curves),
                        ('FEATURE_AUDIT', feature_rows), ('GRANULARITY', granularities), ('TAIL_RECORDS', tails)]:
        write_json(OUT/f'{name}.json', value)
    lines = ['# S零样本正式测试独立报告', '',
        f"60条全新正式记录，20方法，三日各{m['score_duration_s']/3600:g}小时。12 SAR+72计算星，1:6。",
        '同日期、不同场景，模型冻结。2026-12-21至23已对M/L开启，S任务由S新轨道按同一规则新生成；不是新的独立日期。',
        '逐图指标先按日计算，再三日等权；只做零样本，不训练、不微调、不选模、不调参。', '',
        '|方法|P60/90/120/180/240/300/600 %|均值/P50/P90 s|R600 s|未完成合计|', '|---|---|---|---:|---:|']
    for n in m['methods']:
        a = aggregate[n]
        lines.append('|'+n+'|'+'/'.join(f'{a[f"P{d}"]*100:.3f}' for d in DEADLINES)+'|'+'/'.join('NA' if a[k] is None else f'{a[k]:.3f}' for k in ('mean_completed_s', 'P50_completed_s', 'P90_completed_s'))+f'|{a["R600"]:.3f}|{a["unfinished"]}|')
    lines += ['', '## 两套成功口径', '', '按每方法三日等权后跨基线取逐指标最强包络。两套集合完整；全局参考不进入局部包络。']
    for name, scope in scopes.items():
        lines += ['', '### '+name, '', json.dumps(dict(envelope=scope['envelope'], sources=scope['sources']), ensure_ascii=False),
            '', '|模型|P60严格高|均值严格低|P600不下降|R600严格低|全部满足|', '|---|---|---|---|---|---|']
        for model, check in scope['criteria'].items():
            lines.append('|'+model+'|'+'|'.join('不可判定' if check[k] is None else '通过' if check[k] else '未通过' for k in METRICS)+'|'+str(scope['all_pass'][model])+'|')
    if diagnostic:
        lines += ['', '## 微调需求', '', f'仅比较保留局部规则的最低日等权R600={threshold:.6f}s，来源：'+','.join(diagnostic['sources'])+'。CGS/HEFT/强地面不纳入此门槛。', '', '|模型|需要微调|', '|---|---|']
        lines += [f'|{n}|{"是" if flag else "否"}|' for n, flag in diagnostic['needs'].items()]
        lines += ['', '本诊断不启动训练或重选checkpoint；实际微调须另登记并在L测试前冻结。']
    else:
        lines += ['', '不使用测试结果触发微调、选模或调参。PAIRWISE包含MAPPO减基线的逐日配对差及10000次日期bootstrap 95%区间；仅三日，离散描述性区间，不校正选择偏差。']
    lines += ['', '## 审计边界', '',
        'FINAL_AUDIT包含逐日资源分类、暖机/主窗/随访守恒、多跳及冗余控制总字节、计算星及地面利用率。GRANULARITY保存请求/实际粒度。FEATURE_AUDIT逐项核对候选直方图均值/P50/P90/最大、每维越界/类别/饱和计数及连续单元/决策分母、未知占比。只审计保存计数器，不是逐观测或逐包重放；历史编码截断前值不可复原。', '',
        'Orin四块内存已另在特定4096窗口、四块缓冲、单窗及双流配置完成实测，见results/method_cost_20261028/MEMORY_REPORT.md；不外推全队列或在轨内存可行性。'+'S测试已唯一开启。']
    comparison = cross_scale(aggregate, m, provenance)
    write_json(OUT/'S_M_L_COMPARISON.json', comparison)
    comparison_text = comparison_markdown(comparison)
    (OUT/'S_M_L_COMPARISON.md').write_text(comparison_text)
    lines += ['', comparison_text]
    (OUT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(11, 7), layout='constrained')
    for i, n in enumerate(m['methods']):
        ax.step(curves[n]['D'], np.array(curves[n]['P'])*100, where='post', label=n, color=plt.get_cmap('tab20')(i))
    ax.set(xlabel='Deadline (s)', ylabel='Day-equal completion (%)', xlim=(60, 600))
    ax.legend(fontsize=8, ncol=2); ax.grid(alpha=.2)
    fig.savefig(OUT/'curves.png', dpi=160); plt.close(fig)
    write_json(OUT/'REPORT_HASHES.json', {str(p):sha(p) for p in OUT.iterdir() if p.is_file()})
    print(json_summary(audit, diagnostic))


def json_summary(audit, diagnostic):
    import json
    return json.dumps(dict(status=audit['status'], records=audit['records'], hashes=audit['hashes'], diagnostic=diagnostic), ensure_ascii=False)


if __name__ == '__main__':
    main()
