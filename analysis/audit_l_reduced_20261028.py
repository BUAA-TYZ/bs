"""逐图复算L精简验证；资源及特征仅审计保存计数器。"""
from collections import Counter
import math
from pathlib import Path
import numpy as np
from analysis.run_dynamic_heuristics_v2 import read, sha, digest, configuration
from analysis.run_dynamic_learning import write_json
from analysis.run_load_pressure_short import verify
from analysis.audit_dynamic_heuristics_20261026 import close
from analysis.report_m_formal_20261027 import metrics, criteria, wins, paired_bootstrap, DEADLINES, METRICS

ROOT = Path('results/redesign_20261021/main_scenario/l_reduced_validation_20261028')
OUT = ROOT/'independent_audit_20261028'


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
    close(outside/cells, a['outside_cell_fraction'])
    close(a['outside_decisions']/a['decisions'], a['outside_decision_fraction'])
    close(unknown/nodes, a['unknown_node_fraction'])
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


def main(formal=False):
    OUT.mkdir(exist_ok=False)
    m = read(ROOT/'manifest.json'); run = read(ROOT/'run_manifest.json')
    state = read(ROOT/'final_status.json')
    if not formal:
        assert sha(ROOT/'manifest.json') == 'a65d9d4722540ac733ed50ceb2a75955ee59ace09720d02826f8caef52610c42'
    assert state['status'] == 'COMPLETE' and len(state['results']) == 42
    provenance = {**m['hashes'], **run['hashes']}
    for path, expected in provenance.items():
        assert sha(path) == expected, path
    protocols = {read(p)['date']: read(p) for p in m['protocols']}
    for p in protocols.values():
        verify(p)
        assert p['score_end_s']-p['score_start_s'] == 3600
        assert p['sim_duration_s']-p['score_end_s'] >= 600
        assert p['config']['num_sats'] == 336 and len(p['config']['task_source_sat_ids']) == 48
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
            assert r['opening_sha256'] == sha(ROOT.parent/'L_TEST_OPENED.json')
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
            close(a[k], r['latency'][k])
        cfg = p['config']; helpers = set(range(cfg['num_sats']))-set(cfg['task_source_sat_ids'])
        rate = sum(g['bandwidth_mbps']/8*g.get('receive_channels', 1) for g in cfg['ground_stations'])*1e6
        resource = r['resources']
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
        aggregate[name] = {k:float(np.mean([a[k] for a in days.values()])) for k in keys}
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
    scopes = {n:dict(status='INCOMPLETE_NOT_DETERMINABLE', envelope=None) for n in ('fixed_six_local_16', 'dynamic_twelve_local_4_16')}
    audit = dict(status='PASS', records=42, protocols=3, hashes=len(provenance), provenance=provenance,
        daily=daily, day_equal=aggregate, scopes=scopes, finetune_diagnostic=diagnostic,
        limitation='Saved counters audited; observations/packets not independently replayed; encoded pre-clipping values unavailable')
    for name, value in [('FINAL_AUDIT', audit), ('PAIRWISE', pairs), ('full_curves', curves),
                        ('FEATURE_AUDIT', feature_rows), ('GRANULARITY', granularities), ('TAIL_RECORDS', tails)]:
        write_json(OUT/f'{name}.json', value)
    lines = ['# L精简零样本'+('正式测试' if formal else '验证')+'独立报告', '',
        ('42条全新正式测试记录，一次开启12-21至23。' if formal else '42条记录（9复用、33新运行）。')+
        '14方法、三日各1小时。逐图指标先按日计算，再三日等权；不选择现有checkpoint。', '',
        '|方法|P60/90/120/180/240/300/600 %|均值/P50/P90 s|R600 s|未完成合计|', '|---|---|---|---:|---:|']
    for n in m['methods']:
        a = aggregate[n]
        lines.append('|'+n+'|'+'/'.join(f'{a[f"P{d}"]*100:.3f}' for d in DEADLINES)+'|'+'/'.join(f'{a[k]:.3f}' for k in ('mean_completed_s', 'P50_completed_s', 'P90_completed_s'))+f'|{a["R600"]:.3f}|{a["unfinished"]}|')
    lines += ['', '## 两套成功口径', '', 'fixed_six_local_16：不可判定。dynamic_twelve_local_4_16：不可判定。精简后缺完整基线集合，不计算替代包络；逐保留基线胜平负及原四项不等式见PAIRWISE.json。']
    if diagnostic:
        lines += ['', '## 微调需求', '', f'仅比较保留局部规则的最低日等权R600={threshold:.6f}s，来源：'+','.join(diagnostic['sources'])+'。CGS/HEFT/强地面不纳入此门槛。', '', '|模型|需要微调|', '|---|---|']
        lines += [f'|{n}|{"是" if flag else "否"}|' for n, flag in diagnostic['needs'].items()]
        lines += ['', '本诊断不启动训练或重选checkpoint；实际微调须另登记并在L测试前冻结。']
    else:
        lines += ['', '不使用测试结果触发微调、选模或调参。PAIRWISE包含MAPPO减基线的逐日配对差及10000次日期bootstrap 95%区间；仅三日，离散描述性区间，不校正选择偏差。']
    lines += ['', '## 审计边界', '',
        'FINAL_AUDIT包含逐日资源分类、暖机/主窗/随访守恒、多跳及冗余控制总字节、计算星及地面利用率。GRANULARITY保存请求/实际粒度。FEATURE_AUDIT逐项核对候选直方图均值/P50/P90/最大、每维越界/类别/饱和计数及连续单元/决策分母、未知占比。只审计保存计数器，不是逐观测或逐包重放；历史编码截断前值不可复原。', '',
        'Orin四块内存已另在特定4096窗口、四块缓冲、单窗及双流配置完成实测，见results/method_cost_20261028/MEMORY_REPORT.md；不外推全队列或在轨内存可行性。'+('L测试已唯一开启。' if formal else 'L测试仍封存。')]
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
