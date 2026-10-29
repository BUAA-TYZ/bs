"""独立复核S主表、全阶梯、完整包络、配对区间及跨规模对照；不启动仿真。"""
import math
from pathlib import Path
import numpy as np
from PIL import Image
from analysis.run_dynamic_heuristics_v2 import read, sha
from analysis.run_dynamic_learning import write_json

ROOT = Path('results/redesign_20261021/main_scenario/s_formal_test_20261029')
METRICS = ('P60', 'mean_completed_s', 'P600', 'R600')
RULES = ('zero_1', 'zero_2', 'history_1', 'history_2', 'minload_1', 'minload_2')


def close(a, b):
    if a is None or b is None:
        assert a is b
    else:
        assert math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-8), (a, b)


def raw_metrics(rows):
    assert rows
    delivered = [r['latency_seconds'] for r in rows if r['status'] == 'completed']
    a = {f'P{d}':sum(t <= d for t in delivered)/len(rows) for d in (60,90,120,180,240,300,600)}
    a.update(R600=(sum(min(t,600) for t in delivered)+600*(len(rows)-len(delivered)))/len(rows),
        mean_completed_s=sum(delivered)/len(delivered) if delivered else None,
        P50_completed_s=float(np.quantile(delivered,.5)) if delivered else None,
        P90_completed_s=float(np.quantile(delivered,.9)) if delivered else None,
        unfinished=len(rows)-len(delivered), n=len(rows))
    return a


def main():
    out = ROOT/'independent_audit_20261029'
    hashes = read(out/'REPORT_HASHES.json')
    for path, expected in hashes.items():
        assert sha(path) == expected, path
    manifest = read(ROOT/'manifest.json'); audit = read(out/'FINAL_AUDIT.json')
    pairs = read(out/'PAIRWISE.json'); curves = read(out/'full_curves.json')
    assert audit['status'] == 'PASS' and audit['records'] == 60
    assert len(manifest['methods']) == 20 and len(manifest['models']) == 6
    for path, expected in audit['provenance'].items():
        assert sha(path) == expected, path
    daily = {}; outcomes = {}; aggregates = {}
    for short in read(ROOT/'final_status.json')['results']:
        assert short['status'] == 'PASS' and sha(short['path']) == short['sha256']
        row = read(short['path']); protocol = read(row['protocol'])
        ids = set(protocol['scored_task_ids'])
        rows = [o for o in row['outcomes'] if o['task_id'] in ids]
        assert {r['task_id'] for r in rows} == ids and len(rows) == len(ids)
        name, date = row['method'], row['date']
        daily.setdefault(name,{})[date] = raw_metrics(rows)
        outcomes.setdefault(name,{})[date] = rows
        for k,v in daily[name][date].items():
            close(v, audit['daily'][name][date][k])
    for name, days in daily.items():
        assert set(days) == set(manifest['dates'])
        a = {}
        for key in next(iter(days.values())):
            values = [days[d][key] for d in manifest['dates']]
            if key == 'n':
                close(sum(values), audit['day_equal'][name]['scored_images']); continue
            a[key] = None if any(v is None for v in values) else sum(values) if key=='unfinished' else sum(values)/3
            close(a[key], audit['day_equal'][name][key])
        aggregates[name] = a
        x = sorted({60.,600.,*[o['latency_seconds'] for rs in outcomes[name].values() for o in rs
            if o['status']=='completed' and 60 <= o['latency_seconds'] <= 600]})
        assert curves[name]['D'] == x and len(curves[name]['P']) == len(x)
        for d, value in zip(x, curves[name]['P'], strict=True):
            close(sum(sum(o['status']=='completed' and o['latency_seconds']<=d for o in rs)/len(rs)
                for rs in outcomes[name].values())/3, value)
    scopes = dict(fixed_six_local_16=[f'{n}_16' for n in RULES],
                  dynamic_twelve_local_4_16=[f'{n}_{g}' for n in RULES for g in (4,16)])
    assert manifest['comparison_scopes'] == scopes
    for name, baselines in scopes.items():
        scope = audit['scopes'][name]
        assert scope['baselines'] == baselines
        for key in METRICS:
            values = [aggregates[b][key] for b in baselines]
            expected = None if any(v is None for v in values) else (max(values) if key in ('P60','P600') else min(values))
            close(scope['envelope'][key], expected)
            assert scope['sources'][key] == ([] if expected is None else [b for b in baselines if aggregates[b][key] == expected])
        for model, checks in scope['criteria'].items():
            expected = {k: inequality(aggregates[model][k], scope['envelope'][k], k) for k in METRICS}
            assert checks == expected
            assert scope['all_pass'][model] == (None if any(v is None for v in expected.values()) else all(expected.values()))
    indices = np.random.default_rng(20261027).integers(0,3,size=(10000,3))
    count = 0
    assert set(pairs) == set(manifest['models'])
    for model, baselines in pairs.items():
        assert set(baselines) == set(manifest['methods'])-set(manifest['models'])
        for baseline, pair in baselines.items():
            assert pair['group'] == ('global_reference' if baseline.startswith('global_') else 'same_information')
            for key in METRICS:
                a,b = aggregates[model][key], aggregates[baseline][key]
                win = 'NA' if a is None or b is None else 'tie' if a==b else 'win' if ((a>b) if key in ('P60','P600') else (a<b)) else 'loss'
                assert pair['wins'][key] == win and pair['criteria'][key] == inequality(a,b,key)
                deltas = [None if daily[model][d][key] is None or daily[baseline][d][key] is None else
                    daily[model][d][key]-daily[baseline][d][key] for d in manifest['dates']]
                saved = pair['paired'][key]
                if any(v is None for v in deltas):
                    assert all(saved[k] is None for k in ('daily','mean','ci95'))
                else:
                    for x,y in zip(deltas,saved['daily'],strict=True): close(x,y)
                    close(sum(deltas)/3,saved['mean'])
                    limits=np.quantile(np.asarray(deltas)[indices].mean(axis=1),[.025,.975])
                    for x,y in zip(limits,saved['ci95'],strict=True): close(x,y)
                count += 1
    comparison = read(out/'S_M_L_COMPARISON.json')
    data = {'S':aggregates}
    for path, expected in comparison['source_hashes'].items():
        assert sha(path) == expected == manifest['hashes'][path]
        data['M' if 'm_formal_test_' in path else 'L'] = read(path)['day_equal']
    methods = set().union(*(set(v) for v in data.values()))
    assert {(r['method'],r['scope']) for r in comparison['rows']} == {(m,s) for m in methods for s in data}
    assert len(comparison['rows']) == len(methods)*3
    for row in comparison['rows']:
        expected = data[row['scope']].get(row['method'])
        assert row['window_hours'] == (1 if row['scope']=='L' else manifest['score_duration_s']/3600 if row['scope']=='S' else 6)
        assert row['status'] == ('MEASURED' if expected is not None else 'NOT_IN_REGISTERED_SET')
        for k in ('P60','P600','R600','unfinished'):
            close(row[k], None if expected is None else expected[k])
    image = np.asarray(Image.open(out/'curves.png').convert('RGB'))
    assert image.shape[0] > 500 and image.shape[1] > 500 and float(image.std()) > 10
    write_json(out/'FOLLOWUP_VERIFICATION.json', dict(status='PASS', report_hashes=len(hashes),
        records=60, paired_comparisons=count//4, metric_intervals=count, full_curves=True,
        complete_scopes=True, cross_scale_rows=len(comparison['rows']), plot_nonblank=True,
        report_hashes_sha256=sha(out/'REPORT_HASHES.json')))
    print('S followup verification PASS', count, 'metric intervals')


def inequality(a,b,key):
    return None if a is None or b is None else a>=b if key=='P600' else a>b if key=='P60' else a<b


if __name__ == '__main__':
    main()
