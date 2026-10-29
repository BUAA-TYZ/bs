"""复核正式报告，不重跑仿真、不改冻结产物。"""
from datetime import datetime, timezone
from pathlib import Path
import math
import statistics

import numpy as np

from analysis.run_m_formal_20261027 import ROOT, OPEN, MAPPO, GLOBAL
from analysis.run_dynamic_heuristics_v2 import read, sha, configuration, digest
from analysis.run_load_pressure_short import verify
from analysis.run_main_local_panel import verify_main
from analysis.run_dynamic_learning import write_json


def equal(a, b):
    assert math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-10), (a, b)


def main():
    verify_main()
    manifest = read(ROOT/'manifest.json')
    run = read(ROOT/'run_manifest.json')
    state = read(ROOT/'final_status.json')
    audit = read(ROOT/'FINAL_AUDIT.json')
    curves = read(ROOT/'full_curves.json')
    pairs = read(ROOT/'PAIRED_COMPARISONS.json')
    tails = read(ROOT/'TAIL_RECORDS.json')
    assert sha(ROOT/'manifest.json') == '00acb98d55c3c01e3005159907c99980ce02bdd125774256eea51f6ea88f3597'
    assert state['status'] == 'COMPLETE' and len(state['results']) == 99
    assert audit['status'] == 'PASS' and audit['manifest_sha256'] == sha(ROOT/'manifest.json')
    hashes = {**manifest['hashes'], **run['hashes'], **read(ROOT/'REPORT_HASHES.json')}
    hashes.update({r['path']: r['sha256'] for r in state['results']})
    for path, expected in hashes.items():
        assert sha(path) == expected, path
    protocols = {read(path)['date']: read(path) for path in run['protocols']}
    assert set(protocols) == {'2026-12-21', '2026-12-22', '2026-12-23'}
    for p in protocols.values():
        verify(p)
    daily, aggregate, rows_by_method = {}, {}, {}
    for method in manifest['methods']:
        daily[method], rows_by_method[method] = [], []
        for date, p in protocols.items():
            path = ROOT/'validation'/date/f'{method}.json'
            r = read(path)
            assert r['status'] == 'PASS' and r['checks']['passed']
            assert r['method'] == method and r['date'] == date
            assert r['config_sha256'] == digest(configuration(p, method))
            assert r['opening_sha256'] == sha(OPEN) and not r['tests_sealed'] and r['l_tests_sealed']
            if method in manifest['models']:
                assert r['checkpoint'] == manifest['models'][method]
                assert r['checkpoint_sha256'] == sha(r['checkpoint'])
            ids = set(p['scored_task_ids'])
            rows = [v for v in r['outcomes'] if v['task_id'] in ids]
            assert len(rows) == len(ids) and {v['task_id'] for v in rows} == ids
            assert p['sim_duration_s']-p['score_end_s'] >= 600
            assert all(v['observed_age_seconds'] >= 600 and p['score_start_s'] <= v['release_seconds'] < p['score_end_s'] for v in rows)
            done = sorted(v['latency_seconds'] for v in rows if v['status'] == 'completed')
            values = {f'P{d}': sum(t <= d for t in done)/len(rows) for d in (60,90,120,180,240,300,600)}
            values.update(mean_completed_s=statistics.mean(done), P50_completed_s=float(np.quantile(done,.5)),
                          P90_completed_s=float(np.quantile(done,.9)), unfinished=len(rows)-len(done),
                          R600=(math.fsum(min(t,600) for t in done)+600*(len(rows)-len(done)))/len(rows))
            expected = next(v for v in audit['daily'][method] if v['date'] == date)
            for key, value in values.items():
                equal(value, expected[key])
            equal(values['R600'], r['R600'])
            for c in r['curve']:
                equal(values[f"P{c['D_seconds']}"], c['P_lower'])
                assert c['unknown_count'] == 0
            for key in ('mean_completed_s','P50_completed_s','P90_completed_s','unfinished'):
                equal(values[key],r['latency'][key])
            assert tails[f'{method}_{date}'] == [v for v in rows if v['status'] != 'completed' or v['latency_seconds'] > 300]
            assert set(r['granularity']) == ids
            assert all(v['packet_mb'] >= v['unique_mb']-1e-9 and v['requested'] in (4,16,None) and v['actual'] > 0 for v in r['granularity'].values())
            cfg = p['config']
            helpers = set(range(cfg['num_sats']))-set(cfg['task_source_sat_ids'])
            capacity = sum(g['bandwidth_mbps']/8*g.get('receive_channels',1) for g in cfg['ground_stations'])*1e6
            for z in r['resources'].values():
                busy = sum(v for sid,v in z['helper_busy_by_node'].items() if int(sid) in helpers)
                equal(busy,z['helper_busy_s'])
                equal(busy/(len(helpers)*z['duration_s']),z['helper_utilization'])
                equal(z['ground_bytes']/(capacity*z['duration_s']),z['ground_channel_utilization'])
                equal(z.get('raw_ground_bytes',0)+z.get('result_ground_bytes',0),z['ground_bytes'])
                equal(sum(z.get(k,0) for k in ('control_bytes','raw_isl_bytes','result_isl_bytes','raw_ground_bytes','result_ground_bytes')),z['total_transmitted_bytes'])
            for key in ('duration_s','total_transmitted_bytes','control_bytes','helper_busy_s','ground_bytes'):
                equal(sum(r['resources'][s][key] for s in ('warmup','main','followup')),r['resources']['whole'][key])
            daily[method].append(values)
            rows_by_method[method].append(rows)
        aggregate[method] = {k: statistics.mean(d[k] for d in daily[method]) for k in daily[method][0] if k != 'unfinished'}
        aggregate[method]['unfinished'] = sum(d['unfinished'] for d in daily[method])
        for k,v in aggregate[method].items():
            equal(v,audit['day_equal'][method][k])
        xs = sorted({60.,600.,*[r['latency_seconds'] for rows in rows_by_method[method] for r in rows if r['status']=='completed' and 60<=r['latency_seconds']<=600]})
        assert curves[method]['D'] == xs
        for x,y in zip(xs,curves[method]['P'],strict=True):
            equal(y,statistics.mean(sum(r['status']=='completed' and r['latency_seconds']<=x for r in rows)/len(rows) for rows in rows_by_method[method]))
    metrics = ('P60','mean_completed_s','P600','R600')
    def criteria(a,b,k):
        return a[k]>=b[k] if k=='P600' else a[k]>b[k] if k=='P60' else a[k]<b[k]
    for scope, names in manifest['comparison_scopes'].items():
        assert len(names) == (6 if scope.startswith('fixed') else 12)
        for k in metrics:
            best = (max if k.startswith('P') else min)(aggregate[n][k] for n in names)
            equal(best,audit['scopes'][scope]['envelope'][k])
            assert set(audit['scopes'][scope]['sources'][k]) == {n for n in names if aggregate[n][k]==best}
        for n,flags in audit['scopes'][scope]['criteria'].items():
            assert flags == {k:criteria(aggregate[n],audit['scopes'][scope]['envelope'],k) for k in metrics}
    indices = np.random.default_rng(20261027).integers(0,3,size=(10000,3))
    for n in MAPPO:
        assert set(pairs[n]) == set(manifest['methods'])-set(MAPPO)
        for b, pair in pairs[n].items():
            assert pair['group'] == ('global_reference' if b in GLOBAL else 'same_information')
            for k in metrics:
                a,v = aggregate[n][k],aggregate[b][k]
                win = 'tie' if a==v else 'win' if (a>v if k.startswith('P') else a<v) else 'loss'
                assert pair['wins'][k]==win and pair['criteria'][k]==criteria(aggregate[n],aggregate[b],k)
                delta=np.array([a[k]-v[k] for a,v in zip(daily[n],daily[b],strict=True)])
                q=pair['paired'][k]
                np.testing.assert_allclose(delta,q['daily'],atol=1e-10)
                equal(float(delta.mean()),q['mean'])
                np.testing.assert_allclose(np.quantile(delta[indices].mean(axis=1),[.025,.975]),q['ci95'],atol=1e-10)
    local = set(manifest['methods'])-set(GLOBAL)
    for x in ('P600','R600'):
        sign=1 if x=='P600' else -1
        front={n for n in local if not any(sign*aggregate[b][x]>=sign*aggregate[n][x] and aggregate[b]['P60']>=aggregate[n]['P60'] and
                 (sign*aggregate[b][x]>sign*aggregate[n][x] or aggregate[b]['P60']>aggregate[n]['P60']) for b in local-{n})}
        assert front == set(audit['pareto'][x])
    output=ROOT/'FOLLOWUP_AUDIT_20261028.json'
    assert not output.exists()
    write_json(output,dict(status='PASS',at=datetime.now(timezone.utc).isoformat(),records=99,methods=33,
        hashes_checked=len(hashes),script_sha256=sha(__file__),report_hashes_sha256=sha(ROOT/'REPORT_HASHES.json'),
        checks=['frozen inputs/models/protocols','scored IDs and 600 followup','all day-equal metrics and full step curves',
                'resource categories/utilization/segment conservation','granularity and tail records','both complete envelopes',
                '162 pairs and 648 metric bootstrap intervals','local Pareto frontiers'],
        limitations=['Saved resource counters, not independent packet replay','Orin four-block peak memory not measured']))
    print('FOLLOWUP PASS',len(hashes),'hashes; 99 records; 162 pairs; 648 intervals')


if __name__ == '__main__':
    main()
