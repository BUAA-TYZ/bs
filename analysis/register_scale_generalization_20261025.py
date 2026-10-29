"""登记规模修订与只读耗时外推；不生成日期任务、不运行仿真。"""
import hashlib
import json
from pathlib import Path

from analysis.redesign_scenario import write_json, digest
from sim.constellation import walker_tle

MAIN = Path('results/redesign_20261021/main_scenario')
ROOT = MAIN/'scale_generalization_20261025'
PROTOCOL = MAIN/'SCALE_GENERALIZATION_AMENDMENT_20261025.md'
SHAPES = {'S': (12, 72), 'M': (24, 144), 'L': (48, 288), 'L_fallback': (36, 216)}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tle(sar, compute):
    offsets = [180*p/12 for p in range(12)]
    return walker_tle('2026-10-15T00:00:00Z', 12, sar//12, 570, 97.7,
        phasing=1, plane_raan_offsets_deg=offsets) + walker_tle(
        '2026-10-15T00:00:00Z', 12, compute//12, 1015, 98.98, raan_offset_deg=15,
        phasing=1, first_satellite_number=sar+1, plane_raan_offsets_deg=offsets)


def main():
    if ROOT.exists():
        raise FileExistsError('Registration is immutable; append a new amendment')
    template = json.loads((MAIN/'config_template.json').read_text())
    assert json.loads(json.dumps(tle(24,144))) == template['topology']['tle_lines']
    dates = json.loads((MAIN/'date_split.json').read_text())
    rows = []
    for date in dates['validation_dates']:
        for path in sorted((MAIN/'bc_stage2_20261022/validation'/date).glob('*.json')):
            if path.name.endswith('.progress.json'):
                continue
            r = json.loads(path.read_text())
            if r.get('status') == 'PASS' and 'wall_seconds' in r:
                rows.append(dict(path=str(path), sha256=sha(path), date=date,
                    method=path.stem, wall_seconds=r['wall_seconds'], peak_rss_gb=r.get('peak_rss_gb')))
    assert len(rows) == 33
    low, high = min(r['wall_seconds'] for r in rows), max(r['wall_seconds'] for r in rows)
    estimate = dict(scope='Analytic extrapolation only; no L simulation or test data generated',
        base_n=168, large_n=336, base_pairs=168*167//2, large_pairs=336*335//2,
        pair_ratio=(336*335)/(168*167), concurrency=9, timings=rows,
        M_six_hour_wall_s_range=[low,high],
        L_six_hour_planning_wall_h_range=[low*2/3600,high*4/3600],
        L_six_hour_stress_wall_h=high*8/3600,
        assumed_warmup_s=650, followup_s=600,
        L_one_hour_planning_wall_h_range=[low*2/3600*4850/22850,high*4/3600*4850/22850],
        L_one_hour_stress_wall_h=high*8/3600*4850/22850,
        caveats=['Not measured L runtime or a bound', 'Peak-hour arrival density may differ',
            'Data generation, contact cache and audit time not included',
            'Single ground station congestion may change costs', 'Do not divide per-job wall time by 9'])
    hashes = {str(p):sha(p) for p in (Path(__file__),PROTOCOL,MAIN/'config_template.json',
        MAIN/'manifest.json',MAIN/'date_split.json',Path('sim/constellation.py'),
        Path('analysis/prepare_main_bc_stage2.py'),MAIN/'fixed_mappo_frozen_20261025/manifest.json')}
    registration = dict(status='PREREGISTERED_NOT_GENERATED', supersedes='Original section 5 scale definitions',
        scenario_shapes={k:dict(sar=s,compute=c,planes=12,sar_per_plane=s//12,compute_per_plane=c//12,
            total=s+c,tle_formula_sha256=digest(tle(s,c))) for k,(s,c) in SHAPES.items()},
        dates={k:dates[k] for k in ('training_dates','validation_dates','test_dates')},
        tests_sealed=True, M_tle_exact_match=True, input_hashes=hashes,
        runtime_estimate_sha256=digest(estimate), finetune_gate='Validation-day R600 only; never test-triggered',
        finetune_max_episodes_per_triggered_seed=88, finetune_validation_points=[44,88],
        shortage_priority='L zero-shot only; cancel no-teacher, scale finetuning and S first',
        run_manifest_required=True, test_open_marker_created=False)
    write_json(ROOT/'registration.json',registration)
    write_json(ROOT/'RUNTIME_ESTIMATE.json',estimate)
    write_json(ROOT/'REGISTRATION_HASHES.json',{str(p):sha(p) for p in
        (PROTOCOL,ROOT/'registration.json',ROOT/'RUNTIME_ESTIMATE.json')})
    print(json.dumps(dict(registration_sha256=sha(ROOT/'registration.json'),
        protocol_sha256=sha(PROTOCOL), M_tle_exact_match=True,
        L_six_hour_planning_h=estimate['L_six_hour_planning_wall_h_range'],
        L_one_hour_planning_h=estimate['L_one_hour_planning_wall_h_range'],
        L_six_hour_stress_h=estimate['L_six_hour_stress_wall_h']),indent=2))


if __name__=='__main__':main()
