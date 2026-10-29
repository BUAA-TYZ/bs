"""独立核对已下载的八组结果、原始计时、输入hash及功耗配置。"""
import hashlib
import json
import math
from pathlib import Path
import statistics

ROOT = Path('results/method_cost_20261028')
FOLDER = ROOT / 'orin_unloaded_v2'


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def percentile(values, p):
    values = sorted(values)
    index = (len(values)-1)*p/100
    lo = math.floor(index)
    hi = math.ceil(index)
    return values[lo] + (values[hi]-values[lo])*(index-lo)


def main():
    reg = read(ROOT/'registration.json')
    inputs = read(FOLDER/'input_hashes.json')
    for path, expected in inputs.items():
        assert sha(path) == expected, path
    groups = read(FOLDER/'status.json')['completed']
    assert len(groups) == 8
    assert {x['group'] for x in groups} == {
        f'{family}_{device}_t{threads}' for family in ('fixed','dynamic')
        for device in ('cpu','cuda_0') for threads in (1,2)}
    verified = []
    for group in groups:
        path = FOLDER/(group['group']+'.json')
        result = read(path)
        assert sha(path) == group['sha256']
        assert result['status'] == 'PASS' and result['batch'] == 1
        assert result['decisions'] == 67 and result['warmup'] == 50 and result['repeats'] == 20
        for key, source in [('registration_sha256', ROOT/'registration.json'),
                            ('replay_source_sha256', Path('analysis/method_cost_replay_v2_20261028.py')),
                            ('amendment_sha256', ROOT/'REPLAY_CHECK_AMENDMENT.md')]:
            assert result[key] == sha(source)
        assert result['snapshot_reconstruction'] == 'Actor float32 tensors, masks and metadata exact'
        assert result['maximum_probability_absolute_error'] <= reg['probability_atol']
        for raw, summary in [('raw_observation_seconds','observation_seconds'),
                             ('raw_actor_host_seconds','actor_host_inputs_seconds'),
                             ('raw_actor_resident_seconds','actor_resident_inputs_seconds')]:
            values = result[raw]
            assert len(values) == 1340 and all(math.isfinite(x) and x>=0 for x in values)
            expected = dict(n=len(values),mean=statistics.mean(values),
                            **{f'P{p}':percentile(values,p) for p in (50,95,99)})
            for key,value in expected.items():
                assert math.isclose(result[summary][key],value,rel_tol=1e-10,abs_tol=1e-12)
        verified.append(dict(group=group['group'],sha256=sha(path),
                             max_probability_error=result['maximum_probability_absolute_error']))
    before, after = (read(FOLDER/(name+'.json')) for name in ('before','after'))
    assert before['power_returncode'] == after['power_returncode'] == 0
    assert before['power'] == after['power'] and 'MAXN' in before['power']
    stable = {k:v for k,v in before['clocks'].items() if k.endswith(('governor','min_freq','max_freq'))}
    assert all(after['clocks'].get(k)==v for k,v in stable.items())
    report = dict(status='PASS',groups=verified,input_hash_count=len(inputs),
        raw_timing_samples=8*3*1340,power=before['power'],stable_clock_setting_count=len(stable),
        limitation='Stored inputs/results/counters checked; remote forward computations not rerun. Dynamic clocks retained.',
        verifier_sha256=sha(__file__))
    (ROOT/'UNLOADED_FOLLOWUP_AUDIT.json').write_text(json.dumps(report,indent=2))
    print(json.dumps({k:v for k,v in report.items() if k!='groups'}))


if __name__ == '__main__':
    main()
