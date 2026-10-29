"""独立审计已保存的负载重放，汇总同一口径的原始计时。"""
from datetime import datetime
import gzip
import json
import math
from pathlib import Path
import pickle
import re
import statistics
from zoneinfo import ZoneInfo

from analysis.verify_method_cost_unloaded_20261028 import percentile, read, sha

ROOT = Path('results/method_cost_20261028')
LOADED = ROOT/'orin_sar_loaded'
OUT = ROOT/'final_report_20261029'


def summary(values):
    assert values and all(math.isfinite(x) and x >= 0 for x in values)
    return dict(n=len(values), mean=statistics.mean(values), maximum=max(values),
                **{f'P{q}': percentile(values, q) for q in (50, 95, 99)})


def equal(a, b):
    assert math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-12), (a, b)


def telemetry(path):
    rows = []
    for line in path.read_text().splitlines():
        gpu = re.search(r'GR3D_FREQ (\d+)%', line)
        ram = re.search(r'RAM (\d+)/', line)
        assert gpu and ram, line
        epoch = datetime.strptime(line[:19], '%m-%d-%Y %H:%M:%S').replace(
            tzinfo=ZoneInfo('Asia/Shanghai')).timestamp()
        rows.append(dict(epoch=epoch, gpu=int(gpu[1]), ram_mb=int(ram[1])))
    assert rows and all(b['epoch'] >= a['epoch'] for a, b in zip(rows, rows[1:]))
    return rows


def main():
    reg = read(ROOT/'registration.json')
    provenance = dict(reg['hashes'])
    for path, expected in provenance.items():
        assert sha(path) == expected, path
    loaded_reg = read(LOADED/'registration.json')
    for path, expected in loaded_reg['hashes'].items():
        if path.startswith('/home/tianyz/bs_method_cost_20261028/'):
            local = Path(path.removeprefix('/home/tianyz/bs_method_cost_20261028/'))
        elif path.startswith('/home/tianyz/'):
            local = ROOT/'loaded_source_evidence'/Path(path).name
        else:
            local = Path(path)
        assert sha(local) == expected, path
        provenance[str(local)] = expected
    for path, expected in loaded_reg['input_hashes'].items():
        assert sha(path) == expected, path
        provenance[path] = expected
    state = read(ROOT/'ORIN_LOADED_STATUS.json')
    assert state['status'] == 'LOADED_COMPLETE'
    assert [x['group'] for x in state['completed']] == loaded_reg['groups']
    load = read(LOADED/'sar_load/report.json')
    assert load['status'] == 'passed' and sha(LOADED/'sar_load/report.json') == state['load_report_sha256']
    assert load['mode'] == 'streams2' and load['cohort_size'] == 2
    assert load['protocol']['warmup_windows'] == 12 and load['protocol']['cpu_threads'] == 1
    assert load['protocol']['precision'] == 'float32'
    remote_hashes = loaded_reg['hashes']
    for field, suffix in [('script_sha256', 'benchmark_jetson_concurrency.py'),
                          ('baseline_script_sha256', 'calibrate_jetson_sar.py'),
                          ('fixture_sha256', 'workload_fixture.json'),
                          ('weights_sha256', 'yolo11n-obb.pt')]:
        assert load[field] == next(h for p, h in remote_hashes.items() if p.endswith('/'+suffix))
    rows = load['rows']
    assert [r['index'] for r in rows] == list(range(len(rows)))
    assert all(r['windows'] == 2 and r['cohort_s'] > 0 for r in rows)
    ends = [load['measurement_epoch_start_s']+r['elapsed_wall_s'] for r in rows]
    gaps = [b-a for a, b in zip(ends, ends[1:])]
    assert all(g > 0 for g in gaps)
    assert sum(r['windows'] for r in rows) == load['summary']['windows']
    stats = telemetry(LOADED/'tegrastats.log')
    clock_counts = {}
    for folder in (LOADED, ROOT/'orin_unloaded_v2'):
        before, after = (read(folder/(n+'.json')) for n in ('before', 'after'))
        assert before['power_returncode'] == after['power_returncode'] == 0
        assert before['power'] == after['power'] and 'MAXN' in before['power']
        stable = {k:v for k,v in before['clocks'].items() if k.endswith(('governor','min_freq','max_freq'))}
        assert all(after['clocks'].get(k) == v for k,v in stable.items())
        clock_counts[folder.name] = len(stable)
    groups = {}
    loaded_coverage = {}
    for folder, completed in [(ROOT/'orin_unloaded_v2', read(ROOT/'orin_unloaded_v2/status.json')['completed']),
                              (LOADED, state['completed'])]:
        for entry in completed:
            path = folder/(entry['group']+'.json')
            assert sha(path) == entry['sha256']
            r = read(path)
            assert r['status'] == 'PASS' and r['batch'] == 1
            assert (r['decisions'], r['warmup'], r['repeats']) == (67, 50, 20)
            assert r['maximum_probability_absolute_error'] <= reg['probability_atol']
            assert r['snapshot_reconstruction'] == 'Actor float32 tensors, masks and metadata exact'
            for key, source in [('registration_sha256', ROOT/'registration.json'),
                                ('replay_source_sha256', Path('analysis/method_cost_replay_v2_20261028.py')),
                                ('amendment_sha256', ROOT/'REPLAY_CHECK_AMENDMENT.md')]:
                assert r[key] == sha(source)
            for raw, target in [('raw_observation_seconds', 'observation_seconds'),
                                ('raw_actor_host_seconds', 'actor_host_inputs_seconds'),
                                ('raw_actor_resident_seconds', 'actor_resident_inputs_seconds')]:
                s = summary(r[raw]); assert s['n'] == 1340
                for key, value in r[target].items():
                    equal(value, s[key])
            # 逐次相加，禁止相加不同分项的P95来冒充总耗时分位数。
            total = summary([a+b for a,b in zip(r['raw_observation_seconds'], r['raw_actor_host_seconds'], strict=True)])
            compact = {k:v for k,v in r.items() if not k.startswith('raw_')}
            compact['observation_plus_host_actor_seconds'] = total
            groups[folder.name+'/'+entry['group']] = compact
            provenance[str(path)] = sha(path)
            if folder == LOADED:
                start, end = entry['start_epoch_s'], entry['end_epoch_s']
                assert entry['returncode'] == 0
                assert load['measurement_epoch_start_s'] <= start < end <= load['measurement_epoch_end_s']
                assert ends[0] <= start and ends[-1] >= end
                # tegrastats时间戳只有秒精度，取完整内秒，另验前后覆盖。
                selected = [r for r in stats if math.ceil(start) <= r['epoch'] <= math.floor(end)-1]
                assert stats[0]['epoch'] <= math.floor(start) and stats[-1]['epoch'] >= math.floor(end)
                gpu = summary([r['gpu'] for r in selected])
                timestamp_gap = max(b['epoch']-a['epoch'] for a,b in zip(selected, selected[1:]))
                assert timestamp_gap <= 2
                loaded_coverage[entry['group']] = dict(start_epoch_s=start, end_epoch_s=end,
                    duration_s=end-start, gpu_percent=gpu,
                    gpu_nonzero_fraction=sum(r['gpu'] > 0 for r in selected)/len(selected),
                    maximum_telemetry_timestamp_gap_s=timestamp_gap,
                    system_peak_ram_mb=max(r['ram_mb'] for r in selected))
    mac = {}
    for family in ('fixed', 'dynamic'):
        r = read(ROOT/'capture'/family/'result.json')
        assert r['status'] == 'PASS' and r['registration_sha256'] == sha(ROOT/'registration.json')
        assert r['checkpoint_sha256'] == sha(r['checkpoint'])
        decisions = []; candidates = {}
        for path, expected in r['decision_hashes'].items():
            assert sha(path) == expected
            provenance[path] = expected
            with gzip.open(path, 'rb') as f:
                row = pickle.load(f)
            assert reg['score_start_s'] <= row['observation']['time_seconds'] < reg['score_end_s']
            decisions.append(row)
            variants = row['observation'].get('variants', [row['observation']])
            for i, obs in enumerate(variants):
                # 源星和地面占两个槽，其余有效节点为Top-K计算星；-1为padding。
                n = sum(bool(v) for v in obs['node_valid'])-2
                candidates.setdefault(str((4,16)[i] if family=='dynamic' else 16), []).append(n)
        assert len(decisions) == 67
        batches = [b for b in r['control_batches'] if reg['score_start_s'] <= b['sim_seconds'] < reg['score_end_s']]
        assert len(batches) == 3600
        active = [b for b in batches if b['decisions']]
        for b in batches:
            equal(b['total_computation_s'], sum(b[k] for k in
                  ('state_export_s','control_enqueue_s','decision_filter_s','local_view_s','policy_s','executor_admission_s')))
        mac[family] = dict(peak_rss_bytes=r['peak_rss_bytes'],
            decisions={k:summary([d[k] for d in decisions]) for k in ('observation_s','forward_s','sampling_s')},
            decision_compute_s=summary([sum(d[k] for k in ('observation_s','forward_s','sampling_s')) for d in decisions]),
            source_calls={k:summary([c[k] for c in r['policy_calls']]) for k in
                          ('total_s','observation_s','forward_s','sampling_s','commitment_s')},
            source_call_scope='All active source calls across capture, including commitment-only followup calls; no per-call timestamps saved.',
            active_batches={k:summary([b[k] for b in active]) for k in
                            ('state_export_s','control_enqueue_s','decision_filter_s','local_view_s','policy_s','executor_admission_s','total_computation_s')},
            all_scoring_batches={k:summary([b[k] for b in batches]) for k in
                                 ('state_export_s','control_enqueue_s','decision_filter_s','local_view_s','policy_s','executor_admission_s','total_computation_s')},
            retained_helper_candidates={k:summary(v) for k,v in candidates.items()})
        provenance[str(ROOT/'capture'/family/'result.json')] = sha(ROOT/'capture'/family/'result.json')
    for folder in (LOADED, ROOT/'orin_unloaded_v2', ROOT/'orin_memory_batch1', ROOT/'orin_memory_streams2'):
        for path in folder.rglob('*'):
            if path.is_file():
                provenance[str(path)] = sha(path)
    for path in (ROOT/'ORIN_LOADED_STATUS.json', ROOT/'ORIN_LOADED_LAUNCH.json', ROOT/'MEMORY_STATUS.json'):
        provenance[str(path)] = sha(path)
    report = dict(status='PASS', orin=groups, mac=mac, loaded_coverage=loaded_coverage,
        load=dict(cohorts=len(rows), windows=sum(r['windows'] for r in rows),
                  measurement_start_s=load['measurement_epoch_start_s'], measurement_end_s=load['measurement_epoch_end_s'],
                  maximum_cohort_end_gap_s=max(gaps), mean_cohort_end_gap_s=statistics.mean(gaps)),
        stable_clock_settings=clock_counts, provenance=provenance,
        limitation='Stored-result audit, not a second forward run. GPU telemetry is sampled at100ms with whole-second timestamps. Orin reconstruction plus actor excludes sampling/commitment/state export/local-view construction and is not an end-to-end flight measurement.',
        verifier_sha256=sha(__file__))
    OUT.mkdir(exist_ok=True)
    (OUT/'AUDIT_AND_SUMMARY.json').write_text(json.dumps(report, indent=2, ensure_ascii=False))
    lines = ['# METHOD-COST实测汇总', '',
        'Orin MAXN模式0、动态时钟，CPU线程1/2，batch=1，50次预热，每方法67个评分窗决策各20次重复。Mac Apple M5 Pro单进程/单线程，batch=1。单位ms；每格P50/P95/P99。', '',
        '## Orin', '', '|条件/方法/线程|观测候选重建|actor（host输入）|actor（resident输入）|重建+host actor逐次和|进程RSS MiB|', '|---|---|---|---|---|---:|']
    fmt = lambda s: '/'.join(f'{s[f"P{p}"]*1000:.3f}' for p in (50,95,99))
    for name,r in groups.items():
        lines.append('|'+name+'|'+'|'.join(fmt(r[k]) for k in ('observation_seconds','actor_host_inputs_seconds','actor_resident_inputs_seconds','observation_plus_host_actor_seconds'))+f'|{r["peak_rss_bytes"]/2**20:.2f}|')
    lines += ['', 'Orin逐次和不含采样、承诺提交、执行器准入、状态导出/本地视图，不称为完整端到端决策。GPU组仅actor在GPU；观测仍在CPU。host口径包括张量准备及GPU拷贝，resident口径为已驻留输入，CUDA计时同步。', '', '## Mac完整流程', '']
    for family,r in mac.items():
        lines += [f'### {family}', '', '|分母/分项|n|P50/P95/P99 ms|', '|---|---:|---|']
        for scope in ('decisions','source_calls','active_batches','all_scoring_batches'):
            for key,s in r[scope].items():
                lines.append(f'|{scope}/{key}|{s["n"]}|{fmt(s)}|')
        lines += ['', 'source_calls含全录制期活跃源调用（包括仅提交的随访调用），没有逐调用时间戳；active_batches为评分小时内有新决策的批次，all_scoring_batches为全部3600个一秒批次，不同分母不可相加。', '']
    lines += ['## 内存与候选规模', '', '|组|RSS峰值 MiB|CUDA分配/保留峰值 MiB|', '|---|---:|---:|']
    for name,r in groups.items():
        cuda = ('不使用CUDA' if r['cuda_peak_allocated_bytes'] is None else
                f'{r["cuda_peak_allocated_bytes"]/2**20:.3f}/{r["cuda_peak_reserved_bytes"]/2**20:.3f}')
        lines.append(f'|{name}|{r["peak_rss_bytes"]/2**20:.3f}|{cuda}|')
    for family,r in mac.items():
        lines.append(f'|Mac/{family}|{r["peak_rss_bytes"]/2**20:.3f}|不使用CUDA|')
    lines += ['', '固定16块、动态4/16块观测的保留计算星候选均值3.448、P50/P95/P99/最大均4；不含源星与地面，不含padding，不冒充TopK前候选数。', '',
              '## GPU负载覆盖', '', '|CPU组|全进程覆盖 s|GPU均值 %|GPU非零样本比例 %|系统RAM采样峰值 MB|', '|---|---:|---:|---:|---:|']
    for name,c in loaded_coverage.items():
        lines.append(f'|{name}|{c["duration_s"]:.3f}|{c["gpu_percent"]["mean"]:.3f}|{c["gpu_nonzero_fraction"]*100:.3f}|{c["system_peak_ram_mb"]}|')
    lines += ['', '四组GPU利用率P50/P95/P99均99%，但各组并非每个采样时刻均满载；CPU预处理与cohort边界产生空隙。负载cohort结束间隔最大2.053秒，遥测整秒时间戳最大间隔1秒。系统RAM包括独立GPU负载和其他进程，不是actor增量内存。', '',
        '## 审计与适用范围', '',
        f'四组负载CPU过程均被{len(rows)}个连续双流cohort覆盖，共{load["summary"]["windows"]}窗口；负载为原FP32 YOLO11n-OBB受控SAR后处理代理，不是SAR识别精度验证。',
        '完整覆盖、GPU利用率分位数与样本数见AUDIT_AND_SUMMARY.json.loaded_coverage。源/模型/快照hash及全部12组原始计时重新核验；float32输入、掩码、离散元数据逐位一致。float64连续特征末位差最大2.22e-16按已登记修订保留，概率容差atol1e-5/rtol1e-4不变。',
        'Mac PyTorch2.12.0/NumPy2.4.4/Python3.12.13；Orin PyTorch2.3.0/NumPy1.23.5/Python3.10.15。源与冻结权重一致，框架版本不同。67个唯一输入的重复计时不能视为1340个独立任务样本。',
        '仅计算开销，排除快照复制、磁盘录制、传输与等待；重建从合法本地状态开始，通告传输和等待已在仿真通信模型中计入。critic仅地面训练。',
        '四块内存独立实测见../MEMORY_REPORT.md；其特定几何与配置不能外推全队列或在轨可行性。RSS、CUDA及系统统一RAM相互重叠，不能相加。']
    (OUT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    (OUT/'REPORT_HASHES.json').write_text(json.dumps({str(OUT/n):sha(OUT/n) for n in ('AUDIT_AND_SUMMARY.json','REPORT.md')}, indent=2))
    print(json.dumps(dict(status='PASS', groups=len(groups), hashes=len(provenance),
                         load=report['load'], coverage=loaded_coverage), ensure_ascii=False))


if __name__ == '__main__':
    main()
