"""独立核验资源探针；只按墙钟预算选择规模时长，不读取性能指标。"""
from datetime import datetime, timezone
import math
from pathlib import Path

from analysis.run_dynamic_learning import write_json
from analysis.run_scale_probe_20261027 import MAIN, ROOT, METHODS, read, sha, scale_config
from analysis.run_load_pressure_short import verify
from analysis.run_main_local_panel import verify_main

OUT = ROOT / 'independent_audit_20261027'


def main():
    assert not OUT.exists()
    assert sha(ROOT/'registration.json') == 'e10f2ad1c60d024c144dc0741b86fd2106c9fef8262843c6a025c7dad355fe87'
    reg = read(ROOT/'registration.json'); run = read(ROOT/'run_manifest.json')
    final = read(ROOT/'final_status.json'); verify_main()
    hashes = {**reg['hashes'], **run['hashes']}
    for p, h in hashes.items(): assert sha(p) == h, p
    p = read(run['protocol']); verify(p)
    assert p['date'] == '2026-10-16' and p['resource_probe_only']
    assert p['score_end_s']-p['score_start_s'] == 3600 and p['sim_duration_s']-p['score_end_s'] == 600
    assert p['followup_s'] == 600 and len(set(p['scored_task_ids'])) == run['scored_images'] == 131
    assert read(ROOT/'config_template.json') == scale_config(48,288)
    assert final['status'] == 'COMPLETE' and len(final['results']) == len(METHODS) == 9
    records = {}
    for short in final['results']:
        path = short['path']; assert sha(path) == short['sha256']; hashes[path] = short['sha256']
        r = read(path); assert r['method'] not in records
        assert r['status'] == short['status'] == 'PASS' and r['checks']['passed'] and r['checks']['followup600']
        assert r['tests_sealed'] and r['resource_probe_only']
        assert r['protocol_sha256'] == sha(run['protocol']) and r['sim_duration_s'] == p['sim_duration_s']
        assert r['scored_images'] == len(p['scored_task_ids']) and r['all_images'] == len(p['events'])
        assert len(r['action_trace_sha256']) == 64
        for name in ('satellite_work_seconds','ground_work_seconds'):
            assert math.isclose(r['checks'][name]['busy'], r['checks'][name]['expected'], abs_tol=1e-6)
        for key in ('wall_seconds','peak_rss_gb'): assert r[key] == short[key] and r[key] > 0
        records[r['method']] = {k:r[k] for k in ('wall_seconds','setup_seconds','peak_rss_gb','action_trace_sha256')}
    assert set(records) == set(METHODS)
    max_h = max(r['wall_seconds'] for r in records.values())/3600
    min_h = min(r['wall_seconds'] for r in records.values())/3600
    # 每项墙钟已含9并发争用；按完整波次规划，不再把单项耗时除9。
    waves = math.ceil(22*3/9)
    estimates = {}
    for hours in (6,1):
        panel = waves*max_h*hours
        estimates[str(hours)] = dict(methods=22,days=3,tasks=66,waves=waves,
            single_panel_nominal_h=panel,validation_plus_test_nominal_h=2*panel,
            uncertainty_multiplier=2,main_test_reserve_h=35,preparation_audit_reserve_h=12,
            total_with_20pct_margin_h=(2*panel*2+35+12)*1.2)
    now = datetime.now(timezone.utc); cutoff = datetime(2026,11,5,16,tzinfo=timezone.utc)
    remaining = (cutoff-now).total_seconds()/3600
    assert estimates['6']['total_with_20pct_margin_h'] > remaining
    assert estimates['1']['total_with_20pct_margin_h'] < remaining
    decision = dict(status='FROZEN_RESOURCE_DECISION',created_at=now.isoformat(),scenario='L_48_288',
        score_duration_s=3600,validation_dates=['2026-10-22','2026-10-23','2026-10-24'],
        test_dates=['2026-12-21','2026-12-22','2026-12-23'],cancelled=['S','scale_finetuning','no_teacher_ablation'],
        reason='Runtime budget only; six-hour validation plus test panel exceeds remaining deadline budget',
        measured_single_task_h=[min_h,max_h],estimates=estimates,remaining_h=remaining,
        deadline=cutoff.isoformat(),tests_sealed=True,model_selection_from_probe=False,
        limitations=['One peak hour is not a six-hour runtime guarantee',
            'CGS and other rules were not individually probed; 2x planning multiplier is not an upper bound',
            'Different dates/density/congestion can change cost',
            'RSS includes previous acquisition tasks in reused workers; not Orin memory validation',
            'Followup and action hashes verified against saved checks, no independent event replay'])
    write_json(OUT/'AUDIT.json',dict(status='PASS',hashes=hashes,records=records,
        generation_wall_seconds=run['generation_wall_seconds'],total_wall_seconds=final['total_wall_seconds'],
        tests_sealed=True,resource_probe_only=True))
    write_json(OUT/'RESOURCE_DECISION.json',decision)
    lines=['# L档资源探针与时长决策','',
        '9项全部PASS，登记/入口/模型/协议/真值哈希一致；保存的不变量和600秒随访检查通过。未读取或比较探针P/R600。',
        '', '|方法|墙钟小时|初始化秒|进程峰值RSS GB|','|---|---:|---:|---:|']
    for method,r in records.items(): lines.append(f"|{method}|{r['wall_seconds']/3600:.3f}|{r['setup_seconds']:.3f}|{r['peak_rss_gb']:.3f}|")
    lines += ['', f"生成用时{run['generation_wall_seconds']:.2f}秒；单池整批{final['total_wall_seconds']/3600:.3f}小时。",
        '', 'L完整方法清单按6个冻结学习模型+12局部规则+局部CGS/HEFT+全局v3固定/动态，共22方法×3日=66任务，每面板8波。',
        '', '|评分时长|单面板名义小时|验证+测试名义小时|含2倍不确定性、M测试35h、准备审计12h、20%余量|',
        '|---|---:|---:|---:|']
    for h,e in estimates.items(): lines.append(f"|{h}小时|{e['single_panel_nominal_h']:.2f}|{e['validation_plus_test_nominal_h']:.2f}|{e['total_with_20pct_margin_h']:.2f}小时|")
    lines += ['', f'当前剩余约{remaining:.1f}小时。依登记资源降级为L48 SAR+288计算星、每日1小时原始字节高峰，取消S、规模微调和无教师消融，优先保障M正式测试与L零样本。所有同档方法共用同一时长，不依据性能降级。',
        '', '单任务已在9并发下测量，不能再除9。六小时按6倍作保守线性规划；2倍不确定性系数不是保证，另需跟进运行实际速度。',
        '', '当前仍不生成或读取正式测试任务。先冻结M训练输入范围、运行L三日验证，再将精确模型/方法/规模/时长/统计清单统一冻结后一次开启正式测试。',
        '', '电脑RSS为池进程高水位，可能包含先前成像生成任务。Orin上4块峰值内存未实测，此结果不证明实机内存可行。']
    (OUT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    write_json(OUT/'HASHES.json',{str(p):sha(p) for p in [Path(__file__),*OUT.iterdir()]})
    print({'status':'PASS','decision':'L48+288 one hour','estimates':estimates})


if __name__ == '__main__': main()
