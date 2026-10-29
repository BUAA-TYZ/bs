"""仅使用已完成记录的墙钟和RSS，为S登记时长；不生成S轨道或任务。"""
from datetime import datetime, timezone
import math
from pathlib import Path
import subprocess

from analysis.run_dynamic_heuristics_v2 import read, sha
from analysis.run_dynamic_learning import write_json
from analysis.run_local_comparison_20261027 import BASELINES
from analysis.run_m_formal_20261027 import MAPPO

MAIN = Path('results/redesign_20261021/main_scenario')
OUTPUT = MAIN/'S_RESOURCE_ESTIMATE_20261029.json'
METHODS = MAPPO + BASELINES + ('global_v3_16', 'global_v3_dynamic')
END = datetime(2026, 11, 4, 16, tzinfo=timezone.utc)


def estimate():
    assert not OUTPUT.exists(), 'Resource decision is immutable'
    now = datetime.now(timezone.utc)
    assert now < END
    evidence = []
    for scope in ('m', 'l'):
        root = MAIN/f'{scope}_formal_test_2026102{7 if scope == "m" else 8}'
        final = read(root/'final_status.json')
        assert final['status'] == 'COMPLETE'
        for short in final['results']:
            assert short['status'] == 'PASS' and sha(short['path']) == short['sha256']
            row = read(short['path'])
            if row['method'] in METHODS:
                evidence.append(dict(scope=scope.upper(), path=short['path'], sha256=short['sha256'],
                    method=row['method'], date=row['date'], wall_seconds=row['wall_seconds'],
                    peak_rss_gb=row['peak_rss_gb']))
    m = [r for r in evidence if r['scope'] == 'M']
    assert len(m) == 60
    batches = math.ceil(60/9)
    slowest = max(r['wall_seconds'] for r in m)
    # 不假定减小节点数必然线性提速，主估计直接用M最慢任务，压力情形再乘4。
    predicted = (batches*slowest + 2*3600)*1.2
    stress = (batches*slowest*4 + 2*3600)*1.2
    budget = (END-now).total_seconds()
    duration = 21600 if stress < budget else 3600
    memory = int(subprocess.check_output(['sysctl', '-n', 'hw.memsize'], text=True))
    value = dict(created_at=now.isoformat(), deadline=END.isoformat(), evidence=evidence,
        methods=METHODS, tasks=60, workers=9, batches=batches, torch_threads=1,
        M_six_hour_slowest_task_seconds=slowest, preparation_and_audit_reserve_seconds=7200,
        margin_fraction=.2, predicted_six_hour_panel_seconds=predicted,
        stress_six_hour_panel_seconds=stress, available_seconds=budget,
        physical_memory_bytes=memory, observed_peak_rss_gb=max(r['peak_rss_gb'] for r in evidence),
        stress_nine_worker_rss_gb=9*4*max(r['peak_rss_gb'] for r in evidence),
        score_duration_s=duration, decision_basis='Wall clock only; no performance metrics inspected',
        reason='6h retained: 4x M-slowest stress plus preparation/audit and 20% margin fits deadline'
            if duration == 21600 else '1h fallback: 6h stress wall-clock estimate exceeds remaining budget',
        caveats=['Analytic estimate, not a runtime guarantee', 'S arrival density and queue costs are unknown',
                 'No per-job wall time is divided by nine', 'Desktop RSS is not onboard memory evidence'])
    write_json(OUTPUT, value)
    print({k:value[k] for k in ('score_duration_s', 'predicted_six_hour_panel_seconds',
                               'stress_six_hour_panel_seconds', 'available_seconds')})


if __name__ == '__main__':
    estimate()
