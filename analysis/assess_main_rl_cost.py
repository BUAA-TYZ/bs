"""只用已完成计时估算采样预算；不运行环境、模型或训练。"""

import json
from pathlib import Path

from analysis.estimate_load_pressure import OUT, sha
from analysis.freeze_main_scenario import ROOT
from analysis.redesign_scenario import write_json


def main():
    jobs = []
    for segment, methods in [('hour_3600s', [('local', ''), ('ground', ''), ('fifo', '')]),
                             ('six_21600s', [('local', 'neutral'), ('ground', 'neutral'), ('fifo', 'neutral'),
                                            ('v3', 'information'), ('local_1', 'information'), ('local_2', 'information')])]:
        p = json.loads((OUT/segment/'protocol.json').read_text())
        for method, phase in methods:
            f = OUT/segment/phase/f'{method}.json'
            r = json.loads(f.read_text())
            jobs.append({'segment': segment, 'method': method, 'wall_seconds': r['wall_seconds'],
                         'policy_seconds': r['policy_seconds'], 'scored_images': len(p['scored_task_ids']),
                         'unique_scored_whole_image_decisions_upper_bound': len(p['scored_task_ids']),
                         'decision_quote_rows': len(r.get('decisions', [])), 'source': str(f), 'sha256': sha(f)})
    projections = []
    for segment in ['hour_3600s', 'six_21600s']:
        rows = [r for r in jobs if r['segment'] == segment]
        images = rows[0]['scored_images']
        fastest, slowest = min(r['wall_seconds'] for r in rows), max(r['wall_seconds'] for r in rows)
        # 60%是预算折减假设，不是9-worker或神经网络实测。
        low, high = .6*9*3600/slowest, 9*3600/fastest
        projections.append({'segment': segment, 'observed_single_wall_s_range': [fastest, slowest],
                            'mac_workers': 9, 'planning_efficiency_range': [.6, 1.],
                            'episodes_per_hour_range': [low, high],
                            'whole_image_decisions_per_hour_upper_range': [low*images, high*images],
                            'seven_days_at_half_availability_hours': 84,
                            'seven_days_decisions_upper_range': [low*images*84, high*images*84]})
    data = {'scope': 'Analytic budget only, no environment or training run', 'timings': jobs,
            'projections': projections, 'calendar': '2026-10-27 through 2026-11-02 inclusive, 168h calendar / 84h assumed usable',
            'orin_included': False, 'prior_nine_worker_probe': 'results/redesign_20261021/calibration/capacity_20261021/host_probe/REPORT.md',
            'limitations': ['One-hour full v3/neural policy not timed; neutral runtime is optimistic for learner',
                           'Six-hour timings were 3/5-worker panels, not nine-worker high-load measurements',
                           'Repeated same trace/image decisions are correlated, not fresh independent examples',
                           'Warm start and update/GNN tensor costs not measured; budget deduction is hypothetical']}
    write_json(ROOT/'RL_COST_ESTIMATE.json', data)
    lines = ['# GNN + MAPPO 成本评估（未实现、未训练）', '',
             '窗口为2026-10-27至11-02，共168小时。按50%时间可用于采样，预算84小时；其余留给模仿训练、网络更新、验证、检查和停机，不承诺全时满负荷。', '',
             '|片段|已有单次墙钟|本机9 worker片段/小时预算|整图决策/小时上限|84小时整图决策上限|',
             '|---|---:|---:|---:|---:|']
    for r in projections:
        a,b=r['observed_single_wall_s_range']; c,d=r['episodes_per_hour_range'];e,f=r['whole_image_decisions_per_hour_upper_range'];g,h=r['seven_days_decisions_upper_range']
        lines.append(f"|{r['segment']}|{a/60:.1f}-{b/60:.1f}分钟|{c:.1f}-{d:.1f}|{e:.0f}-{f:.0f}|{g:.0f}-{h:.0f}|")
    lines.extend(['', '上述数量只计算72/340张主窗口图像的一次整图选择，不把每0.1秒物理步或无任务的agent算作学习样本。v3六小时955条报价记录中含重复等待/重新报价，不能当955张独立图像；同时决策的源星也受环境耦合。',
        '', '## 可用性判断', '',
        '九worker是此前低负载跨主机短测确认过的本机安全并行上限；高负载6小时只测过3/5并行，表中效率0.6-1是预算假设，尚非9并行高负载实测。1小时12分钟左右来自中性规则，没有测过1小时GNN/v3；完整网络推理和训练会降低吞吐。Orin本轮不连接、不计入：需恢复连接并以相同源码/负载短测CPU仿真和一致性，不能按GPU TOPS折算仿真加速。开发机同理。',
        '训练6日期若各仅一个6小时片段，以当前340图作尺度参照，只有约2040个不同轨道/负载图像状态，实际数尚未生成。这不是20万独立样本；重复rollout只增加策略诱导状态多样性。先模仿全局v3可减少冷启动探索，但教师使用全局动态信息，局部特征可能无法唯一确定教师动作。',
        '现有速度可支持一个受控的、小规模模仿加MAPPO可行性试验，不能据此断言收敛或超过强局部基线。若试验预算要求100万次有效整图决策，84小时即使乐观端也不足；不适合同时大规模调参、多结构、多种子。至少3种子均分预算，仍需给验证留出固定开销。是否值得训练首先取决于本轮最强局部规则与v3的差距。',
        '', '## 加速选项（仅评估）', '',
        '1. 训练使用30-60分钟窗口：冻结从训练日期均匀/分层抽样规则及充分预热、D随访；验证仍用6小时。1小时/6小时已有吞吐可作预算，但不能只挑高分或短队列窗口，也不能把空队列截断当原始长轨迹。',
        '2. 将0.1秒步长试为0.2/0.5秒：理论固定步进次数降2/5倍，端到端收益不超过此值；可能改变链路捕获、窗口边界、批处理和竞争次序。先在开发日对原版逐图按时集合、时延、队列、字节/工时守恒及策略排名复核，预先定容差，不能默认等价。',
        '3. 事件驱动/决策时刻推进：跳过无变化时段，但保留链路/捕获/传输完成、计算完成、队列准入、deadline、控制通告和公平带宽分配的全部事件；同时间事件顺序必须与0.1秒参考一致。潜在收益大但需工程实现和逐事件对照，不能直接以1秒决策间隔替换物理更新。',
        '4. 静态几何/可见性/接触计划跨worker复用或内存映射；动态积压与逐源历史严格隔离。先profile计时占比，用Amdahl上限估收益，再对输出一致性验证。新增Orin或devbox只做独立rollout，先锁版本并核验跨平台结果，不拆迁在跑片段。',
        '', '除已测片段吞吐外，以上均不是已实现速度；本轮没有运行神经模型、生成学习标签、训练或启封测试。原始计时和计算式见RL_COST_ESTIMATE.json。'])
    (ROOT/'RL_COST_ASSESSMENT.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps(projections))


if __name__ == '__main__':
    main()
