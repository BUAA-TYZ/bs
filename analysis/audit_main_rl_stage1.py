"""只复核第一阶段已完成记录，补充分母明确的覆盖率及三种子预算。"""

from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import sys
import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.prepare_main_rl_stage1 import ROOT, read
from analysis.run_main_local_panel import verify_main
from analysis.run_load_pressure_short import verify
from analysis.redesign_scenario import write_json
from analysis.main_rl_interface import terminal_reward, HierarchicalActor, joint_statistics


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit():
    p = read(ROOT/'protocol.json')
    verify(p)
    verify_main()
    assert read(ROOT/'final_status.json')['status'] == 'COMPLETE'
    assert sha(ROOT/'protocol.json') == read(ROOT/'launch.json')['protocol_sha256']
    dev = read(ROOT/'development_audit/AUDIT.json')
    assert dev['status'] == 'PASS'
    for name, expected in dev['evidence_hashes'].items():
        assert sha(Path(name)) == expected, name
    for name, expected in read(ROOT/'FINAL_AUDIT.json')['hashes'].items():
        assert sha(Path(name)) == expected, name
    records = {m: read(ROOT/f'{m}.json') for m in p['methods']}
    ref, teacher, student = (records[m] for m in ('reference', 'teacher', 'student'))
    assert all(r['status'] == 'PASS' and r['checks']['passed'] and r['gradient_updates'] == 0 for r in records.values())
    for key in ('action_trace_sha256', 'dynamics_sha256', 'outcomes'):
        assert ref[key] == teacher[key], key
    wanted = set(p['scored_task_ids'])
    weights = {e['task_id']: e['compute_windows'] for e in p['events'] if e['scored']}
    torch.set_num_threads(1)
    checkpoint = torch.load(ROOT/'probe_actor.pt', map_location='cpu', weights_only=True)
    assert checkpoint['schema'] == 'untrained-throughput-probe-only'
    torch.manual_seed(p['seed'])
    actor = HierarchicalActor().eval()
    assert all(torch.equal(value, checkpoint['state_dict'][key]) for key, value in actor.state_dict().items())
    for task, decision in student['decisions'].items():
        obs = decision['observation']
        mode, positions = decision['mode'], decision['positions']
        assert obs['image_mask'][mode]
        if mode == 0:
            assert len(positions) == obs['block_count'] == len(obs['tile_ids'])
            assert all(obs['position_mask'][i] for i in positions)
            assert decision['targets'] == {tid: obs['node_ids'][i] for tid, i in zip(obs['tile_ids'], positions)}
        else:
            assert positions == [] and decision['targets'] == {}
        with torch.no_grad():
            logits = actor.logits(obs)
            logp, _ = joint_statistics(*logits, mode, positions)
        assert math.isclose(float(logp), decision['log_probability'], abs_tol=1e-5), task
    for record in records.values():
        outcomes = {o['task_id']: o for o in record['outcomes'] if o['task_id'] in wanted}
        assert set(outcomes) == wanted
        assert all(o['observed_age_seconds'] >= 600 for o in outcomes.values())
        assert record['rewards'] == {t: terminal_reward(o) for t, o in outcomes.items()}
    categories = Counter()
    images = []
    helper_windows = helper_topk_windows = 0
    fully_representable = 0
    captured = set(teacher['labels']) & wanted
    for task in sorted(wanted):
        n = weights[task]
        if task not in captured:
            categories['uncaptured_windows'] += n
            images.append({'task_id': task, 'total_windows': n, 'uncaptured': True})
            continue
        row = teacher['labels'][task]
        label, obs = row['label'], row['observation']
        assert label['total_windows'] == n
        counts = {k: label[k] for k in ('represented_windows', 'outside_topk_windows', 'masked_windows',
                                       'uncommitted_windows', 'ground_block_windows')}
        assert math.isclose(sum(counts.values()), n, abs_tol=1e-9)
        # 整图地面也须遵守图像层掩码；仅复核记录，不修改原标签或动作。
        mode_masked = not obs['image_mask'][label['mode']]
        if mode_masked:
            counts['image_mode_masked_windows'] = counts['represented_windows']
            counts['represented_windows'] = 0
        categories.update(counts)
        fully_representable += counts['represented_windows'] == n
        block_weights = dict(zip(obs['tile_ids'], obs['window_weights']))
        assert sum(block_weights.values()) == n
        for tile, target in row['actual_commitments'].items():
            if isinstance(target, int) and target != obs['source']:
                helper_windows += block_weights[tile]
                helper_topk_windows += block_weights[tile] * (target in obs['node_ids'][1:])
        images.append({'task_id': task, 'total_windows': n, 'mode': label['mode'],
                       'image_mode_masked': mode_masked, **counts})
    total = sum(weights.values())
    assert math.isclose(sum(categories.values()), total, abs_tol=1e-9)
    coverage = {'scored_images': len(wanted), 'captured_images': len(captured),
                'fully_representable_images': fully_representable, 'total_scored_windows': total,
                'ground_block_images': sum(r.get('ground_block_windows', 0) > 0 for r in images),
                'counts': dict(categories), 'fractions_all_scored_windows': {k: v/total for k, v in categories.items()},
                'actual_helper_windows': helper_windows, 'actual_helper_topk_windows': helper_topk_windows,
                'helper_topk_coverage': helper_topk_windows/helper_windows if helper_windows else None,
                'topk_denominator': 'All windows actually assigned to non-source satellite helpers; raw-ground/local excluded',
                'outside_topk_note': 'Includes helpers outside the current two-hop candidate pool, not just rank>8',
                'image_rows': images}
    decisions = len(set(student['decisions']) & wanted)
    wall = student['wall_seconds']
    rates = [9*eff*3600/wall for eff in (.6, 1.)]
    episodes = [math.floor(84*r/3) for r in rates]
    budget = {'student_wall_seconds': wall, 'scored_decisions_per_episode': decisions,
              'student_all_decisions': len(student['decisions']),
              'measured_concurrency': 3, 'projected_workers': 9, 'efficiency_assumption': [.6, 1.],
              'projected_episodes_per_hour': rates, 'projected_decisions_per_hour': [r*decisions for r in rates],
              'sampling_wall_hours': 84, 'per_seed_sampling_wall_hours': 28,
              'per_seed_episode_range': episodes, 'per_seed_decision_range': [e*decisions for e in episodes],
              'allocated_budget': {str(s): {'episodes': episodes[0], 'decisions': episodes[0]*decisions}
                                   for s in (1701, 1702, 1703)},
              'allocation_status': 'Provisional conservative cap; no training launched; confirm at stage1 milestone',
              'limitations': 'One date/window, untrained actor, no gradient timing; nine-worker scaling not measured. Repeated rollouts are not independent scenarios. Orin excluded.'}
    write_json(ROOT/'COVERAGE_AND_BUDGET.json', {'coverage': coverage, 'budget': budget})
    lines = ['# 第一汇报点独立复核', '',
             '仅复算已完成记录，不重跑，不训练。', '',
             f'开发日同160目标、24源星、{dev["window_count_actual"]}窗口逐字段一致；1128条最终maritime80到达精确一致。',
             'reference与teacher动作序列哈希、执行轨迹哈希、逐图结果完全一致；三项全部PASS、600秒随访和终结奖励复核通过。', '',
             '| 教师覆盖指标 | 数值 |', '|---|---:|',
             f'| 捕获图像 / 评分图像 | {len(captured)} / {len(wanted)} |',
             f'| 完全可表示图像 / 评分图像 | {fully_representable} / {len(wanted)} |',
             f'| 全评分窗口数 | {total} |']
    for k, v in sorted(categories.items()):
        lines.append(f'| {k} | {v} / {total} = {v/total:.4%} |')
    topk = coverage['helper_topk_coverage']
    lines += [f'| 实际卸载到计算星的窗口Top-K覆盖 | {helper_topk_windows} / {helper_windows}'+
              (f' = {topk:.4%} |' if topk is not None else '，无该类样本 |'), '',
              'Top-K分母只含实际卸载到非源计算星的窗口，排除本地与原图地面。候选外包含不在当时两跳候选池内的节点，并非全部是“第9名以后”。所有不可表示项均以全评分窗口为分母；未捕获也计入，未默默剔除。', '',
              f'本次有{coverage["ground_block_images"]}张图存在部分块送地面、其余块在本地/计算星处理。它们不是整图原图地面分支；现有获批块动作只有本地/计算星，因此不可表示原因并不只剩Top-K外。未扩充动作或修改奖励。窗口软标签仅在可表示质量内归一化，不能将其称为完整复现教师；进入模仿训练前需明确这些混合图的标签处理，本轮在此汇报点停止。逐图承诺保留在teacher.json和COVERAGE_AND_BUDGET.json。', '',
              '| 实测规则 | 墙钟分钟 | 初始化秒 | 推理及派发秒 | 观测秒 | 教师秒 |', '|---|---:|---:|---:|---:|---:|']
    for method, r in records.items():
        lines.append(f'| {method} | {r["wall_seconds"]/60:.3f} | {r["initialization_seconds"]:.3f} | {r["inference_and_dispatch_seconds"]:.3f} | {r["observation_seconds"]:.3f} | {r["teacher_seconds"]:.3f} |')
    lines += ['', f'含网络推理和600秒随访，student单次{wall/60:.3f}分钟、{decisions}个评分整图决策。',
              f'9worker效率假设0.6–1，对应每小时{rates[0]:.2f}–{rates[1]:.2f}片段、{rates[0]*decisions:.0f}–{rates[1]*decisions:.0f}决策。',
              f'10-27至11-02共168小时，预留一半给优化/验证/余量，采样84小时、三种子各28小时。每种子估计{episodes[0]}–{episodes[1]}片段；暂按保守端分配每种子{episodes[0]}片段、{episodes[0]*decisions}个决策，三种子1701/1702/1703相同。',
              '这是单日无梯度探针加效率假设，不是训练或9worker实测。不同训练日及已学习策略的拥塞可能改变速度；重复轨迹不等于独立场景。Orin未计入。纯模仿checkpoint应单独保存，本次只有未训练probe_actor。', '',
              '局部动态状态经显式ISL报文共享带宽，逐跳推进，断链等待、15秒TTL；没有单独添加latency_ms传播时延。详见OBSERVATION_TRANSPORT.md。',
              '固定现有分块规则；动态粒度为后续工作。阶段1完成后停下等确认，未启动BC/MAPPO，未访问验证或封存测试。']
    (ROOT/'MILESTONE_REPORT.md').write_text('\n'.join(lines)+'\n')
    artifacts = [ROOT/'COVERAGE_AND_BUDGET.json', ROOT/'MILESTONE_REPORT.md', ROOT/'OBSERVATION_TRANSPORT.md', ROOT/'probe_actor.pt',
                 ROOT/'unit_test.log', Path(__file__), ROOT/'FINAL_AUDIT.json', ROOT/'launch.json']
    write_json(ROOT/'INDEPENDENT_AUDIT.json', {'status': 'PASS', 'protocol_hash': sha(ROOT/'protocol.json'),
               'source_hash_count': len(p['frozen_protocol']['source_hashes']),
               'student_decisions_mask_logp_recomputed': len(student['decisions']),
               'probe_weights_equal_initial_seed': True,
               'hashes': {str(f): sha(f) for f in artifacts}})
    print(json.dumps({'status': 'PASS', 'coverage': {k: v for k, v in coverage.items() if k != 'image_rows'}, 'budget': budget}))


if __name__ == '__main__':
    audit()
