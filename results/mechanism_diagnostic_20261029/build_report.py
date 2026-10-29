"""只读汇总已完成的正式记录；不导入执行器，不重放或修改原始实验。"""
from collections import Counter
from pathlib import Path
import hashlib
import json
import math
import numpy as np

BASE = Path('/Users/bytedance/Projects/bs')
MAIN = BASE / 'results/redesign_20261021/main_scenario'
OUT = Path(__file__).resolve().parent
RUNS = {'S': 's_formal_test_20261029', 'M': 'm_formal_test_20261027',
        'L': 'l_formal_test_20261028'}
MODELS = [f'{f}_{s}' for f in ('fixed', 'dynamic') for s in (1701, 1702, 1703)]
METHODS = MODELS + ['zero_2_16', 'zero_2_4', 'global_v3_dynamic']
DATES = ['2026-12-21', '2026-12-22', '2026-12-23']
provenance = {}


def read(path, expected=None):
    path = Path(path)
    if not path.is_absolute():
        path = BASE / path
    raw = path.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    if expected is not None:
        assert sha == expected, path
    provenance[str(path)] = sha
    return json.loads(raw)


def mean(values):
    values = list(values)
    return float(np.mean(values)) if values and all(v is not None for v in values) else None


def stats(values):
    values = list(values)
    return {'n': len(values), 'mean': mean(values),
            'P90': float(np.quantile(values, .9)) if values else None,
            'max': max(values) if values else None}


def pct(n, d):
    return 100 * n / d if d else None


def fmt(v):
    return 'NA' if v is None else f'{v:.3f}'


def table(headers, rows):
    return '\n'.join(['|' + '|'.join(headers) + '|', '|' + '|'.join(['---'] * len(headers)) + '|'] +
                     ['|' + '|'.join(str(v) for v in row) + '|' for row in rows]) + '\n'


def mode_map(r):
    method = r['method']
    if method.startswith('fixed_'):
        return {t: 'ground' if v == 1 else 'split16' for t, v in r['choices'].items()}
    if method.startswith('dynamic_'):
        return {t: ('split4', 'split16', 'ground')[v] for t, v in r['choices'].items()}
    if method == 'global_v3_dynamic':
        return {v['task_id']: v['selected'] for v in r['decisions']}
    return None


def is_ground(mode):
    return mode == 'ground'


def summarize(r, p):
    ids = set(p['scored_task_ids'])
    rows = {o['task_id']: o for o in r['outcomes'] if o['task_id'] in ids}
    assert set(rows) == ids and r['status'] == 'PASS'
    assert all(o['observed_age_seconds'] >= 600 for o in rows.values())
    done = [o for o in rows.values() if o['status'] == 'completed']
    lat = [o['latency_seconds'] for o in done]
    assert math.isclose(mean(lat), r['latency']['mean_completed_s'])
    modes = mode_map(r)
    if modes is not None:
        assert set(modes) <= ids
    mode_counts = Counter(modes.values()) if modes is not None else None
    ng = sum(is_ground(v) for v in modes.values()) if modes is not None else None
    ns = len(modes) - ng if ng is not None else None
    n4 = sum(v.endswith('_4') or v == 'split4' for v in modes.values()) if modes else None
    n16 = ns - n4 if ns is not None else None
    gran = r['granularity']
    assert set(gran) == ids
    assert all(v['actual'] in (4, 16) for v in gran.values())
    sources = set(p['config']['task_source_sat_ids'])
    helpers = set(range(p['config']['num_sats'])) - sources
    res = r['resources']['main']
    busy = {int(k): v for k, v in res['helper_busy_by_node'].items()}
    assert math.isclose(sum(busy.get(k, 0) for k in helpers), res['helper_busy_s'])
    resource = {'helper_pct': 100 * res['helper_utilization'],
                'ground_pct': 100 * res['ground_channel_utilization'],
                'helper_max_pct': 100 * max(busy.get(k, 0) for k in helpers) / res['duration_s'],
                'source_pct': 100 * sum(busy.get(k, 0) for k in sources) / (len(sources) * res['duration_s']),
                'raw_isl_GB': res.get('raw_isl_bytes', 0) / 1e9,
                'result_isl_GB': res.get('result_isl_bytes', 0) / 1e9,
                'ground_GB': res['ground_bytes'] / 1e9,
                'control_GB': res['control_bytes'] / 1e9}
    candidate = {}
    f = r.get('feature_audit')
    if f:
        for g in (4, 16):
            key = f'g{g}/after_topk'
            if key not in f['candidates']:
                continue
            value = {}
            for kind in ('before_topk', 'after_topk', 'legal_compute'):
                h = f['candidates'][f'g{g}/{kind}']
                xs = [int(k) for k, n in h['histogram'].items() for _ in range(n)]
                assert len(xs) == f['decisions'] == h['count']
                assert math.isclose(mean(xs), h['mean'])
                value[kind] = stats(xs)
                value[kind]['zero_pct'] = pct(xs.count(0), len(xs))
                value[kind]['histogram'] = h['histogram']
            candidate[str(g)] = value
            assert value['before_topk']['histogram'] == value['after_topk']['histogram']
            assert value['after_topk']['histogram'] == value['legal_compute']['histogram']
            assert value['after_topk']['max'] <= 4
        groups = f['groups']
        nodes = sum(v['rows'] for k, v in groups.items() if not k.endswith('/image'))
        unknown = sum(v['rows'] for k, v in groups.items() if k.endswith('/unknown'))
        compute = sum(v['rows'] for k, v in groups.items() if '/compute/' in k)
        compute_unknown = sum(v['rows'] for k, v in groups.items() if '/compute/unknown' in k)
        ground = sum(v['rows'] for k, v in groups.items() if '/ground/' in k)
        ground_unknown = sum(v['rows'] for k, v in groups.items() if '/ground/unknown' in k)
        unknowns = {'all_pct': pct(unknown, nodes), 'compute_pct': pct(compute_unknown, compute),
                    'ground_pct': pct(ground_unknown, ground), 'node_rows': nodes,
                    'compute_rows': compute, 'unknown_compute_rows': compute_unknown}
    else:
        unknowns = None
    tail = [o for o in done if o['latency_seconds'] > 600]
    tail_ground = [o for o in tail if modes and o['task_id'] in modes and is_ground(modes[o['task_id']])]
    tail_split = [o for o in tail if modes and o['task_id'] in modes and not is_ground(modes[o['task_id']])]
    # 同一已完成集合上的恒等分解；不把它命名为任何物理阶段。
    ready = [o['results_ready_seconds'] - o['release_seconds'] for o in done]
    post = [o['completion_seconds'] - o['results_ready_seconds'] for o in done]
    assert all(math.isclose(a + b, c, abs_tol=1e-7) for a, b, c in zip(ready, post, lat))
    return {'n': len(ids), 'completed': len(done), 'unfinished': len(ids) - len(done),
            'P60': pct(sum(x <= 60 for x in lat), len(ids)),
            'P600': pct(sum(x <= 600 for x in lat), len(ids)),
            'R600': sum(min(o['latency_seconds'], 600) if o['status'] == 'completed' else 600
                        for o in rows.values()) / len(ids),
            'latency': stats(lat), 'ready_total': stats(ready), 'post_ready': stats(post),
            'ground_pct': pct(ng, len(ids)) if ng is not None else None,
            'unacted_pct': pct(len(ids) - len(modes), len(ids)) if modes is not None else None,
            'unacted_ids': sorted(ids - set(modes)) if modes is not None else None,
            'split_pct': pct(ns, len(ids)) if ns is not None else None,
            'split4_cond_pct': pct(n4, ns) if ns is not None else None,
            'split16_cond_pct': pct(n16, ns) if ns is not None else None,
            'mode_counts': mode_counts, 'actual4_pct': pct(sum(v['actual'] == 4 for v in gran.values()), len(ids)),
            'actual16_pct': pct(sum(v['actual'] == 16 for v in gran.values()), len(ids)),
            'candidate': candidate, 'unknown': unknowns, 'resource': resource,
            'tail_count': len(tail), 'tail_contribution_s': sum(o['latency_seconds'] for o in tail) / len(done),
            'non_tail_contribution_s': sum(o['latency_seconds'] for o in done if o['latency_seconds'] <= 600) / len(done),
            'tail_ground_count': len(tail_ground) if modes else None,
            'tail_split_count': len(tail_split) if modes else None,
            'tail_ground_contribution_s': sum(o['latency_seconds'] for o in tail_ground) / len(done) if modes else None,
            'tail_split_contribution_s': sum(o['latency_seconds'] for o in tail_split) / len(done) if modes else None,
            'ground_latency': stats(o['latency_seconds'] for o in done if modes and o['task_id'] in modes and is_ground(modes[o['task_id']])),
            'split_latency': stats(o['latency_seconds'] for o in done if modes and o['task_id'] in modes and not is_ground(modes[o['task_id']])),
            'failure_counts': dict(Counter(o['failure_reason'] or o['status'] for o in rows.values() if o['status'] != 'completed')),
            'outcomes': rows, 'modes': modes}


daily = {}
for scale, folder in RUNS.items():
    root = MAIN / folder
    status = read(root / 'final_status.json')
    assert status['status'] == 'COMPLETE'
    index = {str((BASE / row['path']).resolve()): row['sha256'] for row in status['results']}
    daily[scale] = {}
    for method in METHODS:
        paths = [root / 'validation' / day / f'{method}.json' for day in DATES]
        if not all(p.exists() for p in paths):
            assert scale == 'L' and method == 'zero_2_4' and not any(p.exists() for p in paths)
            daily[scale][method] = None
            continue
        days = {}
        for day, path in zip(DATES, paths):
            r = read(path, index[str(path)])
            p = read(r['protocol'])
            assert p['date'] == day
            days[day] = summarize(r, p)
        daily[scale][method] = days


def avg(days, *keys):
    vals = []
    for d in days.values():
        v = d
        for k in keys:
            v = v[k] if v is not None else None
        vals.append(v)
    return mean(vals)


def paired(days, refdays):
    values = []
    for day, d in days.items():
        a, b = d['outcomes'], refdays[day]['outcomes']
        assert set(a) == set(b)
        ids = [k for k in a if a[k]['status'] == b[k]['status'] == 'completed']
        diff = [a[k]['latency_seconds'] - b[k]['latency_seconds'] for k in ids]
        values.append({'day': day, 'n': len(ids), 'mean_delta_s': mean(diff),
                       'P90_delta_s': float(np.quantile(diff, .9)),
                       'wins': sum(v < 0 for v in diff), 'ties': sum(v == 0 for v in diff),
                       'losses': sum(v > 0 for v in diff)})
    return {'daily': values, 'day_equal_mean_delta_s': mean(v['mean_delta_s'] for v in values),
            'common_completed': sum(v['n'] for v in values)}


pairs = {scale: {m: {ref: paired(days, daily[scale][ref])
                    for ref in ('zero_2_16', 'zero_2_4') if daily[scale][ref] is not None}
                 for m, days in methods.items() if days and m in MODELS + ['global_v3_dynamic']}
         for scale, methods in daily.items()}

lines = ['# S/M/L 只读机制诊断', '',
         '日期：2026-10-29。仅使用已完成正式记录与实现源码核对语义；未重放任何日期，未训练、微调、选模、调参或修改协议/论文。',
         'S/M 为每日6小时，L为每日1小时；2026-12-21至23，同日期、不同场景、模型冻结。除计数合计外，均先逐日计算再三日等权；P90列是逐日P90的等权均值，不是三日合池P90。',
         '78份原始结果均与原final_status记录哈希一致。L未登记zero_2_4，明确NA；不得补造测试结果或用验证日代替。', '',
         '## 结论与证据强度', '',
         '1. 前提需要修正：S全局dynamic相对zero_2_16，P60 36.181%>33.522%、P600 94.324%>94.133%、已交付均值183.521s<239.085s、R600 175.068s<181.605s，四项均更好；但未完成7幅多于4幅。相对zero_2_4，P60和均值更好，但P600和R600更差。不能称全局全面不优，也不能称对所有局部规则全面占优。',
         '2. S主窗整档计算星和下行平均利用率均低，且候选最多4个、小于Top-K=8，Top-K前后计数一致，已选候选未因位置mask进一步减少。没有“总算力/额定下行持续饱和”或“Top-K截断”证据；但S全局dynamic有2幅明确mem_full（12-21、源星2、task_84/85），支持该局部内存约束确实发生过。其余所选S/M/L方法的未完成均为pending。低均值不能排除短时/局部排队、接触可达性或回传窗口限制，更不能证明加容量无效。',
         '3. 固定模型相对S zero_2_16的均值增加62.742~82.405s，其中长尾贡献增加42.053~68.916s，且固定三个种子的全部>600秒已完成图均属分片模式。动态模型均值增加4.785~24.835s，600秒内贡献增加26.482~28.223s、长尾贡献减少1.647~21.935s。这定位了统计来源，尚不能区分分片模式内部的计算、路由、回传等待原因。',
         '4. 候选计算星状态未知比例为0；总未知比例含固定标记unknown的地面特征，不能解释为计算星遥测普遍缺失。观测known不等于真实状态无误差/无陈旧。',
         '5. 正式记录没有分片targets、逐分片等待/传输/计算时间或逐决策邻居集合。五阶段均值/P90、分片去向占比、未经可行性过滤的2跳计算星数无法恢复，以下明确列NA。因此不能把S退化的物理主因断言为计算队列或等待回传；验证日重放也不能还原测试日这些缺失量，本次不启动新重放。', '',
         '## 1. 性能与容量', '',
         '计算星利用率=计算星busy秒/(计算星数×主窗秒)；源星另列。下行利用率=实际发送字节/(所有地面接收通道额定速率×主窗秒)，分母包含无接触时间，不是可见窗口利用率。单星max为各日最忙计算星的日平均利用率再日等权，并非瞬时峰值。', '']
rows = []
for scale, methods in daily.items():
    for m, days in methods.items():
        if days is None:
            rows.append([scale, m] + ['NA'] * 10)
            continue
        rows.append([scale, m, fmt(avg(days, 'P60')), fmt(avg(days, 'P600')),
                     fmt(avg(days, 'latency', 'mean')), fmt(avg(days, 'latency', 'P90')), fmt(avg(days, 'R600')),
                     sum(d['unfinished'] for d in days.values()), fmt(avg(days, 'resource', 'helper_pct')),
                     fmt(avg(days, 'resource', 'helper_max_pct')), fmt(avg(days, 'resource', 'source_pct')),
                     fmt(avg(days, 'resource', 'ground_pct'))])
lines.append(table(['档', '方法', 'P60%', 'P600%', '交付均值s', '交付P90s', 'R600s', '未完成合计',
                    '计算星%', '单计算星max%', '源星%', '下行%'], rows))
lines += ['### 未完成与容量证据', '',
          '所有表格的未完成包含失败和截止时仍pending，不能等同于已失败。下面单列已保存原因；pending没有更细阶段，不能据此指认为等回传。', '']
rows = []
for scale, methods in daily.items():
    for m, days in methods.items():
        if not days:
            rows.append([scale, m, 'NA', 'NA'])
            continue
        counts = Counter()
        for d in days.values():
            counts.update(d['failure_counts'])
        assert set(counts) <= {'pending', 'mem_full'}
        rows.append([scale, m, counts['mem_full'], counts['pending']])
lines.append(table(['档', '方法', 'mem_full合计', 'pending合计'], rows))

lines += ['## 2. 整图决策与粒度', '',
          '原mode=分片分配/整图原始数据送地面，不能严格改名为“全部在轨/全部地面”：分片模式本身允许本星、计算星和ground_block。fixed:0=分片16、1=整图地面；dynamic:0=分片4、1=分片16、2=整图地面。',
          '分片4/16条件比例的分母仅为已记录选择分片模式的图像数；实际4/16比例的分母为全部评分图像。实际块数可能因窗口数不足低于请求数，整图地面也保存粒度，不能混用两组比例。全局用selected=local/cgs_4/16或ground对应分类。S全局dynamic有3幅评分图无选择记录（2幅mem_full、1幅完成），单列缺记录比例，不归入分片或地面，也不能称全部未执行。局部规则未保存整图selected，不能以名称推断。', '']
rows = []
for scale, methods in daily.items():
    for m, days in methods.items():
        rows.append([scale, m] + ([fmt(avg(days, k)) for k in ('split_pct', 'ground_pct', 'unacted_pct', 'split4_cond_pct',
                    'split16_cond_pct', 'actual4_pct', 'actual16_pct')] if days else ['NA'] * 7))
lines.append(table(['档', '方法', '分片模式%', '整图地面%', '选择缺记录%', '分片4条件%', '分片16条件%', '实际4%', '实际16%'], rows))
lines += ['### 分片去向：本星/地面/计算星', '',
          '上述26个有记录的方法-档位组合均未保存完整逐分片targets或执行去向；三类占比均NA。整图ground模式可确定整图走原始数据地面方案，但不能当成全体分片地面比例。global的local_*可识别该整图选本地方案，cgs_*不能推出全部分片落计算星。',
          'action_trace_sha256只保留不可逆摘要；raw/result_relay_hops是整图累计跳数，字节也含重复与多跳，均不能反推出分片数量或最终计算地点。', '']

lines += ['## 3. 候选可用性与未知状态', '',
          'pre是2跳范围与静态输入路由可行性相交后、Top-K之前的计算星数；post是Top-K内非padding计算星数；legal是位置mask判定可用的计算星数，反映决策视图可行性，不是逐包真实到达确认。纯拓扑2跳数未保存，不能把pre冒充它。',
          'dynamic对每张图同时审计4/16两种候选，不只审计最终选中的粒度。未知比例按有效node行统计，排除padding；动态包含两份观测，不能当作独立图像样本。', '']
rows = []
for scale, methods in daily.items():
    for m, days in methods.items():
        if not days or m not in MODELS:
            rows.append([scale, m, 'NA'] + ['NA'] * 7)
            continue
        for g in ('4', '16'):
            if g not in next(iter(days.values()))['candidate']:
                continue
            rows.append([scale, m, g, fmt(avg(days, 'candidate', g, 'before_topk', 'mean')),
                         fmt(avg(days, 'candidate', g, 'after_topk', 'mean')),
                         fmt(avg(days, 'candidate', g, 'legal_compute', 'mean')),
                         fmt(avg(days, 'candidate', g, 'legal_compute', 'P90')),
                         max(d['candidate'][g]['legal_compute']['max'] for d in days.values()),
                         fmt(avg(days, 'candidate', g, 'legal_compute', 'zero_pct')),
                         'NA'])
lines.append(table(['档', '方法', '请求块', 'pre均值', 'post均值', 'legal均值', 'legal P90', 'legal最大', 'legal=0%', '纯2跳数'], rows))
rows = []
for scale, methods in daily.items():
    for m, days in methods.items():
        rows.append([scale, m] + ([fmt(avg(days, 'unknown', k)) for k in ('all_pct', 'compute_pct', 'ground_pct')]
                                  if days and m in MODELS else ['NA'] * 3))
lines.append(table(['档', '方法', '全部有效节点未知%', '计算星未知%', '地面特征未知%'], rows))
lines += ['地面特征由main_bc_interface构造，默认known标志为false；该100%是编码口径，不等于观测到物理地面故障。zero/global没有FeatureAudit，候选与未知统计NA，不借用同档MAPPO值；Top-K=8是学习模型接口规则，不能强加给无该接口的基线。', '']

lines += ['## 4. 时延分解的可识别范围', '',
          table(['请求阶段', 'S均值/P90', 'M均值/P90', 'L均值/P90', '原因'],
                [[x, 'NA', 'NA', 'NA', '缺逐分片事件/服务区间，所有对比方法均如此']
                 for x in ['等待计算', '计算', '星间传输', '等待回传窗口', '下传']]),
          '不能用总busy秒/完成图像数代替关键路径计算时间，也不能用字节/带宽冒充包含并发与窗口的传输耗时。results_ready_seconds在distributed执行器中是最后分片end_downlink（其他pipeline为end_compute），并非“星上计算结束”；因此completion-results_ready不是下传或等回传窗口耗时。',
          '以下唯一可精确闭合的两项为release到results_ready的综合过程，以及results_ready到整图交付的后处理。每张已完成图的两项和严格等于总时延；P90不可相加。', '']
rows = []
for scale, methods in daily.items():
    for m, days in methods.items():
        rows.append([scale, m] + ([fmt(avg(days, a, b)) for a in ('ready_total', 'post_ready') for b in ('mean', 'P90')]
                                  if days else ['NA'] * 4))
lines.append(table(['档', '方法', 'ready前综合均值s', 'ready前综合P90s', 'ready后均值s', 'ready后P90s'], rows))

lines += ['## 5. S均值变差：分布贡献与同图配对', '',
          '长尾贡献=sum(已完成且时延>600的时延)/全部已完成数；非长尾贡献同理，两者加总为已交付均值。不是超过600部分的超额，也不含未完成任务。计数为三日合计，贡献先日内再等权。整图模式分组是相关性，分片模式内真实去向缺失。', '']
rows = []
for scale, methods in daily.items():
    for m, days in methods.items():
        if not days:
            rows.append([scale, m] + ['NA'] * 7)
            continue
        rows.append([scale, m, sum(d['tail_count'] for d in days.values()),
                     fmt(avg(days, 'tail_contribution_s')), fmt(avg(days, 'non_tail_contribution_s')),
                     sum(d['tail_ground_count'] for d in days.values()) if m in MODELS + ['global_v3_dynamic'] else 'NA',
                     sum(d['tail_split_count'] for d in days.values()) if m in MODELS + ['global_v3_dynamic'] else 'NA',
                     fmt(avg(days, 'tail_ground_contribution_s')), fmt(avg(days, 'tail_split_contribution_s'))])
lines.append(table(['档', '方法', '>600完成数', '长尾贡献s', '非长尾贡献s', '整图地面尾数', '分片模式尾数', '地面尾贡献s', '分片尾贡献s'], rows))
rows = []
ref = daily['S']['zero_2_16']
for m in MODELS:
    ds = daily['S'][m]
    delta = avg(ds, 'latency', 'mean') - avg(ref, 'latency', 'mean')
    tail = avg(ds, 'tail_contribution_s') - avg(ref, 'tail_contribution_s')
    rest = avg(ds, 'non_tail_contribution_s') - avg(ref, 'non_tail_contribution_s')
    assert math.isclose(tail + rest, delta, abs_tol=1e-9)
    rows.append([m, fmt(delta), fmt(tail), fmt(rest),
                 pairs['S'][m]['zero_2_16']['common_completed'],
                 fmt(pairs['S'][m]['zero_2_16']['day_equal_mean_delta_s'])])
lines.append(table(['S模型减zero_2_16', '均值差s', '长尾贡献差s', '非长尾贡献差s', '共同完成数', '共同完成均值差s'], rows))
lines += ['### 相对M的变化', '',
          'S与M的图像集合不同，以下只是跨场景统计差，不是同图配对。S的已交付均值也并非每个种子都比M差；dynamic1702的S均值反而略低，但S的P90明显更高。L窗口更短，完成集合与尾部截尾程度不同，不能把L较小的已交付均值直接解释为大规模更快。', '']
rows = []
for m in MODELS:
    s, mm = daily['S'][m], daily['M'][m]
    rows.append([m, fmt(avg(s, 'latency', 'mean') - avg(mm, 'latency', 'mean')),
                 fmt(avg(s, 'latency', 'P90') - avg(mm, 'latency', 'P90')),
                 fmt(avg(s, 'tail_contribution_s') - avg(mm, 'tail_contribution_s')),
                 fmt(avg(s, 'non_tail_contribution_s') - avg(mm, 'non_tail_contribution_s'))])
lines.append(table(['模型 S减M', '均值差s', 'P90差s', '长尾贡献差s', '非长尾贡献差s'], rows))
lines += ['共同完成配对只比较同一日期两方法都完成的同一图像，避免各自完成集合不同造成的直接均值偏差；仍排除未完成，不能取代P600/R600，不能推断反事实原因。', '']
rows = []
for scale in RUNS:
    for m, refs in pairs[scale].items():
        for refname, pair in refs.items():
            rows.append([scale, m, refname, pair['common_completed'], fmt(pair['day_equal_mean_delta_s'])])
lines.append(table(['档', '方法', '基线', '共同完成三日合计', '逐日等权配对均值差s'], rows))

lines += ['## 6. 传输量：辅助证据，不是传输阶段耗时', '',
          '主窗字节量按三日等权，GB为十进制；星间量含多跳、重复，不同粒度输入重叠，不得视为有效载荷或分片数。', '']
rows = []
for scale, methods in daily.items():
    for m, days in methods.items():
        rows.append([scale, m] + ([fmt(avg(days, 'resource', k)) for k in ('raw_isl_GB', 'result_isl_GB', 'ground_GB', 'control_GB')]
                                  if days else ['NA'] * 4))
lines.append(table(['档', '方法', '原始数据ISL GB', '结果ISL GB', '下行GB', '控制GB'], rows))

lines += ['## 7. 可复核来源与限制', '',
          '详细逐日计数、直方图、整图模式分组、同图配对、源文件SHA256见同目录MECHANISM_DATA.json。原结果哈希来自各run的final_status.json；读取前后所有输入字节hash保持一致。',
          '- 决策与候选保存边界：analysis/run_scale_validation_20261027.py:84（candidate_counts）、:181（outcomes）、:205（result）、:211（仅保存mode，未保存targets）。',
          '- 候选范围：analysis/local_information_baselines.py:18；Top-K与mask：analysis/main_rl_interface.py:70；地面分片候选与fixed模式：analysis/main_bc_interface.py:23、:70。',
          '- dynamic模式及提交：analysis/dynamic_learning_interface.py:143；未知节点分母：analysis/scale_feature_audit_20261027.py:74。',
          '- 资源分母：analysis/run_dynamic_heuristics_v2.py:108；results_ready语义：sim/env.py:848。',
          '本诊断没有为缺失测试字段重放测试日，也没有用验证日数据填正式表格。现有数据反驳了全局对zero_2_16全面不优的前提，不支持总容量持续饱和、Top-K截断、计算星普遍未知的断言；局部mem_full有直接记录，但不足以确认五阶段时延主瓶颈。未开展容量干预，不能作因果结论。',
          '完成本报告后停止，无后续自动跟进。']

for source in ['analysis/run_scale_validation_20261027.py', 'analysis/local_information_baselines.py',
               'analysis/main_rl_interface.py', 'analysis/main_bc_interface.py', 'analysis/dynamic_learning_interface.py',
               'analysis/scale_feature_audit_20261027.py', 'analysis/run_dynamic_heuristics_v2.py', 'sim/env.py']:
    p = BASE / source
    provenance[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
for path, sha in provenance.items():
    assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == sha, path
OUT.mkdir(exist_ok=True)
(OUT / 'MECHANISM_REPORT.md').write_text('\n\n'.join(line for line in lines if line) + '\n', encoding='utf-8')
(OUT / 'MECHANISM_DATA.json').write_text(json.dumps({'daily': daily, 'paired_common_completed': pairs,
        'input_sha256': provenance, 'no_replay': True, 'inputs_unchanged': True}, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps({'report': str(OUT / 'MECHANISM_REPORT.md'), 'records': 78,
                  'inputs_verified_unchanged': len(provenance), 'status': 'PASS'}, ensure_ascii=False))
