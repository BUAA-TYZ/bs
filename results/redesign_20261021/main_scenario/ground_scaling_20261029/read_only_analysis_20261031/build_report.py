"""只读分析既有记录；仅向本文件所在的新报告目录写入派生产物。"""
from collections import Counter, defaultdict
from pathlib import Path
import hashlib
import json
import math
import statistics

REPO = Path(__file__).resolve().parents[5]
OUT = Path(__file__).resolve().parent
MAIN = REPO / 'results/redesign_20261021/main_scenario'
ROOT = MAIN / 'ground_scaling_20261029'
HASHES = {}


def read(path):
    path = Path(path)
    if not path.is_absolute():
        path = REPO / path
    data = path.read_bytes()
    HASHES[str(path.relative_to(REPO))] = hashlib.sha256(data).hexdigest()
    return json.loads(data)


def verify(path, expected):
    path = Path(path)
    if not path.is_absolute():
        path = REPO / path
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    assert actual == expected, str(path)
    HASHES[str(path.relative_to(REPO))] = actual


def close(x, y):
    assert x is None and y is None or x is not None and y is not None and math.isclose(x, y, abs_tol=1e-8), (x, y)


def metric(rows):
    done = [r['latency_seconds'] for r in rows if r['status'] == 'completed']
    return dict(P60=sum(v <= 60 for v in done)/len(rows), P600=sum(v <= 600 for v in done)/len(rows),
                mean_completed_s=statistics.mean(done) if done else None,
                R600=sum(min(r['latency_seconds'], 600) if r['status'] == 'completed' else 600 for r in rows)/len(rows),
                unfinished=len(rows)-len(done), n=len(rows))


def aggregate(days):
    return {k: sum(d[k] for d in days) if k in ('unfinished', 'n') else
            statistics.mean(d[k] for d in days) if all(d[k] is not None for d in days) else None for k in days[0]}


def fmt(v, digits=2):
    return 'NA' if v is None else f'{v:.{digits}f}'


def table(headers, rows):
    return '\n'.join(['| '+' | '.join(headers)+' |', '| '+' | '.join(['---']*len(headers))+' |',
                      *['| '+' | '.join(map(str, row))+' |' for row in rows]])+'\n'


def code(path, needle):
    source = REPO/path
    data = source.read_bytes()
    HASHES[path] = hashlib.sha256(data).hexdigest()
    expected = audit['provenance'].get(path) or audit['provenance'].get(str(source))
    if expected:
        assert expected == HASHES[path], path
    lines = data.decode().splitlines()
    line = next(i for i, text in enumerate(lines, 1) if needle in text)
    return f'[{path}:{line}]({source}:{line})'


audit = read(ROOT/'report/FINAL_AUDIT.json')
report_hashes = read(ROOT/'report/REPORT_HASHES.json')
assert audit['status'] == 'PASS'
for path, expected in report_hashes.items():
    if Path(path).name == 'FINAL_AUDIT.json':
        verify(path, expected)
old = {'M': read(MAIN/'m_formal_test_20261027/FINAL_AUDIT.json'),
       'L': read(MAIN/'l_formal_test_20261028/independent_audit_20261028/FINAL_AUDIT.json')}
names = {'fixed': 'GTD-GAT-F（三种子均值）', 'dynamic': 'GTD-GAT-D（三种子均值）',
         'zero_2_16': '局部整图复核 2 跳 16 块', 'minload_2_4': '最小负载 2 跳 4 块',
         'local_heft_16': '局部 HEFT（16）', 'local_cgs_16': '局部 CGS（16）',
         'ground_16': '原图下传（16）', 'local_16': '本地处理（16）',
         'global_v3_16': '全局整图复核 F（16）', 'global_v3_dynamic': '全局整图复核 D（4/16）'}
records, values, coverage = {}, {}, []
for scale in 'SML':
    records[scale], values[scale] = {}, {}
    for setting in ('single', 'scaled'):
        records[scale][setting], values[scale][setting] = {}, {}
        paths = audit['record_paths'][scale][setting]
        methods = list(audit['day_equal'][scale][setting])
        if setting == 'single' and scale in old:
            methods += [m for m in names if m in old[scale]['day_equal'] and m not in methods]
        for method in methods:
            days, rs = [], {}
            for date in sorted(paths):
                path = paths[date].get(method)
                origin = audit
                if path is None:
                    root = 'm_formal_test_20261027' if scale == 'M' else 'l_formal_test_20261028'
                    path = str((MAIN/root/'validation'/date/f'{method}.json').relative_to(REPO))
                    origin = old[scale]
                r = read(path)
                verify(path, origin['provenance'][path])
                assert r['status'] == 'PASS' and r['checks']['passed'] and r['method'] == method
                p = read(r['protocol'])
                ids = set(p['scored_task_ids'])
                rs[date] = r
                rows = [o for o in r['outcomes'] if o['task_id'] in ids]
                assert len(rows) == len(ids) and all(o['observed_age_seconds'] >= 600 for o in rows)
                if method not in paths[date]:
                    ref = read(read(paths[date]['zero_2_16'])['protocol'])
                    assert ids == set(ref['scored_task_ids'])
                    assert all(p[k] == ref[k] for k in ('config', 'score_start_s', 'score_end_s', 'date'))
                d = metric(rows)
                close(d['R600'], r['R600'])
                close(d['mean_completed_s'], r['latency']['mean_completed_s'])
                days.append(d)
            v = aggregate(days)
            reference = audit['day_equal'][scale][setting].get(method, old.get(scale, {}).get('day_equal', {}).get(method))
            for key in ('P60', 'P600', 'mean_completed_s', 'R600', 'unfinished'):
                close(v[key], reference[key])
            records[scale][setting][method] = rs
            values[scale][setting][method] = v
        for family in ('fixed', 'dynamic'):
            members = [values[scale][setting][f'{family}_{seed}'] for seed in (1701, 1702, 1703)]
            values[scale][setting][family] = {k: statistics.mean(m[k] for m in members) for k in members[0]}
        for method in names:
            coverage.append(dict(scale=scale, setting=setting, method=method,
                                 available=method in values[scale][setting]))

text = ['# 地面扩站关键对比与教师机制核对（只读）\n',
        '生成日期：2026-10-31。只读取已完成测试及冻结源码，没有训练、仿真、重放、协议修改或实现修复。原结果和论文目录未修改。\n',
        '## 结论\n',
        '1. 以两种模型共同对照完整十二条局部规则（六规则 × 4/16 块）、逐指标取最好值时，M/L 扩站后的 P60 与 R600 优势均缩小；S 仍是同一单站，差值不变。固定模型若改用原登记的六条 16 块范围，结论是 P60 优势缩小、R600 优势扩大，不能笼统宣称全面扩大。\n',
        '2. L 教师变差主要体现为新增未完成：固定、动态教师各有 15 项由完成变未完成、5 项由未完成变完成，净增 10 项。动态教师的 15 项新增未完成中 13 项选择原图下传。双方均完成任务的 R600 贡献反而改善。交付均值仅统计完成项，其下降不能抵消未完成增多。\n',
        '3. 未发现“只读取第一个站”的实现路径；发现报价容量与执行器约束不完全一致：报价按接触边预留，未联合限制站级单通道及卫星跨站互斥。这是继承的报价近似，不是本次已证明的唯一根因。原图公开几何选站后固定目标，执行时不能改投其他站。\n',
        '4. 固定教师的方案选择、所有方法逐任务选站次数及分片末态未保存，不能由字节量或 action_trace_sha256 反推。缺失基线不填零、不用验证日代替测试日；因此无法给出所有所点名基线的完整双设定比较。\n',
        '## 统计口径与记录覆盖\n',
        'S/M 为每日最忙 6 小时，L 为 1 小时，日期均为 2026-12-21 至 23，600 秒暖机与随访。同日期、同轨道任务的场景变体，模型冻结，非新的独立测试日期。S 扩站栏复用原单站记录，不是重复实验。\n',
        'P60/P600 为百分比；交付均值与 R600 为秒。每个方法先按日计算再三日等权；F/D 再对 1701/1702/1703 等权，未混合任务池。未完成是各种子三日合计后取种子均值，因此允许小数；基线为三日合计。R600 为逐任务 min(交付时延,600)，未完成记 600，不等于仅完成项均值。\n',
        '扩站登记 20 方法仅含六模型、十二局部规则及两全局教师。HEFT/CGS/原图下传仅原 M/L 单站有记录，本地处理仅原 M 单站有记录；S 两栏及 M/L 扩站相应项缺失。补入的原单站记录已核对结果 hash、相同配置/评分任务/窗口，并从 outcomes 复算。NA 表示未评估。\n']
for scale in 'SML':
    text += [f'## {scale} 档关键表\n']
    rows = []
    for method, label in names.items():
        for setting in ('single', 'scaled'):
            v = values[scale][setting].get(method)
            cells = [fmt(v['P60']*100), fmt(v['P600']*100), fmt(v['mean_completed_s']), fmt(v['R600']),
                     fmt(v['unfinished'], 2 if method in ('fixed', 'dynamic') else 0)] if v else ['NA']*5
            rows.append([label, '单站' if setting == 'single' else '扩站', *cells])
    text += [table(['方法', '设定', 'P60 %', 'P600 %', '交付均值 s', 'R600 s', '未完成'], rows)]

text += ['## GTD-GAT 减最好局部规则：优势变化\n',
         '主比较统一使用完整十二条局部规则，F/D 均对照同一集合。逐指标分别取三日等权 P60 最大值和 R600 最小值，可能来自不同规则，不构造一个虚拟方法。ΔP60 正值为模型优势；ΔR600 负值为模型优势；变化为“扩站差值减单站差值”。不把只有单站记录的额外基线混入一侧，也不声称这是所有未评估局部方法的最好值。\n']
gaps = []
best_rows = []
for scale in 'SML':
    for setting in ('single', 'scaled'):
        scope = audit['scopes'][scale][setting]['dynamic_twelve_local_4_16']
        close(scope['envelope']['P60'], max(values[scale][setting][m]['P60'] for m in scope['baselines']))
        close(scope['envelope']['R600'], min(values[scale][setting][m]['R600'] for m in scope['baselines']))
        best_rows.append([scale, setting, fmt(scope['envelope']['P60']*100), ', '.join(scope['sources']['P60']),
                          fmt(scope['envelope']['R600']), ', '.join(scope['sources']['R600'])])
    for family in ('fixed', 'dynamic'):
        d = {}
        for setting in ('single', 'scaled'):
            v = values[scale][setting][family]
            e = audit['scopes'][scale][setting]['dynamic_twelve_local_4_16']['envelope']
            d[setting] = dict(P60_pp=(v['P60']-e['P60'])*100, R600_s=v['R600']-e['R600'])
        gaps.append(dict(scale=scale, family=family, **d,
                         change={k:d['scaled'][k]-d['single'][k] for k in d['single']}))
text += [table(['档位', '模型', 'ΔP60 单站 pp', '扩站 pp', '变化 pp', 'ΔR600 单站 s', '扩站 s', '变化 s'],
               [[g['scale'], g['family'], *[fmt(g[z]['P60_pp']) for z in ('single', 'scaled', 'change')],
                 *[fmt(g[z]['R600_s']) for z in ('single', 'scaled', 'change')]] for g in gaps]),
         table(['档位', '设定', '最好 P60 %', 'P60 来源', '最好 R600 s', 'R600 来源'], best_rows),
         'S：F 的 P60 略领先，但 R600 落后；D 两项均落后。S 两栏完全相同，只能称差距保持，不能称扩站带来收益。M/L：F、D 两项仍领先完整十二规则，但领先幅度均缩小。以上是三日期点估计，不宣称统计显著性或所有日期普遍成立。\n',
         '### 原登记固定六条 16 块范围的敏感性\n',
         '此处仅恢复原正式固定组范围，未改变正式成功标准；动态组本来就是十二条，等同上表。固定六条口径不能与十二条口径混用。\n']
registered = []
for scale in 'SML':
    pairs = []
    for setting in ('single', 'scaled'):
        v = values[scale][setting]['fixed']; e = audit['scopes'][scale][setting]['fixed_six_local_16']['envelope']
        pairs.append(((v['P60']-e['P60'])*100, v['R600']-e['R600']))
    registered.append([scale, fmt(pairs[0][0]), fmt(pairs[1][0]), fmt(pairs[1][0]-pairs[0][0]),
                       fmt(pairs[0][1]), fmt(pairs[1][1]), fmt(pairs[1][1]-pairs[0][1])])
text += [table(['档位 F', 'ΔP60 单站', '扩站', '变化 pp', 'ΔR600 单站', '扩站', '变化 s'], registered),
         '该范围下 M/L 固定组表现为速度覆盖优势缩小、R600 优势扩大，是指标间取舍。既有六模型各自均未满足完整四项成功条件，三种子均值不取代逐种子正式验收。\n',
         '### 若“最好局部基线”也包含 HEFT/CGS 等额外方法\n',
         '则完整双设定比较目前不可判定，不能把上面的十二规则结论泛化到这一更大集合。尤其原 M/L 单站的局部 HEFT 的 P60 高于十二规则中最好值，忽略它会夸大“对所有局部方法”的领先。下表仅给单站已观测集合的最好值；扩站缺 HEFT/CGS/原图/本地处理，完整集合最好值及优势变化均 NA。S 单站也缺这些额外方法。\n']
extra_rows = []
for scale in ('M', 'L'):
    v = values[scale]['single']
    candidates = audit['scopes'][scale]['single']['dynamic_twelve_local_4_16']['baselines'] + [
        m for m in ('local_heft_16','local_cgs_16','ground_16','local_16') if m in v]
    best_p = max(v[m]['P60'] for m in candidates)
    best_r = min(v[m]['R600'] for m in candidates)
    for family in ('fixed', 'dynamic'):
        extra_rows.append([scale,family,fmt(best_p*100),fmt((v[family]['P60']-best_p)*100),
                           fmt(best_r),fmt(v[family]['R600']-best_r),'NA'])
text += [table(['档位','模型','单站已观测最好 P60 %','模型差 pp','单站已观测最好 R600 s','模型差 s','扩站完整集合与变化'],extra_rows)]

text += ['## 多站实现核对\n',
         f'- **全部站点进入状态与计划。** export_state 导出每颗卫星全部 next_gs_windows；contact_plan 遍历每个 gid，用各站 bandwidth/8 和该卫星到该站 backlog 建边；目的集合含全部地面站。见 {code("sim/env.py", "\"next_gs_windows\":")}、{code("sim/scheduler/literature.py", "for gid, window in")}、{code("analysis/sar_three_plan.py", "ground = {ground_node")}。这里的计划是既有预测窗口，不是无限未来或真实未来竞争。\n',
         f'- **已确认报价不完全表达容量。** Calendar.free 以接触边索引保存空闲时刻，ContactPlan.reserve 仅扣该边 volume；来自两颗卫星到同一站的边可各自按 400 Mbps 报价。代码没有站级 receive_channels 联合日历，也没有卫星跨站互斥。执行器却按站选 receive_channels 颗卫星，并用 assigned_sats 限制一颗卫星同一步只能使用一个站。见 {code("analysis/sar_three_plan.py", "self.free = {}")}、{code("sim/scheduler/contact_graph.py", "def reserve(")}、{code("sim/network.py", "assigned_sats = set()")}。因此“看见全部站点的名义容量”成立，“完整预测所有站级竞争”不成立。这一近似单站原先已有，多站新增跨站竞争维度；不等于执行器超发，既有资源守恒审计仍成立。\n',
         f'- **原图选站不看动态拥塞。** choose_station 清除队列，仅以公开几何、带宽、输入字节计算完整输入最早到达站；SharedCalendar 给原图动作填 target_gs_id。执行时按任务固定该站，route_unique 继续向 target_gs_id 路由，不因其他站后来更快而改投。见 {code("analysis/scaled_ground_adapter_20261029.py", "def public_state(")}、{code("analysis/scaled_ground_adapter_20261029.py", "def choose_station(")}、{code("analysis/scaled_ground_adapter_20261029.py", "target_gs_id=gid")}。这是已登记的选站规则，不能把教师称为所有多站时序调度的全局最优解。\n',
         f'- **地面算力共享。** 三站配置各显示 compute_rate=4.734848484848484，但适配器只使用一个 FIFO、一个计算池；多个端口是别名，PoolBusy 更新任一地面键时同步全部键。执行完成输入统一入 pool_owner 队列；pool_owner 取第一个站仅作为共享计算池标识，不代表只可在第一站接收。见 {code("analysis/scaled_ground_adapter_20261029.py", "class PoolBusy")}、{code("analysis/scaled_ground_adapter_20261029.py", "ground_tile_queues[env.pool_owner]")}、{code("analysis/scaled_ground_adapter_20261029.py", "self.pool_owner =")}。没有将算力乘以站数。\n',
         f'- **任一站到达。** 结果路由目的集合遍历所有站；任一站完成下传即 tile DONE。整图仍需全部分片 DONE 再按原聚合时延交付，不要求分片在同一站。原图则固定选择一个接收站，全部唯一像素块到齐后进入共享 FIFO。配置 ground_relay_delay_s=0，aggregation_delay_s=0.010993524485323125，serialized_aggregation=True。见 {code("sim/pipeline/distributed.py", "def advance_downlinks(")}、{code("sim/env.py", "if self.tiles[tid].state != TileState.DONE")}。\n',
         f'- **GTD-GAT 不只编码第一站，但也不展开多个站槽。** 局部状态复制全部站窗口，卫星特征窗口延迟取所有站最小值、地面 backlog 求和；原图特征来自多站报价。固定接口仍只有一个 ground_block 槽，由所有站中公开最早可达站代表，传入其到达时延与共享计算池特征；无可用站时才用排序首站填充且动作 mask=False。动态 4/16 两变体均调用该构造。见 {code("analysis/local_information_baselines.py", "\"next_gs_windows\": deepcopy")}、{code("analysis/main_rl_interface.py", "def window_delay(")}、{code("analysis/scaled_ground_adapter_20261029.py", "def build_observation(")}、{code("analysis/dynamic_learning_interface.py", "variants.append(fixed.build_observation")}。actor 没有每站独立队列/全部窗口序列/站数的新特征，这是一槽压缩的局限，不能称完整多站观测。\n',
         f'- **局部报价也遍历全部站。** 局部信息面保留每站公开带宽/通道/接触计划，地面动态负载依旧明确用未知 mask + 零负载先验；adapter.input_options 对分块只保留公开几何选中的站，其他计算星候选保留。见 {code("analysis/local_information_baselines.py", "for gid, full in state.ground_stations.items()")}、{code("analysis/scaled_ground_adapter_20261029.py", "def input_options(")}。不是偷看所有站真实队列，也不是只用北京。\n',
         '上述结论来自冻结版本代码核对；没有构造新场景运行验证，也没有修改近似。不能仅凭静态不一致量化它造成的测试损失。\n']

text += ['## L 教师退化的逐任务证据\n']
diagnostic = {}
for method in ('global_v3_16', 'global_v3_dynamic'):
    sums, transitions, mode_transitions = defaultdict(float), Counter(), Counter()
    choices = {s: Counter() for s in ('single', 'scaled')}
    counts = {s: Counter() for s in choices}
    bad, daily_rows, new_by_mode = [], [], defaultdict(float)
    for date in sorted(records['L']['single'][method]):
        rs = {s: records['L'][s][method][date] for s in choices}
        ids = set(rs['single']['granularity']); assert ids == set(rs['scaled']['granularity'])
        os = {s: {r['task_id']:r for r in rs[s]['outcomes'] if r['task_id'] in ids} for s in choices}
        ds = {s: {r['task_id']:r for r in rs[s].get('decisions', [])} for s in choices}
        if method == 'global_v3_dynamic':
            assert all(set(ds[s]) == ids and len(rs[s]['decisions']) == len(ids) for s in choices)
        for s in choices:
            choices[s].update(r['selected'] for r in ds[s].values())
            counts[s].update(str(g['actual']) for g in rs[s]['granularity'].values())
        for tid in ids:
            x,y = os['single'][tid], os['scaled'][tid]
            state = ('完成' if x['status']=='completed' else '未完成')+'→'+('完成' if y['status']=='completed' else '未完成')
            transitions[state] += 1
            rx = min(x['latency_seconds'],600) if x['status']=='completed' else 600
            ry = min(y['latency_seconds'],600) if y['status']=='completed' else 600
            sums[state] += (ry-rx)/len(ids)/3
            before,after = ds['single'].get(tid,{}),ds['scaled'].get(tid,{})
            mode_transitions[(before.get('selected','NA'), after.get('selected','NA'))] += 1
            if state == '完成→未完成':
                selected = after.get('selected')
                new_by_mode[selected or 'NA'] += (ry-rx)/len(ids)/3
                bad.append(dict(date=date, task_id=tid, single_latency=x['latency_seconds'],
                    before=before.get('selected'), after=selected, quote=after.get('quotes_seconds',{}).get(selected),
                    observed_age=y['observed_age_seconds'], stage=y['stage'], reason=y['failure_reason']))
        for s in choices:
            v=metric(list(os[s].values()))
            daily_rows.append([date,s,fmt(v['P60']*100),fmt(v['P600']*100),fmt(v['mean_completed_s']),fmt(v['R600']),v['unfinished']])
    delta=values['L']['scaled'][method]['R600']-values['L']['single'][method]['R600']
    close(sum(sums.values()),delta)
    diagnostic[method]=dict(transitions=dict(transitions), R600_contributions=dict(sums),
                           choices={k:dict(v) for k,v in choices.items()}, actual_blocks={k:dict(v) for k,v in counts.items()},
                           mode_transitions=[dict(before=k[0],after=k[1],n=v) for k,v in sorted(mode_transitions.items())],
                           new_unfinished=sorted(bad,key=lambda r:(r['date'],int(r['task_id'].split('_')[1]))),
                           new_unfinished_R600_by_selected=dict(new_by_mode))
    text += [f'### {names[method]}\n', table(['日期','设定','P60 %','P600 %','交付均值 s','R600 s','未完成'],daily_rows),
             table(['同任务状态变化','三日任务数','对 ΔR600 的贡献 s'],
                   [[k,transitions[k],fmt(sums[k],4)] for k in ('完成→完成','完成→未完成','未完成→完成','未完成→未完成')]),
             f'贡献之和 = {delta:.6f} s，与三日等权 R600 差严格相符。各日先除各自评分数（123/126/127），再除 3，不将三天直接混池。\n']
    if method.endswith('dynamic'):
        assert sum(choices['single'].values()) == sum(choices['scaled'].values()) == 376
        assert Counter(r['after'] for r in bad) == {'ground': 13, 'cgs_16': 2}
        text += ['方案计数按三日评分任务合计 376 个，比例为计数/376，与主表三日等权百分比不同；此处每任务有一条已保存选择。CGS 方案可含本地/计算星/分块地面，不等同于每片都协同。\n',
                 table(['方案','单站数','单站比例 %','扩站数','扩站比例 %'],
                       [[k,choices['single'][k],fmt(choices['single'][k]/376*100),choices['scaled'][k],fmt(choices['scaled'][k]/376*100)]
                        for k in ('local_4','local_16','cgs_4','cgs_16','ground')]),
                 table(['实际块数（包含原图）','单站数','扩站数'],[[k,counts['single'][k],counts['scaled'][k]] for k in ('4','16')]),
                 table(['选择迁移（计数）','local_4','local_16','cgs_4','cgs_16','ground'],
                       [[x,*[mode_transitions[(x,y)] for y in ('local_4','local_16','cgs_4','cgs_16','ground')]]
                        for x in ('local_4','local_16','cgs_4','cgs_16','ground')]),
                 '原图下传 61→71；协同方案 287→280；本地方案 28→25。新增未完成 15 项中 13 项为原图方案（10 项前后均为原图，3 项由 cgs_16 转原图），其余 2 项仍为 cgs_16。不能把全部退化解释成“新增加的十次原图选择”：多数新增未完成在单站下也曾选择原图。\n',
                 table(['新增未完成：日期/任务','单站方案','扩站方案','单站实测 s','扩站所选报价 s','扩站已观察 s'],
                       [[f'{r["date"]}/{r["task_id"]}',r['before'],r['after'],fmt(r['single_latency']),fmt(r['quote']),fmt(r['observed_age'])]
                        for r in diagnostic[method]['new_unfinished']]),
                 '上表所选报价均为有限值，但实际在至少 600 秒观察期后仍未完成，说明报价并非交付保证。stage 和 failure_reason 均为 null，无法继续区分原图传输未到齐、计算排队、结果回传或其他具体阶段，也不能由 raw_relay_hops 累计值恢复路由。\n']
    else:
        text += ['固定教师原始结果没有 decisions 字段，记录器只为 global_v3_dynamic 保存决策列表；不能还原固定教师本地/协同/原图选择比例。实际粒度两设定均为 72 项 4 块、304 项 16 块（请求 16，较小图几何实际仅 4），分布没有变化。\n']

text += ['## 选站分布与资源证据边界\n',
         '逐图选择的 target_gs_id 没有写入 decisions/outcomes；tile_lifecycle_log 为空，动作只保存摘要 hash，故“按选择次数”的选站分布两种教师均为 NA。以下仅提供已保存的实际下行流量分布，包含原图和结果、含未完成任务的部分传输，不是选站概率或完成任务数。单站所有下行均为北京；多站三个站均有非零流量，排除“实际只使用第一站”的说法。\n']
station_rows, resource_rows = [], []
for method in ('global_v3_16','global_v3_dynamic'):
    for setting in ('single','scaled'):
        rs=list(records['L'][setting][method].values())
        for segment in ('main','whole'):
            bystation=Counter(); utils=defaultdict(list)
            for r in rs:
                v=r['resources'][segment]
                stations=v.get('stations',{'gs_beijing':dict(bytes=v['ground_bytes'],utilization=v['ground_channel_utilization'])})
                for gid,g in stations.items():
                    bystation[gid]+=g['bytes'];utils[gid].append(g['utilization'])
            for gid in sorted(bystation):
                station_rows.append([method,setting,segment,gid,fmt(bystation[gid]/1e9,3),
                                     fmt(bystation[gid]/sum(bystation.values())*100),fmt(statistics.mean(utils[gid])*100)])
        means={k:statistics.mean(r['resources']['main'][k] for r in rs) for k in
               ('helper_utilization','raw_ground_bytes','result_ground_bytes','raw_isl_bytes')}
        pool=statistics.mean(r['resources']['main']['ground_pool_utilization'] for r in rs) if setting=='scaled' else None
        resource_rows.append([method,setting,fmt(means['helper_utilization']*100,3),fmt(pool*100 if pool is not None else None,3),
                              fmt(means['raw_ground_bytes']/1e9,3),fmt(means['result_ground_bytes']/1e9,3),fmt(means['raw_isl_bytes']/1e9,3)])
text += [table(['教师','设定','区间','接收站','三日总 GB','流量占比 %','日等权利用率 %'],station_rows),
         table(['教师','设定','计算星利用率 %','地面池利用率 %','原图下传 GB/日','结果下传 GB/日','原图星间 GB/日'],resource_rows),
         '利用率分母为整个主窗或全程秒数 × 名义容量，包含无可见窗口时间；不能用低均值证明接触窗口不拥塞。地面共享池主窗平均利用率约 8%–9%，不能支持“整段地面算力饱和”；也不能排除短时排队。没有逐窗口竞争日志，不能计算容量不一致造成的具体损失。原图传输量上升、更多原图选择和原图方案新增未完成相伴，支持定位到该类决策/交付链路，但尚不足以在选站锁定、窗口竞争、转发或其他机制之间完成因果归责。\n',
         '## 完整性与限制\n',
         '本报告重新核对使用的原记录 hash，并从已有 outcomes 复算表内指标；这不是重跑原独立审计，也不是新实验。代码证据与原登记 provenance 中的同名 hash 对齐；输入清单在 ANALYSIS_DATA.json。固定教师缺决策日志、逐任务选站缺失和缺少末态均已保留为 NA。\n',
         'M 扩站部分记录来自 Linux x86，单站为 Mac ARM；完整轨迹跨架构逐位一致未证明，现有跨平台预检不等价于该证明。L 单站/扩站均在 Mac，不能直接将 L 教师退化归因于 devbox。三日点估计与既有 bootstrap 均不能校正日期复用、选择偏差或扩大为总体显著结论。本报告没有重新挑模型、调参数或改变任何正式成功口径。\n',
         '只读分析完成，停止；未修复、未重跑。\n']

# 所有输入在产物落盘前再次核对，避免读取期间被其他任务修改。
for path, expected in list(HASHES.items()):
    verify(path, expected)
data=dict(coverage=coverage, values=values, common_twelve_gaps=gaps, teacher_L=diagnostic,
          input_hashes=HASHES, limitations=['No simulation or replay', 'Missing selected-station counts',
          'No fixed-teacher choices', 'No final tile-stage records', 'Incomplete requested extra-baseline coverage'])
(OUT/'ANALYSIS_DATA.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
(OUT/'MECHANISM_REPORT.md').write_text('\n'.join(text))
hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (Path(__file__),OUT/'ANALYSIS_DATA.json',OUT/'MECHANISM_REPORT.md')}
(OUT/'ANALYSIS_HASHES.json').write_text(json.dumps(hashes,indent=2)+'\n')
print(json.dumps(dict(status='PASS',input_files=len(HASHES),coverage_rows=len(coverage),report=str(OUT/'MECHANISM_REPORT.md')),ensure_ascii=False))
