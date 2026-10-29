"""从冻结记录独立复算，不重跑仿真或修改原汇总。"""
from collections import Counter
import gzip
import json
import math
from pathlib import Path

import torch
from analysis.audit_dynamic_heuristics_20261026 import read, sha, close, mean, percentile, DEADLINES
from analysis.run_dynamic_learning import verify_run
from analysis.run_load_pressure_short import verify

ROOT = Path('results/redesign_20261021/main_scenario/dynamic_learning_20261026')
OUT = ROOT/'independent_audit_20261027'


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)+'\n')


def main():
    assert not OUT.exists(), 'Preserve previous audit'
    torch.set_num_threads(1)
    assert sha(ROOT/'manifest.json') == 'bf38e564198ab5e095c65983b718e2d26af13dbc7ca525a50d2cc1ceebd95997'
    amendment = ROOT.parent/'COMPARISON_SCOPE_AMENDMENT_20261026.md'
    assert sha(amendment) == '97e4963ef0d40381be2f58d5c270bf904f1be57c0fb414b2e659a263fb399616'
    m = verify_run(False)
    final = read(ROOT/'final_status.json'); budget = read(ROOT/'final_budget.json')
    assert final['status'] == 'COMPLETE' and final['tests_sealed']
    assert read(ROOT/'BC_AUDIT.json')['status'] == 'PASS'
    audit = read(ROOT/'FINAL_AUDIT.json'); assert audit['status'] == 'PASS'
    for p, h in {**audit['hashes'], **m['teacher_hashes']}.items():
        assert sha(p) == h, p
    for p in m['train']+m['validation']:
        verify(read(p))
    print('HASHES_AND_15_PROTOCOLS_PASS', flush=True)
    best = read(ROOT/'BEST_CHECKPOINTS.json'); curves = read(ROOT/'full_curves.json')
    daily = {}; aggregates = {}; selected = {}; lineage = {}; tail = {}
    for seed, state in final['states'].items():
        previous = ROOT/'training'/f'pure_imitation_{seed}.pt'
        bc = torch.load(previous, weights_only=True, map_location='cpu')
        fixed = ROOT.parent/'fixed_mappo_frozen_20261025'/f'mappo_{seed}.pt'
        assert bc['parent_sha256'] == sha(fixed) and bc['episodes'] == 0
        assert bc['hyperparameters']['epochs'] == 100 and bc['training'] == 'pure_imitation'
        seen = []; checkpoint_count = 0
        for cp in sorted((ROOT/'checkpoints'/seed).glob('*.pt')):
            data = torch.load(cp, weights_only=True, map_location='cpu')
            assert data['parent_sha256'] == sha(previous), cp
            assert data['fixed_parent_sha256'] == sha(fixed)
            assert data['teacher_hashes'] == m['teacher_hashes']
            assert not data['validation_used_for_gradient']
            assert set(data['train_dates']) == {read(p)['date'] for p in m['train']}
            ep = data['episodes']
            if ep:
                u = read(ROOT/'updates'/seed/f'{ep:04d}.json')
                assert u['status'] == 'PASS' and u['checkpoint_sha256'] == sha(cp)
                count = 0
                for path, h in u['rollout_hashes'].items():
                    assert sha(path) == h
                    with gzip.open(path, 'rt') as stream: r = json.load(stream)
                    j = r['job']; n = j['episode']
                    assert j['kind'] == 'train' and j['seed'] == int(seed)
                    assert j['protocol'] == m['schedule'][seed][n-1] and j['protocol'] in m['train']
                    assert j['checkpoint_sha256'] == sha(previous)
                    assert r['status'] == 'PASS' and r['checks']['passed']
                    assert r['credit_audit']['before_first_decision_events'] == 0
                    seen.append(n); count += len(r['records'])
                assert count == u['samples']
            previous = cp; checkpoint_count += 1
        assert sorted(seen) == list(range(1,177)) and state['episodes'] == 176
        assert budget[seed]['updated_episodes'] == budget[seed]['collected_pass'] == 176
        close(budget[seed]['fraction_of_original_703'], 176/703)
        lineage[seed] = dict(checkpoints=checkpoint_count, updated_episodes=176, collected_pass=176,
                            fixed_parent_sha256=sha(fixed), pure_imitation_sha256=sha(ROOT/'training'/f'pure_imitation_{seed}.pt'))
        streak = 0; candidates = []
        for evaluation in state['evaluations']:
            ep = evaluation['episodes']; key = f'{seed}_{ep}'; daily[key] = []; sets = []
            assert sha(evaluation['checkpoint']) == evaluation['checkpoint_sha256']
            for path, h in evaluation['validation_hashes'].items():
                assert sha(path) == h
                r = read(path); p = read(r['job']['protocol']); ids = set(p['scored_task_ids'])
                assert r['status'] == 'PASS' and r['checks']['passed'] and r['job']['protocol'] in m['validation']
                rows = [o for o in r['outcomes'] if o['task_id'] in ids]
                assert len(rows) == len(ids) and {o['task_id'] for o in rows} == ids
                assert p['sim_duration_s']-p['score_end_s'] >= 600
                assert all(p['score_start_s'] <= o['release_seconds'] < p['score_end_s'] and o['observed_age_seconds'] >= 600 for o in rows)
                done = [o['latency_seconds'] for o in rows if o['status'] == 'completed']
                ps = [sum(o['status'] == 'completed' and o['latency_seconds'] <= d for o in rows)/len(rows) for d in DEADLINES]
                for d, v, old in zip(DEADLINES, ps, r['curve'], strict=True):
                    close(v, old['P_lower']); assert old['D_seconds'] == d and old['unknown_count'] == 0
                    assert set(old['on_time_ids']) == {o['task_id'] for o in rows if o['status'] == 'completed' and o['latency_seconds'] <= d}
                a = dict(P=ps, R600=mean([min(o['latency_seconds'],600) if o['status'] == 'completed' else 600 for o in rows]),
                    mean_completed_s=mean(done), P50_completed_s=percentile(done,.5), P90_completed_s=percentile(done,.9),
                    unfinished_total=len(rows)-len(done), kl_initial=mean([x['kl_initial'] for x in r['policy_metrics']]),
                    entropy=mean([x['entropy'] for x in r['policy_metrics']]))
                for k in ('mean_completed_s','P50_completed_s','P90_completed_s'): close(a[k], r['latency'][k])
                close(a['R600'], r['R600']); assert a['unfinished_total'] == r['latency']['unfinished']
                decisions = r['decisions']; requested = Counter(); actual = Counter()
                assert len({x['task_id'] for x in decisions}) == len(decisions)
                assert {x['task_id'] for x in decisions} | set(r['unacted_scored_ids']) == ids
                assert {x['task_id'] for x in r['policy_metrics']} == {x['task_id'] for x in decisions}
                for row in decisions:
                    mode = row['mode']; obs = row['observation']; assert obs['image_mask'][mode]
                    requested[('onboard_4','onboard_16','raw_ground')[mode]] += 1
                    assert 'critic_observation' not in row
                    if mode < 2:
                        variant = obs['variants'][mode]
                        assert len(row['positions']) == variant['block_count'] == len(row['targets'])
                        assert all(variant['position_mask'][i] for i in row['positions'])
                        actual[str(variant['block_count'])] += 1
                    else: assert not row['positions'] and not row['targets']
                assert dict(requested) == r['choices'] and dict(actual) == r['actual_blocks']
                cfg = p['config']; helpers = set(range(cfg['num_sats']))-set(cfg['task_source_sat_ids'])
                capacity = sum(g['bandwidth_mbps']/8*g.get('receive_channels',1) for g in cfg['ground_stations'])*1e6
                for z in r['resources'].values():
                    busy = math.fsum(v for sid,v in z['helper_busy_by_node'].items() if int(sid) in helpers)
                    close(busy,z['helper_busy_s']); close(busy/(len(helpers)*z['duration_s']),z['helper_utilization'])
                    close(z.get('raw_ground_bytes',0)+z.get('result_ground_bytes',0),z['ground_bytes'])
                    close(z['ground_bytes']/(capacity*z['duration_s']),z['ground_channel_utilization'])
                    close(math.fsum(z.get(k,0) for k in ('control_bytes','raw_isl_bytes','result_isl_bytes','raw_ground_bytes','result_ground_bytes')),z['total_transmitted_bytes'])
                for k in ('duration_s','total_transmitted_bytes','control_bytes','helper_busy_s','ground_bytes'):
                    close(sum(r['resources'][s][k] for s in ('warmup','main','followup')),r['resources']['whole'][k])
                z = r['resources']['main']
                a.update(date=p['date'], n=len(rows), total_bytes=z['total_transmitted_bytes'],
                    helper_utilization=z['helper_utilization'], ground_utilization=z['ground_channel_utilization'],
                    requested=dict(requested), actual=dict(actual))
                daily[key].append(a); sets.append(rows)
                tail[f'{key}_{p["date"]}'] = [dict(task_id=o['task_id'],status=o['status'],latency_seconds=o['latency_seconds'])
                    for o in rows if o['status'] != 'completed' or o['latency_seconds'] > 300]
            a = daily[key]
            agg = {k:mean([v[k] for v in a]) for k in ('R600','mean_completed_s','P50_completed_s','P90_completed_s','kl_initial','entropy','total_bytes','helper_utilization','ground_utilization')}
            agg.update(P=[mean([v['P'][i] for v in a]) for i in range(7)], unfinished_total=sum(v['unfinished_total'] for v in a))
            for k,v in evaluation['metrics'].items():
                if k == 'P':
                    for x,y in zip(agg[k],v,strict=True): close(x,y)
                else: close(agg[k],v)
            for d,y in zip(curves[key]['D'],curves[key]['P'],strict=True):
                close(y,mean([sum(o['status']=='completed' and o['latency_seconds']<=d for o in rows)/len(rows) for rows in sets]))
            aggregates[key] = agg; candidates.append((ep,agg))
            if ep: streak = streak+1 if agg['R600'] > candidates[0][1]['R600']+1e-9 else 0
        init = candidates[0][1]
        chosen = min(candidates, key=lambda x:(x[1]['P'][-1] < init['P'][-1]-1e-12,x[1]['R600'],-x[1]['P'][0],x[1]['mean_completed_s'],x[0]))
        assert chosen[0] == best[seed]['episodes'] and streak == state['bad_streak'] == 0 and not state['stopped']
        selected[seed] = dict(episodes=chosen[0],checkpoint=best[seed]['checkpoint'],sha256=best[seed]['checkpoint_sha256'],metrics=chosen[1],
            delta_initial=dict(P=[x-y for x,y in zip(chosen[1]['P'],init['P'])],R600=chosen[1]['R600']-init['R600'],
                               mean_completed_s=chosen[1]['mean_completed_s']-init['mean_completed_s'],unfinished=chosen[1]['unfinished_total']-init['unfinished_total']))
        print('SEED_PASS',seed,flush=True)
    references = read(ROOT/'summary.json')['references']
    result = dict(status='PASS',manifest_sha256=sha(ROOT/'manifest.json'),comparison_sha256=sha(amendment),
        daily=daily,day_equal=aggregates,selected=selected,lineage=lineage,final_budget=budget,references=references,
        dynamic_twelve_local_4_16='PENDING_COMPLETE_BASELINE_PANEL',tests_sealed=True,
        resource_audit_limit='Saved passive counter conservation, not independent packet replay',
        policy_metric_audit_limit='Daily equal aggregation of recorded exact joint KL and entropy, no resampling',
        orin_four_block_peak_memory_verified=False)
    OUT.mkdir(); dump(OUT/'AUDIT.json',result); dump(OUT/'TAIL_RECORDS.json',tail)
    lines=['# 动态MAPPO完整阶段独立审计','',
        '三种子各完成176个更新片段（新增预算100%，原703预算的25.04%），采样各176；固定父阶段352/703另列，不混入动态预算。27项验证、528项训练采样、15协议、冻结输入/模型父链/教师来源/600随访及选模核验通过。正式测试继续SEALED。','',
        '## 完整验证曲线','',
        '|种子_片段|P60/90/120/180/240/300/600 (%)|均值/P50/P90 s|R600 s|未完成|KL/熵|传输GB|计算星/地面利用率|',
        '|---|---|---|---:|---:|---|---:|---|']
    for key,a in aggregates.items():
        lines.append(f'|{key}|'+ '/'.join(f'{x*100:.3f}' for x in a['P'])+f"|{a['mean_completed_s']:.3f}/{a['P50_completed_s']:.3f}/{a['P90_completed_s']:.3f}|{a['R600']:.3f}|{a['unfinished_total']}|{a['kl_initial']:.4f}/{a['entropy']:.4f}|{a['total_bytes']/1e9:.3f}|{a['helper_utilization']:.3%}/{a['ground_utilization']:.3%}|")
    lines += ['', '## 冻结选择与初始化变化','', '|种子|选中片段|P60变化 pp|P600变化 pp|均值变化 s|R600变化 s|未完成变化|','|---|---:|---:|---:|---:|---:|---:|']
    for seed,s in selected.items():
        d=s['delta_initial']; lines.append(f"|{seed}|{s['episodes']}|{d['P'][0]*100:+.3f}|{d['P'][-1]*100:+.3f}|{d['mean_completed_s']:+.3f}|{d['R600']:+.3f}|{d['unfinished']:+d}|")
    lines += ['', '均按自身动态BC初始化的P600门槛，再R600、P60、均值、最早checkpoint选模。三种子未触发连续两次退化停止。1701/1702的176点比88点R600更差，保留88点；1703选176。选择完全遵循预登记，没有根据测试调整。',
        '', '## 成功标准与参考','',
        '动态正式口径为dynamic_twelve_local_4_16，12项基线尚待完整补齐，P60、均值、P600、R600四项完整包络判定均待补齐。原REPORT.md及BEST_CHECKPOINTS.json中的success只对应旧六项16块参考，不是新口径结论。已完成minload_2@4的验证P600约98.82%，三个选中动态模型均低于它，因此已知P600必要条件不满足；不能宣称四项全部成功。正式测试阈值必须以测试基线重新计算。',
        '', '固定模型单列fixed_six_local_16口径。动态启发式v3及固定各种子的参考指标保存在AUDIT.json references；它们不参与当前动态选模。所选动态三种子P600约95.26%至97.92%，存在种子差异，不能只报告1703。',
        '', '## 尾部与局限','',
        'TAIL_RECORDS.json逐图列出全部验证点超过300秒或未完成的记录；不把这些观察归因于孤立因果。完整60至600秒阶梯曲线见原full_curves.json及curves.png。均值和分位数按每个验证日已交付图计算后日等权；未完成数为三日合计。',
        '', '资源统计为主窗口全部传输（含重叠冗余、多跳、控制及结果），按三日等权；从保存的被动计数器独立核对分段/分类守恒和忙时，未做逐包物理重放。KL和熵为已记录联合分布量的日等权复算。请求/实际粒度按日计数见AUDIT.json，原图地面单列，小图请求16可实际只有4块。',
        '', 'Orin上4块峰值内存尚未实测；仿真执行测试不代表实机内存可行。']
    (OUT/'REPORT.md').write_text('\n'.join(lines)+'\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,3,figsize=(15,4.6),layout='constrained')
    for ax,seed in zip(axes,selected):
        for ep in (0,88,176):
            c=curves[f'{seed}_{ep}']; ax.step(c['D'],[v*100 for v in c['P']],where='post',label=f'episode {ep}')
        ax.set(title=f'Seed {seed}',xlabel='Deadline (s)',ylabel='On-time (%)',xlim=(60,600),ylim=(50,100)); ax.grid(alpha=.2); ax.legend()
    fig.savefig(OUT/'curves.png',dpi=150); plt.close(fig)
    dump(OUT/'HASHES.json',{str(p):sha(p) for p in [Path(__file__),*OUT.iterdir()]})
    print(json.dumps(dict(status='PASS',selected={s:v['episodes'] for s,v in selected.items()},output=str(OUT))))


if __name__ == '__main__': main()
