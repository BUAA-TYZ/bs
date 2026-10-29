"""用户批准的固定粒度提前冻结；不启动仿真。"""
import json
import shutil
from pathlib import Path

from analysis import resume_main_mappo as resume


def main():
    base = resume.base
    root = base.ROOT
    target = base.MAIN / 'fixed_mappo_frozen_20261025'
    if target.exists():
        raise FileExistsError(target)
    resume.verify_resume()
    states = base.read(root / 'progress.json')['states']
    selected = {}
    budget = {}
    for seed, state in states.items():
        candidates = [e for e in state['evaluations'] if e['episodes'] in (176, 352)]
        assert len(candidates) == 2
        choice = min(candidates, key=lambda e: base.selection_key(e, state['initial']))
        assert base.sha(choice['checkpoint']) == choice['checkpoint_sha256']
        updates = sorted((resume.RESUME / 'updates' / seed).glob('*.json'))
        assert int(updates[-1].stem) == state['episodes']
        selected[seed] = choice
        budget[seed] = dict(updated_episodes=state['episodes'], updated_fraction=state['episodes'] / 703,
                            selected_episodes=choice['episodes'], selected_fraction=choice['episodes'] / 703,
                            collected_pass=sum(base.read(p)['status'] == 'PASS' for p in
                                (root / 'rollouts' / seed).glob('*.progress.json')))
    target.mkdir()
    for seed, choice in selected.items():
        for kind, source in [('mappo', Path(choice['checkpoint'])),
                             ('pure_imitation', base.BC / 'training' / f'pure_imitation_{seed}.pt')]:
            dest = target / f'{kind}_{seed}.pt'
            shutil.copyfile(source, dest)
            assert base.sha(source) == base.sha(dest)
    reason = 'User requested early stop: mixed tradeoffs between 176 and 352; redirect compute to dynamic granularity'
    resume.finalize(reason)
    assert base.read(resume.RESUME / 'final_budget.json')['budget'] == {
        s: {k: v for k, v in b.items() if k in ('updated_episodes', 'updated_fraction', 'collected_pass')}
        for s, b in budget.items()}
    protocol = base.MAIN / 'DYNAMIC_REVISION_PROTOCOL_20261025.md'
    manifest = dict(status='FROZEN', reason=reason, tests_sealed=True, selected=selected, budget=budget,
        protocol=str(protocol), protocol_sha256=base.sha(protocol), entry_sha256=base.sha(__file__),
        frozen_files={str(p): base.sha(p) for p in target.glob('*.pt')})
    resume.atomic_json(target / 'manifest.json', manifest)
    lines = ['# 固定粒度模型提前冻结', '', '用户主动停止，176至352指标互有取舍；不宣称统计收敛。测试继续封存。', '',
             '|种子|实际更新/703|选中点|选中点预算比例|完成采样|', '|---|---|---|---|---|']
    for seed, b in budget.items():
        lines.append(f"|{seed}|{b['updated_episodes']}/703 ({b['updated_fraction']:.4%})|{b['selected_episodes']}|{b['selected_fraction']:.4%}|{b['collected_pass']}|")
    lines.extend(['', '仅176/352完整验证点按原规则选模，三个种子纯模仿checkpoint单独复制保存。',
                  '未完成的后续采样保留，不计已训练预算；监督服务已卸载，自动恢复不再启动固定模型。',
                  '原始结果与完整曲线见 ../mappo_20261022/REPORT.md。',
                  '冻结文件SHA256及选模指标见 manifest.json。'])
    (target / 'REPORT.md').write_text('\n'.join(lines) + '\n')
    print(json.dumps(dict(root=str(target), manifest_sha256=base.sha(target / 'manifest.json'), budget=budget)))


if __name__ == '__main__':
    main()
