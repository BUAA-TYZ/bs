"""重放冻结候选的报价和选择；可对指定任务作CGS选择干预，不改公共路由。"""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis import run_sar_frozen as runner
from analysis.report_sar_task_choice import compare
from sim.reproducibility import runtime_metadata
from sim.scheduler.literature import CGSComputeAdapted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--force-cgs', nargs='*', default=[])
    parser.add_argument('--generator', choices=('guarded', 'baseline'), default='guarded')
    args = parser.parse_args()
    if args.out.exists():
        raise ValueError('Use a new diagnostic directory')
    root = Path('results/sar_condition_scan_20261017')
    cfg = json.loads((root / 'configs/compute_10_loopfree.json').read_text())
    reference = json.loads((root / 'records/compute_10_choice.json').read_text())
    meta = runtime_metadata()
    assert meta['source_sha256'] == reference['metrics']['experiment']['source_sha256']
    original = runner.simulator.make_policy
    decisions = []

    def factory(*a, **kw):
        policy = original(*a, **kw)
        select = policy.select_actions
        generate = policy._select_actions_with_plan
        generated = []

        def capture_plan(state, plan):
            actions = generate(state, plan)
            generated[:] = actions
            return actions

        policy._select_actions_with_plan = capture_plan

        def traced(state):
            before = getattr(policy, 'last_prediction', None)
            actions = select(state)
            prediction = getattr(policy, 'last_prediction', None)
            if prediction is before or not prediction or 'quotes' not in prediction:
                return actions
            task_ids = sorted({state.tiles[a.tile_id]['parent_task_id'] for a in actions})
            forced = set(task_ids) & set(args.force_cgs)
            baseline_actions = None
            baseline_quote = None
            if forced and args.generator == 'baseline':
                baseline_actions = CGSComputeAdapted().select_actions(state)
                baseline_quote = policy.quote(state, baseline_actions)
            if forced:
                replacements = {a.tile_id: a for a in (baseline_actions if baseline_actions is not None else generated)}
                actions = [replacements[a.tile_id] if state.tiles[a.tile_id]['parent_task_id'] in forced else a
                           for a in actions]
            assignments = {}
            for action in actions:
                tile = state.tiles[action.tile_id]
                assignments.setdefault(tile['parent_task_id'], []).append({
                    'tile_id': action.tile_id, 'action': action.action_type.value,
                    'target_sat_id': action.target_sat_id, 'target_gs_id': action.target_gs_id,
                    'location': tile['location'], 'compute_cost': tile['compute_cost']})
            decisions.append({'time_seconds': state.time * state.config['dt'],
                              'prediction': deepcopy(prediction), 'forced_cgs_tasks': sorted(forced),
                              'baseline_quote': baseline_quote,
                              'assignments': assignments})
            return actions

        policy.select_actions = traced
        return policy

    runner.simulator.make_policy = factory
    try:
        record = runner.run_one(cfg, 'cgs_task_choice', meta['source_sha256'],
                                runner.sha256_bytes(Path(runner.__file__).read_bytes()), 6)
    finally:
        runner.simulator.make_policy = original
    same = reference['metrics']['task_outcomes'] == record['metrics']['task_outcomes']
    if not args.force_cgs and not same:
        raise AssertionError('Read-only diagnostic changed the replay')
    record.update(decisions=decisions, force_cgs_tasks=args.force_cgs, outcomes_identical_to_reference=same,
                  generator=args.generator,
                  paired=compare(reference, record), config_sha256=runner.digest(cfg),
                  diagnostic_sha256=runner.sha256_bytes(Path(__file__).read_bytes()))
    args.out.mkdir(parents=True)
    (args.out / 'record.json').write_text(json.dumps(record, indent=2, allow_nan=False)+'\n')
    print(json.dumps({k:record[k] for k in ('P300','R300_seconds','wall_seconds','outcomes_identical_to_reference','paired')}), flush=True)


if __name__ == '__main__':
    main()
