"""登记前只用验证日做真实轨道/冻结模型接口检查，不产生正式测试成绩。"""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import time
import torch
from analysis import run_ground_scaling_20261029 as run
from analysis import scaled_ground_adapter_20261029 as adapter
from analysis.run_dynamic_heuristics_v2 import config_from_dict, read, sha
from analysis.local_information_baselines import LocalInformationPlane


def main():
    torch.set_num_threads(1)
    root = run.MAIN/'l_reduced_validation_20261028'
    path = read(root/'run_manifest.json')['protocols'][0]
    p = read(path); assert p['date'] == '2026-10-22'
    manifest = read(root/'manifest.json'); outputs = []
    for count in (1,2,3):
        config = deepcopy(p['config'])
        event = deepcopy(config['scheduled_task_arrivals'][0]); event['release_step'] = 0
        config.update(sim_steps=20, task_arrival_end_step=20, scheduled_task_arrivals=[event])
        first = config['ground_stations'][0]
        config['ground_stations'].extend({**first,**g} for g in run.NEW_STATIONS[:count-1])
        t = time.perf_counter()
        with adapter.installed(run.engine):
            for name in run.METHODS:
                env = run.engine.GranularityEnv(config_from_dict(config))
                try:
                    env.step([]); state = env.export_state()
                    assert state.tiles
                    source = event['source_sat_id']
                    plane = LocalInformationPlane(set(config['task_source_sat_ids'])); plane.history_hops = 2
                    local = plane.view(state,source,2)
                    if name in run.MAPPO:
                        actor = run.engine.fixed.HierarchicalActor() if name.startswith('fixed') else run.engine.dynamic.DynamicActor()
                        actor.load_state_dict(torch.load(manifest['models'][name],weights_only=True,map_location='cpu')['state_dict'])
                        actor.eval()
                        if name.startswith('fixed'):
                            policy = run.engine.fixed.CommittedPolicy(actor,int(name.rsplit('_',1)[1])*1000+source)
                        else:
                            policy = run.engine.dynamic.DynamicPolicy(actor,int(name.rsplit('_',1)[1])*1000+source,env,plane)
                        actions = policy.select_actions(local)
                        outputs.append(dict(stations=count,method=name,action_count=len(actions),decisions=len(policy.decisions)))
                    elif name in run.BASELINES:
                        rule_plane, policies = run.engine.policy_set(name,set(config['task_source_sat_ids']),config['relay_max_hops'])
                        actions = policies[source].select_actions(rule_plane.view(state,source,rule_plane.history_hops))
                        outputs.append(dict(stations=count,method=name,action_count=len(actions)))
                    else:
                        policy = run.engine.ThreePlanChoice(config['relay_max_hops']) if name.endswith('_16') else run.engine.DynamicThreePlan(env)
                        actions = policy.select_actions(state)
                        outputs.append(dict(stations=count,method=name,action_count=len(actions)))
                    env.step(actions)
                finally:
                    env.close()
        print('GEOMETRY PASS',count,'stations',round(time.perf_counter()-t,3),'seconds',flush=True)
    run.write_json(Path('results/ground_scaling_preflight_20261029/GEOMETRY.json'), dict(status='PASS',
        at=datetime.now(timezone.utc).isoformat(), source_protocol=path, source_sha256=sha(path),
        note='Validation-day real orbit and first arrival shifted to step zero, 2-second fixture; interface smoke only, not performance evidence',
        models={p:sha(p) for p in manifest['models'].values()}, cases=outputs))


if __name__ == '__main__':main()
