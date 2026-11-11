"""跨机适配检查只用验证日几何与玩具任务，不运行正式测试。"""
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import asdict
import importlib.metadata
import json
from pathlib import Path
import platform
import sys
import tempfile
import time
from unittest.mock import patch

import numpy as np
import torch
from analysis import run_ground_scaling_20261029 as r
from analysis import scaled_ground_adapter_20261029 as a
from analysis.run_dynamic_heuristics_v2 import config_from_dict
from analysis.local_information_baselines import LocalInformationPlane

DIST = r.ROOT / 'distributed_20261030'


def environment_info():
    return dict(host=platform.node(), system=platform.platform(), python=sys.version,
                packages={n:importlib.metadata.version(n) for n in
                          ('torch','numpy','networkx','skyfield','sgp4','PyYAML','tqdm','matplotlib')})


def jsonable(value):
    if hasattr(value, 'item'):
        return value.item()
    raise TypeError(type(value).__name__)


def tensor_obs(obs):
    if 'variants' in obs:
        return dict(variants=[tensor_obs(v) for v in obs['variants']], image_mask=obs['image_mask'])
    result = {k:obs[k] for k in ('node_ids','node_valid','position_mask','adjacency','image_mask','tile_ids','block_count')}
    for k in ('nodes','image'):
        result[k] = np.asarray(obs[k], dtype=np.float32).tolist()
    return result


def main(host):
    torch.set_num_threads(1)
    start = time.perf_counter()
    manifest = r.verify_manifest()
    if (DIST/'BUNDLE_HASHES.json').exists():
        for p, h in r.read(DIST/'BUNDLE_HASHES.json').items():
            assert r.sha(p) == h, p
    output = DIST/f'preflight_{host}.json'
    assert not output.exists(), output
    validation = Path('results/redesign_20261021/main_scenario/bc_stage2_20261022/data/2026-10-22/six_hours/protocol.json')
    p = r.read(validation)
    assert p['date'] == '2026-10-22'
    q = r.derive_protocol(p, 'M', True)
    event = deepcopy(q['config']['scheduled_task_arrivals'][0]); event['release_step'] = 0
    q['config'].update(sim_steps=20, task_arrival_end_step=20, scheduled_task_arrivals=[event])
    geometry = []
    with a.installed(r.engine):
        for name in r.METHODS:
            env = r.engine.GranularityEnv(config_from_dict(r.configuration(q, name)))
            try:
                env.step([]); state = env.export_state()
                source = event['source_sat_id']
                plane = LocalInformationPlane(set(env.cfg.task_source_sat_ids)); plane.history_hops = 2
                local = plane.view(state, source, 2)
                entry = dict(method=name)
                if name in r.MAPPO:
                    fixed = name.startswith('fixed')
                    actor = (r.engine.fixed.HierarchicalActor() if fixed else r.engine.dynamic.DynamicActor()).eval()
                    actor.load_state_dict(torch.load(manifest['models'][name], weights_only=True, map_location='cpu')['state_dict'])
                    seed = int(name.rsplit('_',1)[1])*1000+source
                    policy = (r.engine.fixed.CommittedPolicy(actor,seed) if fixed else
                              r.engine.dynamic.DynamicPolicy(actor,seed,env,plane))
                    actions = policy.select_actions(local)
                    entry['decisions'] = {}
                    for task, choice in policy.decisions.items():
                        obs = choice['observation']
                        with torch.no_grad():
                            im, pos = actor.logits(obs)
                        positions = [pos] if fixed else pos
                        entry['decisions'][task] = dict(observation=tensor_obs(obs), mode=choice['mode'],
                            positions=choice['positions'], image_prob=im.softmax(-1).tolist(),
                            position_prob=[torch.nan_to_num(x.softmax(-1)).tolist() for x in positions])
                elif name in r.BASELINES:
                    plane, policies = r.engine.policy_set(name,set(env.cfg.task_source_sat_ids),env.cfg.relay_max_hops)
                    actions = policies[source].select_actions(plane.view(state,source,plane.history_hops))
                else:
                    policy = r.engine.ThreePlanChoice(env.cfg.relay_max_hops) if name.endswith('_16') else r.engine.DynamicThreePlan(env)
                    actions = policy.select_actions(state)
                entry['actions'] = [asdict(x) for x in actions]
                env.step(actions); entry['outcomes'] = env.summary()['task_outcomes']
                geometry.append(entry)
            finally:
                env.close()
    sys.path.insert(0, 'tests')
    from test_ground_scaling_20261029 import config, FakeTopology, FakeVisibility
    toy = {}
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp); cfg = asdict(config(2))
        cfg.update(sim_steps=710, decision_tile_limit_per_sat=64)
        cfg['scheduled_task_arrivals'][0]['release_step'] = 20
        proto = dict(date='2026-10-22', config=cfg, scored_task_ids=['task_0'], score_start_s=10,
                     score_end_s=110, sim_duration_s=710)
        ppath = dest/'protocol.json'; r.write_json(ppath,proto)
        r.write_json(dest/'run_manifest.json',dict(hashes={}))
        with ExitStack() as stack:
            for key,value in dict(ROOT=dest,VALID=('2026-10-22',),METHODS=r.METHODS,LEARNED=r.MAPPO,
                                  verify_manifest=lambda:manifest,verify=lambda p:None,check_time=lambda:None).items():
                stack.enter_context(patch.object(r.engine,key,value))
            stack.enter_context(a.installed(r.engine)); factory = r.engine.GranularityEnv
            def make(cfg):
                with patch('sim.env.TopologyModel',FakeTopology),patch('sim.env.VisibilityCache',FakeVisibility):
                    env = factory(cfg)
                env.vis_cache.windows_at=lambda sid,t:{gid:(0,1000) for gid in env.ground_stations}
                return env
            stack.enter_context(patch.object(r.engine,'GranularityEnv',side_effect=make))
            for name in r.METHODS:
                short = r.engine.simulate((str(ppath),name)); assert short['status']=='PASS',short
                row = r.read(short['path'])
                toy[name] = {k:row.get(k) for k in ('outcomes','curve','latency','R600','resources','granularity',
                                                  'action_trace_sha256','choices','feature_audit')}
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(dict(status='PASS',environment=environment_info(),geometry=geometry,toy=toy,
        validation_sha256=r.sha(validation),manifest_sha256=r.sha(r.ROOT/'manifest.json'),
        source_sha256=r.sha(__file__),wall_seconds=time.perf_counter()-start),default=jsonable,indent=2)+'\n')
    print(output,flush=True)


if __name__ == '__main__':
    main(sys.argv[1])
