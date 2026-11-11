"""只在验证日检查新增方法接口；正式日期只校验冻结输入。"""
from copy import deepcopy
from pathlib import Path
import subprocess
import sys
import torch
from analysis import run_ground_representatives_20261031 as r
from analysis.run_dynamic_heuristics_v2 import config_from_dict


def main():
    torch.set_num_threads(1)
    command=[sys.executable,'-B','-m','unittest','discover','-s','tests','-p','test_ground_representatives_20261031.py']
    test=subprocess.run(command,capture_output=True,text=True)
    assert test.returncode==0,test.stdout+test.stderr
    source=r.read(r.original.MAIN/'l_reduced_validation_20261028/run_manifest.json')['protocols'][0]
    p=r.read(source);assert p['date']=='2026-10-22'
    cases=[]
    for count in (1,2,3):
        for method in r.METHODS:
            config=deepcopy(p['config']);event=deepcopy(config['scheduled_task_arrivals'][0]);event['release_step']=0
            config.update(sim_steps=20,task_arrival_end_step=20,scheduled_task_arrivals=[event])
            first=config['ground_stations'][0]
            config['ground_stations'].extend({**first,**g} for g in r.original.NEW_STATIONS[:count-1])
            cfg=config_from_dict(r.original.configuration(dict(config=config),method))
            with r.installed(method,count>1) as engine:
                env=engine.GranularityEnv(cfg)
                try:
                    env.step([]);state=env.export_state();source_sat=event['source_sat_id']
                    if method in engine.BASELINES or method in engine.EXTRAS:
                        plane,policies=engine.policy_set(method,set(config['task_source_sat_ids']),cfg.relay_max_hops)
                        actions=policies[source_sat].select_actions(plane.view(state,source_sat,plane.history_hops))
                    else:actions=engine.ThreePlanChoice(cfg.relay_max_hops).select_actions(state)
                    env.step(actions)
                    cases.append(dict(stations=count,method=method,actions=len(actions)))
                finally:env.close()
    r.write(r.ROOT/'PREFLIGHT.json',dict(status='PASS',at=r.queue.now(),cases=cases,
        validation_protocol=source,validation_sha256=r.sha(source),test_command=command,test_output=test.stdout+test.stderr,
        boundary='18 validation-day geometry interfaces; 24 toy full 600-second followups; six single-station identity comparisons; no formal scores'))
    print('PREFLIGHT PASS',len(cases))


if __name__=='__main__':main()
