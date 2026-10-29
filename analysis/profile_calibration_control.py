"""千星原实现的状态导出/通告短基准；不执行任务或加速实现。"""

import json
from pathlib import Path
import resource
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from analysis.calibrate_redesign_geometry import configuration
from analysis.redesign_scenario import write_json
from sim.config import config_from_dict
from sim.control import ControlPlane
from sim.env import SimulationEnv


def profile():
    cfg=configuration(1056)
    cfg.update(sim_steps=1000,task_arrival_end_step=1000,scheduled_task_arrivals=[])
    env=SimulationEnv(config_from_dict(cfg))
    try:
        env._get_links(0)
        env.time=50
        started=time.perf_counter()
        state=env.export_state()
        export=time.perf_counter()-started
        plane=ControlPlane(env.task_sources)
        started=time.perf_counter()
        plane.enqueue(state)
        enqueue=time.perf_counter()-started
        started=time.perf_counter()
        state=env.export_state()
        warm=time.perf_counter()-started
        result={"export_cold_s":export,"export_warm_s":warm,"control_enqueue_s":enqueue,
                "messages_at_probe":len(plane.messages),"payload_mb_at_probe":sum(m.size_mb for m in plane.messages),
                "full_link_rows":len(state.links),"peak_rss_bytes":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "warm_export_day_extrapolated_hours":warm*86400/3600,
                "enqueue_every_5s_day_extrapolated_hours":enqueue*(86400/5)/3600,
                "scope":"Empty workload at t=5 seconds, first capture. Concurrent 144-node smoke workers; microbenchmark only, not representative full-day mean."}
        write_json(Path("results/redesign_20261021/calibration/capacity_20261021/control_cost_1056.json"),result)
        print(json.dumps(result),flush=True)
    finally:
        env.close()


if __name__=="__main__":
    profile()
