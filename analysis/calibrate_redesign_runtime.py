"""只测原执行器开销，不运行待比较的调度方法或真实图像面板。"""

import argparse
import json
from pathlib import Path
import resource
import sys
import time

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.calibrate_redesign_geometry import ROOT, SHAPES, configuration
from analysis.redesign_scenario import write_json
from sim.config import config_from_dict
from sim.env import SimulationEnv
from sim.routing import delivery_route


def run(count):
    cfg = configuration(count)
    # 空负载用于测框架成本，不把它冒充真实任务执行时间。
    cfg["scheduled_task_arrivals"] = []
    cfg["task_arrival_rate"] = 0
    start = time.perf_counter()
    env = SimulationEnv(config_from_dict(cfg))
    initialized = time.perf_counter()-start
    try:
        env.step([])
        timings = []
        for _ in range(100):
            start = time.perf_counter()
            env.step([])
            timings.append(time.perf_counter()-start)
        links = env._get_links(env.time)
        # 先更新缓存，路由计时排除首次接触预测成本（另有cold/warm测量）。
        env.contact_cache.windows_at(env.time*env.cfg.dt)
        samples = []
        for source in (0, 8, 16):
            for size in (.01, 50., 800.):
                env._temporal_route_waits = {}
                start = time.perf_counter()
                route = delivery_route(env, source, size, links, [source])
                samples.append({"source":source, "size_mb":size,
                                "seconds":time.perf_counter()-start, "route":route})
        row = {"compute_count":count, "init_with_visibility_s":initialized,
               "empty_step_median_s":float(np.median(timings)),
               "empty_day_extrapolated_s":float(np.median(timings))*864000,
               "route_samples":samples, "route_median_s":float(np.median([r["seconds"] for r in samples])),
               "peak_rss_bytes":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
               "scope":"Original executor, empty workload, warm routing at one epoch. Microbenchmark, not full-day wall-time measurement; route calls and queues vary with workload."}
        write_json(ROOT/f"runtime_{count}.json", row)
        print(json.dumps(row), flush=True)
    finally:
        env.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--count", type=int, choices=SHAPES, required=True)
    run(p.parse_args().count)
