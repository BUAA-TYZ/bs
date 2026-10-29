"""启动已登记的全量学习流水线与防休眠；不重跑已有失败记录。"""

import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from analysis.sar_selector_learning import OUT, audit_sources, read, write, now


def launch():
    audit_sources()
    assert (OUT / "manifest.json").exists()
    assert not (OUT / "launch.json").exists(), "Already launched; inspect existing processes before any continuation"
    assert not (OUT / "final_status.json").exists(), "Terminal experiment must not be restarted automatically"
    assert read(ROOT / "results/sar_selector_learning_smoke_20261019.json")["status"] == "PASS"
    env = os.environ.copy()
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS"):
        env[name] = "1"
    log_path = OUT / "panel.log"
    with log_path.open("x") as log:
        proc = subprocess.Popen([sys.executable, str(ROOT / "analysis/sar_selector_learning.py"), "panel"],
                                cwd=ROOT, env=env, stdin=subprocess.DEVNULL, stdout=log,
                                stderr=subprocess.STDOUT, start_new_session=True)
    awake = subprocess.Popen(["/usr/bin/caffeinate", "-is", "-w", str(proc.pid)], stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    record = {"launched_at_utc": now(), "coordinator_pid": proc.pid, "caffeinate_pid": awake.pid,
              "log": str(log_path), "scope": "Full labels -> audit -> weighted training -> validation -> freeze -> stop; no blind tests"}
    write(OUT / "launch.json", record)
    print(record, flush=True)


if __name__ == "__main__":
    launch()
