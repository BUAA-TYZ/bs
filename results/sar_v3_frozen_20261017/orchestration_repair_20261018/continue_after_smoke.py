"""等待已经启动的单任务冒烟，通过后调用已登记编排入口启动剩余面板。"""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import traceback

HERE = Path(__file__).resolve().parent


def main():
    launch = json.loads((HERE / "smoke_launch.json").read_text())
    while subprocess.run(["/bin/ps", "-p", str(launch["pid"]), "-o", "pid="],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        time.sleep(10)
    status = json.loads((HERE / "smoke.json").read_text())
    if status["status"] != "PASS":
        print(json.dumps({"panel_started": False, "smoke_status": status["status"]}), flush=True)
        return
    repair = json.loads((HERE / "repair.json").read_text())
    script = HERE / "resume_sar_formal.py"
    assert hashlib.sha256(script.read_bytes()).hexdigest() == repair["orchestrator_after_sha256"]
    subprocess.run([sys.executable, str(script), "launch-panel", "--out", str(HERE.parent)], check=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        trace = traceback.format_exc()
        (HERE / "GATE_FAILED.json").write_text(json.dumps({"traceback": trace}, indent=2) + "\n")
        raise
