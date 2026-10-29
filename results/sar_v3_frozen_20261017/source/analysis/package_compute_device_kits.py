"""打包可独立运行的设备采集工具及当前实验方案，不包含模型或历史数据。"""

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import zipfile


ROOT = Path(__file__).resolve().parents[1]
STARTER = '''"""Collect device information without installing or changing anything."""
import sys

if sys.version_info < (3, 8):
    print("Python 3.8+ is required. Keep the current system/JetPack unchanged.")
    print("Return your Python version and device model for a compatible collector.")
    sys.exit(2)

from datetime import datetime, timezone
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent
stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
output = root / "results" / ("__DEVICE___device_" + stamp + ".json")
print("Collecting __DEVICE__ inventory. No settings will be changed.", flush=True)
result = subprocess.run([
    sys.executable, str(root / "analysis" / "collect_compute_device.py"),
    "--output", str(output),
], cwd=str(root), check=False)
if result.returncode == 0:
    print("Return this JSON report: " + str(output))
    print("This was inventory collection, not a performance benchmark.")
sys.exit(result.returncode)
'''

WINDOWS_STARTER = '''@echo off
cd /d "%~dp0"
where py >nul 2>nul
if not errorlevel 1 (
    py -3 run_collect.py
) else (
    python run_collect.py
)
set "COLLECT_EXIT_CODE=%ERRORLEVEL%"
echo.
pause
exit /b %COLLECT_EXIT_CODE%
'''

README = '''# {title}设备采集包

此包用于实验第一步：确认设备与软件版本。附带完整参数标定方案，暂不执行
GPU推理、训练、测速或功率模式修改，不包含模型权重和图像数据。

## 运行

先完整解压，再进入这个目录。只需要Python 3.8或以上，不用安装任何pip依赖，
无需sudo或管理员权限。采集过程中会显示一次开始提示，部分命令可能等待数秒。

{instructions}

程序结束后，将 `results/{device}_device_时间戳.json` 带回来即可。
每次生成新文件，不覆盖旧报告；路径相对于本包，不要求原项目目录存在。
如果通过桌面入口运行窗口停留，看到结果后按任意键关闭。

## 缺失信息怎么处理

- `not_found`、`command_failed`、`timeout`等状态也属于有效记录，请保留完整JSON。
- 不要因为工具缺失或权限不足就重装JetPack、驱动或Python。
- Python低于3.8时，先记下版本和设备型号；不要升级系统Python来迁就本工具。
- Python包版本只代表运行本脚本的环境；有现成推理环境时可在该环境中再采一次。
- 另记录电源、散热器/风扇、室温和是否连接显示器，写入 `device_notes.txt`。

{device_notes}

## 文件

- `run_collect.py`：设备专用入口，自动为报告命名。
- `analysis/collect_compute_device.py`：完整标准库采集脚本，可独立运行。
- `EXPERIMENT_PLAN.md`：当前Jetson与RTX 2060共同的实验方案，仅保留最新第0节。
- `device_notes.txt`：手工环境记录模板。
- `SHA256SUMS.json`：包内文件校验摘要。

方案中的E1–E5是后续测量协议；目前可直接执行的是E0设备采集。
具体CUDA/TensorRT测速入口将在设备型号与版本确认后适配，无需自行训练识别器。
'''

NOTES = '''设备名称：
电源规格：
散热器及风扇：
室温（摄氏度）：
是否连接显示器：
执行脚本时使用的环境名称（如已知）：
其他占用CPU/GPU的程序：
采集异常说明：
'''


def build(output, stamp):
    collector = (ROOT / "analysis/collect_compute_device.py").read_bytes()
    document = (ROOT / "docs/thesis/compute_calibration_evidence.md").read_text()
    marker = "## 0. Jetson + RTX 2060 参数标定方案\n"
    _, found, rest = document.partition(marker)
    body, separator, _ = rest.partition("\n---\n")
    if not found or not separator:
        raise ValueError("Current protocol section boundaries were not found")
    protocol = ("# Jetson + RTX 2060 参数标定方案\n\n"
                "独立分发快照；先按本包README完成E0设备采集。下文的项目根目录"
                "命令可在本包根目录使用，其他工作负载代码引用用于解释后续协议。\n\n"
                + body.strip() + "\n")
    profiles = {
        "jetson": {
            "title": "Jetson",
            "instructions": "在终端执行：\n\n```bash\npython3 run_collect.py\n```",
            "device_notes": "Jetson上没有nvidia-smi不代表采集失败。L4T/JetPack及板卡型号会另行查询；\n功率模式或时钟查询若权限不足，报告会保留失败状态。",
        },
        "rtx2060": {
            "title": "RTX 2060",
            "instructions": "Windows可双击 `start_windows.cmd`，或在终端执行：\n\n```powershell\npy -3 run_collect.py\n```\n\n没有py启动器时用 `python run_collect.py`；Linux执行：\n\n```bash\npython3 run_collect.py\n```",
            "device_notes": "Windows的python命令若跳转Microsoft Store，说明当前命令没有找到可用解释器。\nNVIDIA驱动的nvidia-smi若未在PATH中，GPU字段会显示not_found；保留该报告，\n不要把它解释为没有GPU或自动重装驱动。",
        },
    }
    output.mkdir(parents=True, exist_ok=True)
    destinations = {device: output / (device + "_setup_" + stamp + ".zip")
                    for device in profiles}
    if any(path.exists() for path in destinations.values()):
        raise FileExistsError("Bundle already exists; choose another output directory")
    for device, profile in profiles.items():
        files = {
            "README.md": README.format(device=device, **profile).encode("utf-8"),
            "run_collect.py": STARTER.replace("__DEVICE__", device).encode("utf-8"),
            "analysis/collect_compute_device.py": collector,
            "EXPERIMENT_PLAN.md": protocol.encode("utf-8"),
            "device_notes.txt": NOTES.encode("utf-8"),
        }
        if device == "rtx2060":
            files["start_windows.cmd"] = WINDOWS_STARTER.replace("\n", "\r\n").encode("ascii")
        checksums = {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}
        files["SHA256SUMS.json"] = (json.dumps(checksums, indent=2) + "\n").encode("utf-8")
        archive_root = device + "_setup/"
        with zipfile.ZipFile(destinations[device], "x", zipfile.ZIP_DEFLATED) as archive:
            for name, data in files.items():
                archive.writestr(archive_root + name, data)
        print(str(destinations[device]) + " (" + str(destinations[device].stat().st_size) + " bytes)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    args = parser.parse_args()
    if len(args.date) != 8 or not args.date.isdigit():
        parser.error("--date must be YYYYMMDD")
    build(args.output, args.date)
