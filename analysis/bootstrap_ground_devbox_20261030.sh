#!/usr/bin/env bash
set -euo pipefail
task_root="$HOME/bs_ground_scaling_20261030"
mkdir -p "$task_root/bootstrap"
cd "$task_root"
if [[ ! -x bootstrap/bin/uv ]]; then
  if command -v python3.13 >/dev/null; then bootstrap_python=python3.13; else bootstrap_python=python3; fi
  "$bootstrap_python" -m pip install --index-url https://pypi.org/simple --target bootstrap uv
fi
bootstrap/bin/uv python install 3.12.13
if [[ ! -x .venv/bin/python ]]; then bootstrap/bin/uv venv --python 3.12.13 .venv; fi
bootstrap/bin/uv pip install --python .venv/bin/python --index-url https://download.pytorch.org/whl/cpu torch==2.12.0
bootstrap/bin/uv pip install --python .venv/bin/python numpy==2.4.4 networkx==3.6.1 skyfield==1.54 sgp4==2.25 PyYAML==6.0.3 tqdm==4.67.3 matplotlib==3.11.2
bootstrap/bin/uv pip install --python .venv/bin/python jplephem==2.24 certifi==2026.4.22 python-dateutil==2.9.0.post0 six==1.17.0 sympy==1.14.0 mpmath==1.3.0 filelock==3.29.0 fsspec==2026.4.0 jinja2==3.1.6 markupsafe==3.0.3 typing-extensions==4.15.0
bootstrap/bin/uv pip install --python .venv/bin/python shapely==2.2.0 scipy==1.18.1 charset-normalizer==3.5.2 setuptools==81.0.0
.venv/bin/python -c 'import sys,importlib.metadata as m; print(sys.version); print({n:m.version(n) for n in ["torch","numpy","networkx","skyfield","sgp4","PyYAML","tqdm","matplotlib"]})'
if [[ ! -e /Users/bytedance/Projects/bs ]]; then
  sudo -n mkdir -p /Users/bytedance/Projects
  sudo -n ln -s "$task_root" /Users/bytedance/Projects/bs
fi
test "$(readlink -f /Users/bytedance/Projects/bs)" = "$(readlink -f "$task_root")"
