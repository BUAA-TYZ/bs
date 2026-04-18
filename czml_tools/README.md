# CZML Tools — 卫星与地面站可视化

使用 [Cesium](https://cesium.com/) 将仿真中的卫星轨道和地面站渲染到三维地球上。

## 依赖

```bash
pip install skyfield pyyaml
```

## 快速开始（推荐）

直接从仿真 config 生成，自动读取 TLE 文件路径、地面站配置和起始时间：

```bash
python czml_tools/tle_to_czml.py \
  --config examples/config.yaml \
  --out czml_tools/sim_visualization.czml \
  --duration-hours 3 \
  --step-seconds 30 \
  --trail-seconds 2400 \
  --max-sats 20
```

然后启动本地服务器并打开页面：

```bash
python -m http.server 8000
# 浏览器访问 http://localhost:8000/czml_tools/viewer.html
```

## 参数说明

### `--config`（推荐，与 `--tle-file` 二选一）

从仿真配置文件中自动读取：
- `topology.tle_file` — TLE 文件路径
- `topology.start_time_utc` — 仿真起始时间
- `ground_stations` — 地面站列表（id、经纬度、带宽、仰角门限）

### `--tle-file`（手动指定 TLE）

直接指定 TLE 文件，格式为 `name + line1 + line2`，每组三行。

### 其他参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--out` | （必填）| 输出 CZML 文件路径 |
| `--start-utc` | config 中读取或当前时间 | 轨迹起始 UTC 时间，如 `2026-03-20T17:30:00Z` |
| `--duration-hours` | 3.0 | 轨迹时长（小时）|
| `--step-seconds` | 60 | 轨迹采样间隔（秒），越小越平滑，文件越大 |
| `--trail-seconds` | 3600 | 可见尾迹长度（秒）|
| `--max-sats` | 0（全部）| 最多输出的卫星数量 |
| `--gs-file` | — | 额外的地面站 YAML 文件 |

## Viewer 功能

`viewer.html` 是一个交互式三维可视化界面，支持：

| 功能 | 说明 |
|------|------|
| 加载 / 刷新 | 输入框指定 CZML 路径后点击加载 |
| 播放速度 | 滑块调节时间倍速（1× ～ 600×，默认 60×）|
| 播放 / 暂停 / 重置 | 控制时间轴 |
| 标签切换 | 显示/隐藏卫星和地面站标签 |
| 轨迹切换 | 显示/隐藏卫星尾迹 |
| 地面站切换 | 显示/隐藏地面站标记 |
| 点击选中 | 点击实体在状态栏显示名称和 ID |
| 复位视角 | 飞回当前数据集的包围盒 |

## 可视化说明

- **卫星**：彩色点 + 尾迹，颜色按名称哈希自动分配，标签显示名称、倾角和 RAAN
- **地面站**：黄色大点，标签显示 ID、经纬度、带宽（Mbps）和最小仰角要求

## 示例：手动指定 TLE 和地面站

```bash
# 只生成卫星轨道（无地面站）
python czml_tools/tle_to_czml.py \
  --tle-file data/tle/global_spread_inc53_24.tle \
  --out czml_tools/sats_only.czml \
  --start-utc 2026-03-20T17:30:00Z \
  --duration-hours 2

# 使用仿真 config（自动带地面站）
python czml_tools/tle_to_czml.py \
  --config examples/config.yaml \
  --out czml_tools/sim_visualization.czml \
  --duration-hours 3 \
  --step-seconds 30
```

## 已生成的 CZML 文件

| 文件 | 说明 |
|------|------|
| `sim_visualization.czml` | 当前仿真配置（20 颗 inc=53° 全球分散卫星 + 北京/乌鲁木齐地面站，17:30 UTC 起始）|
