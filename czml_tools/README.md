# CZML Tools

`czml_tools/tle_to_czml.py` 用于把 TLE 转成 Cesium 可直接加载的 CZML。

## 依赖

```bash
pip install skyfield
```

## 用法

```bash
python czml_tools/tle_to_czml.py \
  --tle-file data/tle/starlink_cluster_24.tle \
  --out czml_tools/starlink_cluster_24.czml \
  --start-utc 2026-03-20T00:00:00Z \
  --duration-hours 3 \
  --step-seconds 60 \
  --max-sats 24
```

## 参数说明

- `--tle-file`: TLE 输入文件
- `--out`: CZML 输出路径
- `--start-utc`: 轨迹起始 UTC 时间（默认当前 UTC）
- `--duration-hours`: 轨迹长度（小时）
- `--step-seconds`: 轨迹采样间隔（秒）
- `--trail-seconds`: 尾迹长度（秒）
- `--max-sats`: 最多输出多少颗卫星（按 TLE 顺序截断）

## Cesium 使用

1. 在 `czml_tools/viewer.html` 里填你的 token（`YOUR_CESIUM_ION_TOKEN_HERE`）。
2. 启动本地静态服务器：

```bash
python -m http.server 8000
```

3. 打开 `http://localhost:8000/czml_tools/viewer.html`。

如果你要在自己的页面中加载 CZML，可参考：

```js
const dataSource = await Cesium.CzmlDataSource.load("czml_tools/starlink_cluster_24.czml");
viewer.dataSources.add(dataSource);
viewer.zoomTo(dataSource);
```
