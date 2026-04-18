# 参数调研说明（2026-04-10）

本文件给出 `examples/config_researched.yaml` 的取值依据。  
注意：很多商业卫星的精确链路和算力参数不公开，因此这里采用“公开事实 + 工程映射”的方式。

## 1) 图像体量（`image_size_mb`）

- 配置值：`700 MB`
- 依据：Copernicus Sentinel-2 产品说明中，L1C 单 tile 数据量常见在约 700 MB 量级（随压缩和内容波动）。
- 解释：该量级能反映“原图很大、窗口期难全量下传”的核心矛盾。

## 2) 地面下行带宽（`ground_stations[].bandwidth_mbps`）

- 配置值：`300 Mbps`（两站统一）
- 依据：USGS Landsat-8 任务页面给出 X-band 直接下行速率 `384 Mbps`。
- 解释：仿真中取 300 Mbps 作为保守工程值，避免过于理想化。

## 3) 最小仰角（`ground_stations[].min_elevation_deg`）

- 配置值：`12 deg`
- 依据：地面站工程实践通常使用 `10~15 deg` 仰角门限以平衡可用窗口和链路质量（低仰角损耗与遮挡更明显）。
- 解释：取 12 度作为中间值，比 0 度更接近实际。

## 4) 星上显存（`vram_capacity_gb`）

- 配置值：`8 GB`
- 依据：NVIDIA Jetson TX2 公开规格为 8GB LPDDR4，常被作为边缘/在轨 AI 参考量级。
- 解释：不是断言“卫星都用 TX2”，而是用公开硬件量级约束“星上显存紧张”这一事实。

## 5) 星间链路带宽（`bandwidth_mbps_min/max`）

- 配置值：`100~600 Mbps`
- 依据：公开资料显示空间激光链路已可达 Gbps 级（例如 ESA EDRS 公开 1.8 Gbps 量级），但具体星座实装和可用带宽高度不确定。
- 解释：这里取“中档、偏保守”区间，避免把分布式协同优势建立在不现实的超高速 ISL 上。

## 6) 算力映射（`compute_rate` 与 `ground_stations[].compute_rate`）

- 配置值：卫星 `0.6`，地面 `8.0`（抽象单位）
- 依据：星上边缘算力与地面机房通常存在明显量级差异（公开硬件规格可支持这一趋势）。
- 解释：该仿真采用抽象 compute_cost / compute_rate，重点保证“星上慢、地面快”的相对关系。

---

## 参考链接

1. Copernicus Sentinel-2 官方文档入口（产品与格式说明）：  
   https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi
2. USGS Landsat-8（含 X-band 384 Mbps）：  
   https://www.usgs.gov/landsat-missions/landsat-8
3. NVIDIA Jetson TX2 模块规格：  
   https://www.nvidia.com/en-us/autonomous-machines/embedded-systems/jetson-tx2/
4. ESA SpaceDataHighway / EDRS（公开激光链路 Gbps 量级）：  
   https://www.esa.int/Applications/Connectivity_and_Secure_Communications/SpaceDataHighway

