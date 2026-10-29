# 固定验证集学习退化审计

Validation diagnostic only; no new simulation or training; no test results read

12 个相同验证 case，按 case 等权统计 J；完成任务数跨 case 汇总。全部比较已核验任务轨迹、配置、源码与年龄指标。

| 方法 | J | 受限时延/s | 完成任务 | 源星/辅助星/地面计算图像当量 | 地面发送/MB | 卫星队列占用/tile-s |
|---|---:|---:|---:|---|---:|---:|
| local | 0.915916575 | 1279.356 | 1015/1015 | 1015.000/0.000/0.000 | 8.961568 | 20634.0 |
| cgs_compute_adapted | 0.915916575 | 1278.829 | 1015/1015 | 1011.000/1.400/2.600 | 336.097832 | 20650.0 |
| bc | 0.793284368 | 3002.029 | 1015/1015 | 1015.000/0.000/0.000 | 8.961568 | 18417689.0 |
| bc_balanced | 0.788915278 | 3206.136 | 1012/1015 | 828.200/4.000/179.800 | 22631.670000 | 33545706.0 |

## 逐场景定位

| case | 方法 | J | 相对 local 的 J 差 | WAIT 提议 | 地面提议 | 地面计算图像当量 | 地面发送/MB |
|---|---|---:|---:|---:|---:|---:|---:|
| validation_near_high_1 | bc | 0.704762 | -0.154762 | 108132 | 0 | 0.000 | 0.785400 |
| validation_near_high_1 | bc_balanced | 0.859524 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.785400 |
| validation_near_high_1 | cgs_compute_adapted | 0.859524 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.785400 |
| validation_near_high_1 | local | 0.859524 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.785400 |
| validation_near_high_2 | bc | 0.645161 | -0.233871 | 156244 | 0 | 0.000 | 0.695640 |
| validation_near_high_2 | bc_balanced | 0.879032 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.695640 |
| validation_near_high_2 | cgs_compute_adapted | 0.879032 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.695640 |
| validation_near_high_2 | local | 0.879032 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.695640 |
| validation_near_high_fast_ground_1 | bc | 0.626168 | -0.238318 | 128160 | 0 | 0.000 | 0.800360 |
| validation_near_high_fast_ground_1 | bc_balanced | 0.514019 | -0.350467 | 未记录 | 未记录 | 50.000 | 6291.882360 |
| validation_near_high_fast_ground_1 | cgs_compute_adapted | 0.864486 | +0.000000 | 未记录 | 未记录 | 0.600 | 76.293344 |
| validation_near_high_fast_ground_1 | local | 0.864486 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.800360 |
| validation_near_high_fast_ground_2 | bc | 0.678161 | -0.204023 | 118996 | 0 | 0.000 | 0.650760 |
| validation_near_high_fast_ground_2 | bc_balanced | 0.534483 | -0.347701 | 未记录 | 未记录 | 38.000 | 4781.850640 |
| validation_near_high_fast_ground_2 | cgs_compute_adapted | 0.882184 | +0.000000 | 未记录 | 未记录 | 0.600 | 76.143744 |
| validation_near_high_fast_ground_2 | local | 0.882184 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.650760 |
| validation_near_light_1 | bc | 0.904762 | +0.000000 | 9776 | 0 | 0.000 | 0.157080 |
| validation_near_light_1 | bc_balanced | 0.904762 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.157080 |
| validation_near_light_1 | cgs_compute_adapted | 0.904762 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.157080 |
| validation_near_light_1 | local | 0.904762 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.157080 |
| validation_near_light_2 | bc | 0.723214 | -0.133929 | 62880 | 0 | 0.000 | 0.209440 |
| validation_near_light_2 | bc_balanced | 0.857143 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.209440 |
| validation_near_light_2 | cgs_compute_adapted | 0.857143 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.209440 |
| validation_near_light_2 | local | 0.857143 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.209440 |
| validation_near_small_image_1 | bc | 0.850610 | +0.000000 | 7486 | 0 | 0.000 | 1.270016 |
| validation_near_small_image_1 | bc_balanced | 0.850610 | +0.000000 | 未记录 | 未记录 | 0.000 | 1.270016 |
| validation_near_small_image_1 | cgs_compute_adapted | 0.850610 | +0.000000 | 未记录 | 未记录 | 0.000 | 1.270016 |
| validation_near_small_image_1 | local | 0.850610 | +0.000000 | 未记录 | 未记录 | 0.000 | 1.270016 |
| validation_near_small_image_2 | bc | 0.783708 | -0.109551 | 41675 | 0 | 0.000 | 1.378432 |
| validation_near_small_image_2 | bc_balanced | 0.893258 | +0.000000 | 未记录 | 未记录 | 0.000 | 1.378432 |
| validation_near_small_image_2 | cgs_compute_adapted | 0.893258 | +0.000000 | 未记录 | 未记录 | 0.000 | 1.378432 |
| validation_near_small_image_2 | local | 0.893258 | +0.000000 | 未记录 | 未记录 | 0.000 | 1.378432 |
| validation_uniform_high_1 | bc | 0.846983 | -0.153017 | 104060 | 0 | 0.000 | 0.867680 |
| validation_uniform_high_1 | bc_balanced | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.867680 |
| validation_uniform_high_1 | cgs_compute_adapted | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.867680 |
| validation_uniform_high_1 | local | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.867680 |
| validation_uniform_high_2 | bc | 0.776471 | -0.223529 | 130940 | 0 | 0.000 | 0.635800 |
| validation_uniform_high_2 | bc_balanced | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.635800 |
| validation_uniform_high_2 | cgs_compute_adapted | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.635800 |
| validation_uniform_high_2 | local | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.635800 |
| validation_uniform_high_fast_ground_1 | bc | 0.995370 | -0.004630 | 36368 | 0 | 0.000 | 0.807840 |
| validation_uniform_high_fast_ground_1 | bc_balanced | 0.634259 | -0.365741 | 未记录 | 未记录 | 44.800 | 5637.617312 |
| validation_uniform_high_fast_ground_1 | cgs_compute_adapted | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.800 | 101.465152 |
| validation_uniform_high_fast_ground_1 | local | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.807840 |
| validation_uniform_high_fast_ground_2 | bc | 0.984043 | -0.015957 | 50044 | 0 | 0.000 | 0.703120 |
| validation_uniform_high_fast_ground_2 | bc_balanced | 0.539894 | -0.460106 | 未记录 | 未记录 | 47.000 | 5914.320200 |
| validation_uniform_high_fast_ground_2 | cgs_compute_adapted | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.600 | 76.196104 |
| validation_uniform_high_fast_ground_2 | local | 1.000000 | +0.000000 | 未记录 | 未记录 | 0.000 | 0.703120 |

## 证据边界

- Only original BC validation stores per-action proposal counts; balanced BC and rules do not.
- Proposal counts include repeated pending decisions and are not executed task counts.
- Source/non-source identifies node role, not the originating node of each task.
- Work divided by per-image cost is an image-equivalent quantity, not a per-task executor trace.
- Queue occupancy covers all items in satellite.queue, not exclusively WAIT actions.
- Paired timing/traffic evidence does not isolate a causal contribution of WAIT or ground offload.
- No decision-level logits, teacher labels on learner states or tile event trace were recorded.

每方法每场景的五个最大延迟任务、跨年龄门槛的损失/收益数、实际资源与网络量保存在 summary.json；不将同时出现的现象写成干预已证实的原因。
