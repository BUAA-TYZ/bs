# Jetson SSH运行检查与单窗口试跑

日期：2026-10-15。用户授权使用 `ssh gpu`，远端确认为NVIDIA Jetson AGX Orin
Developer Kit。这是现有环境可执行性与短时量级检查，尚未完成SAR负载标定。

## 环境与执行位置

- 解释器：`/home/tianyz/anaconda3/envs/dota/bin/python`，Python3.10.15。
- 独立目录：`/home/tianyz/bs_jetson_20261015_7TtC5t`。
- CUDA12.2、PyTorch2.3.0、cuDNN8.9.4、torchvision0.18.0a0、Ultralytics8.3.13。
- GPU Orin，SM8.7；系统与CUDA可见共享RAM 32,188,686,336 B。
- 使用既有MAXN；未修改功率、频率、驱动、JetPack或依赖。
- 非交互SSH默认 `/usr/bin/python3` 对应torch2.1/torchvision0.16，且没有Ultralytics；
  它不是用户回报的环境，未用作本次模型试跑。

## 运行检查

`runtime.json` 中PyTorch FP32/FP16矩阵乘法与卷积、NumPy数据往返、torchvision
CUDA NMS均通过，实际GPU执行而非CPU回退。`dota` 环境无法导入TensorRT，
系统原生TensorRT8.6.2库存在；没有为该可选路径安装任何软件。

## 模型试跑

官方YOLO11n-OBB，官方光学boats.jpg左上1024×1024裁片，batch1。FP32和FP16
分别独立加载，5次预热、20次正式；记录实际参数dtype与cuda:0，PyTorch CPU线程1。
计时含内存原图裁切/BGR转换、预处理、模型推理、窗内OBB后处理、全部结果回CPU、
OBB1编码；不含图像解码、模型加载、预热、日志写入、全图去重或通信。

| 精度 | 正式次数 | 平均/ms | P50/ms | P95/ms | 每窗输出框 |
| --- | ---: | ---: | ---: | ---: | ---: |
| FP32 | 20 | 52.114 | 49.236 | 68.444 | 105 |
| FP16 | 20 | 50.640 | 49.554 | 57.816 | 105 |

每窗业务消息均为4228 B，即28+40×105。此处是光学样例的模型输出，不能代表
SAR船型任务的输出密度或准确率；相同框数也不证明两种精度的预测逐框相同。
两种精度的短时均值接近，不据此宣称FP16有稳定加速或选定正式精度。
PyTorch分配峰值FP32/FP16分别242,746,368/194,772,480 B，reserved峰值分别
379,584,512/337,641,472 B；它们不是整个系统或所有驱动分配的内存峰值。

初次FP16失败是测试脚本提前将模型转half，触发Ultralytics8.3.13在Conv/BN
融合时的Half/float类型冲突。修正为以FP32加载，由AutoBackend先融合再按
half参数转换；未修改远端框架。`smoke_report_initial.json` 保留失败与初次
FP32数据，`smoke_report.json` 是修正后两种精度完整试跑。

修正后FP16启动时终端出现一次cuDNN执行计划警告：

```text
CUDNN_BACKEND_EXECUTION_PLAN_DESCRIPTOR: cudnnFinalize Descriptor Failed
cudnn_status: CUDNN_STATUS_NOT_SUPPORTED
```

之后5次预热与20次正式均完成；具体采用哪个卷积计划尚未剖析。这不是进程
失败，但正式校准需要保留该情况，不能称为完全无警告的TensorRT优化结果。

## 资源与复现

从本机已缓存资源复制，远端SHA256一致，无新增下载：

- [官方yolo11n-obb.pt](https://github.com/ultralytics/assets/releases/download/v8.3.0/yolo11n-obb.pt)：
  `b62898ebf38940ca4df323863e45ee9d84a1a46d5d11ebdde529fb33aa9f3a32`。
- [官方boats.jpg](https://github.com/ultralytics/assets/releases/download/v0.0.0/boats.jpg)：
  `8c5ada657cf8110a9f8aaac954c1dd96cde0187315b581276c32b0d1863e756f`。

原始与修正脚本分别保存在 `smoke_jetson_model.py`、`smoke_jetson_model_fixed.py`。
远端修正后命令如下；重跑必须换一个不存在的输出目录，保留本次快照：

```bash
/home/tianyz/anaconda3/envs/dota/bin/python /home/tianyz/bs_jetson_20261015_7TtC5t/smoke_jetson_model_fixed.py \
  --weights /home/tianyz/bs_jetson_20261015_7TtC5t/yolo11n-obb.pt \
  --image /home/tianyz/bs_jetson_20261015_7TtC5t/boats.jpg \
  --output /home/tianyz/bs_jetson_20261015_7TtC5t/smoke_new_run
```

本轮未训练、未做15分钟热稳态实验、未切换功率模式、未改仿真参数。正式
校准还需固定SAR输入清单、后处理负载、热稳态、功率和全图聚合测量。
本轮SSH运行均已结束，结束时该用户无遗留Python测试进程。
