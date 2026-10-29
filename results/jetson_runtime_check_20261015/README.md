# Jetson 第一步：验证现有GPU运行环境

针对本次回报的 AGX Orin / JetPack 6.0 / Python 3.10 / PyTorch 2.3 环境。
此包只做小张量检查，不训练、不下载模型，不安装或升级软件，不改功率/时钟。

完整解压，在与刚才采集报告相同的Python环境中执行：

```bash
python3 check_jetson_runtime.py
```

检查通常很快，每个子检查最多等待60秒；总共最多3个。结束后将本包
`results/jetson_runtime_时间戳.json` 带回来，成功或失败都保留该文件。
每次生成新报告，不覆盖之前结果。

检查内容：

1. PyTorch是否实际执行CUDA FP32/FP16矩阵运算、卷积及NumPy数据往返。
2. torchvision的CUDA NMS扩展是否可调用，核对输出索引。
3. 当前Python能否导入TensorRT。此项是可选信息，不阻塞PyTorch路径。

`pytorch_path_ready` 表示前两项通过，可以继续下一步模型试跑；不代表已经
完成模型性能测试。`needs_attention` 表示需根据JSON定位环境问题，暂时不用
自行重装依赖。系统的TensorRT 8.6.2库已经存在，但Python绑定可能在其他环境。

MAXN是功率模式名称，不代表已经锁定最高时钟，也不是实际运行功率测量。
本次保持现状，先确认可运行，再安排功耗和持续性能测试。

本包已在开发机检查无CUDA时的失败报告路径，尚未在这台Jetson执行。
