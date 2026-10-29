# 固定 overlap 实验复现

> 版本说明：本页保留历史协议、测量或实现依据，其中“当前”“本轮”指各节原始日期。
> 2026-10-15主实验参数与状态以[冻结清单](../../results/sla_frozen_protocol_20261015/manifest.json)
> 和[工作简报](../codex_brief.md)为准；此页不授权重启旧训练或替换当前P300口径。

更新于 2026-10-14。本文记录实际脚本的执行顺序与产物关系，不是实验结果报告。本轮已通过完整回归 233 项；汇总说明调整后另复跑 5 项报告测试通过。基线、BC、等权 BC 和八回合 PPO 的验证/测试共 288 条记录已全部完成，见 `results/overlap_proxy_study_20261014/summary.md`。步长核验 12 条已完成，小 actor 位于 `checkpoints/overlap_proxy_actor_20261014/`；目标设备标定和多种子收敛没有完成。下面的新目录命令是复现入口，不代表它们本身已经运行。

本轮固定 1024 像素窗口、824 像素步长、名义 overlap 200 像素，末窗贴边、不 padding，相邻 2×2 窗口组成调度包。完整定义见 [fixed_overlap_protocol.md](fixed_overlap_protocol.md)，参数依据见 [parameter_decisions.md](parameter_decisions.md)。历史 128 MB、8 片及其他抽象负载实验不能与本轮成绩并表。

## 1. 环境与独立运行目录

所有命令从仓库根目录执行，使用当前项目 `.venv/bin/python`。本轮实际为 Python 3.12.13；不要根据历史 `.python-version` 自动切换解释器。依赖分别记录在 `requirements.txt`、`requirements-analysis.txt`、`requirements-workload.txt`。先确认 `.venv/bin/python -m pip check` 通过，保留各产物中的运行时版本。

下面示例创建一轮独立复现。每次重跑更换 `RUN_ID`；这些目录应尚不存在，不要覆盖 20261014 原归档。

```bash
RUN_ID=20261014_repro01
ASSETS=data/workload_assets
CALIBRATION="results/image_workload_${RUN_ID}"
PROTOCOL="examples/overlap_${RUN_ID}"
TRAIN="checkpoints/overlap_mappo_${RUN_ID}"
BALANCED="checkpoints/overlap_equal_bc_${RUN_ID}"
VAL_BASE="results/overlap_val_baselines_${RUN_ID}"
TEST_BASE="results/overlap_test_baselines_${RUN_ID}"
VAL_LEARNED="results/overlap_val_training_export_${RUN_ID}"
VAL_BALANCED="results/overlap_val_equal_bc_${RUN_ID}"
TEST_LEARNED="results/overlap_test_learned_${RUN_ID}"
REPORT="results/overlap_report_${RUN_ID}"
```

协议生成、评估、等权 BC、验证导出、汇总和 actor 导出拒绝已有输出目录。原训练拒绝含既有 `run.json`、日志或 checkpoint 的目录；benchmark 拒绝已含 `summary.json` 的目录。即使某脚本没有拦截不完整的旧目录，本复现流程也不复用它：保留失败现场，另取新目录。唯一续训例外见第 9 节。

当前核心源码 SHA256 为 `9c941294963730c6622444101441fe9c8b60977783e6bf30f65b3317f83fbf03`。一轮执行期间不修改 `sim/**/*.py`、根目录 Python 文件、配置、分布或 checkpoint；分析脚本另有自身摘要。改变源码或运行时后重新建立整轮证据，不能只更改输出文件内的摘要。

## 2. 真实算子测量与资源核验

当前测量脚本面向 macOS CPU/MPS，生成协议需要两个后端的 P0019/P0007 测量。先结束本项目训练、仿真和测试，再测服务时间；不能在满载机器上测量后仍沿用归档中的空闲条件描述。CPU/MPS 是同一台机器的受控 profile，不是星载设备与地面服务器。

```bash
.venv/bin/python analysis/benchmark_image_workload.py \
  --assets "$ASSETS" --output "$CALIBRATION" \
  --devices cpu mps --warmup 3 --repeats 3 --threads 4 \
  --include-mirror
```

`--include-mirror` 必须保留，否则不能获得协议生成器需要的两张原图。脚本在使用缓存前自动核验资源，缺失时才下载；不应绕过校验直接调用模型加载。固定官方资源摘要如下：

| 文件 | SHA256 |
| --- | --- |
| `yolo11n-obb.pt` | `b62898ebf38940ca4df323863e45ee9d84a1a46d5d11ebdde529fb33aa9f3a32` |
| `dota8.zip` | `6f901955660fc5c613a0389f44240db359f07741effb48afc99d17155ee3e7ec` |
| `boats.jpg` | `8c5ada657cf8110a9f8aaac954c1dd96cde0187315b581276c32b0d1863e756f` |

DOTA 原图来自第三方镜像 `isaaccorley/dota` 的固定 revision `672e63236622f7da6ee37fca44c50ac368b77cab`。`$ASSETS/dota_mirror/provenance.json` 记录原图、标签、成员名、地址和各自 SHA256；缓存复用逐文件核验。该镜像不冒充官方站点。归档的 `protocol.json` 再记录输入、标签、模型来源、软件与设备；输出同时包括 `windows.csv`、`images.json`、`summary.json`、实际检测结果与可视化。

如果只想复跑既有冻结调度协议，可跳过重新计时，直接令 `CALIBRATION=results/image_workload_calibration_20261014`，再执行第 3 节。此时明确写“复用已归档标定”，不能称为新的硬件测量。若重新计时，均值会有变化，随后生成的是新一轮受控协议，所有训练和基线均须使用该协议重跑。

## 3. 生成并冻结协议

```bash
.venv/bin/python analysis/build_overlap_protocol.py \
  --calibration-dir "$CALIBRATION" --output-dir "$PROTOCOL" --dt 1
```

产物为 `config.json`、`distribution.json`、`parameter_reasons.json`。最后一项记录四份测量输入的 SHA256、几何/字节/计算派生值、资源假设及局限；应与测量目录一起归档。

**24 星、705 km、98.2°的验证/测试星座必须通过 `ScenarioDistribution` 实例化。** 基础 `config.json` 仍有继承的旧 TLE 占位信息；不能只把这个文件交给通用 `main.py`，再把所得运行称为本协议。以下所有调度命令同时传入 config 和 distribution。训练使用 12/24 星随机化，验证与测试各为六种条件、每种两个种子，共 12 个 case。

主指标固定为任务年龄 30/60/120/240 分钟的完整交付率等权均值，次指标为 4 小时受限平均时延；每例 2 小时到达加 4 小时排空。validation/test 之间不合并样本、不根据 test 改方法或选 checkpoint。

## 4. 九个规则基线

```bash
.venv/bin/python analysis/evaluate_overlap.py \
  --config "$PROTOCOL/config.json" --distribution "$PROTOCOL/distribution.json" \
  --split validation --workers 4 --out-dir "$VAL_BASE" \
  --methods ground_compute ground_tiles ground_relay local greedy window_aware \
  reservation_calendar heft_adapted cgs_compute_adapted

.venv/bin/python analysis/evaluate_overlap.py \
  --config "$PROTOCOL/config.json" --distribution "$PROTOCOL/distribution.json" \
  --split test --workers 4 --out-dir "$TEST_BASE" \
  --methods ground_compute ground_tiles ground_relay local greedy window_aware \
  reservation_calendar heft_adapted cgs_compute_adapted
```

每个目录应有 108 条独立结果，以及完整结束后才生成的 `summary.json`。`run.json` 或部分 case 文件存在不代表运行完成。保留逐任务结果、配置、轨道、工作负载和任务流摘要。`heft_adapted`、`cgs_compute_adapted` 是明确适配的算法，不改称原论文全部系统的精确复现。

## 5. 原 BC 加八轮 MAPPO

```bash
.venv/bin/python train_marl.py \
  --config "$PROTOCOL/config.json" --scenario-distribution "$PROTOCOL/distribution.json" \
  --algorithm mappo --features overlap_contact --encoder task_attention \
  --hidden 64 --critic-pooling \
  --warmstart-episodes 8 --warmstart-mode pooled --warmstart-epochs 30 \
  --warmstart-teacher cgs_compute_adapted \
  --episodes 8 --rollout-steps 21600 --ppo-epochs 2 --minibatch 512 \
  --lr 0.0001 --warmstart-lr 0.001 --gamma 1 --lam 1 --reward-mode task \
  --delivery-horizons-minutes 30,60,120,240 \
  --actor-batching active --actor-epochs 4 --target-kl 0.01 --ent-coef 0.002 \
  --eval-every 8 --torch-threads 1 --device cpu --seed 1701 --out-dir "$TRAIN"
```

`initial.pt` 是随机初始化；`teacher_bc.pt` 是八场景合并示范训练后的 BC；`warmstart.pt` 与其权重相同，但含初始验证。`last.pt` 是最后已完成 PPO episode，最终应为 8；`best.pt` 由 validation 选择，可能仍为 episode 0，不能因为文件名叫 best 就称为 PPO 改进模型。

保存整个目录，特别是 `run.json`、`warmstart.json` 和完整 `train_log.jsonl`。本轮只有一个学习种子、八轮 PPO，是试验预算，不构成多种子稳定优势证明。

## 6. 同轮初始化的 episode 等权 BC

等权 BC 是独立对照：使用同一个 `initial.pt`、相同教师和八个训练 case，按每个 episode 的有效 tile 标签数归一化损失。它不继承已拟合的 BC 权重，也不在此脚本中执行 PPO 或评估。

等待同轮 `warmstart.json` 完整产生，再计算与原 BC 相同的优化更新数。既有冻结轮为 3210；重新计时生成的新协议可能改变示范行数，因此从同轮日志计算：

```bash
BC_UPDATES=$(.venv/bin/python -c 'import json,math,sys; d=json.load(open(sys.argv[1])); print(30*math.ceil(sum(e["samples"] for e in d["episodes"])/512))' "$TRAIN/warmstart.json")

.venv/bin/python analysis/train_episode_equal_bc.py \
  --initial-checkpoint "$TRAIN/initial.pt" \
  --config "$PROTOCOL/config.json" --distribution "$PROTOCOL/distribution.json" \
  --out-dir "$BALANCED" --workers 4 --episodes 8 --seed 1701 \
  --lr 0.001 --epochs 30 --minibatch 512 --expected-updates "$BC_UPDATES"
```

产物为 `demonstrations/index.json`、八份逐场景张量缓存、`run.json`、拟合诊断、`summary.json` 和独立的 `teacher_bc.pt`。缓存索引绑定 core source、初始 checkpoint、config、distribution、teacher 和每份缓存 SHA256。

`--cache-dir` 只用于复用已经完整且经这些校验一致的缓存；新输出目录仍须为空。不要把旧 20261014 缓存接到本轮新初始化，或只复制 `initial.pt` 而丢掉其父训练元数据。拟合准确率是训练标签上的诊断，不是调度性能。

## 7. 导出验证与测试三个 checkpoint

原训练结束、日志到第 8 轮且含末轮验证后，直接导出训练中已有的 BC/PPO 验证轨迹，避免重复仿真：

```bash
.venv/bin/python analysis/export_overlap_validation.py \
  --training-dir "$TRAIN" \
  --config "$PROTOCOL/config.json" --distribution "$PROTOCOL/distribution.json" \
  --out-dir "$VAL_LEARNED"

.venv/bin/python analysis/evaluate_overlap.py \
  --config "$PROTOCOL/config.json" --distribution "$PROTOCOL/distribution.json" \
  --split validation --methods --checkpoint "bc_balanced=$BALANCED/teacher_bc.pt" \
  --workers 4 --out-dir "$VAL_BALANCED"

.venv/bin/python analysis/evaluate_overlap.py \
  --config "$PROTOCOL/config.json" --distribution "$PROTOCOL/distribution.json" \
  --split test --methods \
  --checkpoint "bc=$TRAIN/teacher_bc.pt" \
  --checkpoint "ppo=$TRAIN/last.pt" \
  --checkpoint "bc_balanced=$BALANCED/teacher_bc.pt" \
  --workers 4 --out-dir "$TEST_LEARNED"
```

空的 `--methods` 有意关闭默认九基线，仅评估后面的 checkpoint。`bc`、`ppo` 和 `bc_balanced` 的命名必须在各目录保持一致。本例导出 validation 为 24 条，等权 BC validation 为 12 条，三模型 test 为 36 条。

严格导出校验原 BC 与 warmstart 的权重、末轮 checkpoint 与日志、源代码/依赖、场景/轨道/任务流及完整观察期；缺失证据时拒绝导出。原训练没有测量逐 case 墙时和决策耗时，导出字段为 null，不能补造为零或从其他运行移植。

## 8. 最终合并与 actor 导出

只在所有输入目录完整结束后合并：

```bash
.venv/bin/python analysis/report_overlap_study.py \
  --panel "validation=$VAL_BASE" \
  --panel "validation=$VAL_LEARNED" \
  --panel "validation=$VAL_BALANCED" \
  --panel "test=$TEST_BASE" \
  --panel "test=$TEST_LEARNED" \
  --training-dir "$TRAIN" --out-dir "$REPORT"
```

同名 panel 只合并严格配对且方法不重复的目录：case、源码、有效配置、任务流、轨道、工作负载与观察期必须一致。validation 和 test 分别报告。输出包含可追溯 `summary.json`、`summary.md`、按方法/条件的表格与 PNG/PDF 图，以及原训练诊断。

下面示例导出末轮 PPO actor，用于核验网络尺寸和本机推理；这不预设它是获胜方法。实际候选选择只依据 validation，需要导出其他候选时更换 checkpoint 并使用另一个输出目录。

```bash
.venv/bin/python export_actor.py \
  --checkpoint "$TRAIN/last.pt" --out-dir "results/overlap_actor_ppo_${RUN_ID}"
```

`actor.pt` 是仅 actor 的 TorchScript；`profile.json` 记录 checkpoint SHA256、参数量、文件量和 CPU 网络计时。导出会检查十组随机输入的 logits 一致性；计时不含轨道预测、特征、动作掩码、通信或真实星载硬件，不能写为端到端调度耗时。

## 9. 可选：精确续训与并行验证

此步骤只用于减少末轮验证墙时，不改变训练目标、预算、场景或 PPO 实现。必须先停止原训练进程，并确认 `last.pt` 的 episode 与 `train_log.jsonl` 最后一条一致，且在 1 到 7 之间。不能让两个进程同时写同一训练目录。

```bash
.venv/bin/python analysis/resume_parallel_validation.py \
  --training-dir "$TRAIN" --workers 4
```

包装器读取原全部参数，只把 `resume=false` 改为 true；恢复 CPU 模型、优化器和 Python/NumPy/Torch RNG。每个 worker 一次加载当前模型，执行原逐 case 验证，按原顺序汇总，保持父 RNG 不变。不能在初始验证尚未完成时中断，再拿 `teacher_bc.pt` 代替可续训的 `last.pt`：前者没有完整优化器/RNG 续训状态。

`resume_parallel_validation_source/` 保存原 run、last、日志和包装器源码；`resume_parallel_validation.json` 保存原/最终文件摘要、参数差异、输入、worker 与验证调用摘要。原日志完整保留并追加。第 7 节导出器只接受带完整且已完成侧车的这一续训路径，不接受任意缺乏来源的 resume 结果，也不能在事后删除侧车来绕过校验。

## 10. 产物之间的关系

| 上游 | 下游 | 不能省略的关联 |
| --- | --- | --- |
| 资源缓存及 provenance | 算子测量 | 权重、原图、标签摘要；冻结推理与合并参数 |
| 测量四份主文件 | config、distribution、parameter_reasons | 标定摘要、字节/服务时间映射与假设 |
| 同轮 config + distribution + source | 基线、原训练、等权 BC | 实例化轨道、场景、任务流与依赖版本 |
| 原训练 `initial.pt` | 等权 BC 示范缓存与独立 checkpoint | 同一初始化摘要与八个训练 case |
| BC/PPO checkpoint + 原训练日志 | 验证导出 | 权重身份、完整 episode、原始 per-case 结果 |
| 固定三个 checkpoint | 测试评估 | 模型摘要；不利用 test 反馈重选方法 |
| 完整基线与模型评估目录 | 最终 report | 同 panel 严格配对，不跨协议并表 |
| validation 选定候选或明确标名的对照 | actor.pt + profile.json | 导出来源、等价性检查、计时范围 |

这些运行产物与缓存应保留为整轮集合。更换协议、初始化、核心实现或统计口径时建立新的集合；不能用旧归档的某个局部文件补齐新一轮缺失的实验证据。
