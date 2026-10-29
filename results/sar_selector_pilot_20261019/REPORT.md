# 监督式方案选择器：40状态试点

结论：**BLOCKED**。试点后停止，未生成全量标签、未训练、未运行新盲测。

训练日2026-10-15；五条件，每格8个预先抽取状态，共观察39/40状态、117/120条分支。
缺失抽样状态：[{'condition': 'load_4', 'task_id': 'task_535'}]。若缺失非空，以下比例只描述已取得记录，不能判为完整试点通过。
一致率 **58.97%**（可判定状态中v3等于最快标签）；不可判定率 **0.00%**（分母固定40）。
并列最快状态6个；将选中任意并列最快均计为正确时，一致率为0.6153846153846154，只作补充，不替代预登记标签判定。
可判定最快标签：本地15、CGS21、地面3。
停止规则：一致率≥90%或不可判定率>40%则停止；否则也必须先汇报、等待用户确认才能全量。执行/审计错误单列BLOCKED，不冒充不可判定或P300=0。

## 分条件结果

|条件|完成状态|可判定|本地标签|CGS标签|地面标签|一致|不可判定|选错损失均值 s（准确子集）|
|---|---:|---:|---:|---:|---:|---:|---:|---:|
|base|8/8|8|4|4|0|4/8|0/8|73.4175 (n=4)|
|compute_4|8/8|8|3|4|1|6/8|0/8|150.0700 (n=2)|
|compute_10|8/8|8|2|4|2|6/8|0/8|206.0050 (n=2)|
|load_2|8/8|8|3|5|0|3/8|0/8|209.0340 (n=5)|
|load_4|7/8|7|3|4|0|4/7|0/8|44.5433 (n=3)|

## 选错方案的时延损失

损失=同状态v3所选方案实际完整交付时延减最快方案实际时延，不截断到300秒。若v3分支未完成，只报告删失下界，不把7200秒当作精确时延。
选错16状态：准确观测16、删失0。
准确观测子集：均值136.53874999999988秒，中位数19.00000000000091秒，最大946.2900000000001秒。
全部选错状态均值：136.53874999999988秒。
精确平局按本地、CGS、地面顺序；其他并列最快选项可能计为标签不一致，但损失为0；逐状态保留并列信息。

## 审计与成本

冻结源码159份（快照及工作区）、5份试点配置、抽样清单、日期/方法哈希核对通过。已完成条件报告确认控制分支通过32/40；失败完整性断言另保留7条通过的控制记录。无干预逐决策已完整记录712次，未含失败条件母进程计数。
fork复制完整环境、在途传输、影子日历、调用栈与RNG；逐分支核验继承指纹，原选择对应分支与母轨迹比较完成时间。Python报告原生线程fork兼容性警告，实际退出及重放审计均核验；不据此保证跨机器恢复能力，未永久序列化完整环境。
每分支最长7200仿真秒，已完成赢家须早于其他删失下界。报价为None不代表物理不可行，仍强制原计划执行。当日到达固定，只延长末图观察，未生成次日任务。私有真值和真实未来仅用于离线回放。
已有计时记录CPU合计2.534核小时；计时完整性=False，若为False则未含失败条件母进程完整耗时，只是下界。分支合计2412.9 worker秒，非面板墙钟。

## 新盲测封存

2026-12-15至12-17，五条件×v3/冻结学习选择器/旧两方案，共45次全天；只登记，未生成、查看或运行。须模型冻结且获准后再启封。
日期SHA256：f067265f9ffb3d6a7dcb6159d9ca2069e2b8a2752bce71dcb235b93c3299211f。
批准的方法补充登记SHA256：d1f9610a5456bda97efcc2c1c49fc77bb923e503f02f18910daede6a1afd3ed4。
修改版见../sar_v3_frozen_20261017/LEARNING_PROPOSAL.md，最多分类/回归两个变体，同输入同数据，验证P300优先、R300次之。
定位为监督式方案选择器，不称多智能体强化学习；局部反事实最快标签不构成整日全局最优，也不保证学习后P300提高。

## 逐状态记录

单位秒。NA为未完成；删失下界及原报价详见pilot_metrics.json。

|条件|图像|v3|最快标签|本地实际|CGS实际|地面实际|v3损失|
|---|---|---|---|---:|---:|---:|---:|
|base|task_7|cgs|cgs|35.17|20.36|280.31|0.00|
|base|task_28|local|local|385.73|385.73|418.51|0.00|
|base|task_60|ground|cgs|35.11|17.08|24.11|7.03|
|base|task_85|local|local|910.44|910.44|920.52|0.00|
|base|task_102|cgs|cgs|35.18|24.49|328.55|0.00|
|base|task_117|cgs|local|110.99|111.06|142.80|0.07|
|base|task_137|ground|cgs|35.14|19.77|33.71|13.94|
|base|task_169|cgs|local|45.56|318.19|61.72|272.63|
|compute_4|task_19|local|local|1075.75|1075.76|1095.80|0.00|
|compute_4|task_43|cgs|ground|136.52|376.66|79.65|297.01|
|compute_4|task_48|ground|cgs|336.16|31.12|34.25|3.13|
|compute_4|task_72|local|local|885.44|885.44|912.12|0.00|
|compute_4|task_102|cgs|cgs|190.70|32.55|336.87|0.00|
|compute_4|task_132|local|local|216.27|216.38|318.66|0.00|
|compute_4|task_151|cgs|cgs|210.70|29.30|252.90|0.00|
|compute_4|task_166|cgs|cgs|136.54|90.53|126.46|0.00|
|compute_10|task_17|ground|ground|1315.99|1316.00|121.73|0.00|
|compute_10|task_27|cgs|cgs|905.36|415.61|478.94|0.00|
|compute_10|task_63|local|local|920.42|920.42|960.93|0.00|
|compute_10|task_69|cgs|cgs|1521.09|75.16|75.16|0.00|
|compute_10|task_100|cgs|local|945.58|945.59|985.99|0.01|
|compute_10|task_117|cgs|cgs|500.30|128.11|556.27|0.00|
|compute_10|task_154|cgs|cgs|1015.55|46.48|1056.17|0.00|
|compute_10|task_160|cgs|ground|485.29|610.93|198.93|412.00|
|load_2|task_6|ground|cgs|35.18|21.90|968.19|946.29|
|load_2|task_51|cgs|local|56.00|56.00|84.59|0.00|
|load_2|task_109|cgs|cgs|35.17|14.24|51.96|0.00|
|load_2|task_135|cgs|cgs|35.14|13.72|69.03|0.00|
|load_2|task_211|cgs|cgs|2690.28|1290.28|2712.41|0.00|
|load_2|task_232|local|cgs|265.31|166.83|183.26|98.48|
|load_2|task_306|cgs|local|90.71|90.76|122.79|0.05|
|load_2|task_326|cgs|local|65.51|65.86|78.82|0.35|
|load_4|task_49|ground|local|73.37|173.49|140.29|66.92|
|load_4|task_118|ground|local|45.70|85.89|69.76|24.06|
|load_4|task_218|local|cgs|73.18|30.53|1109.49|42.65|
|load_4|task_323|cgs|cgs|56.45|41.71|43.68|0.00|
|load_4|task_438|local|local|937.19|937.37|958.63|0.00|
|load_4|task_613|cgs|cgs|91.49|91.35|122.92|0.00|
|load_4|task_658|cgs|cgs|66.04|43.37|228.51|0.00|

## 抽样完整性异常

本次误把到达图像作为抽样母体；load_4/task_535未进入完整图首次选择，因而没有该状态的三分支。已运行117条分支均正常，不属于仿真算法失败。
这不是不可判定标签，而是未取得决策状态。不能把0/40不可判定解释为完整40状态覆盖，也不能以39状态替代验收。本轮不替换样本、不重跑；停止等待用户确认如何补齐。
建议后续只修正抽样母体为实际可干预的首次整图决策状态，在查看补充标签前固定抽取规则和哈希；不动方法或场景。
以下保留失败条件的原始完整性断言和配置：

[
  {
    "status": "FAILED",
    "condition": "load_4",
    "captured_states": 7,
    "traceback": "Traceback (most recent call last):\n  File \"/Users/bytedance/Projects/bs/analysis/sar_selector_pilot.py\", line 320, in worker\n    audits = ctl.audit_controls()\n             ^^^^^^^^^^^^^^^^^^^^\n  File \"/Users/bytedance/Projects/bs/analysis/sar_selector_pilot.py\", line 278, in audit_controls\n    assert len(audits) == 8 and all(a[\"passed\"] for a in audits), audits\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\nAssertionError: [{'task_id': 'task_49', 'rule_choice': 'ground', 'passed': True, 'parent_completion_seconds': 6555.29, 'branch_completion_seconds': 6555.29}, {'task_id': 'task_118', 'rule_choice': 'ground', 'passed': True, 'parent_completion_seconds': 15054.76, 'branch_completion_seconds': 15054.76}, {'task_id': 'task_218', 'rule_choice': 'local', 'passed': True, 'parent_completion_seconds': 28268.18, 'branch_completion_seconds': 28268.18}, {'task_id': 'task_323', 'rule_choice': 'cgs', 'passed': True, 'parent_completion_seconds': 40961.71, 'branch_completion_seconds': 40961.71}, {'task_id': 'task_438', 'rule_choice': 'local', 'passed': True, 'parent_completion_seconds': 55352.19, 'branch_completion_seconds': 55352.19}, {'task_id': 'task_613', 'rule_choice': 'cgs', 'passed': True, 'parent_completion_seconds': 72901.35, 'branch_completion_seconds': 72901.35}, {'task_id': 'task_658', 'rule_choice': 'cgs', 'passed': True, 'parent_completion_seconds': 80318.37, 'branch_completion_seconds': 80318.37}]\n",
    "job": {
      "condition": "load_4",
      "config": "/Users/bytedance/Projects/bs/results/sar_selector_pilot_20261019/configs/load_4.json",
      "config_sha256": "a51a67eca6554e59a1b45c24664870bf9add099ec8007b472073412b36ac9ed7",
      "raw_hops": 8,
      "sampling_seed": 20261023,
      "samples": [
        {
          "task_id": "task_49",
          "arrival_index": 49,
          "release_seconds": 6415.0
        },
        {
          "task_id": "task_118",
          "arrival_index": 118,
          "release_seconds": 14985.0
        },
        {
          "task_id": "task_218",
          "arrival_index": 218,
          "release_seconds": 28195.0
        },
        {
          "task_id": "task_323",
          "arrival_index": 323,
          "release_seconds": 40920.0
        },
        {
          "task_id": "task_438",
          "arrival_index": 438,
          "release_seconds": 54415.0
        },
        {
          "task_id": "task_535",
          "arrival_index": 535,
          "release_seconds": 65915.0
        },
        {
          "task_id": "task_613",
          "arrival_index": 613,
          "release_seconds": 72810.0
        },
        {
          "task_id": "task_658",
          "arrival_index": 658,
          "release_seconds": 80275.0
        }
      ]
    }
  }
]
