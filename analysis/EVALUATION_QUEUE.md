# HTTP评估队列

`evaluation_queue.py`提供SQLite事务队列，`run_ground_queue_20261030.py`是当前扩站实验的冻结入口适配器。

当前部署在devbox3的内网`10.251.250.34:18765`，Mac及三台devbox通过HTTP访问。凭据文件位于各机用户目录`.local/state/bs-evaluation-queue/token`，文件权限0600；不提交凭据。内网HTTP没有TLS，仅用于当前受信任机器网络。

## HTTP接口

所有接口为POST JSON，并要求`Authorization: Bearer <token>`。

| 路径 | 用途 |
| --- | --- |
| `/experiments` | 提交不可覆盖的实验规范和任务列表；相同内容可幂等重传 |
| `/release` | 提交旧执行器收尾证据，导入既有PASS并释放从未开始的任务 |
| `/claim` | 按可用槽数原子领取；必须提供唯一request编号，重传保留原分配 |
| `/heartbeat` | 更新owner在途任务的心跳，不改变归属 |
| `/finish` | 上传结果原字节及SHA256，验证任务身份；相同结果重复提交幂等 |
| `/status` | 查询状态、归属、结果索引及心跳超时标记 |
| `/block` | 停止新领取，保留所有在途归属 |

任务状态为RESERVED、PENDING、RUNNING、PASS、FAILED。心跳过期不回收任务，不存在自动重跑。SQLite数据库和`artifacts/`必须一起保留；仅重启服务不能重新初始化数据库或重新创建worker。worker异常退出后先核查其进程、领取记录及输出，不能直接启动替代worker。

## 当前实验

入口`python -B -m analysis.run_ground_queue_20261030 launch <role>`，role为server、local、devbox1、devbox2、devbox3或coordinator。启动排他，存在launch_request时拒绝重复启动。server只在devbox3启动；coordinator在Mac运行，负责最终下载和原报告审计。运行前必须已登记且部署匹配的源码、输入、模型以及凭据，不能拿此命令直接重跑已启动角色。

Mac并发9，三台devbox各6，均spawn和Torch/BLAS单线程。空闲槽立刻领任务；任何失败在服务端阻断后续领取，已领取任务收尾。L保持Mac执行，L全部完成后Mac自动帮助M。

读`http_queue_20261030/QUEUE_STATUS.json`及每角色的`launch.json/status.json/final_status.json/supervisor_failure.json`。旧v2显式停止后的BLOCKED是否为正常交接，以新`LEGACY_RELEASE.json`的逐任务证据为准，不能仅凭状态字符串判断。

## 后续实验

新建独立实验id，先冻结任务规范与科学协议，再通过`/experiments`提交。没有旧执行器的新任务也先保留为RESERVED，以`/release`空completed集合明确释放。每项任务需要id、job、output、legacy_host和允许hosts；实验需要各机并发上限与截止时间。

本HTTP层不接受任意shell命令。后续实验应增加对应的worker适配器，调用其冻结评估入口，并保留相同的领取、哈希校验和结果提交流程。增加机器时同时登记host及输入/运行环境，不能仅靠修改运行中的实验spec绕过冻结检查。

验证：`PYTHONPATH=tests:. .venv/bin/python -B -m unittest test_evaluation_queue -v`。
