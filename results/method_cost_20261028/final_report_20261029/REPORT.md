# METHOD-COST实测汇总

Orin MAXN模式0、动态时钟，CPU线程1/2，batch=1，50次预热，每方法67个评分窗决策各20次重复。Mac Apple M5 Pro单进程/单线程，batch=1。单位ms；每格P50/P95/P99。

## Orin

|条件/方法/线程|观测候选重建|actor（host输入）|actor（resident输入）|重建+host actor逐次和|进程RSS MiB|
|---|---|---|---|---|---:|
|orin_unloaded_v2/fixed_cpu_t1|705.115/738.371/750.445|1.760/1.865/1.889|1.089/1.167/1.190|706.887/740.170/752.164|812.62|
|orin_unloaded_v2/dynamic_cpu_t1|1148.976/1194.030/1650.235|2.769/2.886/2.945|1.944/2.004/2.043|1151.711/1196.767/1652.875|816.40|
|orin_unloaded_v2/fixed_cpu_t2|690.568/726.123/740.630|1.988/2.065/2.088|1.281/1.388/1.411|692.500/728.116/742.686|817.32|
|orin_unloaded_v2/dynamic_cpu_t2|1163.417/1201.553/1671.833|3.232/3.329/3.380|2.360/2.434/2.477|1166.606/1204.786/1674.907|815.56|
|orin_unloaded_v2/fixed_cuda_0_t1|695.747/729.371/742.522|4.072/4.620/4.682|2.023/2.087/2.120|700.107/733.660/746.747|1057.96|
|orin_unloaded_v2/dynamic_cuda_0_t1|1150.615/1203.221/1638.998|5.950/6.538/7.241|3.550/3.673/3.735|1156.466/1209.225/1645.166|1159.44|
|orin_unloaded_v2/fixed_cuda_0_t2|698.826/729.332/741.525|4.187/4.702/4.759|2.090/2.155/2.196|703.144/733.455/746.176|1052.41|
|orin_unloaded_v2/dynamic_cuda_0_t2|1164.034/1198.933/1672.485|6.225/6.903/7.495|3.708/3.814/3.869|1170.428/1205.553/1678.661|1050.41|
|orin_sar_loaded/fixed_cpu_t1|698.536/736.362/750.202|1.780/1.970/2.046|1.088/1.172/1.244|700.294/738.046/752.075|799.39|
|orin_sar_loaded/dynamic_cpu_t1|1180.615/1226.307/1752.117|2.878/3.243/3.481|1.994/2.199/2.420|1183.492/1229.172/1754.869|801.43|
|orin_sar_loaded/fixed_cpu_t2|708.639/747.229/768.507|1.962/2.470/2.879|1.186/1.657/2.003|710.667/749.283/770.494|799.88|
|orin_sar_loaded/dynamic_cpu_t2|1175.326/1216.253/1757.846|3.033/3.707/4.248|2.056/2.544/3.121|1178.399/1219.574/1760.920|800.94|

Orin逐次和不含采样、承诺提交、执行器准入、状态导出/本地视图，不称为完整端到端决策。GPU组仅actor在GPU；观测仍在CPU。host口径包括张量准备及GPU拷贝，resident口径为已驻留输入，CUDA计时同步。

## Mac完整流程

### fixed

|分母/分项|n|P50/P95/P99 ms|
|---|---:|---|
|decisions/observation_s|67|119.384/159.371/185.502|
|decisions/forward_s|67|0.275/0.315/0.376|
|decisions/sampling_s|67|0.184/0.223/1.540|
|source_calls/total_s|67|148.756/187.475/211.742|
|source_calls/observation_s|67|119.384/159.371/185.502|
|source_calls/forward_s|67|0.275/0.315/0.376|
|source_calls/sampling_s|67|0.184/0.223/1.540|
|source_calls/commitment_s|67|26.959/31.278/31.616|
|active_batches/state_export_s|64|7.860/72.990/77.488|
|active_batches/control_enqueue_s|64|0.003/0.003/0.004|
|active_batches/decision_filter_s|64|0.073/0.090/0.108|
|active_batches/local_view_s|64|13.605/80.023/95.207|
|active_batches/policy_s|64|148.864/222.089/298.192|
|active_batches/executor_admission_s|64|86.920/219.907/299.233|
|active_batches/total_computation_s|64|286.291/521.733/606.789|
|all_scoring_batches/state_export_s|3600|7.795/80.954/133.750|
|all_scoring_batches/control_enqueue_s|3600|0.002/255.905/300.658|
|all_scoring_batches/decision_filter_s|3600|0.031/0.039/0.073|
|all_scoring_batches/local_view_s|3600|0.000/0.000/13.436|
|all_scoring_batches/policy_s|3600|0.000/0.000/145.025|
|all_scoring_batches/executor_admission_s|3600|0.000/0.000/62.170|
|all_scoring_batches/total_computation_s|3600|8.419/321.278/396.672|

source_calls含全录制期活跃源调用（包括仅提交的随访调用），没有逐调用时间戳；active_batches为评分小时内有新决策的批次，all_scoring_batches为全部3600个一秒批次，不同分母不可相加。

### dynamic

|分母/分项|n|P50/P95/P99 ms|
|---|---:|---|
|decisions/observation_s|67|191.285/223.541/263.061|
|decisions/forward_s|67|0.377/0.458/0.568|
|decisions/sampling_s|67|0.180/0.205/0.349|
|source_calls/total_s|67|272.863/314.998/345.954|
|source_calls/observation_s|67|191.285/223.541/263.061|
|source_calls/forward_s|67|0.377/0.458/0.568|
|source_calls/sampling_s|67|0.180/0.205/0.349|
|source_calls/commitment_s|67|78.588/114.246/118.595|
|active_batches/state_export_s|64|8.806/71.619/74.714|
|active_batches/control_enqueue_s|64|0.002/0.003/0.003|
|active_batches/decision_filter_s|64|0.073/0.083/0.114|
|active_batches/local_view_s|64|13.288/72.097/83.794|
|active_batches/policy_s|64|273.598/359.007/536.320|
|active_batches/executor_admission_s|64|39.850/240.481/443.295|
|active_batches/total_computation_s|64|372.101/815.605/895.466|
|all_scoring_batches/state_export_s|3600|8.133/81.459/166.173|
|all_scoring_batches/control_enqueue_s|3600|0.002/257.721/309.457|
|all_scoring_batches/decision_filter_s|3600|0.030/0.038/0.074|
|all_scoring_batches/local_view_s|3600|0.000/0.000/13.213|
|all_scoring_batches/policy_s|3600|0.000/0.000/267.477|
|all_scoring_batches/executor_admission_s|3600|0.000/0.000/25.643|
|all_scoring_batches/total_computation_s|3600|8.931/323.862/424.004|

source_calls含全录制期活跃源调用（包括仅提交的随访调用），没有逐调用时间戳；active_batches为评分小时内有新决策的批次，all_scoring_batches为全部3600个一秒批次，不同分母不可相加。

## 内存与候选规模

|组|RSS峰值 MiB|CUDA分配/保留峰值 MiB|
|---|---:|---:|
|orin_unloaded_v2/fixed_cpu_t1|812.617|不使用CUDA|
|orin_unloaded_v2/dynamic_cpu_t1|816.402|不使用CUDA|
|orin_unloaded_v2/fixed_cpu_t2|817.320|不使用CUDA|
|orin_unloaded_v2/dynamic_cpu_t2|815.559|不使用CUDA|
|orin_unloaded_v2/fixed_cuda_0_t1|1057.961|9.268/22.000|
|orin_unloaded_v2/dynamic_cuda_0_t1|1159.441|9.274/22.000|
|orin_unloaded_v2/fixed_cuda_0_t2|1052.410|9.268/22.000|
|orin_unloaded_v2/dynamic_cuda_0_t2|1050.410|9.274/22.000|
|orin_sar_loaded/fixed_cpu_t1|799.391|不使用CUDA|
|orin_sar_loaded/dynamic_cpu_t1|801.430|不使用CUDA|
|orin_sar_loaded/fixed_cpu_t2|799.883|不使用CUDA|
|orin_sar_loaded/dynamic_cpu_t2|800.941|不使用CUDA|
|Mac/fixed|315.781|不使用CUDA|
|Mac/dynamic|326.891|不使用CUDA|

固定16块、动态4/16块观测的保留计算星候选均值3.448、P50/P95/P99/最大均4；不含源星与地面，不含padding，不冒充TopK前候选数。

## GPU负载覆盖

|CPU组|全进程覆盖 s|GPU均值 %|GPU非零样本比例 %|系统RAM采样峰值 MB|
|---|---:|---:|---:|---:|
|fixed_cpu_t1|965.036|68.723|72.598|18761|
|dynamic_cpu_t1|1673.710|72.811|76.572|19088|
|fixed_cpu_t2|980.976|73.282|77.049|19033|
|dynamic_cpu_t2|1667.658|70.578|74.065|19063|

四组GPU利用率P50/P95/P99均99%，但各组并非每个采样时刻均满载；CPU预处理与cohort边界产生空隙。负载cohort结束间隔最大2.053秒，遥测整秒时间戳最大间隔1秒。系统RAM包括独立GPU负载和其他进程，不是actor增量内存。

## 审计与适用范围

四组负载CPU过程均被4455个连续双流cohort覆盖，共8910窗口；负载为原FP32 YOLO11n-OBB受控SAR后处理代理，不是SAR识别精度验证。
完整覆盖、GPU利用率分位数与样本数见AUDIT_AND_SUMMARY.json.loaded_coverage。源/模型/快照hash及全部12组原始计时重新核验；float32输入、掩码、离散元数据逐位一致。float64连续特征末位差最大2.22e-16按已登记修订保留，概率容差atol1e-5/rtol1e-4不变。
Mac PyTorch2.12.0/NumPy2.4.4/Python3.12.13；Orin PyTorch2.3.0/NumPy1.23.5/Python3.10.15。源与冻结权重一致，框架版本不同。67个唯一输入的重复计时不能视为1340个独立任务样本。
仅计算开销，排除快照复制、磁盘录制、传输与等待；重建从合法本地状态开始，通告传输和等待已在仿真通信模型中计入。critic仅地面训练。
四块内存独立实测见../MEMORY_REPORT.md；其特定几何与配置不能外推全队列或在轨可行性。RSS、CUDA及系统统一RAM相互重叠，不能相加。
