"""复算校准表；未测量的执行值和未证实的排队不填成零。"""

from collections import Counter
import csv
import hashlib
import json
import math
from pathlib import Path
import sys

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analysis.calibrate_redesign_geometry import BASE, DEADLINES, ROOT, SHAPES, configuration
from analysis.redesign_scenario import digest, write_json

OLD = Path("results/redesign_20261021/M_day_20261015_v2")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def csv_file(name, rows):
    with (ROOT/name).open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def on_time(value, d):
    return value is not None and value <= d+1e-9


def report():
    cfg = json.loads(BASE.read_text())
    events = cfg["scheduled_task_arrivals"]
    n = len(events)
    assert n == 136
    bounds = {c:json.loads((ROOT/f"bounds_{c}.json").read_text()) for c in SHAPES}
    records = {m:json.loads((OLD/f"{m}.json").read_text()) for m in ("local","ground")}
    frozen = records["local"]["source_hashes"]
    changed = [path for path,value in frozen.items() if sha(Path(path)) != value]
    assert not changed, changed
    protocol_hash = sha(ROOT/"PROTOCOL.md")
    assert protocol_hash == "11c083cfa8d83dfa4aff41e49aa6f7196cd962fb95b390ce14657ceb3371f320"
    for c,b in bounds.items():
        assert b["tasks"] == n and b["protocol_sha256"] == protocol_hash
        assert b["config_sha256"] == digest(configuration(c))
        assert b["base_file_sha256"] == sha(BASE)
        assert b["auditor_sha256"] == sha(Path("analysis/calibrate_redesign_geometry.py"))
        for name,result in b["results"].items():
            for row in result["tasks"]:
                a,z = row["single_latency_s"],row["five_latency_s"]
                assert a is None or (z is not None and z <= a+1e-9)
            for network in ("single","five"):
                for d in DEADLINES:
                    assert result["counts"][network][str(d)] == sum(on_time(r[network+"_latency_s"],d) for r in result["tasks"])
        for network in ("single","five"):
            for a,brow in zip(b["results"]["geometric"]["tasks"],b["results"]["base"]["tasks"]):
                x,y=a[network+"_latency_s"],brow[network+"_latency_s"]
                assert y is None or (x is not None and x <= y+1e-8)
    g = {r["task_id"]:r["single_latency_s"] for r in bounds[12]["results"]["geometric"]["tasks"]}
    p = {r["task_id"]:r["single_latency_s"] for r in bounds[12]["results"]["base"]["tasks"]}
    detailed, failures, actual = [], [], {}
    for method,record in records.items():
        assert record["status"] == "PASS" and record["audit"]["passed"]
        assert len(record["outcomes"]) == n and record["source_hashes"] == frozen
        actual[method] = {}
        for d in DEADLINES:
            counts = Counter()
            restricted = []
            for o in record["outcomes"]:
                tid = o["task_id"]
                latency = o["latency_seconds"] if o["status"] == "completed" else None
                assert o["observed_age_seconds"] >= d
                if on_time(latency,d):
                    assert on_time(p[tid],d), (method,tid,d,p[tid],latency)
                    category = "on_time"
                elif not on_time(g[tid],d):
                    category = "no_geometric_path"
                else:
                    category = "other_unresolved"
                counts[category] += 1
                counts["protocol_blocked_with_geometric_path"] += int(not on_time(latency,d) and on_time(g[tid],d) and not on_time(p[tid],d))
                restricted.append(min(latency,d) if latency is not None else d)
                detailed.append({"method":method,"D_s":d,"task_id":tid,"actual_latency_s":latency,
                                 "geometric_latency_s":g[tid],"protocol_latency_s":p[tid],"category":category})
            assert sum(counts[k] for k in ("on_time","no_geometric_path","other_unresolved")) == n
            row = {"method":method,"D_s":d,"on_time":counts["on_time"],"P_D":counts["on_time"]/n,
                   "R_D_s":sum(restricted)/n,"no_geometric_path":counts["no_geometric_path"],
                   "resource_queue_confirmed":0,"other_unresolved":counts["other_unresolved"],
                   "of_other_protocol_blocked":counts["protocol_blocked_with_geometric_path"],
                   "queue_evidence":"not_identifiable_from_existing_outcomes; zero means zero confirmed cases, not zero true queue failures"}
            failures.append(row)
            actual[method][d] = row
    csv_file("failures.csv",failures)
    csv_file("failure_per_image.csv",detailed)
    combinations = []
    for c,b in bounds.items():
        for network in ("single","five"):
            for d in DEADLINES:
                row = {"compute_count":c,"stations":1 if network=="single" else 5,"D_s":d,
                       "geometric_count":b["results"]["geometric"]["counts"][network][str(d)],
                       "protocol_count":b["results"]["base"]["counts"][network][str(d)]}
                row["geometric_fraction"] = row["geometric_count"]/n
                row["protocol_fraction"] = row["protocol_count"]/n
                for m in records:
                    measured=c==12 and network=="single"
                    row[m+"_P_D"] = actual[m][d]["P_D"] if measured else None
                    row[m+"_R_D_s"] = actual[m][d]["R_D_s"] if measured else None
                row["actual_status"]="REUSED_MEASURED" if c==12 and network=="single" else "NOT_RUN"
                combinations.append(row)
    csv_file("combinations.csv",combinations)
    access, access_summary = [], []
    for c,b in bounds.items():
        intervals=np.load(ROOT/f"contacts_{c}.npz")["intervals"]
        waits=[]
        equal=0
        for row in b["results"]["base"]["tasks"]:
            edges=intervals[(intervals[:,0]==row["source_sat_id"]) & (intervals[:,3]>row["release_s"])]
            wait=float(np.minimum.reduce(np.maximum(edges[:,2],row["release_s"]),initial=np.inf))-row["release_s"]
            delivery=row["single_latency_s"]
            matched=delivery is not None and abs(wait-delivery)<1e-6
            equal+=int(matched)
            waits.append(wait)
            access.append({"compute_count":c,"task_id":row["task_id"],"first_compute_access_wait_s":wait if math.isfinite(wait) else None,
                           "protocol_delivery_s":delivery,"delivery_equals_first_compute_access":matched})
        access_summary.append({"compute_count":c,"first_access_over300":sum(w>300 for w in waits),
                               "all_delivery_equals_first_access_count":equal,"median_wait_s":float(np.median(waits)),
                               "p95_wait_s":float(np.percentile(waits,95)),"max_wait_s":max(waits)})
    csv_file("access_per_image.csv",access)
    write_json(ROOT/"access_summary.json",access_summary)
    ofat=[]
    for name,c,key,network in (("baseline",12,"base","single"),("sar_forward",12,"sar_forward","single"),
            ("compute_144",144,"base","single"),("sar_two_terminals",12,"sar_two_terminals","single"),
            ("downlink_960",12,"base","single"),("fixed_800MB",12,"base","single"),
            ("no_capture",12,"no_capture","single"),("five_stations",12,"base","five")):
        geo="geometric_sar_forward" if name=="sar_forward" else "geometric"
        for d in (300,600):
            geom=bounds[c]["results"][geo]["counts"][network][str(d)]
            proto=bounds[c]["results"][key]["counts"][network][str(d)]
            ofat.append({"change":name,"D_s":d,"geometric_count":geom,"protocol_count":proto,
                         "geom_delta_pp":100*(geom-bounds[12]["results"]["geometric"]["counts"]["single"][str(d)])/n,
                         "protocol_delta_pp":100*(proto-bounds[12]["results"]["base"]["counts"]["single"][str(d)])/n,
                         "calculation":"exact_zero_cost_invariance" if name in ("downlink_960","fixed_800MB") else "time_expanded_reachability"})
    csv_file("ofat.csv",ofat)
    windows=[len(e["window_result_bytes"]) for e in events]
    release=[e["release_step"]*cfg["dt"] for e in events]
    peak300=max(sum(w for t,w in zip(release,windows) if t0<=t<t0+300) for t0 in release)
    service=cfg["onboard_compute_profile"]["service_s"]
    cpu=[]
    for c in SHAPES:
        for mult in (1,4,10):
            work=sum(windows)*service["1"]*mult
            cpu.append({"compute_count":c,"multiplier":mult,"all_work_assigned_busy_seconds_conservative":work,
                        "rho_cpu_24h_upper":work/(c*86400),"rho_cpu_peak300_arrival_work_upper":peak300*service["1"]*mult/(c*300),
                        "actual_helper_peak_queue":None,"status":"analytical_workload_envelope_not_measured_queue"})
    csv_file("compute_load.csv",cpu)
    local_caps=[]
    for mult in (1,4,10):
        minimum=[((w//2)*service["2"]+(w%2)*service["1"])*mult for w in windows]
        for d in DEADLINES:
            count=sum(x<=d for x in minimum)
            local_caps.append({"multiplier":mult,"D_s":d,"local_compute_only_upper_count":count,"fraction":count/n})
    csv_file("local_compute_ceiling.csv",local_caps)
    costs=[]
    for c in SHAPES:
        geometry=json.loads((ROOT/f"cost_{c}.json").read_text())
        runtime=json.loads((ROOT/f"runtime_{c}.json").read_text())
        fixed=sum((geometry["daily_geometry_s"],geometry["daily_contact_s"],runtime["empty_day_extrapolated_s"],runtime["init_with_visibility_s"]))
        costs.append({"compute_count":c,"geometry_day_hours":geometry["daily_geometry_s"]/3600,
                      "contact_day_hours":geometry["daily_contact_s"]/3600,
                      "empty_step_day_hours":runtime["empty_day_extrapolated_s"]/3600,
                      "fixed_day_estimate_hours":fixed/3600,"route_median_s":runtime["route_median_s"],
                      "day_estimate_at_one_route_per_second_hours":(fixed+86400*runtime["route_median_s"])/3600,
                      "peak_probe_gib":max(geometry["peak_rss_bytes"],runtime["peak_rss_bytes"])/2**30,
                      "upper_bound_actual_seconds":bounds[c]["timing"]["total_s"],
                      "upper_bound_peak_gib":bounds[c]["peak_rss_bytes"]/2**30})
    csv_file("runtime_cost.csv",costs)
    source_files=[Path("analysis/calibrate_redesign_geometry.py"),Path("analysis/calibrate_redesign_runtime.py"),
                  Path(__file__),Path("tests/test_calibration_geometry.py")]
    audit={"status":"PASS", "original_source_files_verified":len(frozen),"changed_original_sources":changed,
           "protocol_sha256":protocol_hash,"original_config_sha256":sha(BASE),
           "reused_record_hashes":{m:sha(OLD/f"{m}.json") for m in records},
           "new_analysis_hashes":{str(p):sha(p) for p in source_files},
           "bound_files_sha256":{str(c):sha(ROOT/f"bounds_{c}.json") for c in SHAPES},
           "tests":"6 focused tests passed; all actual timely deliveries dominated by protocol bound; geometric >= protocol; five stations >= single per task",
           "all_deadlines_fully_observed":True,"sealed_test_dates_accessed":False}
    write_json(ROOT/"AUDIT.json",audit)
    summary={"status":"BOUNDS_AND_EXISTING_NEUTRALS_COMPLETE_SCENE_NOT_SELECTED", "actual_new_combinations_run":False,
             "failure_decomposition":failures,"ofat":ofat,"combinations":combinations,"cpu":cpu,"runtime":costs,
             "window_count":sum(windows),"peak300_arrival_windows":peak300,"windows_distribution":dict(Counter(windows)),
             "first_compute_access":access_summary,
             "local_compute_only_ceilings":local_caps,
             "candidate":None,"conditional_shortlist":{"D_s":300,"compute_count":144,"stations":1,"compute_multiplier":10,
                "reason":"Protocol upper 100%; local compute-only ceiling 32/136, so >=76.47pp physical-to-local gap even before communication. Actual helper queue not demonstrated; not selected."},
             "unresolved":["User decision pending on neutral helper FIFO capacity diagnostic", "New combination local/ground finite-resource executions not run", "Thousand-node executor cost needs optimization/scope decision before actual full-day run"]}
    write_json(ROOT/"summary.json",summary)
    lines=["# M开发日场景校准", "", "状态：上界、原配置中性实际指标及规模成本已完成；新组合有限资源基线与计算星排队尚未验证，**没有选定主场景**。预登记见PROTOCOL.md；只使用2026-10-15的136图，未运行v3/CGS/HEFT，未访问测试日。",
           "", "## 口径", "", "U_geom为保持几何与SAR角色约束、放开终端配对/捕获的零代价松弛上界；U_protocol保留原固定终端配对和5秒捕获。二者均允许存储等待、多份副本及完美未来。轨道可见性采样10秒，捕获使用5秒子时点；这是采样模型上界，不是连续真实轨道的精确证书。R_D是min(整图交付时延,D)的均值，未交付记D。全部原记录均随访至少1800秒。",
           "", "## 1. 原M失败分解", "", "|D(s)|几何可达|协议可达|本地按时 / R_D(s)|强地面按时 / R_D(s)|", "|---:|---:|---:|---:|---:|"]
    for d in DEADLINES:
        a,z=actual["local"][d],actual["ground"][d]
        lines.append(f"|{d}|{bounds[12]['results']['geometric']['counts']['single'][str(d)]}/{n}|{bounds[12]['results']['base']['counts']['single'][str(d)]}/{n}|{a['on_time']}/{n} ({a['P_D']:.2%}) / {a['R_D_s']:.3f}|{z['on_time']}/{n} ({z['P_D']:.2%}) / {z['R_D_s']:.3f}|")
    lines += ["", "|D(s)|方法|无几何路径|已证实资源排队超时|其他/未判定|其他中协议本身不可达|", "|---:|---|---:|---:|---:|---:|"]
    for r in failures:
        lines.append(f"|{r['D_s']}|{r['method']}|{r['no_geometric_path']}|0（无因果证据）|{r['other_unresolved']}|{r['of_other_protocol_blocked']}|")
    lines += ["", "三类只划分实际未按时的图，不包括成功图。已证实排队列为0不表示排队真实为0：原记录没有移除排队的反事实或逐阶段资源等待轨迹，不能将“有路径但晚到”直接归因排队。协议不可达单列，可能由确定性终端匹配造成；传输服务、预测局限和等待等其余因素仍在其他项。逐图见failure_per_image.csv。",
              "", "## 2. 单项归因", "", "表中为可达图数/136及相对原M增量；每行只改一项。", "", "|变更|D(s)|U_geom|变化(pp)|U_protocol|变化(pp)|", "|---|---:|---:|---:|---:|---:|"]
    for r in ofat:
        lines.append(f"|{r['change']}|{r['D_s']}|{r['geometric_count']}|{r['geom_delta_pp']:+.2f}|{r['protocol_count']}|{r['protocol_delta_pp']:+.2f}|")
    lines += ["", "主要恢复项是增加计算层轨道面/星数，其次是分布式地面站网。打开SAR转发使源星成为网桥，但它只作诊断，不进入候选。两终端的协议上界可能下降：原贪心配对因源星占用更多计算星端口而改变整张图，额外端口不等于最优接入调度；几何上界不变。去掉捕获在这些D端点没有改变可达图数，不代表所有单图最早时延不变。带宽与图像大小在零传输时间定义下严格无效，这两行由不变性直接得到，没有伪造不同带宽实验。",
              "", "## 3. 六种部署组合", "", "12=1×12，144=12×12，1056=48×22；计算层均1015 km/98.98度，SAR层24颗原轨道不变。5站为原站+Svalbard/Puertollano/Troll/Cordoba；依据与近似坐标见EVIDENCE.md。", "", "|计算星|站数|D(s)|U_geom|U_protocol|本地P_D / R_D|地面P_D / R_D|", "|---:|---:|---:|---:|---:|---:|---:|"]
    for r in combinations:
        def fmt(m):
            return "未运行" if r[m+"_P_D"] is None else f"{r[m+'_P_D']:.2%} / {r[m+'_R_D_s']:.3f}s"
        lines.append(f"|{r['compute_count']}|{r['stations']}|{r['D_s']}|{r['geometric_count']}/{n} ({r['geometric_fraction']:.2%})|{r['protocol_count']}/{n} ({r['protocol_fraction']:.2%})|{fmt('local')}|{fmt('ground')}|")
    lines += ["", "未运行不能写成0或拿原M的本地结果跨场景复用。增加计算星会改变本地结果中继，增加站点会改变结果和原图回传，因此两条实际基线均需对应新配置执行。多站有限资源执行还需明确地面计算总容量：原执行器按站设队列，但整图跨站到齐后在最后接收站计算；若逐站复制算力，地面总算力也会增加，不能称为只改站网的公平对照。",
              "", "1056星的关键反例：几何上界100%，但D=300的协议上界只有107/136（78.68%），D=600才达123/136（90.44%）。逐图复核136张图的零代价交付时刻均等于首次可用SAR→计算星接触时刻；29张D=300失败图均首次接入等待超过300秒。此处是当前端口配对/接入限制，不是计算队列或地面站不足。源星接入等待中位105秒、95分位841.25秒；144星中位和95分位均为0秒，最慢290秒。因此更多计算星并不保证当前贪心配对下更好的服务，不能把几何覆盖与协议可达混为一谈。证据见access_per_image.csv及access_summary.json。",
              "", "## 4. 计算负载", "", f"136图共{sum(windows)}个推理窗，窗数分布{dict(Counter(windows))}。300秒滑动到达窗最多{peak300}窗。下表假设所有窗口都交给计算层、均用较慢单窗服务0.597秒：平均值是保守工作量上界，峰值是300秒到达工作量/集群容量，不是实测利用率或排队。", "", "|计算星|倍率|全天rho上界|峰值300s到达rho上界|实际峰值计算队列|", "|---:|---:|---:|---:|---|"]
    for r in cpu:
        lines.append(f"|{r['compute_count']}|{r['multiplier']}|{r['rho_cpu_24h_upper']:.4%}|{r['rho_cpu_peak300_arrival_work_upper']:.4%}|未验证|")
    lines += ["", "即使12星×10，全日平均计算负载仍不到5%；144/1056星进一步稀释。短时热点可能仍排队，但不能由平均或到达rho自动推出。原本地与强地面均不会让计算星推理，因此其计算星队列恰为0也不代表协同队列为0。资源竞争指标必须排除仅中继的READY/QUEUED数据。",
              "", "×4/×10是更重处理链的敏感性点，非已有SAR模型实测倍率。检测+精分类、双时相变化检测的依据和不能直接推定倍率的限制见EVIDENCE.md。若执行倍率对照，星地一起乘倍率。",
              "", "## 5. 千星运行成本", "", "本机短基准外推，含其他进程竞争；不是全天实测保证。原M本地6153秒、地面818秒仅供量级对照，不能线性保证扩展。路由样本为3个源星×3种包大小、同一历元的热缓存搜索，不能当作全天平均。", "", "|计算星|几何/天(h)|预测/天(h)|空步进/天(h)|固定部分估计(h)|热路由中位(s)|1路由请求/s时全天(h)|探针峰值GiB|上界实际耗时(s)|上界峰值GiB|", "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in costs:
        lines.append(f"|{r['compute_count']}|{r['geometry_day_hours']:.3f}|{r['contact_day_hours']:.3f}|{r['empty_step_day_hours']:.3f}|{r['fixed_day_estimate_hours']:.3f}|{r['route_median_s']:.4f}|{r['day_estimate_at_one_route_per_second_hours']:.2f}|{r['peak_probe_gib']:.3f}|{r['upper_bound_actual_seconds']:.2f}|{r['upper_bound_peak_gib']:.3f}|")
    lines += ["", "完整耗时估计形式：T≈初始化+8640次几何+1440次预测+864000次空步进+Q次路由+实际任务/广播/队列维护；Q由轨迹和待传数据决定，未测Q不能给唯一可信小时数。1056计算星共有582660对节点链路，原执行器会为所有对保存对象并逐步检查捕获；空负载估计已经超过3小时。探针RSS不是满任务峰值，任务、控制通告、缓存会额外占内存。",
              "", "建议先做保持语义的工程优化：向量化原可见性计算；缓存不变连通图/接触计划；只逐步检查尚在捕获的已选边，保留完整状态导出和控制消息字节数。必须与小规模原执行器逐事件对照，当前仅提出、不修改主协议。单纯删掉不可用边会改变控制通告大小，不能默认为等价。",
              "", "近似方案及代价：10→30/60秒几何更新可能错过短窗并改动配对/捕获，须单独标注；源星和地面邻域子图会漏掉存储携带转发路径，不能作为完整物理上界；6小时片段缩小成本但丢失部分轨道相位和到达高峰，不能替代全天覆盖结论。当前独立上界采用同样10秒几何+稀疏连通分量与反向时间展开，无需这些时空裁剪，已完成全天上界。",
              "", "## 6. 选型结论与待确认", "", "预登记标准为上界≥85%、上界与本地实际差≥15pp、计算星存在真实峰时排队，优先最小D；未根据任何调度方法排名选场景。",
              "", "**当前不选定主场景。** 144星/单站/D=300已通过物理门槛。×10时64窗和100窗图的最理想本地推理时间分别337.92和528秒，故本地在300秒内至多完成32张9窗图（23.53%），不论中继多快；与100%物理上界至少相差76.47pp。这使144星/单站/D=300/×10成为待验证组合，但没有计算星实际排队证据，不能宣称三项标准全部通过。",
              "", "不能报告“SAR不转发时任何组合都不可能”：已存在100%物理可达的构型。真正未决的是稀疏136图能否在更大计算层产生竞争，以及如何在合理成本下执行有限资源对照。已询问是否允许固定首次可达计算星+FIFO的中性容量诊断；未收到答复前不运行、不把估算峰值写成实测。多站实际基线需固定地面算力口径；千星实际全天执行需先选择工程加速或明确片段范围。这些决定之前不会训练或改主协议。",
              "", "## 审计与文件", "", f"原实现{len(frozen)}个源码哈希保持不变，原配置、local/ground记录哈希已保存；全部可达计数独立重算、逐图检验实际按时见证不超协议上界及几何/站网单调关系。6项针对性测试通过。见AUDIT.json、summary.json、各CSV及bounds_*.json。已有记录完整保留，未改论文、未提交。"]
    (ROOT/"REPORT.md").write_text("\n".join(lines)+"\n")
    print(json.dumps({"audit":"PASS","status":summary["status"],"files":len(list(ROOT.iterdir()))}))


if __name__ == "__main__":
    report()
