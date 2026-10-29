/* 结果分析：只读取 result.json / tiles.json / events.jsonl 的引擎输出。
 * - 任务“已完成”= status === "completed"（全部分片结果到达地面并完成聚合），计算结束不算完成；
 * - 时延只覆盖已完成任务，并始终与完成率并列显示；null 显示“暂无数据”；
 * - 事件 time 与 tiles.json 时间字段是步号，秒 = 步 × dt。 */
"use strict";
(function () {
  const { $, $$, esc } = WB;
  const EVENT_LABEL = {
    created: "分片生成", queued: "入队", local_ready: "本地就绪", tx_start: "原始分片星间传输开始", tx_end: "原始分片星间传输结束",
    compute_start: "星上计算开始", computed: "星上计算结束（尚未交付）", result_tx_start: "结果星间中继开始", result_tx_end: "结果星间中继结束",
    downlink_start: "结果下传开始", downlink_end: "结果下传结束", done: "分片结果到达地面",
    tx_start_ground: "原始分片下传开始", tx_end_ground: "原始分片下传结束", ground_compute_start: "地面计算开始", ground_compute_end: "地面计算结束",
    ground_upload_start: "整图上传开始", ground_upload_end: "整图上传结束", failed: "分片失败", task_failed: "任务失败", task_delivered: "任务完整交付",
  };
  const STATE_LABEL = {
    CREATED: "已生成", QUEUED: "排队", TRANSFERRING: "星间传输", RESULT_TRANSFERRING: "结果中继", READY: "就绪", RUNNING: "计算中",
    COMPUTED: "已计算（未交付）", DOWNLINKING: "下传中", GROUND_QUEUED: "地面排队", GROUND_RUNNING: "地面计算", DONE: "已到达地面", FAILED: "失败",
  };
  const FAILURE_LABEL = { mem_full: "存储不足", vram_oom: "显存不足", link_down: "链路中断", deadline_miss: "超过截止期", no_route: "无可用路由", aggregation_deadline_miss: "聚合超过截止期" };
  const TASK_COLS = [
    { key: "task_id", label: "任务", type: "id" },
    { key: "source_sat_id", label: "源星", type: "num" },
    { key: "status", label: "状态", type: "str" },
    { key: "release_seconds", label: "到达 s", type: "num" },
    { key: "deadline_seconds", label: "截止 s", type: "num" },
    { key: "results_ready_seconds", label: "结果就绪 s", type: "num", title: "全部分片结果到达地面的时刻（聚合前）" },
    { key: "completion_seconds", label: "完整交付 s", type: "num", title: "聚合完成、任务交付的时刻" },
    { key: "latency_seconds", label: "交付时延 s", type: "num", title: "完整交付时刻 − 到达时刻；仅已完成任务有值" },
    { key: "failure_reason", label: "失败原因", type: "str" },
    { key: "raw_relay_hops", label: "原始中继跳", type: "num" },
    { key: "result_relay_hops", label: "结果中继跳", type: "num" },
  ];

  const A = { detail: null, roles: WB.satRoles(null), result: null, tiles: null, tilesPromise: null, filter: "all", search: "", sort: { key: "release_seconds", dir: 1 }, task: null, seq: 0 };
  const roles = () => A.roles;
  WB.analysisState = A;

  const pane = () => $("#paneAnalysis");
  const dtOf = () => (A.detail ? A.detail.request.config.dt : 1);
  const failLabel = (r) => (r ? `${FAILURE_LABEL[r] || r} <span class="mono muted">${esc(r)}</span>` : "");
  const runUrl = (suffix) => `/api/runs/${encodeURIComponent(A.detail.id)}/${suffix}`;

  function taskStatusBadge(o) {
    if (o.status === "completed") return WB.badge("completed", "已完成");
    if (o.status === "failed") return WB.badge("failed", "失败");
    return WB.badge("pending", o.stage === "aggregating" ? "未完成·聚合中" : "未完成");
  }

  // ── 头部、下载 ───────────────────────────────────────────────────────────
  function headerHtml() {
    const d = A.detail, req = d.request, cfg = req.config, st = d.status;
    const files = new Set(d.files || []);
    const dl = [
      ["config.yaml", "配置 YAML"], ["request.json", "运行请求"], ["result.json", "结果 JSON"], ["tasks.csv", "任务表 CSV"],
      ["events.jsonl", "生命周期事件"], ["snapshots.json", "过程快照"], ["tiles.json", "分片终态"], ["worker.log", "worker 日志"],
    ].filter(([name]) => (name === "tasks.csv" ? files.has("result.json") : files.has(name)));
    return `<div class="toolbar">
        <h3 style="margin:0">运行 <span class="mono">${esc(d.id)}</span></h3>${WB.badge(st.state)}
        <span>${esc(WB.policyLabel(req.policy, req.checkpoint))}</span>
        <span class="muted">种子 ${esc(cfg.seed ?? 0)} · ${esc(A.roles.summary(cfg.num_sats))} · ${esc(cfg.sim_steps)} 步 × dt ${esc(cfg.dt)} s · 配置哈希 <span class="mono">${esc(d.config_hash || "—")}</span></span>
        <button class="btn small" type="button" data-act="replay">在回放中查看</button>
        <button class="btn small" type="button" data-act="form" title="把该运行的配置载入表单，可换策略构成同配置同种子对比">配置载入表单</button>
      </div>
      <div class="downloads note">下载：${dl.map(([name, label]) => `<a href="${runUrl("download/" + name)}" download>${esc(label)}</a>`).join("")}
        <a href="${runUrl("download/bundle.zip")}" download><b>全部打包 ZIP</b></a></div>
      <div class="note">CLI 复现：<span class="mono" id="cliCmd">${esc(req.cli_command || "—")}</span> <button class="btn small" type="button" data-act="copy">复制</button></div>
      ${(req.warnings || []).length ? `<div class="msg warning"><ul>${req.warnings.map((w) => `<li>${esc(w)}</li>`).join("")}</ul></div>` : ""}`;
  }

  function bindHeader() {
    const p = pane();
    $$("[data-act]", p).forEach((b) => b.addEventListener("click", () => {
      const act = b.dataset.act;
      if (act === "replay") WB.showTab("replay");
      else if (act === "form") WB.loadConfigIntoForm(A.detail.request);
      else if (act === "copy") {
        const text = A.detail.request.cli_command || "";
        (navigator.clipboard ? navigator.clipboard.writeText(text) : Promise.reject(new Error("剪贴板不可用")))
          .then(() => WB.toast("已复制 CLI 命令", "info", 1500))
          .catch(() => { const r = document.createRange(); r.selectNodeContents($("#cliCmd")); getSelection().removeAllRanges(); getSelection().addRange(r); WB.toast("请按 Ctrl/⌘+C 复制已选中的命令", "warning"); });
      }
    }));
  }

  // ── 汇总 ─────────────────────────────────────────────────────────────────
  function summaryHtml(r) {
    const o = r.overall, l = r.latency, exp = r.experiment || {};
    const aggregating = o.aggregating_tasks || 0;
    const pct = (a, b) => (b ? ` <span class="muted">${((a / b) * 100).toFixed(1)}%</span>` : "");
    const tileFailures = Object.entries(r.failures || {}).sort((a, b) => b[1] - a[1]);
    const taskFailures = {};
    for (const t of r.task_outcomes) if (t.status === "failed") taskFailures[t.failure_reason || "未记录"] = (taskFailures[t.failure_reason || "未记录"] || 0) + 1;
    const taskFailRows = Object.entries(taskFailures).sort((a, b) => b[1] - a[1]);
    return `<div class="grid2">
      <div><h3>完成情况 <span class="muted" style="font-weight:400">（完成 = 全部分片结果到达地面并完成聚合）</span></h3>
        <table class="data"><tr><th></th><th class="num">总数</th><th class="num">已完成</th><th class="num">失败</th><th class="num">未完成</th><th class="num">完成率</th></tr>
          <tr><td>任务</td><td class="num">${o.total_tasks}</td><td class="num ok">${o.completed_tasks}</td><td class="num err">${o.failed_tasks}</td>
            <td class="num" title="含聚合中 ${aggregating} 个">${o.pending_tasks}${aggregating ? `<span class="muted">（聚合中 ${aggregating}）</span>` : ""}</td><td class="num"><b>${WB.fmtPct(o.task_completion_rate)}</b></td></tr>
          <tr><td>分片</td><td class="num">${o.total_tiles}</td><td class="num ok">${o.completed_tiles}</td><td class="num err">${o.failed_tiles}</td><td class="num">${o.pending_tiles}</td><td class="num"><b>${WB.fmtPct(o.tile_completion_rate)}</b></td></tr></table>
        <div class="note">分片“已完成”= 结果已到达地面（DONE）；任务失败 = 任一分片失败或聚合超过截止期；未完成 = 仿真结束时既未交付也未失败。</div>
      </div>
      <div><h3>交付时延 <span class="muted" style="font-weight:400">（仅统计已完成；单位 ${esc(l.unit === "seconds" ? "秒" : l.unit)}）</span></h3>
        <table class="data"><tr><th></th><th class="num">统计样本 / 完成率</th><th class="num">均值</th><th class="num">P95</th><th class="num">P99</th></tr>
          <tr><td>任务</td><td class="num">${o.completed_tasks} / ${o.total_tasks} · ${WB.fmtPct(o.task_completion_rate)}</td><td class="num">${WB.fmtSec(l.task_mean)}</td><td class="num">${WB.fmtSec(l.task_p95)}</td><td class="num">${WB.fmtSec(l.task_p99)}</td></tr>
          <tr><td>分片</td><td class="num">${o.completed_tiles} / ${o.total_tiles} · ${WB.fmtPct(o.tile_completion_rate)}</td><td class="num">${WB.fmtSec(l.tile_mean)}</td><td class="num">${WB.fmtSec(l.tile_p95)}</td><td class="num">${WB.fmtSec(l.tile_p99)}</td></tr></table>
        <div class="note">总体 population = <span class="mono">${esc(l.population)}</span>。完成率低时，时延只反映少数已完成任务，不能单独作为策略优劣依据。</div>
      </div>
      <div><h3>失败原因</h3>
        <div class="row" style="align-items:flex-start;gap:18px">
          <div class="grow"><div class="note">分片失败（result.failures）</div>${tileFailures.length
            ? `<table class="data"><tr><th>原因</th><th class="num">分片数</th></tr>${tileFailures.map(([k, v]) => `<tr><td>${failLabel(k)}</td><td class="num">${v}</td></tr>`).join("")}</table>`
            : '<div class="note">无分片失败</div>'}</div>
          <div class="grow"><div class="note">失败任务（task_outcomes.failure_reason）</div>${taskFailRows.length
            ? `<table class="data"><tr><th>原因</th><th class="num">任务数</th></tr>${taskFailRows.map(([k, v]) => `<tr><td>${failLabel(k)}</td><td class="num">${v}</td></tr>`).join("")}</table>`
            : '<div class="note">无失败任务</div>'}</div>
        </div>
      </div>
      <div><h3>实验信息</h3><dl class="kv">
        <dt>策略 / 执行模式</dt><dd><span class="mono">${esc(exp.policy)}</span> · pipeline_mode <span class="mono">${esc(exp.config && exp.config.pipeline_mode)}</span>${exp.checkpoint ? ` · <span class="mono">${esc(exp.checkpoint)}</span>` : ""}</dd>
        <dt>仿真时长</dt><dd>${WB.fmtSec(exp.elapsed_sim_seconds)}（${esc(A.detail.request.config.sim_steps)} 步 × ${esc(dtOf())} s）</dd>
        <dt>交付语义</dt><dd class="mono">${esc(exp.delivery_semantics || "—")}</dd>
        <dt>任务轨迹哈希</dt><dd class="mono" title="同配置同种子下应一致">${esc((exp.task_trace_sha256 || "").slice(0, 16))}</dd>
        <dt>TLE / 代码哈希</dt><dd class="mono">${esc((exp.tle_sha256 || "").slice(0, 12))} / ${esc((exp.source_sha256 || "").slice(0, 12))}</dd>
        <dt>模型版本</dt><dd>${esc(r.model_version)} · Python ${esc(exp.python_version || "")}</dd>
        <dt>墙钟用时</dt><dd>${WB.fmtSec(A.detail.status.wall_seconds)}</dd></dl></div>
    </div>`;
  }

  function resourceHtml(r) {
    const res = r.resource, cfg = A.detail.request.config, exp = r.experiment || {};
    const T = exp.elapsed_sim_seconds;
    const n = cfg.num_sats;
    const rates = cfg.satellite_compute_rates || null;
    const memCap = cfg.mem_capacity_gb, vramCap = cfg.vram_capacity_gb;
    const val = (obj, i) => (obj && Object.prototype.hasOwnProperty.call(obj, String(i)) ? obj[String(i)] : null);
    let rows = "";
    for (let i = 0; i < n; i++) {
      const busy = val(res.compute_busy_time, i);
      const mem = val(res.mem_peak_gb, i), vram = val(res.vram_peak_gb, i);
      const role = roles().role(i);
      rows += `<tr><td>${esc(roles().name(i))}</td><td>${role === roles().source ? esc(role.label) : `<span class="muted">${esc(role.label)}</span>`}</td>
        <td class="num">${WB.fmt(rates ? rates[i] : cfg.compute_rate, 3)}</td>
        <td class="num">${WB.fmt(val(res.avg_queue_len, i), 3)}</td>
        <td class="num">${busy === null ? '<span class="muted" title="compute_busy_time 中没有该星的记录（未进行星上计算）">无记录</span>' : WB.fmtSec(busy)}</td>
        <td class="num">${busy !== null && WB.isNum(T) && T > 0 ? WB.fmtPct(busy / T) : WB.NA}</td>
        <td class="num">${WB.fmt(mem, 3)}${WB.isNum(mem) && memCap ? `<span class="muted"> / ${memCap}</span>` : ""}</td>
        <td class="num">${WB.fmt(vram, 3)}${WB.isNum(vram) && vramCap ? `<span class="muted"> / ${vramCap}</span>` : ""}</td></tr>`;
    }
    const net = r.network || { sent_mb: {}, capacity_mb: {}, utilization: {} };
    const keys = Object.keys(net.capacity_mb || {}).sort((a, b) => (net.sent_mb[b] || 0) - (net.sent_mb[a] || 0));
    const netRow = (k) => `<tr><td class="mono">${esc(k)}</td><td class="num">${WB.fmtMB(net.sent_mb[k])}</td><td class="num">${WB.fmtMB(net.capacity_mb[k])}</td><td class="num">${WB.fmtPct(net.utilization[k])}</td></tr>`;
    const isl = keys.filter((k) => k.startsWith("isl:")), ground = keys.filter((k) => !k.startsWith("isl:"));
    return `<div class="grid2">
      <div><h3>星上资源</h3>
        <table class="data"><tr><th>卫星</th><th>角色</th><th class="num">算力</th><th class="num">平均队列</th><th class="num">计算忙碌</th><th class="num" title="计算忙碌时间 / 仿真时长">忙碌占比</th><th class="num">存储峰值 GB</th><th class="num">显存峰值 GB</th></tr>${rows}</table>
        <div class="note">平均队列为逐步执行队列长度的均值；计算忙碌时间来自 result.resource.compute_busy_time。</div></div>
      <div><h3>链路传输量 <span class="muted" style="font-weight:400">（实际发送 / 链路可用容量）</span></h3>
        ${keys.length ? `<div class="tablewrap" style="max-height:300px"><table class="data"><tr><th>链路</th><th class="num">实际发送</th><th class="num">可用容量</th><th class="num">利用率</th></tr>
          ${ground.map(netRow).join("")}${isl.map(netRow).join("")}</table></div>` : '<div class="note">无链路传输记录</div>'}
        <div class="note"><span class="mono">isl:a->b</span> 为有向星间链路，容量只在链路连通的步累计（带宽/8 × dt）；<span class="mono">ground:*</span> 为地面站接收，容量按每步全部接收通道累计（不论是否有卫星可见），因此利用率偏低属正常。</div></div>
    </div>`;
  }

  // ── 图表 ─────────────────────────────────────────────────────────────────
  function setupCanvas(cv) {
    const w = cv.clientWidth, h = cv.clientHeight, ratio = window.devicePixelRatio || 1;
    cv.width = Math.round(w * ratio); cv.height = Math.round(h * ratio);
    const g = cv.getContext("2d");
    g.setTransform(ratio, 0, 0, ratio, 0, 0);
    g.clearRect(0, 0, w, h);
    g.font = "10px sans-serif";
    return { g, w, h };
  }

  function axes(g, w, h, pad, xmax, ymax, xlabel, ylabel) {
    g.strokeStyle = "#2d3746"; g.fillStyle = "#8b97a8"; g.lineWidth = 1;
    g.beginPath(); g.moveTo(pad.l, pad.t); g.lineTo(pad.l, h - pad.b); g.lineTo(w - pad.r, h - pad.b); g.stroke();
    for (let k = 0; k <= 4; k++) {
      const x = pad.l + (k / 4) * (w - pad.l - pad.r);
      const y = h - pad.b - (k / 4) * (h - pad.t - pad.b);
      g.fillText(String(Math.round((xmax * k) / 4)), x - 8, h - pad.b + 12);
      g.fillText(String(Math.round((ymax * k) / 4 * 10) / 10), 4, y + 3);
      g.strokeStyle = "#1c232d"; g.beginPath(); g.moveTo(pad.l + 1, y); g.lineTo(w - pad.r, y); g.stroke();
    }
    g.fillText(xlabel, w - pad.r - g.measureText(xlabel).width, h - 3);
    g.fillText(ylabel, pad.l + 4, pad.t + 8);
  }

  function drawDelivery(r) {
    const cv = $("#chDelivery");
    if (!cv) return;
    const { g, w, h } = setupCanvas(cv);
    const T = (r.experiment && r.experiment.elapsed_sim_seconds) || A.detail.request.config.sim_steps * dtOf();
    const rel = r.task_outcomes.map((o) => o.release_seconds).sort((a, b) => a - b);
    const done = r.task_outcomes.filter((o) => o.status === "completed").map((o) => o.completion_seconds).sort((a, b) => a - b);
    const pad = { l: 34, r: 8, t: 8, b: 18 };
    const ymax = Math.max(1, rel.length);
    axes(g, w, h, pad, T, ymax, "仿真时刻 s", "任务数");
    const X = (t) => pad.l + (t / T) * (w - pad.l - pad.r), Y = (v) => h - pad.b - (v / ymax) * (h - pad.t - pad.b);
    const step = (vals, color) => {
      g.strokeStyle = color; g.lineWidth = 1.5; g.beginPath(); g.moveTo(X(0), Y(0));
      vals.forEach((t, k) => { g.lineTo(X(t), Y(k)); g.lineTo(X(t), Y(k + 1)); });
      g.lineTo(X(T), Y(vals.length)); g.stroke();
    };
    step(rel, "#8b97a8");
    step(done, "#4cc38a");
  }

  function drawLatencyHist(r) {
    const cv = $("#chLatency");
    if (!cv) return;
    const { g, w, h } = setupCanvas(cv);
    const lat = r.task_outcomes.filter((o) => o.status === "completed").map((o) => o.latency_seconds);
    if (!lat.length) {
      g.fillStyle = "#8b97a8"; g.font = "12px sans-serif"; g.fillText("暂无数据：没有已完成任务，无法统计交付时延", 12, h / 2);
      return;
    }
    const maxL = Math.max(...lat), bins = Math.min(30, Math.max(5, Math.ceil(Math.sqrt(lat.length) * 2)));
    const width = maxL > 0 ? maxL / bins : 1;
    const counts = new Array(bins).fill(0);
    lat.forEach((v) => (counts[Math.min(bins - 1, Math.floor(v / width))] += 1));
    const pad = { l: 34, r: 8, t: 8, b: 18 };
    const ymax = Math.max(...counts);
    axes(g, w, h, pad, width * bins, ymax, "交付时延 s", "任务数");
    const bw = (w - pad.l - pad.r) / bins;
    g.fillStyle = "rgba(78,161,255,.75)";
    counts.forEach((c, k) => { const bh = (c / ymax) * (h - pad.t - pad.b); g.fillRect(pad.l + k * bw + 1, h - pad.b - bh, bw - 2, bh); });
  }

  // ── 任务表 ───────────────────────────────────────────────────────────────
  function filteredTasks() {
    const r = A.result;
    let rows = r.task_outcomes;
    if (A.filter === "aggregating") rows = rows.filter((o) => o.stage === "aggregating");
    else if (A.filter !== "all") rows = rows.filter((o) => o.status === A.filter);
    if (A.search) {
      const q = A.search.toLowerCase();
      rows = rows.filter((o) => o.task_id.toLowerCase().includes(q) || String(o.source_sat_id) === q || roles().name(o.source_sat_id).toLowerCase() === q);
    }
    const { key, dir } = A.sort;
    const num = (s) => { const m = /(\d+)$/.exec(s); return m ? +m[1] : 0; };
    return rows.slice().sort((a, b) => {
      let va = a[key], vb = b[key];
      if (key === "task_id") { va = num(va); vb = num(vb); }
      if (va === vb) return num(a.task_id) - num(b.task_id);
      if (va === null || va === undefined) return 1;      // 无数据始终排在后面
      if (vb === null || vb === undefined) return -1;
      return (va < vb ? -1 : 1) * dir;
    });
  }

  function taskTableHtml() {
    const o = A.result.overall;
    const counts = { all: o.total_tasks, completed: o.completed_tasks, failed: o.failed_tasks, pending: o.pending_tasks, aggregating: o.aggregating_tasks || 0 };
    const opts = [["all", "全部"], ["completed", "已完成（完整交付）"], ["failed", "失败"], ["pending", "未完成"], ["aggregating", "未完成·聚合中"]];
    return `<h3>任务列表</h3>
      <div class="toolbar">
        <label class="inline">状态 <select id="tFilter">${opts.map(([v, l]) => `<option value="${v}" ${A.filter === v ? "selected" : ""}>${l}（${counts[v]}）</option>`).join("")}</select></label>
        <label class="inline">搜索 <input id="tSearch" type="text" placeholder="task_12、源星编号或名称" value="${esc(A.search)}" style="width:150px"/></label>
        <span class="muted" id="tCount"></span>
        <span class="muted">点击表头排序，点击行查看分片生命周期</span>
      </div>
      <div class="tablewrap" id="tWrap"></div>`;
  }

  function renderTaskRows() {
    const rows = filteredTasks();
    const { key, dir } = A.sort;
    const head = TASK_COLS.map((c) => `<th class="sortable ${c.type === "num" ? "num" : ""}" data-key="${c.key}" title="${esc(c.title || "")}">${esc(c.label)}${key === c.key ? (dir > 0 ? " ▲" : " ▼") : ""}</th>`).join("");
    const cell = (o, c) => {
      const v = o[c.key];
      if (c.key === "status") return taskStatusBadge(o);
      if (c.key === "failure_reason") return v ? failLabel(v) : "";
      if (c.key === "source_sat_id") return esc(roles().name(v));
      if (c.type === "num") return v === null || v === undefined ? (c.key === "deadline_seconds" ? '<span class="muted">无</span>' : WB.NA) : WB.fmt(v, 1);
      return esc(v);
    };
    const body = rows.slice(0, 2000).map((o) => `<tr class="clickable ${o.task_id === A.task ? "sel" : ""}" data-task="${esc(o.task_id)}">${TASK_COLS.map((c) =>
      `<td class="${c.type === "num" ? "num" : ""} ${c.type === "id" ? "mono" : ""}">${cell(o, c)}</td>`).join("")}</tr>`).join("");
    $("#tWrap").innerHTML = rows.length
      ? `<table class="data"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`
      : '<div class="empty">没有符合筛选条件的任务</div>';
    $("#tCount").textContent = `显示 ${Math.min(rows.length, 2000)} / ${rows.length} 个`;
    $$("#tWrap th[data-key]").forEach((th) => th.addEventListener("click", () => {
      A.sort = { key: th.dataset.key, dir: A.sort.key === th.dataset.key ? -A.sort.dir : 1 };
      renderTaskRows();
    }));
    $$("#tWrap tr[data-task]").forEach((tr) => tr.addEventListener("click", () => showTask(tr.dataset.task)));
  }

  // ── 任务详情 ─────────────────────────────────────────────────────────────
  function loadTiles() {
    if (A.tiles) return Promise.resolve(A.tiles);
    if (!A.tilesPromise) {
      const seq = A.seq;
      A.tilesPromise = WB.api(runUrl("tiles")).then((data) => {
        if (seq !== A.seq) return null;
        const idx = Object.fromEntries(data.fields.map((f, i) => [f, i]));
        A.tiles = { idx, byTask: {} };
        for (const row of data.rows) (A.tiles.byTask[row[idx.task_id]] = A.tiles.byTask[row[idx.task_id]] || []).push(row);
        return A.tiles;
      }).catch((e) => { A.tilesPromise = null; throw e; });
    }
    return A.tilesPromise;
  }

  /** 分片时间线：直接画 tiles.json 的步号区间，不推断缺失的阶段 */
  function ganttHtml(rows, idx, outcome, endStep) {
    const releaseStep = Math.round(outcome.release_seconds / dtOf());
    const doneStep = WB.isNum(outcome.completion_seconds) ? Math.round(outcome.completion_seconds / dtOf()) : null;
    let hi = doneStep ?? endStep;
    for (const r of rows) for (const f of ["end_tx", "end_compute", "end_downlink", "start_downlink"]) if (WB.isNum(r[idx[f]])) hi = Math.max(hi, r[idx[f]]);
    const lo = Math.min(releaseStep, ...rows.map((r) => r[idx.created]).filter(WB.isNum));
    const span = Math.max(1, hi - lo);
    const pos = (s) => (((s - lo) / span) * 100).toFixed(2);
    const seg = (a, b, color, label) => (WB.isNum(a)
      ? `<div title="${esc(label)}：步 ${a} → ${WB.isNum(b) ? b : "未结束"}" style="position:absolute;top:2px;bottom:2px;left:${pos(a)}%;width:${Math.max(0.4, ((WB.isNum(b) ? b : hi) - a) / span * 100).toFixed(2)}%;background:${color};opacity:${WB.isNum(b) ? 0.9 : 0.45}"></div>` : "");
    const lines = rows.map((r) => `<div class="row" style="flex-wrap:nowrap;gap:6px"><span class="mono" style="width:120px;flex:none">${esc(r[idx.tile_id].replace(outcome.task_id + "_", ""))}</span>
      <div style="position:relative;height:12px;flex:1;background:#0f141b;border:1px solid #222a35">
        ${seg(r[idx.start_tx], r[idx.end_tx], "#ff4fd8", "原始分片传输 start_tx→end_tx")}
        ${seg(r[idx.start_compute], r[idx.end_compute], "#4cc38a", "计算 start_compute→end_compute")}
        ${seg(r[idx.start_downlink], r[idx.end_downlink], "#ff9a3d", "下传 start_downlink→end_downlink")}
        ${doneStep !== null ? `<div title="任务完整交付：步 ${doneStep}" style="position:absolute;top:-2px;bottom:-2px;left:${pos(doneStep)}%;width:2px;background:#fff"></div>` : ""}
      </div><span style="width:110px;flex:none" class="${r[idx.state] === "FAILED" ? "err" : r[idx.state] === "DONE" ? "ok" : "muted"}">${esc(STATE_LABEL[r[idx.state]] || r[idx.state])}</span></div>`).join("");
    return `<div class="legend" style="margin:4px 0"><span><i style="background:#ff4fd8"></i>原始分片传输</span><span><i style="background:#4cc38a"></i>计算</span><span><i style="background:#ff9a3d"></i>下传</span><span><i style="background:#fff;width:2px;height:10px"></i>任务完整交付</span><span>半透明 = 仿真结束时未结束</span>
      <span class="grow" style="text-align:right">步 ${lo}（${WB.fmt(lo * dtOf(), 0)} s） → 步 ${hi}（${WB.fmt(hi * dtOf(), 0)} s）</span></div>${lines}`;
  }

  function tileTableHtml(rows, idx) {
    const d = dtOf();
    const st = (v) => (WB.isNum(v) ? `${v}<span class="muted"> · ${WB.fmt(v * d, 0)}s</span>` : '<span class="muted">—</span>');
    const path = (p) => (p && p.length ? p.map((s) => esc(roles().name(s))).join("→") : '<span class="muted">—</span>');
    return `<div class="tablewrap" style="max-height:260px"><table class="data"><tr><th>分片</th><th>终态</th><th>位置</th><th>失败原因</th><th class="num">大小 MB</th>
      <th class="num">生成</th><th class="num">传输开始</th><th class="num">传输结束</th><th class="num">计算开始</th><th class="num">计算结束</th><th class="num">下传开始</th><th class="num">到达地面</th><th>原始中继路径</th><th>结果路径</th></tr>
      ${rows.map((r) => `<tr><td class="mono">${esc(r[idx.tile_id])}</td><td>${esc(STATE_LABEL[r[idx.state]] || r[idx.state])}</td>
        <td>${r[idx.location] === null || r[idx.location] === undefined ? '<span class="muted">—</span>' : esc(roles().node(r[idx.location]))}</td>
        <td>${r[idx.failure_reason] ? failLabel(r[idx.failure_reason]) : ""}</td><td class="num">${WB.fmt(r[idx.data_size_mb], 2)}</td>
        ${["created", "start_tx", "end_tx", "start_compute", "end_compute", "start_downlink", "end_downlink"].map((f) => `<td class="num">${st(r[idx[f]])}</td>`).join("")}
        <td class="mono">${path(r[idx.raw_relay_path])}</td><td class="mono">${path(r[idx.result_path])}</td></tr>`).join("")}</table></div>
      <div class="note">时间列为步号（· 后为秒 = 步 × ${d}）。“计算结束”不等于任务完成；任务完成以“完整交付”为准。</div>`;
  }

  function eventsHtml(data) {
    const node = (v) => (v === null || v === undefined ? "" : esc(roles().node(v)));
    const extra = (e) => {
      const parts = [];
      if (e.reason) parts.push(failLabel(e.reason));
      if (WB.isNum(e.remaining_mb)) parts.push(`剩余 ${WB.fmt(e.remaining_mb, 2)} MB`);
      for (const [k, v] of Object.entries(e)) {
        if (["time", "time_seconds", "event", "task_id", "tile_id", "state", "sat_from", "sat_to", "location", "reason", "remaining_mb"].includes(k)) continue;
        parts.push(`<span class="mono">${esc(k)}=${esc(JSON.stringify(v))}</span>`);
      }
      return parts.join("；");
    };
    if (!data.events.length) return '<div class="note">没有该任务的生命周期事件</div>';
    return `<div class="tablewrap" style="max-height:320px"><table class="data"><tr><th class="num">步</th><th class="num">秒</th><th>事件</th><th>分片</th><th>源→目的</th><th>状态</th><th>附加</th></tr>
      ${data.events.map((e) => `<tr class="clickable ${e.event === "task_delivered" ? "ok" : e.event === "failed" || e.event === "task_failed" ? "err" : ""}" data-step="${e.time}" title="在回放中定位到步 ${e.time}">
        <td class="num">${e.time}</td><td class="num">${WB.fmt(e.time_seconds, 1)}</td><td>${esc(EVENT_LABEL[e.event] || e.event)} <span class="mono muted">${esc(e.event)}</span></td>
        <td class="mono">${esc(e.tile_id ? e.tile_id.replace(e.task_id + "_", "") : "整图")}</td><td class="mono">${node(e.sat_from)}${e.sat_from !== null && e.sat_from !== undefined || e.sat_to !== null && e.sat_to !== undefined ? "→" : ""}${node(e.sat_to)}</td>
        <td>${e.state ? esc(STATE_LABEL[e.state] || e.state) : ""}</td><td>${extra(e)}</td></tr>`).join("")}</table></div>
      ${data.truncated ? '<div class="note warn">事件过多，仅显示前 20000 条</div>' : ""}
      <div class="note">点击事件行可在回放中定位到该步（事件 time 为步号）。task_delivered 记录的步号为交付时刻。</div>`;
  }

  async function showTask(taskId) {
    A.task = taskId;
    $$("#tWrap tr[data-task]").forEach((tr) => tr.classList.toggle("sel", tr.dataset.task === taskId));
    const box = $("#taskDetail");
    const outcome = A.result.task_outcomes.find((o) => o.task_id === taskId);
    if (!outcome) { box.innerHTML = `<div class="msg error">任务 ${esc(taskId)} 不在结果中</div>`; return; }
    const d = dtOf();
    const releaseStep = Math.round(outcome.release_seconds / d);
    box.innerHTML = `<h3>任务 <span class="mono">${esc(taskId)}</span> ${taskStatusBadge(outcome)}
        <button class="btn small" type="button" id="tdReplay" title="切换到回放并定位到任务到达时刻，仅显示该任务的分片流">在回放中查看（步 ${releaseStep}）</button></h3>
      <dl class="kv"><dt>源星</dt><dd>${esc(roles().name(outcome.source_sat_id))}</dd>
        <dt>到达</dt><dd>${WB.fmtSec(outcome.release_seconds)}（步 ${releaseStep}）</dd>
        <dt>截止</dt><dd>${WB.isNum(outcome.deadline_seconds) ? WB.fmtSec(outcome.deadline_seconds) : '<span class="muted">无截止期</span>'}</dd>
        <dt>结果就绪</dt><dd>${WB.fmtSec(outcome.results_ready_seconds)}<span class="muted">（全部分片结果到达地面，聚合前）</span></dd>
        <dt>完整交付</dt><dd>${WB.fmtSec(outcome.completion_seconds)}</dd>
        <dt>交付时延</dt><dd>${WB.fmtSec(outcome.latency_seconds)}${outcome.status !== "completed" ? '<span class="muted">（未完成任务不计入时延统计）</span>' : ""}</dd>
        <dt>失败原因</dt><dd>${outcome.failure_reason ? failLabel(outcome.failure_reason) : '<span class="muted">无</span>'}</dd>
        <dt>观测龄期</dt><dd>${WB.fmtSec(outcome.observed_age_seconds)}<span class="muted">（仿真结束时距到达的时长）</span></dd></dl>
      <h3>分片生命周期</h3><div id="tdTiles"><div class="note">载入分片数据…</div></div>
      <h3>事件</h3><div id="tdEvents"><div class="note">载入事件…</div></div>`;
    $("#tdReplay").addEventListener("click", () => toReplay(taskId, releaseStep));
    box.scrollIntoView({ block: "nearest" });
    const seq = A.seq;
    loadTiles().then((tiles) => {
      if (seq !== A.seq || A.task !== taskId || !tiles) return;
      const rows = (tiles.byTask[taskId] || []).slice().sort((a, b) => +a[tiles.idx.tile_id].split("_").pop() - +b[tiles.idx.tile_id].split("_").pop());
      $("#tdTiles").innerHTML = rows.length
        ? ganttHtml(rows, tiles.idx, outcome, A.detail.request.config.sim_steps) + tileTableHtml(rows, tiles.idx)
        : '<div class="note">tiles.json 中没有该任务的分片</div>';
    }).catch((e) => { if (A.task === taskId) $("#tdTiles").innerHTML = `<div class="msg error">分片数据载入失败：${esc(e.message)}</div>`; });
    try {
      const data = await WB.api(runUrl("events?task_id=" + encodeURIComponent(taskId)));
      if (seq !== A.seq || A.task !== taskId) return;
      $("#tdEvents").innerHTML = eventsHtml(data);
      $$("#tdEvents tr[data-step]").forEach((tr) => tr.addEventListener("click", () => toReplay(taskId, +tr.dataset.step)));
    } catch (e) {
      if (A.task === taskId) $("#tdEvents").innerHTML = `<div class="msg error">事件载入失败：${esc(e.message)}</div>`;
    }
  }

  function toReplay(taskId, step) {
    if (!WB.replayState || !WB.replayState.meta || WB.replayState.detail.id !== A.detail.id) {
      WB.toast("回放尚未载入该运行的过程快照", "warning");
      return;
    }
    WB.showTab("replay");
    WB.replay.focusTask(taskId, step);
  }

  // ── 载入 ─────────────────────────────────────────────────────────────────
  function renderEmpty(html) { pane().innerHTML = `<div class="empty">${html}</div>`; }

  function renderNoResult() {
    const d = A.detail, st = d.status;
    const files = d.files || [];
    pane().innerHTML = headerHtml() + `<div class="msg ${st.state === "failed" ? "error" : "warning"}">
      该运行没有最终结果（状态：${esc(WB.RUN_STATE_LABEL[st.state] || st.state)}），不显示完成率、时延等指标。
      ${st.error ? `<div><b>原因：</b>${esc(st.error)}</div>` : ""}
      ${files.includes("snapshots.json") ? "<div>已保存的部分过程快照可在“仿真回放”中用于诊断，但不能作为实验结果。</div>" : ""}
      ${st.traceback ? `<details><summary>Traceback / worker 日志</summary><pre class="trace">${esc(st.traceback)}</pre></details>` : ""}</div>`;
    bindHeader();
  }

  async function load(detail) {
    const seq = ++A.seq;
    A.detail = detail; A.result = null; A.tiles = null; A.tilesPromise = null; A.task = null;
    A.roles = WB.satRoles(detail.request.config);
    if (!(detail.files || []).includes("result.json")) {
      if (detail.status.state === "running" || detail.status.state === "queued") renderEmpty("运行尚未结束，结束后显示结果分析。");
      else renderNoResult();
      return;
    }
    renderEmpty("载入结果…");
    let result;
    try {
      result = await WB.api(runUrl("result"));
    } catch (e) {
      if (seq === A.seq) pane().innerHTML = `<div class="msg error">结果载入失败：${esc(e.message)}</div>`;
      return;
    }
    if (seq !== A.seq) return;
    A.result = result;
    pane().innerHTML = headerHtml() + summaryHtml(result)
      + `<div class="grid2"><div><h3>任务到达与完整交付（累计）</h3><canvas class="chart" id="chDelivery"></canvas>
          <div class="legend"><span><i style="background:#8b97a8"></i>累计到达</span><span><i style="background:#4cc38a"></i>累计完整交付</span><span>失败任务无失败时刻字段，未画出</span></div></div>
        <div><h3>已完成任务的交付时延分布</h3><canvas class="chart" id="chLatency"></canvas>
          <div class="legend"><span>样本：${result.overall.completed_tasks} 个已完成任务 / 共 ${result.overall.total_tasks} 个（完成率 ${WB.fmtPct(result.overall.task_completion_rate)}）</span></div></div></div>`
      + resourceHtml(result) + taskTableHtml() + '<div id="taskDetail"></div>';
    bindHeader();
    $("#tFilter").addEventListener("change", (e) => { A.filter = e.target.value; renderTaskRows(); });
    $("#tSearch").addEventListener("input", (e) => { A.search = e.target.value.trim(); renderTaskRows(); });
    renderTaskRows();
    redraw();
  }

  function redraw() {
    if (!A.result || pane().classList.contains("hidden")) return;
    drawDelivery(A.result);
    drawLatencyHist(A.result);
  }

  function openTask(taskId) {
    if (!A.result) { WB.toast("该运行没有结果，无法查看任务详情", "warning"); return; }
    WB.showTab("analysis");
    if (!A.result.task_outcomes.some((o) => o.task_id === taskId)) { WB.toast(`任务 ${taskId} 不在结果中`, "warning"); return; }
    A.filter = "all"; A.search = "";
    $("#tFilter").value = "all"; $("#tSearch").value = "";
    renderTaskRows();
    showTask(taskId);
    const tr = $(`#tWrap tr[data-task="${CSS.escape(taskId)}"]`);
    if (tr) tr.scrollIntoView({ block: "center" });
  }

  function init() {
    renderEmpty("尚未载入运行。运行结束后自动载入；也可在“历史与对比”中选择运行。");
    window.addEventListener("resize", redraw);
  }

  WB.analysis = { init, load, openTask, redraw };
})();
