/* 历史运行与对比：列表来自 /api/runs，对比来自 /api/compare（配置差异与配对指标均由后端计算）。
 * 页面只陈列真实实验数据，不给出“哪个策略更好”的结论。 */
"use strict";
(function () {
  const { $, $$, esc } = WB;
  const H = { runs: [], selected: [], sameOnly: null, loading: false, compare: null };
  WB.historyState = H;
  const COLORS = ["#4ea1ff", "#4cc38a", "#f0b429", "#ff4fd8", "#38d0ff", "#ff9a3d", "#b18cff", "#f2545b"];

  function note(text) { $("#hNote").innerHTML = text || ""; }

  function updateButtons() {
    $("#hCompare").disabled = H.selected.length < 2;
    $("#hSame").disabled = H.selected.length < 1 && !H.sameOnly;
    $("#hSame").textContent = H.sameOnly ? "显示全部运行" : "只看同配置同种子";
    note(H.selected.length ? `已选 ${H.selected.length} 个（对比以第一个所选为参照）` : "勾选两个及以上已结束的运行进行对比");
  }

  async function refresh() {
    if (H.loading) return;
    H.loading = true;
    if (!H.runs.length) $("#historyTable").innerHTML = '<div class="empty">载入历史运行…</div>';
    try {
      const data = await WB.api("/api/runs");
      H.runs = data.runs;
      H.selected = H.selected.filter((id) => H.runs.some((r) => r.id === id));
      render();
      if (data.active_run) setTimeout(() => { if (WB.currentTab === "history") refresh(); }, 2000);
    } catch (e) {
      $("#historyTable").innerHTML = `<div class="msg error">读取历史运行失败：${esc(e.message)}</div>`;
    } finally {
      H.loading = false;
    }
  }

  function render() {
    let runs = H.runs;
    if (H.sameOnly) runs = runs.filter((r) => r.config_hash === H.sameOnly);
    if (!H.runs.length) {
      $("#historyTable").innerHTML = '<div class="empty">还没有运行记录。在左侧选择配置与策略后点击“运行仿真”。</div>';
      updateButtons();
      return;
    }
    const hashCount = {};
    for (const r of H.runs) if (r.config_hash) hashCount[r.config_hash] = (hashCount[r.config_hash] || 0) + 1;
    const rows = runs.map((r) => {
      const s = r.summary, o = s && s.overall, l = s && s.latency;
      const live = r.state === "running" || r.state === "queued";
      const sel = H.selected.includes(r.id);
      const progress = live && r.progress ? `${r.progress.steps_done}/${r.progress.sim_steps} 步` : "";
      return `<tr class="${sel ? "sel" : ""} ${WB.loaded && WB.loaded.id === r.id ? "loaded" : ""}">
        <td><input type="checkbox" data-sel="${esc(r.id)}" ${sel ? "checked" : ""} ${s ? "" : 'disabled title="没有结果的运行不能参与对比"'}/></td>
        <td class="mono">${esc(r.id)}${r.rerun_of ? `<div class="muted" title="重新运行自">↻ ${esc(r.rerun_of)}</div>` : ""}</td>
        <td>${WB.badge(r.state)}${progress ? ` <span class="muted">${progress}</span>` : ""}${r.error && !live ? `<div class="muted" title="${esc(r.error)}" style="max-width:180px;overflow:hidden;text-overflow:ellipsis">${esc(r.error)}</div>` : ""}</td>
        <td title="${esc(r.checkpoint || "")}">${esc(WB.policyLabel(r.policy, r.checkpoint))}</td>
        <td class="num">${esc(r.seed)}</td>
        <td class="num">${esc(r.num_sats)} 星 · ${esc(r.sim_steps)}×${esc(r.dt)} s</td>
        <td class="mono" title="规范化配置（含种子、不含执行模式）的哈希；相同哈希的运行共有 ${hashCount[r.config_hash] || 0} 个">${esc(r.config_hash || "—")}${hashCount[r.config_hash] > 1 ? ` <span class="tag">×${hashCount[r.config_hash]}</span>` : ""}</td>
        <td class="num">${o ? `${o.completed_tasks}/${o.total_tasks} · <b>${WB.fmtPct(o.task_completion_rate)}</b>` : WB.NA}</td>
        <td class="num">${o ? `${o.failed_tasks} / ${o.pending_tasks}` : WB.NA}</td>
        <td class="num" title="仅统计已完成任务">${l ? WB.fmtSec(l.task_mean) : WB.NA}</td>
        <td class="num">${WB.fmtSec(r.wall_seconds)}</td>
        <td class="mono muted">${esc(r.base_config || "页面编辑")}</td>
        <td style="white-space:nowrap">
          <button class="btn small" type="button" data-act="replay" data-id="${esc(r.id)}" ${live ? "disabled" : ""}>回放</button>
          <button class="btn small" type="button" data-act="analysis" data-id="${esc(r.id)}" ${live ? "disabled" : ""}>分析</button>
          <button class="btn small" type="button" data-act="rerun" data-id="${esc(r.id)}" ${live || WB.activeRunId ? "disabled" : ""} title="相同配置、策略与种子在新目录重新运行">重跑</button>
          <button class="btn small" type="button" data-act="form" data-id="${esc(r.id)}" title="配置载入表单，可换策略构成同配置同种子对比">载入表单</button>
          ${live ? `<button class="btn small" type="button" data-act="track" data-id="${esc(r.id)}">查看进度</button>` : ""}
        </td></tr>`;
    }).join("");
    $("#historyTable").innerHTML = `${H.sameOnly ? `<div class="msg info">仅显示配置哈希 <span class="mono">${esc(H.sameOnly)}</span> 的运行（同配置同种子，策略可不同）。</div>` : ""}
      <div class="tablewrap" style="max-height:46vh"><table class="data"><thead><tr><th></th><th>运行</th><th>状态</th><th>策略</th><th class="num">种子</th><th class="num">规模</th><th>配置哈希</th>
        <th class="num">任务完成（完整交付）</th><th class="num">失败 / 未完成</th><th class="num">任务时延均值</th><th class="num">墙钟</th><th>基础配置</th><th></th></tr></thead><tbody>${rows || '<tr><td colspan="13" class="muted">没有符合条件的运行</td></tr>'}</tbody></table></div>
      <div class="note">任务时延均值仅统计已完成任务，须结合完成率解读；“暂无数据”表示没有已完成任务。</div>`;
    $$("#historyTable input[data-sel]").forEach((cb) => cb.addEventListener("change", () => {
      const id = cb.dataset.sel;
      H.selected = cb.checked ? [...H.selected.filter((x) => x !== id), id] : H.selected.filter((x) => x !== id);
      render();
    }));
    $$("#historyTable button[data-act]").forEach((b) => b.addEventListener("click", () => act(b.dataset.act, b.dataset.id)));
    updateButtons();
  }

  async function act(action, id) {
    if (action === "replay" || action === "analysis") {
      WB.showTab(action);
      await WB.loadRun(id);
      if (action === "analysis") WB.analysis.redraw();
      render();
    } else if (action === "rerun") {
      await WB.rerun(id);
    } else if (action === "form") {
      try {
        const d = await WB.api(`/api/runs/${encodeURIComponent(id)}`);
        WB.loadConfigIntoForm(d.request);
      } catch (e) { WB.toast(e.message, "error"); }
    } else if (action === "track") {
      try {
        const d = await WB.api(`/api/runs/${encodeURIComponent(id)}`);
        WB.showRunInPanel(d);
        if ($("#sidebar").classList.contains("collapsed")) $("#sidebar").classList.remove("collapsed");
      } catch (e) { WB.toast(e.message, "error"); }
    }
  }

  // ── 对比 ─────────────────────────────────────────────────────────────────
  async function compare() {
    if (H.selected.length < 2) return;
    const box = $("#compareBox");
    box.innerHTML = '<div class="note">对比计算中…</div>';
    let data;
    try {
      data = await WB.api("/api/compare?ids=" + H.selected.map(encodeURIComponent).join(","));
    } catch (e) {
      box.innerHTML = `<div class="msg error">无法对比：${esc(e.message)}</div>`;
      return;
    }
    H.compare = data;
    renderCompare(data);
    box.scrollIntoView({ block: "start", behavior: "smooth" });
  }

  const fmtVal = (v) => (v === null || v === undefined ? '<span class="muted">null</span>' : esc(typeof v === "object" ? JSON.stringify(v) : String(v)));

  function renderCompare(data) {
    const box = $("#compareBox");
    const runs = data.runs;
    let html = `<h3>运行对比 <span class="muted" style="font-weight:400">（参照：<span class="mono">${esc(runs[0].id)}</span>）</span></h3>`;
    if (data.comparable) {
      html += `<div class="msg info">可比：规范化配置（含种子 ${esc(data.seeds.join(","))}）一致，任务到达轨迹哈希与 TLE 哈希一致；差异仅来自策略 / 执行模式。</div>`;
    } else {
      html += `<div class="msg warning"><b>不可直接比较：</b>${data.config_diffs.length ? `存在 ${data.config_diffs.length} 处配置差异` : "配置一致但任务轨迹或 TLE 不同"}。下表仅并列展示各自结果，未计算配对指标；不同配置或种子下的数值差异不能归因于策略。</div>`;
    }
    for (const n of data.notes || []) html += `<div class="msg warning">${esc(n)}</div>`;
    if (data.config_diffs.length) {
      html += `<h3>配置差异</h3><div class="tablewrap" style="max-height:260px"><table class="data"><tr><th>配置键</th>${runs.map((r) => `<th class="mono">${esc(r.id)}</th>`).join("")}</tr>
        ${data.config_diffs.map((d) => `<tr><td class="mono">${esc(d.key)}</td>${d.values.map((v, i) => `<td class="mono ${i && JSON.stringify(v) !== JSON.stringify(d.values[0]) ? "warn" : ""}">${fmtVal(v)}</td>`).join("")}</tr>`).join("")}</table></div>`;
    }
    // 各运行指标
    const metric = (label, fn, title) => `<tr><td title="${esc(title || "")}">${label}</td>${runs.map((r) => `<td class="num">${fn(r)}</td>`).join("")}</tr>`;
    const fails = (r) => { const e = Object.entries(r.failures || {}); return e.length ? e.map(([k, v]) => `${esc(k)}: ${v}`).join("<br>") : '<span class="muted">无</span>'; };
    html += `<h3>各运行结果</h3><div class="tablewrap" style="max-height:none"><table class="data">
      <tr><th>指标</th>${runs.map((r, i) => `<th class="num"><span style="color:${COLORS[i % COLORS.length]}">■</span> ${esc(r.label)}<div class="mono muted">${esc(r.id)}</div></th>`).join("")}</tr>
      ${metric("任务完成（完整交付）", (r) => `${r.overall.completed_tasks}/${r.overall.total_tasks} · <b>${WB.fmtPct(r.overall.task_completion_rate)}</b>`)}
      ${metric("分片完成（到达地面）", (r) => `${r.overall.completed_tiles}/${r.overall.total_tiles} · ${WB.fmtPct(r.overall.tile_completion_rate)}`)}
      ${metric("失败任务", (r) => r.overall.failed_tasks)}
      ${metric("未完成任务", (r) => `${r.overall.pending_tasks}${r.overall.aggregating_tasks ? `<span class="muted">（聚合中 ${r.overall.aggregating_tasks}）</span>` : ""}`)}
      ${metric("任务时延均值（各自已完成）", (r) => WB.fmtSec(r.latency.task_mean), "各运行只统计自己的已完成任务，样本集合不同")}
      ${metric("任务时延 P95 / P99", (r) => `${WB.fmtSec(r.latency.task_p95)} / ${WB.fmtSec(r.latency.task_p99)}`)}
      ${metric("分片时延均值", (r) => WB.fmtSec(r.latency.tile_mean))}
      ${metric("分片失败原因", fails)}
      ${metric("仿真时长", (r) => WB.fmtSec(r.elapsed_sim_seconds))}
      ${metric("任务轨迹哈希", (r) => `<span class="mono">${esc((r.task_trace_sha256 || "").slice(0, 12))}</span>`)}
    </table></div>
    <div class="note">各运行的时延只覆盖各自的已完成任务，完成集合不同时直接比较均值存在选择偏差；可比运行请参考下方配对指标。</div>`;
    // 配对指标
    if (data.paired.length) {
      const P = data.paired;
      const prow = (label, fn, title) => `<tr><td title="${esc(title || "")}">${label}</td>${P.map((p) => `<td class="num">${fn(p)}</td>`).join("")}</tr>`;
      html += `<h3>配对指标 <span class="muted" style="font-weight:400">（逐任务配对；population = <span class="mono">${esc(P[0].latency_population)}</span>）</span></h3>
      <div class="tablewrap" style="max-height:none"><table class="data">
        <tr><th>指标（参照 R / 候选 C）</th>${P.map((p) => `<th class="num">R=${esc(runs[0].label)}<br>C=${esc(runs.find((r) => r.id === p.candidate).label)}<div class="mono muted">${esc(p.candidate)}</div></th>`).join("")}</tr>
        ${prow("任务总数", (p) => p.total_tasks)}
        ${prow("两者都完成", (p) => p.common_completed_tasks)}
        ${prow("仅 R 完成", (p) => p.reference_only_completed_tasks)}
        ${prow("仅 C 完成", (p) => p.candidate_only_completed_tasks)}
        ${prow("两者都未完成", (p) => p.neither_completed_tasks)}
        ${prow("失败任务 R / C", (p) => `${p.reference_failed_tasks} / ${p.candidate_failed_tasks}`)}
        ${prow("未完成任务 R / C", (p) => `${p.reference_pending_tasks} / ${p.candidate_pending_tasks}`)}
        ${prow("共同完成任务时延均值 R / C", (p) => `${WB.fmtSec(p.reference_latency_mean_seconds)} / ${WB.fmtSec(p.candidate_latency_mean_seconds)}`)}
        ${prow("共同完成任务时延 P95 R / C", (p) => `${WB.fmtSec(p.reference_latency_p95_seconds)} / ${WB.fmtSec(p.candidate_latency_p95_seconds)}`)}
        ${prow("逐任务时延差均值 C − R", (p) => WB.fmtSec(p.candidate_minus_reference_latency_mean_seconds), "负值表示在共同完成的任务上 C 的交付时延较短")}
        ${prow("均值比 R / C", (p) => WB.fmt(p.ratio_of_mean_latencies_reference_over_candidate, 3))}
        ${prow("C 更快 / 更慢 / 相同（任务数）", (p) => `${p.candidate_faster_tasks} / ${p.candidate_slower_tasks} / ${p.equal_latency_tasks}`)}
      </table></div>
      <div class="note">配对时延只覆盖两者都完成的任务（共同完成集合），仍存在选择偏差；完成数差异需单独看上方行。单一种子的单次运行不足以得出策略优劣结论，需多种子重复实验。</div>`;
    }
    html += `<h3>累计完整交付任务数</h3><canvas class="chart" id="chCompare"></canvas>
      <div class="legend">${runs.map((r, i) => `<span><i style="background:${COLORS[i % COLORS.length]}"></i>${esc(r.label)} <span class="mono">${esc(r.id)}</span></span>`).join("")}<span><i style="background:#8b97a8"></i>累计到达（参照）</span></div>`;
    box.innerHTML = html;
    drawCompare(data);
  }

  function drawCompare(data) {
    const cv = $("#chCompare");
    if (!cv || !cv.clientWidth) return;
    const w = cv.clientWidth, h = cv.clientHeight, ratio = window.devicePixelRatio || 1;
    cv.width = Math.round(w * ratio); cv.height = Math.round(h * ratio);
    const g = cv.getContext("2d");
    g.setTransform(ratio, 0, 0, ratio, 0, 0);
    g.font = "10px sans-serif";
    const T = Math.max(...data.runs.map((r) => r.elapsed_sim_seconds || 0), 1);
    const ymax = Math.max(1, ...data.runs.map((r) => r.delivery.length));
    const pad = { l: 34, r: 8, t: 8, b: 18 };
    const X = (t) => pad.l + (t / T) * (w - pad.l - pad.r), Y = (v) => h - pad.b - (v / ymax) * (h - pad.t - pad.b);
    g.strokeStyle = "#2d3746"; g.fillStyle = "#8b97a8";
    g.beginPath(); g.moveTo(pad.l, pad.t); g.lineTo(pad.l, h - pad.b); g.lineTo(w - pad.r, h - pad.b); g.stroke();
    for (let k = 0; k <= 4; k++) {
      g.fillText(String(Math.round((T * k) / 4)), X((T * k) / 4) - 8, h - pad.b + 12);
      g.fillText(String(Math.round((ymax * k) / 4)), 4, Y((ymax * k) / 4) + 3);
    }
    g.fillText("仿真时刻 s", w - pad.r - 50, h - 3);
    const stepLine = (vals, color, end) => {
      g.strokeStyle = color; g.lineWidth = 1.5; g.beginPath(); g.moveTo(X(0), Y(0));
      vals.forEach((t, k) => { g.lineTo(X(t), Y(k)); g.lineTo(X(t), Y(k + 1)); });
      g.lineTo(X(end), Y(vals.length)); g.stroke();
    };
    stepLine(data.runs[0].delivery.map((d) => d[0]).sort((a, b) => a - b), "#8b97a8", data.runs[0].elapsed_sim_seconds || T);
    data.runs.forEach((r, i) => stepLine(r.delivery.map((d) => d[1]).filter(WB.isNum).sort((a, b) => a - b), COLORS[i % COLORS.length], r.elapsed_sim_seconds || T));
  }

  function init() {
    $("#hRefresh").addEventListener("click", refresh);
    $("#hCompare").addEventListener("click", compare);
    $("#hClear").addEventListener("click", () => { H.selected = []; H.sameOnly = null; $("#compareBox").innerHTML = ""; render(); });
    $("#hSame").addEventListener("click", () => {
      if (H.sameOnly) H.sameOnly = null;
      else {
        const first = H.runs.find((r) => r.id === H.selected[0]);
        if (!first || !first.config_hash) return;
        H.sameOnly = first.config_hash;
        H.selected = H.selected.filter((id) => (H.runs.find((r) => r.id === id) || {}).config_hash === H.sameOnly);
      }
      render();
    });
    window.addEventListener("resize", () => { if (H.compare && WB.currentTab === "history") drawCompare(H.compare); });
    updateButtons();
  }

  WB.history = { init, refresh };
})();
