/* 仿真工作台核心：API、通用格式化、场景配置表单与仿真执行面板。
 * 前端只展示后端输出，不计算调度、轨道或指标。时间约定：事件 time 为步号，时延为秒（秒 = 步 × dt）。 */
"use strict";

const WB = (window.WB = {
  meta: null,
  baseConfig: null,     // 所选配置文件（或历史运行）的原始配置
  working: null,        // 表单编辑中的配置
  baseLabel: "",
  activeRunId: null,
  loaded: null,         // 当前载入回放/分析的运行详情
  policyByName: {},
});

// ── 通用工具 ────────────────────────────────────────────────────────────────
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
WB.$ = $; WB.$$ = $$; WB.esc = esc;

WB.api = async function api(path, opts = {}) {
  const init = { method: opts.method || "GET", headers: {} };
  if (opts.body !== undefined) {
    init.body = JSON.stringify(opts.body);
    init.headers["Content-Type"] = "application/json";
  }
  let res;
  try {
    res = await fetch(path, init);
  } catch (e) {
    const err = new Error("无法连接工作台服务：" + e.message);
    err.network = true;
    throw err;
  }
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (e) { data = null; }
  if (!res.ok) {
    const err = new Error((data && data.error) || `HTTP ${res.status}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
};

const NA = '<span class="muted">暂无数据</span>';
WB.NA = NA;
WB.isNum = (v) => typeof v === "number" && Number.isFinite(v);
WB.fmt = (v, digits = 2) => (WB.isNum(v) ? (Number.isInteger(v) ? String(v) : v.toFixed(digits)) : NA);
WB.fmtSec = (v) => (WB.isNum(v) ? `${WB.fmt(v, 1)} s` : NA);
WB.fmtPct = (v) => (WB.isNum(v) ? `${(v * 100).toFixed(1)}%` : NA);
WB.fmtMB = (v) => (WB.isNum(v) ? (v >= 1000 ? `${(v / 1000).toFixed(2)} GB` : `${v.toFixed(1)} MB`) : NA);
/** 步号 → “步 n（t s）”，dt 来自该运行配置 */
WB.fmtStep = (step, dt) => (WB.isNum(step) ? `步 ${step}<span class="muted">（${WB.fmt(step * dt, 1)} s）</span>` : NA);
WB.utcAt = (startIso, seconds) => {
  const t = Date.parse(startIso);
  if (!Number.isFinite(t)) return "";
  return new Date(t + seconds * 1000).toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
};
WB.STATUS_LABEL = { completed: "已完成（完整交付）", failed: "失败", pending: "未完成" };
WB.RUN_STATE_LABEL = { queued: "排队", running: "运行中", completed: "已结束", failed: "失败", cancelled: "已取消" };
WB.badge = (state, label) => `<span class="badge ${esc(state)}">${esc(label || WB.RUN_STATE_LABEL[state] || state || "未知")}</span>`;
WB.policyLabel = (name, checkpoint) => {
  const p = WB.policyByName[name];
  const ck = checkpoint ? `（${checkpoint.split("/").slice(-2, -1)[0] || checkpoint}）` : "";
  return `${name}${ck}` + (p ? ` · ${p.label}` : "");
};
/** 卫星分类与显示名：任务源（成像）星与计算/中继星用不同前缀，编号沿用仿真 sat_id（与配置、事件日志一致）。
 * 分类来自 task_source_sat_ids（未设置时全部为任务源）；任务源载荷按 image_tiling.transport_components
 * 推断：分量名均以 SAR 开头时记为 SAR 星，否则为通用成像星。 */
WB.satRoles = (cfg) => {
  const ids = cfg && Array.isArray(cfg.task_source_sat_ids) ? new Set(cfg.task_source_sat_ids) : null;
  const comps = (cfg && cfg.image_tiling && cfg.image_tiling.transport_components) || [];
  const sar = comps.length > 0 && comps.every((c) => /^sar/i.test(String(c.name || "")));
  const source = sar ? { kind: "sar", prefix: "SAR", label: "SAR 成像星" } : { kind: "img", prefix: "IMG", label: "成像星" };
  const compute = { kind: "compute", prefix: "C", label: "计算/中继星" };
  const role = (i) => (!ids || ids.has(i) ? source : compute);
  const name = (i) => role(i).prefix + i;
  const summary = (n) => {
    const k = Array.from({ length: n }, (_, i) => i).filter((i) => role(i) === source).length;
    return k === n ? `${n} 星（均为${source.label}）` : `${n} 星（${source.label} ${k} · ${compute.label} ${n - k}）`;
  };
  return { source, compute, role, name, summary, node: (v) => (typeof v === "number" ? name(v) : v) };
};

WB.toast = function toast(message, kind = "info", ms = 5000) {
  const box = document.createElement("div");
  box.className = `msg ${kind}`;
  box.textContent = message;
  $("#toast").appendChild(box);
  setTimeout(() => box.remove(), ms);
};

WB.getPath = (obj, key, dflt) => {
  let node = obj;
  for (const part of key.split(".")) {
    if (node === null || typeof node !== "object" || !(part in node)) return dflt;
    node = node[part];
  }
  return node;
};
WB.setPath = (obj, key, value) => {
  const parts = key.split(".");
  let node = obj;
  for (const part of parts.slice(0, -1)) {
    if (node[part] === null || typeof node[part] !== "object") node[part] = {};
    node = node[part];
  }
  node[parts[parts.length - 1]] = value;
};
const clone = (v) => JSON.parse(JSON.stringify(v ?? null));
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

// ── 标签页 ─────────────────────────────────────────────────────────────────
WB.showTab = function showTab(name) {
  $$("#tabs button").forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
  $("#paneReplay").classList.toggle("hidden", name !== "replay");
  $("#paneAnalysis").classList.toggle("hidden", name !== "analysis");
  $("#paneHistory").classList.toggle("hidden", name !== "history");
  WB.currentTab = name;
  if (name === "history" && WB.history) WB.history.refresh();
  if (name === "replay" && WB.replay) WB.replay.resize();
  if (name === "analysis" && WB.analysis) WB.analysis.redraw();
};

// ── 配置表单 ───────────────────────────────────────────────────────────────
function fieldDomId(key) { return "f-" + key.replace(/\./g, "__"); }

function displayValue(field, value) {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.join(", ");
  return String(value);
}

function parseValue(type, text) {
  const s = String(text).trim();
  const intRe = /^[-+]?\d+$/;
  if (type === "int" || type === "int_or_null") {
    if (s === "" && type === "int_or_null") return null;
    return intRe.test(s) ? parseInt(s, 10) : s;   // 非法输入原样提交，由后端给出字段错误
  }
  if (type === "float") {
    const v = Number(s);
    return s !== "" && Number.isFinite(v) ? v : s;
  }
  if (type === "int_list_or_null" || type === "float_list_or_null") {
    if (s === "") return null;
    const parts = s.split(/[,，\s]+/).filter(Boolean);
    const isInt = type.startsWith("int");
    const vals = parts.map((p) => (isInt ? (intRe.test(p) ? parseInt(p, 10) : NaN) : Number(p)));
    return vals.every(Number.isFinite) ? vals : s;
  }
  return String(text);
}

function renderField(field) {
  const value = WB.getPath(WB.working, field.key, field.default);
  const base = WB.getPath(WB.baseConfig, field.key, field.default);
  const wrap = document.createElement("div");
  wrap.className = "field" + (same(value, base) ? "" : " modified");
  wrap.id = fieldDomId(field.key);
  wrap.dataset.key = field.key;
  const unit = field.unit ? `<span class="unit">${esc(field.unit)}</span>` : "";
  const title = esc((field.help ? field.help + "；" : "") + "配置键 " + field.key + (field.default !== null && field.default !== undefined ? `，引擎默认 ${displayValue(field, field.default)}` : ""));
  if (field.type === "bool") {
    wrap.classList.add("bool");
    wrap.innerHTML = `<label title="${title}"><input type="checkbox" ${value ? "checked" : ""}/> ${esc(field.label)} <span class="mod">●</span></label><div class="ferr"></div>`;
  } else if (field.type === "enum" || field.type === "tle") {
    const opts = field.type === "enum"
      ? field.options.map((o) => [o, o])
      : WB.meta.tles.map((t) => [t.path, `${t.path.replace(/^data\/tle\//, "")}（${t.count} 条）`]);
    if (value !== null && value !== undefined && !opts.some(([v]) => v === value)) opts.unshift([value, `${value}（不在 data/tle 中）`]);
    if (field.type === "tle" && (value === null || value === undefined)) opts.unshift(["", "（配置使用 tle_lines 或未指定）"]);
    wrap.classList.add("wide");
    wrap.innerHTML = `<label title="${title}">${esc(field.label)} ${unit}<span class="mod">●</span></label>
      <select>${opts.map(([v, l]) => `<option value="${esc(v)}" ${v === value ? "selected" : ""}>${esc(l)}</option>`).join("")}</select><div class="ferr"></div>`;
  } else {
    if (field.type.endsWith("list_or_null") || field.type === "str") wrap.classList.add("wide");
    const ph = field.type.endsWith("_or_null") ? "留空 = 未设置" : "";
    wrap.innerHTML = `<label title="${title}">${esc(field.label)} ${unit}<span class="mod">●</span></label>
      <input type="text" spellcheck="false" placeholder="${ph}" value="${esc(displayValue(field, value))}"/><div class="ferr"></div>`;
  }
  const input = $("input,select", wrap);
  const onChange = () => {
    let v;
    if (field.type === "bool") v = input.checked;
    else if (field.type === "enum") v = input.value;
    else if (field.type === "tle") v = input.value || null;
    else v = parseValue(field.type, input.value);
    if (field.type === "tle" && v === null) {
      const topo = WB.working.topology || {};
      delete topo.tle_file;
    } else {
      WB.setPath(WB.working, field.key, v);
    }
    wrap.classList.toggle("modified", !same(WB.getPath(WB.working, field.key, field.default), base));
    wrap.classList.remove("invalid");
    formChanged();
  };
  input.addEventListener("change", onChange);
  if (input.tagName === "INPUT" && input.type === "text") input.addEventListener("input", () => { onChange(); });
  return wrap;
}

function renderGroundStations(field) {
  const wrap = document.createElement("div");
  wrap.className = "field wide";
  wrap.id = fieldDomId(field.key);
  wrap.dataset.key = field.key;
  const list = WB.working.ground_stations || [];
  const cols = WB.meta.gs_fields;
  const head = cols.map((c) => `<th title="${esc(c.key)}">${esc(c.label)}${c.unit ? `<span class="muted">${esc(c.unit)}</span>` : ""}</th>`).join("");
  const rows = list.map((gs, idx) => `<tr>${cols.map((c) => {
    const v = gs[c.key] ?? c.default ?? "";
    if (c.readonly) return `<td class="mono" title="ID 与 MARL checkpoint 的地面站顺序绑定，表单中不可修改">${esc(v)}</td>`;
    return `<td><input type="text" data-idx="${idx}" data-gkey="${esc(c.key)}" data-gtype="${esc(c.type)}" value="${esc(v)}" id="gs-${idx}-${esc(c.key)}"/></td>`;
  }).join("")}</tr>`).join("");
  wrap.innerHTML = `<label>地面站（${list.length} 个；增删请编辑配置文件）</label>
    ${list.length ? `<div style="overflow-x:auto"><table class="gs"><tr>${head}</tr>${rows}</table></div>` : '<div class="note err">配置中没有地面站</div>'}
    <div class="ferr"></div>`;
  $$("input", wrap).forEach((input) => input.addEventListener("input", () => {
    const gs = WB.working.ground_stations[+input.dataset.idx];
    gs[input.dataset.gkey] = parseValue(input.dataset.gtype, input.value);
    input.classList.remove("invalid");
    formChanged();
  }));
  return wrap;
}

function renderForm() {
  const box = $("#formGroups");
  box.innerHTML = "";
  const groups = WB.meta.field_groups;
  for (const [gid, glabel] of Object.entries(groups)) {
    const details = document.createElement("details");
    details.className = "group";
    details.dataset.group = gid;
    details.open = gid === "basic" || gid === "load";
    const fields = WB.meta.fields.filter((f) => f.group === gid);
    details.innerHTML = `<summary>${esc(glabel)}<span class="cnt"></span></summary>`;
    const grid = document.createElement("div");
    grid.className = "fields";
    for (const f of fields) grid.appendChild(f.type === "ground_stations" ? renderGroundStations(f) : renderField(f));
    details.appendChild(grid);
    box.appendChild(details);
  }
  updateModifiedCounts();
}

function updateModifiedCounts() {
  $$("#formGroups details.group").forEach((d) => {
    const n = $$(".field.modified", d).length;
    $(".cnt", d).textContent = n ? `已修改 ${n}` : "";
  });
  const n = $$("#formGroups .field.modified").length + (same(WB.working?.ground_stations, WB.baseConfig?.ground_stations) ? 0 : 1);
  $("#formState").textContent = n ? `相对「${WB.baseLabel}」修改 ${n} 项` : `与「${WB.baseLabel}」一致`;
}

let validateTimer = null;
function formChanged() {
  updateModifiedCounts();
  clearTimeout(validateTimer);
  validateTimer = setTimeout(() => validate(false), 450);
}

function currentRequest() {
  const policy = $("#selPolicy").value;
  return {
    config: WB.working, policy,
    checkpoint: policy === "marl" ? $("#selCkpt").value : "",
    base_config: WB.baseSource || null,
  };
}

function clearFieldErrors() {
  $$("#sidebar .field.invalid").forEach((f) => f.classList.remove("invalid"));
  $$("#sidebar input.invalid").forEach((i) => i.classList.remove("invalid"));
  $$("#sidebar .ferr").forEach((e) => (e.textContent = ""));
}

function focusField(key) {
  let target = null;
  const gsMatch = key.match(/^ground_stations\.(\d+)\.(\w+)$/);
  if (gsMatch) target = $(`#gs-${gsMatch[1]}-${gsMatch[2]}`);
  if (!target) target = document.getElementById(fieldDomId(key));
  if (key === "policy") target = $("#selPolicy");
  if (!target) return;
  const details = target.closest("details");
  if (details) details.open = true;
  target.scrollIntoView({ block: "center", behavior: "smooth" });
  const input = target.matches("input,select") ? target : $("input,select", target);
  if (input) input.focus();
}

function showValidation(result, { fromRun = false } = {}) {
  clearFieldErrors();
  const box = $("#validationBox");
  const errors = result.errors || [];
  const warnings = result.warnings || [];
  const unplaced = [];
  for (const e of errors) {
    const gsMatch = e.field.match(/^ground_stations\.(\d+)\.(\w+)$/);
    let wrap = null;
    if (gsMatch) {
      const input = $(`#gs-${gsMatch[1]}-${gsMatch[2]}`);
      if (input) input.classList.add("invalid");
      wrap = document.getElementById(fieldDomId("ground_stations"));
    } else {
      wrap = document.getElementById(fieldDomId(e.field));
    }
    if (wrap) {
      wrap.classList.add("invalid");
      const ferr = $(".ferr", wrap);
      ferr.textContent = (ferr.textContent ? ferr.textContent + "；" : "") + e.message;
      const details = wrap.closest("details");
      if (details) details.open = true;
    } else {
      unplaced.push(e);
    }
  }
  let html = "";
  if (errors.length) {
    html += `<div class="msg error"><b>${fromRun ? "未启动：" : ""}校验发现 ${errors.length} 个问题</b><ul>${errors.map((e) =>
      `<li><a data-field="${esc(e.field)}" class="mono">${esc(e.field)}</a>：${esc(e.message)}</li>`).join("")}</ul></div>`;
  } else if (result.ok) {
    html += `<div class="msg info ok">校验通过</div>`;
  }
  if (warnings.length) html += `<div class="msg warning"><ul>${warnings.map((w) => `<li>${esc(w)}</li>`).join("")}</ul></div>`;
  box.innerHTML = html;
  $$("a[data-field]", box).forEach((a) => a.addEventListener("click", () => focusField(a.dataset.field)));
}

let validateSeq = 0;
async function validate(explicit) {
  if (!WB.working) return null;
  const seq = ++validateSeq;
  if (explicit) $("#validationBox").innerHTML = '<div class="msg info">校验中…</div>';
  try {
    const result = await WB.api("/api/validate", { method: "POST", body: currentRequest() });
    if (seq === validateSeq) showValidation(result);
    return result;
  } catch (e) {
    if (seq === validateSeq) $("#validationBox").innerHTML = `<div class="msg error">校验请求失败：${esc(e.message)}</div>`;
    return null;
  }
}

async function selectConfig(path) {
  const cfg = WB.meta.configs.find((c) => c.path === path);
  $("#configDesc").innerHTML = cfg
    ? `${esc(cfg.description || "")} <span class="muted">· ${cfg.num_sats ?? "?"} 星 · ${cfg.sim_steps ?? "?"} 步 · dt ${cfg.dt ?? "?"} s</span>` +
      (cfg.available ? "" : `<div class="err">不可运行：${esc(cfg.problems.join("；"))}</div>`)
    : "";
  try {
    const data = await WB.api("/api/config?path=" + encodeURIComponent(path));
    setBaseConfig(data.config, cfg ? cfg.name : path, path);
  } catch (e) {
    $("#formGroups").innerHTML = `<div class="msg error">读取配置失败：${esc(e.message)}</div>`;
  }
}

function setBaseConfig(config, label, source) {
  WB.baseConfig = clone(config);
  WB.working = clone(config);
  WB.baseLabel = label;
  WB.baseSource = source;
  renderForm();
  validate(false);
}

/** 把历史运行保存的配置载入表单（用于同配置同种子换策略运行） */
WB.loadConfigIntoForm = function (request) {
  const cfg = clone(request.config);
  delete cfg.pipeline_mode;      // 执行模式由策略选择决定
  delete cfg.tile_lifecycle_log;
  const sel = $("#selConfig");
  let opt = $('option[data-run]', sel);
  if (!opt) { opt = document.createElement("option"); opt.dataset.run = "1"; sel.prepend(opt); }
  opt.value = "__run__";
  opt.textContent = `运行 ${request.run_id} 的配置`;
  sel.value = "__run__";
  $("#configDesc").innerHTML = `<span class="muted">基于 ${esc(request.base_config || "页面编辑")}；可换策略后运行以构成同配置同种子对比。</span>`;
  setBaseConfig(cfg, `运行 ${request.run_id}`, request.base_config || `runs/${request.run_id}/config.yaml`);
  $("#selPolicy").value = request.policy;
  onPolicyChange();
  if (request.checkpoint) $("#selCkpt").value = request.checkpoint;
  if ($("#sidebar").classList.contains("collapsed")) $("#sidebar").classList.remove("collapsed");
  WB.toast("已载入运行配置到表单", "info", 2500);
};

function renderPolicySelect() {
  const sel = $("#selPolicy");
  const groups = WB.meta.policy_groups;
  sel.innerHTML = Object.entries(groups).map(([gid, glabel]) =>
    `<optgroup label="${esc(glabel)}">${WB.meta.policies.filter((p) => p.group === gid).map((p) =>
      `<option value="${esc(p.name)}">${esc(p.name)} — ${esc(p.label)}</option>`).join("")}</optgroup>`).join("");
  sel.value = "window_aware";
  sel.addEventListener("change", () => { onPolicyChange(); formChanged(); });
  onPolicyChange();
}

let checkpointsLoaded = false;
async function onPolicyChange() {
  const name = $("#selPolicy").value;
  const p = WB.policyByName[name];
  $("#policyDesc").textContent = p ? p.description : "";
  const isMarl = name === "marl";
  $("#ckptBox").classList.toggle("hidden", !isMarl);
  if (isMarl && !checkpointsLoaded) {
    checkpointsLoaded = true;
    try {
      const data = await WB.api("/api/checkpoints");
      const byDir = {};
      for (const c of data.checkpoints) (byDir[c.dir] = byDir[c.dir] || []).push(c);
      $("#selCkpt").innerHTML = '<option value="">（请选择 checkpoint）</option>' + Object.entries(byDir).map(([dir, list]) =>
        `<optgroup label="${esc(dir)}">${list.map((c) => `<option value="${esc(c.path)}">${esc(c.path)}（${c.size_mb} MB）</option>`).join("")}</optgroup>`).join("");
      if (!data.checkpoints.length) $("#ckptInfo").innerHTML = '<span class="err">checkpoints/ 下没有 best.pt / last.pt</span>';
    } catch (e) {
      checkpointsLoaded = false;
      $("#ckptInfo").innerHTML = `<span class="err">读取 checkpoint 列表失败：${esc(e.message)}</span>`;
    }
  }
}

async function onCheckpointChange() {
  const path = $("#selCkpt").value;
  const info = $("#ckptInfo");
  if (!path) { info.textContent = ""; formChanged(); return; }
  info.textContent = "读取 checkpoint…";
  try {
    const d = await WB.api("/api/checkpoint?path=" + encodeURIComponent(path));
    info.innerHTML = d.ok
      ? `<dl class="kv"><dt>算法</dt><dd>${esc(d.algorithm)} · 模型 ${esc(d.model_version)} · 观测 v${esc(d.observation_version)}</dd>
         <dt>训练配置</dt><dd class="mono">${esc(d.training_config || "—")}</dd>
         <dt>地面站</dt><dd class="mono">${esc((d.ground_station_ids || []).join(", "))}</dd>
         <dt>训练/验证种子</dt><dd class="mono">${esc((d.training_seeds || []).join(","))} / ${esc((d.validation_seeds || []).join(","))}</dd></dl>`
      : `<span class="err">${esc(d.error)}</span>`;
  } catch (e) {
    info.innerHTML = `<span class="err">${esc(e.message)}</span>`;
  }
  formChanged();
}

// ── 运行与进度 ─────────────────────────────────────────────────────────────
async function startRun() {
  const btn = $("#btnRun");
  btn.disabled = true;
  try {
    const detail = await WB.api("/api/runs", { method: "POST", body: currentRequest() });
    showValidation({ ok: true, errors: [], warnings: detail.request.warnings || [] });
    WB.toast(`已启动运行 ${detail.id}`, "info", 2500);
    trackRun(detail.id, detail);
  } catch (e) {
    if (e.status === 400 && e.data && e.data.errors) showValidation(e.data, { fromRun: true });
    else WB.toast(e.message, "error", 7000);
  } finally {
    updateRunButtons();
  }
}

function updateRunButtons() {
  const busy = !!WB.activeRunId;
  $("#btnRun").disabled = busy;
  $("#btnRun").title = busy ? "首版一次只允许运行一个实验" : "";
}

let pollTimer = null;
function trackRun(id, detail) {
  WB.activeRunId = id;
  WB.trackedRunId = id;
  updateRunButtons();
  if (detail) renderRunBox(detail);
  clearTimeout(pollTimer);
  poll();
}

async function poll() {
  const id = WB.trackedRunId;
  if (!id) return;
  let detail;
  try {
    detail = await WB.api(`/api/runs/${encodeURIComponent(id)}`);
    setServerState(true);
  } catch (e) {
    setServerState(false, e.message);
    pollTimer = setTimeout(poll, 2000);
    return;
  }
  renderRunBox(detail);
  const state = detail.status.state;
  if (state === "queued" || state === "running") {
    pollTimer = setTimeout(poll, 600);
    return;
  }
  // 结束
  if (WB.activeRunId === id) {
    WB.activeRunId = null;
    updateRunButtons();
    if (state === "completed") {
      WB.toast(`运行 ${id} 已结束，已载入回放与分析`, "info", 4000);
      WB.loadRun(id);
    } else if (state === "cancelled") {
      WB.toast(`运行 ${id} 已取消`, "warning", 4000);
    } else {
      WB.toast(`运行 ${id} 失败：${detail.status.error || ""}`, "error", 8000);
    }
    if (WB.history && WB.currentTab === "history") WB.history.refresh();
  }
}

function renderRunBox(d) {
  const st = d.status || {};
  const pr = d.progress || {};
  const req = d.request || {};
  const cfg = req.config || {};
  const steps = pr.sim_steps ?? cfg.sim_steps;
  const done = st.state === "completed" ? steps : st.state === "cancelled" ? (st.cancelled_at_step ?? pr.steps_done) : pr.steps_done;
  const frac = steps ? Math.min(1, (done || 0) / steps) : 0;
  const c = pr.counters || {};
  const live = st.state === "queued" || st.state === "running";
  const phase = { starting: "启动 worker", initializing: "初始化环境（轨道、可见性、策略）", simulating: "仿真步进", writing: "写出结果", czml: "生成轨道 CZML", done: "结束" }[st.phase] || st.phase || "";
  $("#runBadge").innerHTML = WB.badge(st.state);
  let html = `<div class="row"><span class="mono grow" title="运行目录 runs/${esc(d.id)}">${esc(d.id)}</span></div>
    <div class="note">${esc(WB.policyLabel(req.policy, req.checkpoint))} · 种子 ${esc(cfg.seed ?? 0)} · dt ${esc(cfg.dt)} s</div>
    <div class="progress" title="后台仿真已执行步数"><div style="width:${(frac * 100).toFixed(1)}%"></div></div>
    <div class="row" style="margin-top:3px"><span class="grow">已执行 <b>${done ?? 0}</b> / ${steps ?? "?"} 步 <span class="muted">（仿真时刻 ${WB.fmt((done || 0) * (cfg.dt || 0), 0)} s）</span></span>
      <span class="muted">${esc(phase)}${pr.elapsed_wall_s != null && live ? ` · 墙钟 ${pr.elapsed_wall_s} s` : st.wall_seconds != null ? ` · 用时 ${st.wall_seconds} s` : ""}</span></div>`;
  if (pr.counters) {
    html += `<div class="note">进度计数（运行中实时）：任务 ${c.total_tasks} 个，完整交付 ${c.completed_tasks}，失败 ${c.failed_tasks}，聚合中 ${c.aggregating_tasks}；分片 ${c.total_tiles} 个，交付 ${c.completed_tiles}，失败 ${c.failed_tiles}</div>`;
  }
  if (st.state === "failed") {
    html += `<div class="msg error"><b>失败原因：</b>${esc(st.error || "未知")}${st.traceback ? `<details><summary>Traceback / worker 日志</summary><pre class="trace">${esc(st.traceback)}</pre></details>` : ""}</div>`;
  } else if (st.state === "cancelled") {
    html += `<div class="msg warning">${esc(st.error || "已取消")}${st.cancelled_at_step != null ? `（停止于第 ${st.cancelled_at_step} 步）` : ""}</div>`;
  } else if (st.state === "completed" && st.czml_error) {
    html += `<div class="msg warning">轨道 CZML 生成失败（不影响结果）：${esc(st.czml_error)}</div>`;
  }
  if (live && st.cancel_requested_at) html += `<div class="note warn">已请求取消，等待当前步结束…</div>`;
  html += `<div class="row" style="margin-top:5px">
    ${live ? `<button class="btn danger small" data-act="cancel" ${st.cancel_requested_at ? "disabled" : ""}>取消运行</button>` : ""}
    ${!live ? `<button class="btn small" data-act="rerun" title="用完全相同的配置、策略与种子在新目录重新运行">重新运行</button>` : ""}
    ${!live && (d.files || []).includes("snapshots.json") ? `<button class="btn small primary" data-act="load">载入回放/分析</button>` : ""}
    ${!live ? `<button class="btn small" data-act="form" title="把该运行的配置载入表单，可换策略构成对比">配置载入表单</button>` : ""}
  </div>`;
  $("#runBox").innerHTML = html;
  $$("#runBox button[data-act]").forEach((b) => b.addEventListener("click", () => runAction(b.dataset.act, d)));
}

async function runAction(act, d) {
  try {
    if (act === "cancel") {
      const detail = await WB.api(`/api/runs/${encodeURIComponent(d.id)}/cancel`, { method: "POST" });
      renderRunBox(detail);
    } else if (act === "rerun") {
      WB.rerun(d.id);
    } else if (act === "load") {
      WB.loadRun(d.id);
    } else if (act === "form") {
      WB.loadConfigIntoForm(d.request);
    }
  } catch (e) {
    WB.toast(e.message, "error", 6000);
  }
}

WB.rerun = async function (id) {
  try {
    const detail = await WB.api(`/api/runs/${encodeURIComponent(id)}/rerun`, { method: "POST" });
    WB.toast(`已重新运行：${detail.id}`, "info", 2500);
    trackRun(detail.id, detail);
    WB.showTab("replay");
    if ($("#sidebar").classList.contains("collapsed")) $("#sidebar").classList.remove("collapsed");
  } catch (e) {
    WB.toast(e.message, "error", 6000);
  }
};

WB.showRunInPanel = function (detail) {
  if (WB.activeRunId) return;   // 不覆盖正在执行的运行
  WB.trackedRunId = detail.id;
  renderRunBox(detail);
};

/** 载入某次运行到回放与分析（两者共享同一运行）；只采用最后一次请求，避免启动时的自动载入覆盖用户的选择 */
WB.loadRunSeq = 0;
WB.loadRun = async function (id) {
  const seq = ++WB.loadRunSeq;
  let detail;
  try {
    detail = await WB.api(`/api/runs/${encodeURIComponent(id)}`);
  } catch (e) {
    if (seq === WB.loadRunSeq) WB.toast("载入运行失败：" + e.message, "error");
    return;
  }
  if (seq !== WB.loadRunSeq) return;
  WB.loaded = detail;
  WB.showRunInPanel(detail);
  await Promise.all([WB.replay.load(detail), WB.analysis.load(detail)]);
};

function setServerState(ok, message) {
  const el = $("#serverState");
  el.innerHTML = ok
    ? `服务正常 · 运行目录 <span class="mono">${esc(WB.meta?.runs_dir || "")}</span>`
    : `<span class="err">服务不可用：${esc(message || "")}</span>`;
}

// ── 启动 ───────────────────────────────────────────────────────────────────
async function init() {
  $$("#tabs button").forEach((b) => b.addEventListener("click", () => WB.showTab(b.dataset.tab)));
  $("#sidebarToggle").addEventListener("click", () => $("#sidebar").classList.toggle("collapsed"));
  $("#btnValidate").addEventListener("click", () => validate(true));
  $("#btnRun").addEventListener("click", startRun);
  $("#btnReset").addEventListener("click", () => {
    if ($("#selConfig").value === "__run__") setBaseConfig(WB.baseConfig, WB.baseLabel, WB.baseSource);
    else selectConfig($("#selConfig").value);
  });
  $("#selCkpt").addEventListener("change", onCheckpointChange);
  WB.replay.init();
  WB.analysis.init();
  WB.history.init();
  try {
    WB.meta = await WB.api("/api/meta");
  } catch (e) {
    setServerState(false, e.message);
    $("#formGroups").innerHTML = `<div class="msg error">无法读取工作台元数据：${esc(e.message)}</div>`;
    return;
  }
  setServerState(true);
  WB.meta.policies.forEach((p) => (WB.policyByName[p.name] = p));
  const sel = $("#selConfig");
  sel.innerHTML = WB.meta.configs.map((c) =>
    `<option value="${esc(c.path)}" ${c.available ? "" : "disabled"}>${esc(c.name)}${c.available ? "" : "（缺少 TLE，不可运行）"}</option>`).join("");
  sel.value = WB.meta.configs.find((c) => c.name === "config_sar_current" && c.available)?.path
    || WB.meta.configs.find((c) => c.name === "config_smoke" && c.available)?.path
    || WB.meta.configs.find((c) => c.available)?.path;
  sel.addEventListener("change", () => {
    $('option[data-run]', sel)?.remove();
    selectConfig(sel.value);
  });
  renderPolicySelect();
  WB.replay.setupCesium(WB.meta.cesium);
  await selectConfig(sel.value);
  if (WB.meta.active_run) {
    trackRun(WB.meta.active_run);
  }
  // 默认载入最近一次有过程数据的运行，便于直接查看；用户已主动选择运行时不再覆盖
  try {
    const { runs } = await WB.api("/api/runs");
    const latest = runs.find((r) => r.state === "completed");
    if (latest && !WB.loadRunSeq) WB.loadRun(latest.id);
  } catch (e) { /* 历史为空或读取失败时保持空状态 */ }
}

window.addEventListener("DOMContentLoaded", init);
