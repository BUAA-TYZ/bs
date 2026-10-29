/* 仿真回放：Cesium/CZML 三维场景 + 采样过程快照（snapshots.json）。
 *
 * 时间对应（与 workbench/observer.py 一致）：回放时刻 t 秒对应步 n = floor(t / dt)。
 * - 链路 / 星地可见 / 实际传输：取覆盖步 n 的快照（链路为该快照末步的拓扑，传输量为快照区间内累计）；
 * - 卫星与地面站状态、计数：取最近一个“状态时刻 ≤ t”的快照（第 k 步快照的状态时刻为 (k+1)·dt）。
 * 所有数值都来自仿真输出；没有快照的时段明确显示为无过程数据。 */
"use strict";
(function () {
  const { $, $$, esc } = WB;
  const LAYERS = [
    { key: "labels", label: "卫星标签", on: true },
    { key: "orbits", label: "轨道线", on: true },
    { key: "gs", label: "地面站", on: true },
    { key: "links", label: "可见星间链路", on: true, swatch: "#7f8ea3", title: "拓扑中处于连通状态的星间链路（几何可见且满足距离/邻居约束），不代表有数据传输" },
    { key: "isl", label: "实际星间传输", on: true, swatch: "#38d0ff", title: "快照区间内 network_sent_mb 的实际发送量，箭头指向接收星，线宽随 MB 增大" },
    { key: "gsvis", label: "星地几何可见", on: false, swatch: "#d8c25a", title: "满足地面站最小仰角的星地几何可见关系，不代表正在传输" },
    { key: "gsflow", label: "星地实际传输", on: true, swatch: "#ff9a3d", title: "下传结果、原始分片或整图上传，快照区间内实际发送量 > 0" },
    { key: "taskflow", label: "任务流向", on: false, swatch: "#ff4fd8", title: "未选任务时按（类型, 源, 目的）汇总全部流；选中任务时仅显示该任务的分片流" },
  ];
  const FLOW_LABEL = { isl_raw: "原始分片星间传输", isl_result: "结果星间中继", downlink: "结果下传", ground_tile: "原始分片下传", ground_task: "整图上传" };
  const FLOW_STATUS = { active: "进行中", done: "本区间完成", aborted: "中止/撤销" };

  const R = {
    viewer: null, cesiumError: null, imageryNote: "", ds: null, dyn: null, startJD: null,
    detail: null, meta: null, snaps: [], t: 0, step: -1, playing: false, speed: 60, lastFrame: 0,
    roles: WB.satRoles(null), selected: null, task: null, layers: Object.fromEntries(LAYERS.map((l) => [l.key, l.on])),
    renderKey: "", loadSeq: 0, detailsHtml: "", detailsAt: 0,
  };
  WB.replayState = R;

  // ── Cesium ───────────────────────────────────────────────────────────────
  function loadScript(src, timeoutMs) {
    return new Promise((resolve, reject) => {
      const s = document.createElement("script");
      const timer = setTimeout(() => reject(new Error("加载超时")), timeoutMs);
      s.src = src;
      s.onload = () => { clearTimeout(timer); resolve(); };
      s.onerror = () => { clearTimeout(timer); reject(new Error("脚本加载失败")); };
      document.head.appendChild(s);
    });
  }

  function overlay(html) { $("#globeOverlay").innerHTML = html || ""; }

  async function setupCesium(cfg) {
    const base = (cfg.base_url || "").replace(/\/?$/, "/");
    window.CESIUM_BASE_URL = base;
    const css = document.createElement("link");
    css.rel = "stylesheet";
    css.href = base + "Widgets/widgets.css";
    document.head.appendChild(css);
    overlay('<div class="msg info">正在加载 Cesium 三维地球…</div>');
    try {
      await loadScript(base + "Cesium.js", 20000);
    } catch (e) {
      R.cesiumError = `Cesium 不可用（${e.message}：${base}Cesium.js）`;
      overlay(`<div class="msg warning"><b>三维地球不可用</b><br>${esc(R.cesiumError)}。<br>
        可设置环境变量 <span class="mono">WORKBENCH_CESIUM_URL</span> 指向本地 Cesium Build 目录后重启服务。<br>
        时间轴、实体详情与结果分析仍可使用（右侧实体列表可选择卫星/地面站）。</div>`);
      updateLoadedInfo();
      return;
    }
    try {
      let baseLayer;
      if (cfg.ion_token) {
        Cesium.Ion.defaultAccessToken = cfg.ion_token;
        baseLayer = Cesium.ImageryLayer.fromWorldImagery();
        R.imageryNote = "Cesium ion 影像";
      } else {
        baseLayer = Cesium.ImageryLayer.fromProviderAsync(
          Cesium.TileMapServiceImageryProvider.fromUrl(Cesium.buildModuleUrl("Assets/Textures/NaturalEarthII")));
        R.imageryNote = "离线底图 NaturalEarthII（未配置 ion 令牌）";
      }
      baseLayer.errorEvent.addEventListener((err) => {
        R.imageryNote = "底图加载失败，显示纯色地球：" + (err && err.message ? err.message : err);
        updateLoadedInfo();
      });
      R.viewer = new Cesium.Viewer("globe", {
        baseLayer, baseLayerPicker: false, geocoder: false, animation: false, timeline: false,
        homeButton: true, sceneModePicker: true, navigationHelpButton: false, fullscreenButton: false,
        infoBox: false, selectionIndicator: true, shouldAnimate: false,
      });
      R.viewer.scene.globe.baseColor = Cesium.Color.fromCssColorString("#1d2a3a");
      R.viewer.clock.shouldAnimate = false;
      R.dyn = new Cesium.CustomDataSource("workbench-dynamic");
      R.viewer.dataSources.add(R.dyn);
      R.viewer.selectedEntityChanged.addEventListener(onCesiumSelect);
      // 链路线、传输线与轨道线都与卫星图标重叠，默认 scene.pick 常拾取到线而选不中卫星；
      // 改为 drillPick 取点击点附近的全部对象，优先卫星与地面站。
      const handler = R.viewer.screenSpaceEventHandler;
      handler.setInputAction((m) => {
        const e = pickNode(m.position);
        if (e) select(nodeOf(e), { indicator: true });
      }, Cesium.ScreenSpaceEventType.LEFT_CLICK);
      handler.setInputAction((m) => {
        R.viewer.trackedEntity = pickNode(m.position) || undefined;
      }, Cesium.ScreenSpaceEventType.LEFT_DOUBLE_CLICK);
      overlay(R.detail ? "" : '<div class="msg info">尚未载入运行。完成一次运行或在“历史与对比”中载入运行后即可回放。</div>');
      if (R.detail) await loadCzml(R.detail.id, R.loadSeq);
    } catch (e) {
      R.cesiumError = "Cesium 初始化失败：" + e.message;
      R.viewer = null;
      overlay(`<div class="msg error">${esc(R.cesiumError)}</div>`);
    }
    updateLoadedInfo();
  }

  function pickNode(position) {
    if (!R.viewer || !R.ds || !position) return null;
    let picked = [];
    try { picked = R.viewer.scene.drillPick(position, 12, 11, 11); } catch (e) { return null; }
    // 多个卫星/地面站图标重叠时，取屏幕中心离点击点最近的（地面站图标底边锚定在站址，中心在其上方）
    const toWin = Cesium.SceneTransforms.worldToWindowCoordinates || Cesium.SceneTransforms.wgs84ToWindowCoordinates;
    const time = R.viewer.clock.currentTime;
    let best = null, bestD = Infinity;
    for (const p of picked) {
      const id = p && p.id && typeof p.id.id === "string" ? p.id.id : "";
      if (!(id.startsWith("sat-") || id.startsWith("gs-")) || ent(id) !== p.id) continue;
      const pos = p.id.position && p.id.position.getValue(time);
      const w = pos && toWin(R.viewer.scene, pos);
      const d = w ? Math.hypot(w.x - position.x, w.y - (id.startsWith("gs-") ? 14 : 0) - position.y) : Infinity;
      if (!best || d < bestD) { best = p.id; bestD = d; }
    }
    return best;
  }
  const nodeOf = (e) => (e.id.startsWith("sat-") ? { type: "sat", id: +e.id.slice(4) } : { type: "gs", id: e.id.slice(3) });

  function onCesiumSelect(entity) {
    if (!entity || R.syncingSelection) return;
    const id = entity.id || "";
    if (id.startsWith("sat-")) select({ type: "sat", id: +id.slice(4) });
    else if (id.startsWith("gs-")) select({ type: "gs", id: id.slice(3) });
    else if (entity.wbFlow) { /* 动态线：保留当前选择 */ }
  }

  async function loadCzml(runId, seq) {
    if (!R.viewer) return;
    if (R.ds) { R.viewer.dataSources.remove(R.ds, true); R.ds = null; }
    R.dyn.entities.removeAll();
    if (!(R.detail.files || []).includes("czml.json")) {
      overlay(`<div class="msg warning">该运行没有轨道 CZML${R.detail.status.czml_error ? "：" + esc(R.detail.status.czml_error) : ""}；过程数据仍可在时间轴与详情中查看。</div>`);
      return;
    }
    overlay('<div class="msg info">载入轨道 CZML…</div>');
    try {
      const czml = await WB.api(`/api/runs/${encodeURIComponent(runId)}/czml`);
      if (seq !== R.loadSeq) return;
      const ds = await Cesium.CzmlDataSource.load(czml);
      if (seq !== R.loadSeq) return;
      R.ds = ds;
      await R.viewer.dataSources.add(ds);
      R.viewer.clockTrackedDataSource = undefined;
      R.viewer.clock.shouldAnimate = false;
      R.startJD = Cesium.JulianDate.fromIso8601(R.meta.start_time_utc);
      overlay("");
      styleGroundStations();
      styleSatellites();
      applyStaticLayers();
      R.renderKey = "";
      setTime(R.t, true);
      // 同步设定初始视角（以当前选中实体为中心）；不用 viewer.flyTo，其异步落地会覆盖用户随后的跳转
      const sel = R.selected;
      focusEntity(ent(sel && sel.type === "gs" ? `gs-${sel.id}` : `sat-${sel ? sel.id : 0}`), 1.5e7);
    } catch (e) {
      overlay(`<div class="msg error">轨道 CZML 载入失败：${esc(e.message)}</div>`);
    }
  }

  const ent = (id) => (R.ds ? R.ds.entities.getById(id) : null);

  // 地面站图标：抛物面天线，白色主体经 billboard.color 着色，底边中点锚定在站址
  const GS_ICON = "data:image/svg+xml;base64," + btoa(`<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 40 40">
<g fill="#fff" stroke="#111820" stroke-width="2" stroke-linejoin="round" stroke-linecap="round">
<path d="M17 25 L20 28"/><path d="M13 38 L20 28 L27 38 Z"/>
<path d="M10 8 C8 22 21 32 36 29 Z"/><path d="M23 18.5 L29 11"/><circle cx="30.5" cy="9.5" r="2.5"/>
</g></svg>`);

  // CZML 中的地面站只是 1 像素方块，替换为天线图标并精简标签（坐标/带宽见详情面板；不改动 CZML 文件本身）
  function styleGroundStations() {
    for (const e of R.ds.entities.values) {
      if (!e.id.startsWith("gs-")) continue;
      e.billboard = new Cesium.BillboardGraphics({
        image: GS_ICON, color: Cesium.Color.fromBytes(255, 220, 50), scale: 0.7,
        verticalOrigin: Cesium.VerticalOrigin.BOTTOM, horizontalOrigin: Cesium.HorizontalOrigin.CENTER,
      });
      if (e.label) {
        e.label.text = new Cesium.ConstantProperty(e.id.slice(3));
        e.label.pixelOffset = new Cesium.ConstantProperty(new Cesium.Cartesian2(20, -14));
      }
    }
  }

  // 卫星图标按角色区分：主体为白色，billboard.color 叠加运行状态色（执行中/有排队/空闲）
  const svgUri = (body) => "data:image/svg+xml;base64," + btoa(`<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 64 64">${body}</svg>`);
  // 成像星：星体 + 长条相控阵天线板 + 下方波束弧
  const SOURCE_ICON = svgUri(`<g stroke="#111820" stroke-width="2" stroke-linejoin="round" fill="#fff">
<rect x="23" y="9" width="18" height="15" rx="2"/><rect x="3" y="25" width="58" height="12" rx="1.5"/></g>
<path d="M13 25v12M22.5 25v12M32 25v12M41.5 25v12M51 25v12" stroke="#111820" stroke-width="1.3"/>
<g fill="none" stroke-linecap="round"><path d="M21 45 Q32 52 43 45M13 53 Q32 63 51 53" stroke="#111820" stroke-width="5.5"/>
<path d="M21 45 Q32 52 43 45M13 53 Q32 63 51 53" stroke="#fff" stroke-width="2.5"/></g>`);
  // 计算/中继星：芯片主体 + 两侧太阳能板
  const COMPUTE_ICON = svgUri(`<g stroke="#111820" stroke-width="2" stroke-linejoin="round" fill="#fff">
<rect x="2" y="25" width="15" height="14" rx="1.5"/><rect x="47" y="25" width="15" height="14" rx="1.5"/>
<rect x="16" y="30" width="6" height="4"/><rect x="42" y="30" width="6" height="4"/>
<rect x="25" y="12" width="4" height="9"/><rect x="35" y="12" width="4" height="9"/><rect x="25" y="43" width="4" height="9"/><rect x="35" y="43" width="4" height="9"/>
<rect x="20" y="19" width="24" height="26" rx="2.5"/></g>
<path d="M9.5 25v14M54.5 25v14" stroke="#111820" stroke-width="1.3"/><rect x="26.5" y="26.5" width="11" height="11" rx="1" fill="#111820"/>`);
  const ROLE_STYLE = {
    source: { icon: SOURCE_ICON, label: "#8fd3ff" },
    compute: { icon: COMPUTE_ICON, label: "#d4b4ff" },
  };
  const roleStyle = (i) => ROLE_STYLE[R.roles.role(i) === R.roles.compute ? "compute" : "source"];

  function styleSatellites() {
    for (let i = 0; i < R.meta.num_sats; i++) {
      const e = ent(`sat-${i}`);
      if (!e) continue;
      const st = roleStyle(i);
      if (e.billboard) e.billboard.image = new Cesium.ConstantProperty(st.icon);
      if (e.label) e.label.fillColor = new Cesium.ConstantProperty(Cesium.Color.fromCssColorString(st.label));
    }
  }

  function applyStaticLayers() {
    if (!R.ds) return;
    for (const e of R.ds.entities.values) {
      if (e.id.startsWith("orbit-")) e.show = R.layers.orbits;
      else if (e.id.startsWith("gs-")) e.show = R.layers.gs;
      else if (e.id.startsWith("sat-") && e.label) e.label.show = R.layers.labels;
    }
  }

  // ── 快照查询 ─────────────────────────────────────────────────────────────
  const dt = () => R.meta.dt;
  const endSeconds = () => R.meta.sim_steps * R.meta.dt;
  function stepAt(t) { return Math.max(0, Math.min(R.meta.sim_steps - 1, Math.floor(t / dt() + 1e-9))); }
  /** 覆盖步 n 的快照下标；超出已记录范围返回 -1 */
  function snapContaining(n) {
    const s = R.snaps;
    let lo = 0, hi = s.length - 1, ans = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (s[mid].span[1] >= n) { ans = mid; hi = mid - 1; } else lo = mid + 1;
    }
    return ans >= 0 && s[ans].span[0] <= n ? ans : -1;
  }
  /** 状态时刻 ≤ n·dt 的最近快照下标（状态时刻 = (span[1]+1)·dt）；没有时返回 -1 */
  function stateSnapAt(n) {
    const s = R.snaps;
    let lo = 0, hi = s.length - 1, ans = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (s[mid].span[1] + 1 <= n) { ans = mid; lo = mid + 1; } else hi = mid - 1;
    }
    return ans;
  }

  // ── 时间控制 ─────────────────────────────────────────────────────────────
  function setTime(t, force) {
    if (!R.meta) return;
    R.t = Math.max(0, Math.min(endSeconds(), t));
    const n = stepAt(R.t);
    const slider = $("#tlSlider");
    if (+slider.value !== n) slider.value = n;
    if (R.viewer && R.startJD) R.viewer.clock.currentTime = Cesium.JulianDate.addSeconds(R.startJD, R.t, new Cesium.JulianDate());
    updateReadout();
    if (n !== R.step || force) {
      R.step = n;
      renderStep(force);
    }
  }

  function updateReadout() {
    const n = stepAt(R.t);
    const utc = WB.utcAt(R.meta.start_time_utc, R.t);
    $("#tlReadout").innerHTML = `t = ${R.t.toFixed(1)} s / ${endSeconds().toFixed(0)} s · 步 ${n} / ${R.meta.sim_steps - 1} · <span class="muted">${esc(utc)}</span>` +
      (R.playing ? "" : R.t >= endSeconds() - 1e-6 ? ' · <span class="warn">回放结束</span>' : "");
  }

  function setPlaying(on) {
    if (!R.meta) return;
    if (on && R.t >= endSeconds() - 1e-6) setTime(0);
    const wasPlaying = R.playing;
    R.playing = on;
    $("#tlPlay").textContent = on ? "⏸ 暂停" : "▶ 播放";
    R.lastFrame = performance.now();
    if (on) requestAnimationFrame(frame);
    else if (wasPlaying) renderStep(true);  // 补上播放期间被节流的详情
    updateReadout();
  }

  function frame(now) {
    if (!R.playing) return;
    const elapsed = Math.min(0.25, (now - R.lastFrame) / 1000);
    R.lastFrame = now;
    const next = R.t + elapsed * R.speed;
    if (next >= endSeconds()) {
      setTime(endSeconds());
      setPlaying(false);
      return;
    }
    setTime(next);
    requestAnimationFrame(frame);
  }

  function jumpSnapshot(dir) {
    if (!R.snaps.length) return;
    const n = stepAt(R.t);
    const idx = snapContaining(n);
    let target;
    if (dir > 0) {
      const nextIdx = idx >= 0 ? idx + 1 : R.snaps.findIndex((s) => s.span[0] > n);
      if (nextIdx < 0 || nextIdx >= R.snaps.length) return;
      target = R.snaps[nextIdx].span[1];
    } else {
      const prevIdx = idx >= 0 ? idx - 1 : stateSnapAt(n);
      if (prevIdx < 0) { setTime(0); return; }
      target = R.snaps[prevIdx].span[1];
    }
    setTime(target * dt());
  }
  WB.replaySeekStep = (step) => setTime(step * dt());

  // ── 渲染当前步 ───────────────────────────────────────────────────────────
  function renderStep(force) {
    const n = R.step;
    const li = snapContaining(n);
    const si = stateSnapAt(n);
    renderDynamic(li >= 0 ? R.snaps[li] : null, si >= 0 ? R.snaps[si] : null);
    // 播放时详情面板最多约 4 次/秒刷新，避免整块重建打断阅读和点击
    const now = performance.now();
    if (force || !R.playing || now - R.detailsAt >= 250) {
      R.detailsAt = now;
      renderDetails(li, si);
    }
    drawTimeline();
  }

  function satPos(i) { const e = ent(`sat-${i}`); return e ? e.position : null; }
  function nodePos(node) {
    if (typeof node === "number") return satPos(node);
    const e = ent(`gs-${node}`);
    return e ? e.position : null;
  }

  function addLine(a, b, opts) {
    const pa = nodePos(a), pb = nodePos(b);
    if (!pa || !pb) return;
    const scratch = [new Cesium.Cartesian3(), new Cesium.Cartesian3()];
    const e = R.dyn.entities.add({
      polyline: {
        positions: new Cesium.CallbackProperty((time) => {
          const x = pa.getValue(time, scratch[0]);
          const y = pb.getValue(time, scratch[1]);
          return x && y ? [x, y] : [];
        }, false),
        width: opts.width, material: opts.material, arcType: Cesium.ArcType.NONE,
      },
    });
    e.wbFlow = true;
  }

  function satColor(row) {
    if (!row) return Cesium.Color.WHITE;
    if (row[1]) return Cesium.Color.fromCssColorString("#4cc38a");
    if (row[0] > 0) return Cesium.Color.fromCssColorString("#ffb347");
    return Cesium.Color.WHITE;
  }

  function renderDynamic(linkSnap, stateSnap) {
    if (!R.viewer || !R.ds) return;
    const L = R.layers;
    const key = [linkSnap ? linkSnap.step : -1, stateSnap ? stateSnap.step : -1, JSON.stringify(L), R.task, JSON.stringify(R.selected)].join("|");
    if (key === R.renderKey) return;
    R.renderKey = key;
    const C = Cesium.Color;
    R.dyn.entities.suspendEvents();
    R.dyn.entities.removeAll();
    if (linkSnap) {
      if (L.links) for (const [i, j] of linkSnap.links) {
        addLine(i, j, { width: 1.2, material: new Cesium.PolylineDashMaterialProperty({ color: C.fromCssColorString("#7f8ea3").withAlpha(0.75), dashLength: 10 }) });
      }
      if (L.isl) {
        const maxMb = Math.max(1e-9, ...linkSnap.isl_sent.map((r) => r[2]));
        for (const [src, dst, mb] of linkSnap.isl_sent) {
          addLine(src, dst, { width: 7 + 7 * Math.min(1, mb / maxMb), material: new Cesium.PolylineArrowMaterialProperty(C.fromCssColorString("#38d0ff").withAlpha(0.9)) });
        }
      }
      if (L.gsvis) for (const [sat, gi] of linkSnap.gs_visible) {
        addLine(sat, R.meta.gs_ids[gi], { width: 1.2, material: new Cesium.PolylineDashMaterialProperty({ color: C.fromCssColorString("#d8c25a").withAlpha(0.8), dashLength: 8 }) });
      }
      const groups = flowGroups(linkSnap);
      if (L.gsflow) {
        const seen = new Set();
        for (const [kind, src, dst, , moving] of groups) {
          if (typeof dst !== "string" || !moving) continue;
          const k = `${src}>${dst}`;
          if (seen.has(k)) continue;
          seen.add(k);
          addLine(src, dst, { width: 9, material: new Cesium.PolylineArrowMaterialProperty(C.fromCssColorString(kind === "downlink" ? "#ff9a3d" : "#ff6a3d").withAlpha(0.95)) });
        }
      }
      if (L.taskflow) {
        const seen = new Set();
        const edges = R.task
          ? linkSnap.flows.filter(([kind, id]) => flowBelongsToTask(kind, id, R.task)).map(([kind, , src, dst]) => [kind, src, dst])
          : groups;
        for (const [kind, src, dst] of edges) {
          const k = `${kind}|${src}>${dst}`;
          if (seen.has(k)) continue;
          seen.add(k);
          const color = kind === "isl_result" || kind === "downlink" ? "#7dff8a" : "#ff4fd8";
          addLine(src, dst, { width: 10, material: new Cesium.PolylineGlowMaterialProperty({ color: C.fromCssColorString(color), glowPower: 0.25 }) });
        }
      }
    }
    R.dyn.entities.resumeEvents();
    // 卫星着色：执行中 绿 / 有排队 橙 / 空闲 白（状态时刻的快照）
    for (let i = 0; i < R.meta.num_sats; i++) {
      const e = ent(`sat-${i}`);
      if (!e || !e.billboard) continue;
      const row = stateSnap ? stateSnap.sats[i] : null;
      const sel = R.selected && R.selected.type === "sat" && R.selected.id === i;
      e.billboard.color = new Cesium.ConstantProperty(satColor(row));
      e.billboard.scale = new Cesium.ConstantProperty(sel ? 0.75 : 0.5);
      if (e.label) e.label.text = new Cesium.ConstantProperty(R.roles.name(i) + (row && row[0] ? ` Q${row[0]}` : ""));
    }
    for (const gs of R.meta.gs_ids) {
      const e = ent(`gs-${gs}`);
      if (!e || !e.billboard) continue;
      e.billboard.scale = new Cesium.ConstantProperty(R.selected && R.selected.type === "gs" && R.selected.id === gs ? 0.9 : 0.7);
    }
  }

  // 按（类型, 源, 目的）的完整汇总；旧快照文件无 flow_groups 时由完整的逐分片明细聚合
  function flowGroups(snap) {
    if (snap.flow_groups) return snap.flow_groups;
    if (!snap._groups) {
      const m = new Map();
      for (const [kind, , src, dst, sent, , status] of snap.flows) {
        const k = `${kind}|${src}|${dst}`;
        if (!m.has(k)) m.set(k, [kind, src, dst, 0, 0, 0, 0, 0]);
        const g = m.get(k);
        g[3] += 1; g[4] += sent === null || sent > 1e-9 ? 1 : 0; g[5] += sent || 0;
        g[6] += status === "done" ? 1 : 0; g[7] += status === "aborted" ? 1 : 0;
      }
      snap._groups = [...m.values()];
    }
    return snap._groups;
  }

  function flowBelongsToTask(kind, id, taskId) {
    return kind === "ground_task" ? id === taskId : String(id).startsWith(taskId + "_tile_");
  }

  // ── 详情面板 ─────────────────────────────────────────────────────────────
  function bar(used, cap) {
    if (!WB.isNum(used) || !WB.isNum(cap) || cap <= 0) return "";
    const f = Math.min(1, used / cap);
    return `<div class="bar"><div style="width:${(f * 100).toFixed(1)}%;background:${f > 0.9 ? "var(--err)" : "var(--accent)"}"></div></div>`;
  }

  function setDetails(html) {
    if (html === R.detailsHtml) return;
    R.detailsHtml = html;
    $("#details").innerHTML = html;
  }

  function renderDetails(li, si) {
    if (!R.meta) { setDetails('<div class="box"><div class="note">尚未载入运行。</div></div>'); return; }
    const n = R.step, d = dt();
    const linkSnap = li >= 0 ? R.snaps[li] : null;
    const stateSnap = si >= 0 ? R.snaps[si] : null;
    let html = '<div class="box"><h2>当前时刻数据来源</h2>';
    if (!R.meta.complete && n > R.meta.last_step) {
      html += `<div class="msg warning">该运行在第 ${R.meta.last_step + 1} 步取消，此时刻无过程数据。</div>`;
    }
    html += `<dl class="kv"><dt>链路/传输</dt><dd>${linkSnap ? `快照 #${li}：步 ${linkSnap.span[0]}–${linkSnap.span[1]}${linkSnap.span[0] !== linkSnap.span[1] ? "（区间累计；链路取末步）" : ""}` : WB.NA}</dd>
      <dt>状态</dt><dd>${stateSnap ? `第 ${stateSnap.step} 步结束（${WB.fmt((stateSnap.step + 1) * d, 1)} s）` : '<span class="muted">仿真开始时刻，尚无状态快照</span>'}</dd>
      <dt>采样</dt><dd>每 ${R.meta.stride} 步一个快照，共 ${R.snaps.length} 个</dd></dl></div>`;
    if (stateSnap) {
      const c = stateSnap.counters;
      html += `<div class="box"><h2>全局计数 <span class="right muted">状态时刻</span></h2><dl class="kv">
        <dt>任务</dt><dd>${c.total_tasks} 个 · 完整交付 <span class="ok">${c.completed_tasks}</span> · 失败 <span class="err">${c.failed_tasks}</span> · 聚合中 ${c.aggregating_tasks}</dd>
        <dt>分片</dt><dd>${c.total_tiles} 个 · 已交付地面 ${c.completed_tiles} · 失败 ${c.failed_tiles}</dd></dl>
        <div class="note">任务“完整交付”= 全部分片结果到达地面并完成聚合，不是计算结束。</div></div>`;
    }
    // 实体列表
    html += '<div class="box"><h2>实体选择 <span class="right muted" title="按钮选择时视角直接跳到该实体上方；双击地球上的实体可持续跟随">点击地球或下方按钮（按钮跳转视角）</span></h2><div class="satlist">';
    for (let i = 0; i < R.meta.num_sats; i++) {
      const row = stateSnap ? stateSnap.sats[i] : null;
      const cls = row && row[1] ? "exec" : row && row[0] ? "queue" : "";
      const sel = R.selected && R.selected.type === "sat" && R.selected.id === i;
      const role = R.roles.role(i);
      html += `<button type="button" data-sat="${i}" class="role-${role.kind} ${sel ? "sel" : ""}" title="${esc(role.label)}"><span class="dot ${cls}"></span>${esc(R.roles.name(i))}</button>`;
    }
    for (const gs of R.meta.gs_ids) {
      const sel = R.selected && R.selected.type === "gs" && R.selected.id === gs;
      html += `<button type="button" data-gs="${esc(gs)}" class="${sel ? "sel" : ""}" style="grid-column: span 2">⌂ ${esc(gs)}</button>`;
    }
    const { source, compute } = R.roles;
    const hasCompute = R.meta.satellites.some((s) => !s.task_source);
    html += `</div><div class="note"><span class="dot exec"></span>执行中 <span class="dot queue"></span>有排队 <span class="dot"></span>空闲</div>
      <div class="note"><span class="rolemark role-${source.kind}">${source.prefix}</span>${esc(source.label)}（产生遥感任务）${hasCompute ? ` <span class="rolemark role-compute">${compute.prefix}</span>${esc(compute.label)}（不产生任务）` : ""}；编号为仿真 sat_id</div></div>`;
    if (R.selected) html += R.selected.type === "sat" ? satDetail(R.selected.id, linkSnap, stateSnap) : gsDetail(R.selected.id, linkSnap, stateSnap);
    if (R.task) html += taskFlowDetail(linkSnap);
    setDetails(html);
  }

  // 详情面板随回放时间重建，鼠标按下与抬起可能落在不同节点上而不产生 click；
  // 因此在容器上委托：鼠标在按下时即响应，触屏与键盘仍走 click。
  function bindDetails() {
    const box = $("#details");
    let mouseActAt = -1e9;
    const act = (e) => {
      const t = e.target.closest("[data-sat],[data-gs],[data-task],#clearTask");
      if (!t || !box.contains(t)) return;
      if (t.dataset.sat !== undefined) select({ type: "sat", id: +t.dataset.sat }, { indicator: true, focus: true });
      else if (t.dataset.gs !== undefined) select({ type: "gs", id: t.dataset.gs }, { indicator: true, focus: true });
      else if (t.dataset.task !== undefined) WB.analysis.openTask(t.dataset.task);
      else focusTask(null);
    };
    box.addEventListener("pointerdown", (e) => {
      if (e.pointerType === "mouse" && e.button === 0) { mouseActAt = e.timeStamp; act(e); }
    });
    box.addEventListener("click", (e) => { if (e.timeStamp - mouseActAt > 800) act(e); });
  }

  function omittedNote(snap) {
    return snap.flows_omitted ? `<div class="note warn">本快照共有 ${snap.flows.length + snap.flows_omitted} 条分片流，逐分片明细只保留 ${snap.flows.length} 条（优先保留本区间结束、发送量大的），另 ${snap.flows_omitted} 条未逐条记录；按链路的汇总（卫星/地面站详情）完整。</div>` : "";
  }

  function groupRows(snap, filter) {
    const rows = flowGroups(snap).filter(filter);
    if (!rows.length) return '<div class="note">本快照区间无传输流</div>';
    rows.sort((a, b) => b[5] - a[5]);
    return `<table class="mini"><tr><th>类型</th><th>源→目的</th><th class="num" title="本区间内存在的分片流（含等待信道、未发送的）">流</th><th class="num" title="本区间实际发送量 > 0（或未知）的流">发送</th><th class="num" title="本快照区间内实际发送量">MB</th><th class="num">完成</th><th class="num">中止</th></tr>${rows.slice(0, 30).map(([kind, src, dst, n, moving, sent, done, aborted]) =>
      `<tr><td>${esc(FLOW_LABEL[kind] || kind)}</td><td class="mono">${esc(fmtNode(src))}→${esc(fmtNode(dst))}</td><td class="num">${n}</td><td class="num">${moving}</td><td class="num">${WB.fmt(sent, 2)}</td><td class="num">${done}</td><td class="num">${aborted}</td></tr>`).join("")}</table>${rows.length > 30 ? `<div class="note">另有 ${rows.length - 30} 组</div>` : ""}`;
  }

  function flowRows(snap, filter) {
    const rows = snap.flows.filter(filter);
    if (!rows.length) return `<div class="note">${snap.flows_omitted ? "逐分片明细中无匹配记录" : "本快照区间无记录"}</div>${omittedNote(snap)}`;
    return `<table class="mini"><tr><th>类型</th><th>对象</th><th>源→目的</th><th class="num">本区间 MB</th><th>状态</th></tr>${rows.slice(0, 40).map(([kind, id, src, dst, sent, rem, status]) =>
      `<tr><td>${esc(FLOW_LABEL[kind] || kind)}</td><td class="mono" data-task="${esc(String(id).replace(/_tile_\d+$/, ""))}" style="cursor:pointer" title="查看任务生命周期">${esc(id)}</td><td class="mono">${esc(fmtNode(src))}→${esc(fmtNode(dst))}</td>
       <td class="num">${sent === null ? '<span class="muted" title="同一步内开始并被撤销，发送量无法从对象恢复">未知</span>' : WB.fmt(sent, 2)}${rem !== null ? `<div class="muted">剩 ${WB.fmt(rem, 1)}</div>` : ""}</td><td>${esc(FLOW_STATUS[status] || status)}</td></tr>`).join("")}</table>${rows.length > 40 ? `<div class="note">另有 ${rows.length - 40} 条</div>` : ""}${omittedNote(snap)}`;
  }
  const fmtNode = (v) => R.roles.node(v);

  function satDetail(i, linkSnap, stateSnap) {
    const info = R.meta.satellites[i];
    const row = stateSnap ? stateSnap.sats[i] : null;
    const F = R.meta.fields.sats;
    let html = `<div class="box"><h2>${esc(R.roles.role(i).label)} ${esc(R.roles.name(i))} <span class="right muted">sat_id ${i}${tleName(i) ? " · " + esc(tleName(i)) : ""}</span></h2><dl class="kv">
      <dt>角色</dt><dd>${info.task_source ? `${esc(R.roles.source.label)}：任务源，产生遥感任务，也可参与计算/中继` : `${esc(R.roles.compute.label)}：不产生任务，只参与计算与中继`}</dd>
      <dt>单窗服务率</dt><dd>${WB.fmt(info.compute_rate, 3)} U/s</dd>
      ${info.onboard_compute_profile ? `<dt>并发组服务</dt><dd>${Object.entries(info.onboard_compute_profile.service_s).map(([n, s]) => `${n} 窗 / ${WB.fmt(s, 3)} s`).join("；")}</dd>` : ""}
      ${info.shared_memory_gb ? `<dt>共享物理内存</dt><dd>${WB.fmt(info.shared_memory_gb, 2)} GB</dd>` : ""}</dl></div>`;
    html += `<div class="box"><h2>状态 <span class="right muted">${stateSnap ? `第 ${stateSnap.step} 步结束` : "无"}</span></h2>`;
    if (!row) {
      html += '<div class="note">尚无状态快照。</div></div>';
    } else {
      const queue = stateSnap.queues[String(i)] || [];
      html += `<dl class="kv">
        <dt>执行队列</dt><dd>${row[0]} 个分片${queue.length ? `<div class="mono muted">${queue.map(esc).join("<br>")}${row[0] > queue.length ? "<br>…" : ""}</div>` : ""}</dd>
        <dt>正在计算</dt><dd>${row[1] ? `<span class="mono">${(row[6] || [row[1]]).map(esc).join("<br>")}</span><div class="muted">剩余计算 ${WB.fmtSec(row[2])}</div>` : '<span class="muted">空闲</span>'}</dd>
        <dt>数据缓存</dt><dd>${WB.fmt(row[3], 3)} / ${WB.fmt(info.mem_capacity_gb, 2)} GB${bar(row[3], info.mem_capacity_gb)}</dd>
        <dt>执行内存</dt><dd>${WB.fmt(row[4], 3)} / ${WB.fmt(info.vram_capacity_gb, 2)} GB${bar(row[4], info.vram_capacity_gb)}</dd>
        ${row[5] ? `<dt>待上传整图</dt><dd>${row[5]} 个（地面整图模式）</dd>` : ""}
      </dl>`;
      if (![6, 7].includes(F.length)) html += '<div class="note warn">快照字段与前端不匹配</div>';
      html += "</div>";
    }
    if (linkSnap) {
      const nb = linkSnap.links.filter(([a, b]) => a === i || b === i).map(([a, b, bw]) => `${R.roles.name(a === i ? b : a)}（${WB.fmt(bw, 1)} Mbps）`);
      const gsv = linkSnap.gs_visible.filter(([s]) => s === i).map(([, g]) => R.meta.gs_ids[g]);
      const out = linkSnap.isl_sent.filter(([s]) => s === i), inc = linkSnap.isl_sent.filter(([, d]) => d === i);
      const sum = (rows) => rows.reduce((a, r) => a + r[2], 0);
      html += `<div class="box"><h2>链路与传输 <span class="right muted">步 ${linkSnap.span[0]}–${linkSnap.span[1]}</span></h2><dl class="kv">
        <dt>可见星间链路</dt><dd>${nb.length ? esc(nb.join("、")) : '<span class="muted">无</span>'}</dd>
        <dt>几何可见地面站</dt><dd>${gsv.length ? esc(gsv.join("、")) : '<span class="muted">无</span>'}</dd>
        <dt>实际发送</dt><dd>${out.length ? `${WB.fmt(sum(out), 2)} MB → ${out.map((r) => esc(R.roles.name(r[1]))).join("、")}` : '<span class="muted">0</span>'}</dd>
        <dt>实际接收</dt><dd>${inc.length ? `${WB.fmt(sum(inc), 2)} MB ← ${inc.map((r) => esc(R.roles.name(r[0]))).join("、")}` : '<span class="muted">0</span>'}</dd></dl>
        <h3 style="margin-top:6px">涉及该星的传输流（汇总）</h3>${groupRows(linkSnap, (g) => g[1] === i || g[2] === i)}
        <h3 style="margin-top:6px">分片明细</h3>${flowRows(linkSnap, (f) => f[2] === i || f[3] === i)}</div>`;
    }
    return html;
  }

  function gsDetail(gsId, linkSnap, stateSnap) {
    const info = R.meta.ground_stations.find((g) => g.gs_id === gsId) || {};
    const gi = R.meta.gs_ids.indexOf(gsId);
    const row = stateSnap ? stateSnap.ground[gsId] : null;
    let html = `<div class="box"><h2>地面站 ${esc(gsId)}</h2><dl class="kv">
      <dt>每通道带宽</dt><dd>${WB.fmt(info.bandwidth_mbps, 1)} Mbps × ${info.receive_channels} 通道</dd>
      <dt>地面算力</dt><dd>${WB.fmt(info.compute_rate, 2)}</dd><dt>最小仰角</dt><dd>${WB.fmt(info.min_elevation_deg, 1)}°</dd></dl></div>`;
    html += `<div class="box"><h2>状态 <span class="right muted">${stateSnap ? `第 ${stateSnap.step} 步结束` : "无"}</span></h2>`;
    html += row ? `<dl class="kv"><dt>分片计算队列</dt><dd>${row[0]}${row[1] ? `，计算中 <span class="mono">${esc(row[1])}</span>` : ""}</dd>
      <dt>整图计算队列</dt><dd>${row[2]}${row[3] ? `，计算中 <span class="mono">${esc(row[3])}</span>（剩 ${WB.fmtSec(row[4])}）` : ""}</dd></dl>` : '<div class="note">尚无状态快照。</div>';
    html += "</div>";
    if (linkSnap) {
      const vis = linkSnap.gs_visible.filter(([, g]) => g === gi).map(([s]) => esc(R.roles.name(s)));
      html += `<div class="box"><h2>接收 <span class="right muted">步 ${linkSnap.span[0]}–${linkSnap.span[1]}</span></h2><dl class="kv">
        <dt>几何可见卫星</dt><dd>${vis.length ? vis.join("、") : '<span class="muted">无</span>'}</dd>
        <dt>实际接收</dt><dd>${WB.isNum(linkSnap.gs_sent[gsId]) ? WB.fmt(linkSnap.gs_sent[gsId], 2) + " MB" : '<span class="muted">0</span>'}</dd></dl>
        <h3 style="margin-top:6px">下行流（汇总）</h3>${groupRows(linkSnap, (g) => g[2] === gsId)}
        <h3 style="margin-top:6px">分片明细</h3>${flowRows(linkSnap, (f) => f[3] === gsId)}</div>`;
    }
    return html;
  }

  function taskFlowDetail(linkSnap) {
    return `<div class="box"><h2>任务流向：<span class="mono">${esc(R.task)}</span><span class="right"><button class="btn small" id="clearTask" type="button">显示全部</button></span></h2>
      ${linkSnap ? flowRows(linkSnap, (f) => flowBelongsToTask(f[0], f[1], R.task)) : '<div class="note">此时刻无过程数据</div>'}
      <div class="note">仅显示本快照区间内该任务分片的实际流；完整生命周期见“结果分析”。</div></div>`;
  }

  function tleName(i) {
    const e = ent(`sat-${i}`);
    const props = e && e.properties ? e.properties.getValue(Cesium.JulianDate.now()) : null;
    return props && props.tle_name ? props.tle_name : "";
  }

  /** 镜头瞬间移到实体正上方（默认保持当前高度与朝向，不做飞行动画）；双击跟随中则改为跟随新实体 */
  function focusEntity(e, height) {
    const v = R.viewer;
    if (!v || !e) return;
    if (v.trackedEntity && height === undefined) { v.trackedEntity = e; return; }
    const p = e.position && e.position.getValue(v.clock.currentTime);
    const c = p && Cesium.Cartographic.fromCartesian(p);
    if (!c) return;
    v.trackedEntity = undefined;
    v.camera.cancelFlight();
    v.camera.setView({
      destination: Cesium.Cartesian3.fromRadians(c.longitude, c.latitude,
        height ?? Math.max(v.camera.positionCartographic.height, c.height + 1.5e6)),
      orientation: { heading: v.camera.heading, pitch: -Cesium.Math.PI_OVER_TWO, roll: 0 },
      endTransform: Cesium.Matrix4.IDENTITY,
    });
  }

  function select(sel, opts = {}) {
    R.selected = sel;
    if (R.viewer && (opts.indicator || opts.focus)) {
      const e = ent(sel.type === "sat" ? `sat-${sel.id}` : `gs-${sel.id}`);
      if (opts.indicator) {
        R.syncingSelection = true;
        R.viewer.selectedEntity = e || undefined;
        R.syncingSelection = false;
      }
      if (opts.focus) focusEntity(e);
    }
    R.renderKey = "";
    renderStep(true);
  }

  function focusTask(taskId, step) {
    R.task = taskId;
    if (taskId) {
      R.layers.taskflow = true;
      const cb = $('#layerBox input[data-layer="taskflow"]');
      if (cb) cb.checked = true;
    }
    if (WB.isNum(step) && R.meta) setTime(step * dt(), true);
    else { R.renderKey = ""; renderStep(true); }
  }

  // ── 时间轴图 ─────────────────────────────────────────────────────────────
  function drawTimeline() {
    const cv = $("#tlChart");
    const w = cv.clientWidth, h = cv.clientHeight;
    if (!w) return;
    const ratio = window.devicePixelRatio || 1;
    if (cv.width !== Math.round(w * ratio)) { cv.width = Math.round(w * ratio); cv.height = Math.round(h * ratio); }
    const g = cv.getContext("2d");
    g.setTransform(ratio, 0, 0, ratio, 0, 0);
    g.clearRect(0, 0, w, h);
    g.fillStyle = "#0f141b";
    g.fillRect(0, 0, w, h);
    if (!R.meta || !R.snaps.length) {
      g.fillStyle = "#8b97a8"; g.font = "11px sans-serif"; g.fillText(R.meta ? "无过程快照" : "未载入运行", 8, 20);
      return;
    }
    const N = R.meta.sim_steps;
    const x = (step) => (step / Math.max(1, N - 1)) * (w - 2) + 1;
    const last = R.snaps[R.snaps.length - 1];
    const maxTasks = Math.max(1, last.counters.total_tasks);
    const isl = R.snaps.map((s) => s.isl_sent.reduce((a, r) => a + r[2], 0) / Math.max(1, s.span[1] - s.span[0] + 1));
    const maxIsl = Math.max(1e-9, ...isl);
    g.fillStyle = "rgba(56,208,255,.28)";
    R.snaps.forEach((s, k) => {
      const bh = (isl[k] / maxIsl) * (h - 4);
      g.fillRect(x(s.span[0]), h - bh, Math.max(1, x(s.span[1] + 1) - x(s.span[0])), bh);
    });
    const line = (color, fn) => {
      g.strokeStyle = color; g.lineWidth = 1.4; g.beginPath();
      R.snaps.forEach((s, k) => { const y = h - 2 - (fn(s.counters) / maxTasks) * (h - 6); k ? g.lineTo(x(s.step), y) : g.moveTo(x(s.step), y); });
      g.stroke();
    };
    line("#8b97a8", (c) => c.total_tasks);
    line("#4cc38a", (c) => c.completed_tasks);
    line("#f2545b", (c) => c.failed_tasks);
    if (!R.meta.complete) {
      g.fillStyle = "rgba(240,180,41,.18)";
      g.fillRect(x(R.meta.last_step + 1), 0, w - x(R.meta.last_step + 1), h);
    }
    g.strokeStyle = "#ffffff"; g.lineWidth = 1;
    const cx = x(R.step);
    g.beginPath(); g.moveTo(cx, 0); g.lineTo(cx, h); g.stroke();
  }

  // ── 加载运行 ─────────────────────────────────────────────────────────────
  function updateLoadedInfo() {
    const d = R.detail;
    const el = $("#loadedInfo");
    if (!d) { el.innerHTML = `未载入运行${R.imageryNote ? ` · <span class="muted">${esc(R.imageryNote)}</span>` : ""}`; return; }
    const req = d.request, cfg = req.config;
    el.innerHTML = `回放：<span class="mono">${esc(d.id)}</span> ${WB.badge(d.status.state)} · ${esc(WB.policyLabel(req.policy, req.checkpoint))} · 种子 ${esc(cfg.seed ?? 0)} · ${esc(R.roles.summary(cfg.num_sats))} · dt ${esc(cfg.dt)} s` +
      (R.meta && !R.meta.complete ? ` · <span class="warn">部分快照（取消于第 ${R.meta.last_step + 1} 步）</span>` : "") +
      `<div class="muted">${esc(R.cesiumError || R.imageryNote)}</div>`;
  }

  async function load(detail) {
    const seq = ++R.loadSeq;
    setPlaying(false);
    R.detail = detail; R.meta = null; R.snaps = []; R.step = -1; R.t = 0; R.task = null; R.renderKey = "";
    R.roles = WB.satRoles(detail.request.config);
    ["#tlPlay", "#tlPrev", "#tlNext", "#tlSpeed", "#tlSlider"].forEach((s) => ($(s).disabled = true));
    updateLoadedInfo();
    if (!(detail.files || []).includes("snapshots.json")) {
      setDetails(`<div class="box"><div class="msg warning">该运行没有过程快照（状态：${esc(WB.RUN_STATE_LABEL[detail.status.state] || detail.status.state)}）。</div></div>`);
      overlay(R.viewer ? `<div class="msg warning">该运行没有过程快照，无法回放。</div>` : $("#globeOverlay").innerHTML);
      drawTimeline();
      return;
    }
    setDetails('<div class="box"><div class="note">载入过程快照…</div></div>');
    let data;
    try {
      data = await WB.api(`/api/runs/${encodeURIComponent(detail.id)}/snapshots`);
    } catch (e) {
      if (seq === R.loadSeq) setDetails(`<div class="box"><div class="msg error">快照载入失败：${esc(e.message)}</div></div>`);
      return;
    }
    if (seq !== R.loadSeq) return;
    R.snaps = data.snapshots;
    delete data.snapshots;
    R.meta = data;
    if (!R.selected || (R.selected.type === "sat" && R.selected.id >= data.num_sats) || (R.selected.type === "gs" && !data.gs_ids.includes(R.selected.id))) {
      R.selected = { type: "sat", id: data.task_sources[0] ?? 0 };
    }
    const slider = $("#tlSlider");
    slider.max = String(data.sim_steps - 1);
    ["#tlPlay", "#tlPrev", "#tlNext", "#tlSpeed", "#tlSlider"].forEach((s) => ($(s).disabled = false));
    updateLoadedInfo();
    setTime(0, true);
    if (R.viewer) await loadCzml(detail.id, seq);
  }

  function init() {
    const box = $("#layerBox");
    box.innerHTML = LAYERS.map((l) => `<label class="inline" title="${esc(l.title || "")}"><input type="checkbox" data-layer="${l.key}" ${l.on ? "checked" : ""}/>${l.swatch ? `<span class="swatch" style="background:${l.swatch}"></span>` : ""}${esc(l.label)}</label>`).join("");
    $$("input[data-layer]", box).forEach((cb) => cb.addEventListener("change", () => {
      R.layers[cb.dataset.layer] = cb.checked;
      if (["labels", "orbits", "gs"].includes(cb.dataset.layer)) applyStaticLayers();
      R.renderKey = "";
      if (R.meta) renderStep(true);
    }));
    $("#tlPlay").addEventListener("click", () => setPlaying(!R.playing));
    $("#tlPrev").addEventListener("click", () => jumpSnapshot(-1));
    $("#tlNext").addEventListener("click", () => jumpSnapshot(1));
    $("#tlSpeed").addEventListener("change", (e) => { R.speed = +e.target.value; });
    R.speed = +$("#tlSpeed").value;
    $("#tlSlider").addEventListener("input", (e) => { if (R.meta) setTime(+e.target.value * dt()); });
    $("#tlChart").addEventListener("click", (e) => {
      if (!R.meta) return;
      const rect = e.target.getBoundingClientRect();
      const f = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
      setTime(Math.round(f * (R.meta.sim_steps - 1)) * dt());
    });
    window.addEventListener("resize", () => drawTimeline());
    document.addEventListener("keydown", (e) => {
      if (WB.currentTab !== "replay" && WB.currentTab !== undefined) return;
      if (e.target.matches("input,select,textarea")) return;
      if (e.code === "Space" && R.meta) { e.preventDefault(); setPlaying(!R.playing); }
      if (e.code === "ArrowRight" && R.meta) jumpSnapshot(1);
      if (e.code === "ArrowLeft" && R.meta) jumpSnapshot(-1);
    });
    bindDetails();
    renderDetails(-1, -1);
    drawTimeline();
  }

  WB.replay = {
    init, setupCesium, load, focusTask,
    resize() { drawTimeline(); if (R.viewer) R.viewer.resize(); },
  };
})();
