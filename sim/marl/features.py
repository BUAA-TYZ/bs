"""可消融的任务与星地窗口特征；只读取当前状态及轨道预测，不读取未来任务。"""

import numpy as np

from sim.marl.observation import MAX_NEIGHBORS, MAX_TILES_PER_SAT, build_obs, obs_dim

TASK_EXTRA_DIM = 6 + 1 + MAX_NEIGHBORS


def feature_dim(num_gs, feature_set="base"):
    if feature_set == "base":
        return obs_dim(num_gs)
    if feature_set not in ("task_window", "contact_window", "role_contact"):
        raise ValueError(f"Unknown feature set: {feature_set}")
    size = obs_dim(num_gs) + 2 * num_gs + 2 * MAX_NEIGHBORS + MAX_TILES_PER_SAT * TASK_EXTRA_DIM
    return size + (3 + 3 * num_gs + MAX_NEIGHBORS * (5 + 2 * num_gs) + MAX_TILES_PER_SAT
                   if feature_set in ("contact_window", "role_contact") else 0)


def build_features(state, sat_id, feature_set="base", task_features=True, window_features=True):
    # 延迟导入避免 decision 与特征构建形成模块循环。
    from sim.marl.decision import candidate_neighbors, candidate_tiles

    obs = build_obs(state, sat_id)
    gs_count = len(state.ground_stations)
    nb_start = 5 + gs_count * 4
    tile_start = nb_start + MAX_NEIGHBORS * 6
    if not window_features:
        for k in range(gs_count):
            obs[5 + k * 4 + 1:5 + k * 4 + 3] = 0
        for k in range(MAX_NEIGHBORS):
            obs[nb_start + k * 6 + 5] = 0
    if not task_features:
        for k in range(MAX_TILES_PER_SAT):
            obs[tile_start + k * 7 + 5:tile_start + k * 7 + 7] = 0
    if feature_set == "base":
        return obs
    feature_dim(gs_count, feature_set)
    image_mb = max(1e-6, state.config["image_size_mb"])
    horizon = max(1e-6, state.config["gs_window_lookahead_s"])
    sat = state.satellites[sat_id]
    extras = []
    for gs_id, gs in state.ground_stations.items():
        backlog = sat.get("ground_backlog_mb", {}).get(gs_id, 0.0)
        window = sat.get("next_gs_windows", {}).get(gs_id)
        # 按可见卫星数估算长期轮转份额，不承诺当前时隙获准接入。
        share = min(1.0, gs["receive_channels"] / max(1, gs.get("visible_contenders", 1)))
        capacity = 0.0 if window is None else window[1] * gs["bandwidth_mbps"] / 8 * share
        extras.extend((min(1.0, backlog / image_mb),
                       min(1.0, capacity / image_mb) if window_features else 0.0))
    neighbors = candidate_neighbors(state, sat_id)
    for k in range(MAX_NEIGHBORS):
        if k >= len(neighbors):
            extras.extend((0.0, 0.0))
            continue
        nb = neighbors[k]
        nb_sat = state.satellites[nb]
        extras.extend((min(1.0, sat.get("isl_backlog_mb", {}).get(nb, 0.0) / image_mb),
                       min(1.0, sum(nb_sat.get("ground_backlog_mb", {}).values()) / image_mb)))
    ids = candidate_tiles(state, sat_id)
    if feature_set == "role_contact":
        # 有界单调变换保留高负载差异；旧 checkpoint 的输入定义不变。
        obs[1] = sat["queue_len"] / (16 + sat["queue_len"])
        for j, gs in enumerate(state.ground_stations.values()):
            obs[5 + j * 4 + 3] = gs["bandwidth_mbps"] / (300 + gs["bandwidth_mbps"])
        from sim.topology import link_key
        for j, nb in enumerate(neighbors):
            bandwidth = state.links[link_key(sat_id, nb)]["bandwidth_mbps"]
            queue = state.satellites[nb]["queue_len"]
            obs[nb_start + j * 6 + 1] = bandwidth / (300 + bandwidth)
            obs[nb_start + j * 6 + 2] = queue / (16 + queue)
        for j, tid in enumerate(ids):
            cost = state.tiles[tid]["compute_cost"]
            obs[tile_start + j * 7 + 2] = cost / (20 + cost)
    for k in range(MAX_TILES_PER_SAT):
        values = np.zeros(TASK_EXTRA_DIM, dtype=np.float32)
        if k < len(ids):
            tile = state.tiles[ids[k]]
            progress = tile["task_progress"]
            total = max(1, progress["total"])
            if task_features:
                values[:4] = [progress[key] / total for key in ("done", "failed", "running", "awaiting_delivery")]
                locations = tile["sibling_locations"]
                values[6] = locations.get(sat_id, 0) / total
                for j, nb in enumerate(neighbors):
                    values[7 + j] = locations.get(nb, 0) / total
            remaining = tile["deadline_remaining_s"]
            values[4:6] = (float(remaining is not None),
                          min(1.0, remaining / horizon) if remaining is not None else 0.0)
        extras.extend(values)
    if feature_set in ("contact_window", "role_contact"):
        from sim.topology import link_key
        extras.append(state.config.get("remaining_sim_seconds", horizon) /
                      max(1, state.config.get("sim_duration_seconds", horizon)))
        extras.extend((sat["compute_rate"] / (1 + sat["compute_rate"]),
                       min(1, sat.get("executing_remaining_s", 0) / horizon)))
        for gs in state.ground_stations.values():
            extras.extend((min(1, (gs["queue_len"] + gs["running"]) / 100),
                           gs["compute_rate"] / (1 + gs["compute_rate"]),
                           min(1, gs.get("visible_contenders", 1) / 100)))
        for k in range(MAX_NEIGHBORS):
            values = np.zeros(5 + 2 * gs_count, dtype=np.float32)
            if k < len(neighbors):
                nb = neighbors[k]
                nb_sat = state.satellites[nb]
                values[3] = min(1, nb_sat.get("executing_remaining_s", 0) / horizon)
                values[-1] = nb_sat["compute_rate"] / (1 + nb_sat["compute_rate"])
                if window_features:
                    windows = state.links[link_key(sat_id, nb)].get("contact_windows", [])
                    current = next((w for w in windows if w[0] == 0), None)
                    future = next((w for w in windows if w[0] > 0), None)
                    values[:3] = (current[1] / horizon if current else 0,
                                  future[0] / horizon if future else 1,
                                  future[1] / horizon if future else 0)
                    for j, gs in enumerate(state.ground_stations):
                        window = nb_sat.get("next_gs_windows", {}).get(gs)
                        values[4 + 2 * j:6 + 2 * j] = (
                            min(1, window[0] / horizon) if window else 1,
                            min(1, window[1] / horizon) if window else 0)
            extras.extend(values)
        extras.extend(float(state.tiles[ids[k]]["state"] == "READY") if k < len(ids) else 0.0
                      for k in range(MAX_TILES_PER_SAT))
    return np.concatenate((obs, np.asarray(extras, dtype=np.float32)))
