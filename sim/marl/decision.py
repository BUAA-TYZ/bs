"""训练、推理与基线共用的候选集合、动作编码和可行性约束。"""

from dataclasses import replace

import numpy as np

from sim.entities import Action, ActionType, EnvState
from sim.marl.observation import MAX_NEIGHBORS, MAX_TILES_PER_SAT
from sim.marl.features import build_features
from sim.topology import link_key
from sim.roles import can_receive, can_compute

OBSERVATION_VERSION = 2  # 旧协议；新 profile 的版本由 features.observation_version_for 决定。


def candidate_tiles(state: EnvState, sat_id: int, limit: int = MAX_TILES_PER_SAT) -> list[str]:
    if type(limit) is not int or limit < 1:
        raise ValueError("Decision tile limit must be a positive integer")
    tiles = [tid for tid, tile in state.tiles.items()
             if tile["location"] == sat_id and not tile["in_transfer"]]
    tiles.sort(key=lambda tid: state.tiles[tid]["waiting_time"], reverse=True)
    return tiles[:limit]


def decision_state(state: EnvState, tile_limit_per_sat: int = MAX_TILES_PER_SAT) -> EnvState:
    selected = {tid for sat_id in state.satellites
                for tid in candidate_tiles(state, sat_id, tile_limit_per_sat)}
    return replace(state, tiles={tid: tile for tid, tile in state.tiles.items() if tid in selected})


def candidate_neighbors(state: EnvState, sat_id: int) -> list[int]:
    return sorted((nb for nb in state.neighbors.get(sat_id, []) if can_receive(state.config, nb)),
                  key=lambda nb: state.links[link_key(sat_id, nb)]["bandwidth_mbps"],
                  reverse=True)[:MAX_NEIGHBORS]


def action_mask(state: EnvState, sat_id: int, tile_ids: list[str]) -> np.ndarray:
    mask = np.zeros((MAX_TILES_PER_SAT, 2 + MAX_NEIGHBORS + len(state.ground_stations)), dtype=bool)
    mask[:, 0] = True  # 空 slot 也保留 WAIT，避免全屏蔽分布。
    visible_gs = {opt["gs_id"] for opt in state.ground_options.get(sat_id, [])}
    for slot, tid in enumerate(tile_ids):
        tile = state.tiles[tid]
        # LOCAL 是进入计算队列，不要求当前空闲或当前显存足够。
        mask[slot, 1] = (tile["vram_req_gb"] <= state.config["vram_capacity_gb"]
                         and can_compute(state.config, sat_id, tile.get("source_sat_id", sat_id)))
        for k, nb in enumerate(candidate_neighbors(state, sat_id)):
            link = state.links[link_key(sat_id, nb)]
            mask[slot, 2 + k] = bool(link["up"] and link["bandwidth_mbps"] > 0
                                     and state.satellites[nb]["mem_remaining_gb"] >= tile["data_size_gb"])
        for k, gs_id in enumerate(state.ground_stations):
            mask[slot, 2 + MAX_NEIGHBORS + k] = gs_id in visible_gs
    return mask


def build_batch(state: EnvState, feature_set="base", task_features=True, window_features=True):
    if state.config.get("decision_tile_limit_per_sat", MAX_TILES_PER_SAT) != MAX_TILES_PER_SAT:
        raise ValueError(f"MARL requires decision_tile_limit_per_sat={MAX_TILES_PER_SAT}")
    sat_ids = sorted(state.satellites)
    tile_ids = {sid: candidate_tiles(state, sid) for sid in sat_ids}
    obs = np.stack([build_features(state, sid, feature_set, task_features, window_features) for sid in sat_ids])
    tile_masks = np.array([[slot < len(tile_ids[sid]) for slot in range(MAX_TILES_PER_SAT)]
                          for sid in sat_ids], dtype=bool)
    masks = np.stack([action_mask(state, sid, tile_ids[sid]) for sid in sat_ids])
    return obs, tile_masks, masks, tile_ids


def decode_action(index: int, tile_id: str, state: EnvState, sat_id: int) -> Action:
    if index == 1:
        return Action(tile_id, ActionType.LOCAL)
    if 2 <= index < 2 + MAX_NEIGHBORS:
        neighbors = candidate_neighbors(state, sat_id)
        if index - 2 < len(neighbors):
            return Action(tile_id, ActionType.OFFLOAD, target_sat_id=neighbors[index - 2])
    if index >= 2 + MAX_NEIGHBORS:
        stations = list(state.ground_stations)
        slot = index - 2 - MAX_NEIGHBORS
        if slot < len(stations):
            return Action(tile_id, ActionType.OFFLOAD, target_gs_id=stations[slot])
    return Action(tile_id, ActionType.WAIT)


def decode_batch(actions: np.ndarray, state: EnvState, tile_ids: dict) -> list[Action]:
    return [decode_action(int(actions[row, slot]), tid, state, sid)
            for row, sid in enumerate(sorted(state.satellites))
            for slot, tid in enumerate(tile_ids[sid])]
