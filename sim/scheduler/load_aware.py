from __future__ import annotations

from typing import List

from sim.entities import Action, ActionType, EnvState
from sim.topology import link_key
from sim.scheduler.base import SchedulerPolicy


class LoadAwareResourceFit(SchedulerPolicy):
    def select_actions(self, env_state: EnvState) -> List[Action]:
        actions: List[Action] = []
        dt = env_state.config["dt"]

        for tile_id, tile in env_state.tiles.items():
            if tile["state"] not in ("QUEUED", "READY"):
                continue
            src = tile["location"]
            if tile["in_transfer"]:
                continue

            best_action = Action(tile_id=tile_id, action_type=ActionType.WAIT)
            best_score = float("inf")

            def score_for(sat_id: int, extra_tx: float) -> float:
                sat = env_state.satellites[sat_id]
                queue_penalty = sat["queue_len"] * dt
                vram_headroom = sat["vram_remaining_gb"]
                mem_headroom = sat["mem_remaining_gb"]
                # Penalize low headroom
                headroom_penalty = 0.0
                if vram_headroom < tile["vram_req_gb"]:
                    headroom_penalty += 1e6
                if mem_headroom < tile["data_size_gb"]:
                    headroom_penalty += 1e6
                compute_time = tile["compute_cost"] / sat["compute_rate"]
                return queue_penalty + extra_tx + compute_time + headroom_penalty

            local_score = score_for(src, 0.0)
            if local_score < best_score:
                best_score = local_score
                best_action = Action(tile_id=tile_id, action_type=ActionType.LOCAL)

            for nb in env_state.neighbors.get(src, []):
                lk = env_state.links.get(link_key(src, nb))
                if not lk or not lk["up"]:
                    continue
                bw = lk["bandwidth_mbps"]
                tx_time = (tile["data_size_mb"] * 8.0) / max(1e-6, bw)
                score = score_for(nb, tx_time)
                if score < best_score:
                    best_score = score
                    best_action = Action(tile_id=tile_id, action_type=ActionType.OFFLOAD, target_sat_id=nb)

            actions.append(best_action)

        return actions
