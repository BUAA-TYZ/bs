from __future__ import annotations

from typing import List

from sim.entities import Action, ActionType, EnvState
from sim.topology import link_key
from sim.scheduler.base import SchedulerPolicy


class GreedyEarliestFinish(SchedulerPolicy):
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
            best_finish = float("inf")

            # Local estimate
            local_queue = env_state.satellites[src]["queue_len"]
            local_rate = env_state.satellites[src]["compute_rate"]
            local_finish = local_queue * dt + tile["compute_cost"] / local_rate
            if local_finish < best_finish:
                best_finish = local_finish
                best_action = Action(tile_id=tile_id, action_type=ActionType.LOCAL)

            # Neighbor offload
            for nb in env_state.neighbors.get(src, []):
                lk = env_state.links.get(link_key(src, nb))
                if not lk or not lk["up"]:
                    continue
                bw = lk["bandwidth_mbps"]
                tx_time = (tile["data_size_mb"] * 8.0) / max(1e-6, bw)
                nb_queue = env_state.satellites[nb]["queue_len"]
                nb_rate = env_state.satellites[nb]["compute_rate"]
                finish = nb_queue * dt + tx_time + tile["compute_cost"] / nb_rate
                if finish < best_finish:
                    best_finish = finish
                    best_action = Action(tile_id=tile_id, action_type=ActionType.OFFLOAD, target_sat_id=nb)

            actions.append(best_action)

        return actions
