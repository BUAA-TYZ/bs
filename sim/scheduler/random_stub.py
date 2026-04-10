from __future__ import annotations

import random
from typing import List

from sim.entities import Action, ActionType, EnvState
from sim.scheduler.base import SchedulerPolicy


class RandomPolicy(SchedulerPolicy):
    def __init__(self, seed: int = 0) -> None:
        self.rng = random.Random(seed)

    def select_actions(self, env_state: EnvState) -> List[Action]:
        actions: List[Action] = []
        for tile_id, tile in env_state.tiles.items():
            if tile["state"] not in ("QUEUED", "READY"):
                continue
            if tile["in_transfer"]:
                continue
            src = tile["location"]
            choices = [ActionType.LOCAL, ActionType.WAIT]
            neighbors = env_state.neighbors.get(src, [])
            ground_opts = env_state.ground_options.get(src, [])
            if neighbors:
                choices.append(ActionType.OFFLOAD)
            if ground_opts:
                choices.append(ActionType.OFFLOAD)
            pick = self.rng.choice(choices)
            if pick == ActionType.OFFLOAD:
                if neighbors and (not ground_opts or self.rng.random() < 0.5):
                    nb = self.rng.choice(neighbors)
                    actions.append(Action(tile_id=tile_id, action_type=pick, target_sat_id=nb))
                else:
                    gs = self.rng.choice(ground_opts)["gs_id"]
                    actions.append(Action(tile_id=tile_id, action_type=pick, target_gs_id=gs))
            else:
                actions.append(Action(tile_id=tile_id, action_type=pick))
        return actions


class StubPolicy(SchedulerPolicy):
    def select_actions(self, env_state: EnvState) -> List[Action]:
        return []
