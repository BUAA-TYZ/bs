"""全部本地计算对照，用于区分卸载协作收益和简单本地执行偏好。"""

from sim.entities import Action, ActionType
from sim.scheduler.base import SchedulerPolicy


class LocalOnlyPolicy(SchedulerPolicy):
    def select_actions(self, env_state):
        return [Action(tid, ActionType.LOCAL if tile["vram_req_gb"] <= env_state.config["vram_capacity_gb"]
                       else ActionType.WAIT)
                for tid, tile in env_state.tiles.items()
                if tile["state"] in ("QUEUED", "READY") and not tile["in_transfer"]]
