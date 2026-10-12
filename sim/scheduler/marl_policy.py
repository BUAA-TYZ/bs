"""IPPO / MAPPO 的分布式执行接口；推理只使用每星局部观测。"""

import torch

from sim.marl.actor import SatActorCritic
from sim.marl.decision import OBSERVATION_VERSION, build_batch, decode_batch
from sim.scheduler.base import SchedulerPolicy


class MARLPolicy(SchedulerPolicy):
    def __init__(self, checkpoint_path: str, device: str = "cpu", deterministic: bool = True):
        self.device = torch.device(device)
        self.deterministic = deterministic
        checkpoint = torch.load(checkpoint_path, map_location=self.device, weights_only=True)
        if not isinstance(checkpoint, dict) or checkpoint.get("observation_version") != OBSERVATION_VERSION:
            raise ValueError("Legacy/incompatible checkpoint: retrain with the current observation and simulator version")
        self.metadata = checkpoint
        self.gs_ids = checkpoint["ground_station_ids"]
        self.model = SatActorCritic(**checkpoint["model_config"]).to(self.device)
        self.model.load_state_dict(checkpoint["model_state"])
        self.model.eval()

    def select_actions(self, env_state):
        if list(env_state.ground_stations) != self.gs_ids:
            raise ValueError("Ground station order/IDs differ from the training checkpoint")
        obs, _, masks, tile_ids = build_batch(env_state, **self.model.feature_options)
        with torch.no_grad():
            logits, _ = self.model(torch.as_tensor(obs, device=self.device))
            masks_t = torch.as_tensor(masks, device=self.device)
            logits = logits.masked_fill(~masks_t, torch.finfo(logits.dtype).min)
            if self.deterministic:
                actions = logits.argmax(dim=-1)
            else:
                actions = torch.distributions.Categorical(logits=logits).sample()
        return decode_batch(actions.cpu().numpy(), env_state, tile_ids)
