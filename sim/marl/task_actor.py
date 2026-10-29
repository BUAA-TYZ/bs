"""按 tile 条件化的目的地注意力；邻星槽位共享参数，不使用卫星编号。"""

import torch
from torch import nn

from sim.marl.features import TASK_EXTRA_DIM, feature_dim
from sim.marl.observation import MAX_NEIGHBORS, MAX_TILES_PER_SAT, obs_dim


class TaskDestinationActor(nn.Module):
    def __init__(self, num_gs, hidden, feature_set="task_window"):
        super().__init__()
        self.num_gs = num_gs
        self.hidden = hidden
        self.predictive = feature_set in ("contact_window", "role_contact", "overlap_contact")
        self.tile_encoder = nn.Linear(7 + 6 + int(self.predictive), hidden)
        self.self_encoder = nn.Linear(5 + 1 + (3 if self.predictive else 0), hidden)
        self.neighbor_encoder = nn.Linear(6 + 2 + 1 + (5 + 2 * num_gs if self.predictive else 0), hidden)
        self.ground_encoder = nn.Linear(4 + 2 + (3 if self.predictive else 0), hidden)
        self.kind = nn.Embedding(4, hidden)  # WAIT / LOCAL / 邻星 / 地面站
        self.query = nn.Linear(hidden, hidden, bias=False)
        self.key = nn.Linear(hidden, hidden, bias=False)
        self.value = nn.Linear(hidden, hidden, bias=False)
        self.score = nn.Sequential(nn.Linear(3 * hidden, hidden), nn.GELU(), nn.Linear(hidden, 1))
        nn.init.orthogonal_(self.score[-1].weight, gain=0.01)
        nn.init.zeros_(self.score[-1].bias)

    def forward(self, obs):
        batch = len(obs)
        g = self.num_gs
        nb_start = 5 + 4 * g
        tile_start = nb_start + 6 * MAX_NEIGHBORS
        extra = obs[:, obs_dim(g):feature_dim(g, "task_window")]
        gs_extra = extra[:, :2 * g].reshape(batch, g, 2)
        nb_extra = extra[:, 2 * g:2 * g + 2 * MAX_NEIGHBORS].reshape(batch, MAX_NEIGHBORS, 2)
        tile_extra = extra[:, 2 * g + 2 * MAX_NEIGHBORS:].reshape(batch, MAX_TILES_PER_SAT, TASK_EXTRA_DIM)
        tiles = obs[:, tile_start:obs_dim(g)].reshape(batch, MAX_TILES_PER_SAT, 7)
        tile_inputs = [tiles, tile_extra[:, :, :6]]
        if self.predictive:
            tile_inputs.append(obs[:, -MAX_TILES_PER_SAT:, None])
        query = torch.tanh(self.tile_encoder(torch.cat(tile_inputs, dim=-1)))
        self_state = obs[:, None, :5].expand(-1, MAX_TILES_PER_SAT, -1)
        self_inputs = [self_state, tile_extra[:, :, 6:7]]
        if self.predictive:
            contacts = obs[:, feature_dim(g, "task_window"):]
            self_inputs.append(contacts[:, None, :3].expand(-1, MAX_TILES_PER_SAT, -1))
        self_token = self.self_encoder(torch.cat(self_inputs, dim=-1))
        neighbors = obs[:, nb_start:tile_start].reshape(batch, MAX_NEIGHBORS, 6)
        nb_state = torch.cat((neighbors, nb_extra), dim=-1)[:, None].expand(-1, MAX_TILES_PER_SAT, -1, -1)
        nb_inputs = [nb_state, tile_extra[:, :, 7:, None]]
        if self.predictive:
            nb_inputs.append(contacts[:, 3 + 3 * g:-MAX_TILES_PER_SAT].reshape(batch, 1, MAX_NEIGHBORS, 5 + 2 * g)
                             .expand(-1, MAX_TILES_PER_SAT, -1, -1))
        nb_tokens = self.neighbor_encoder(torch.cat(nb_inputs, dim=-1))
        gs_state = obs[:, 5:nb_start].reshape(batch, g, 4)
        gs_inputs = [gs_state, gs_extra]
        if self.predictive:
            gs_inputs.append(contacts[:, 3:3 + 3 * g].reshape(batch, g, 3))
        gs_tokens = self.ground_encoder(torch.cat(gs_inputs, dim=-1))
        gs_tokens = gs_tokens[:, None].expand(-1, MAX_TILES_PER_SAT, -1, -1)
        tokens = torch.cat(((self_token + self.kind.weight[0])[:, :, None],
                            (self_token + self.kind.weight[1])[:, :, None],
                            nb_tokens + self.kind.weight[2], gs_tokens + self.kind.weight[3]), dim=2)
        tokens = torch.tanh(tokens)
        valid = torch.cat((torch.ones(batch, 2, device=obs.device, dtype=torch.bool),
                           neighbors[:, :, 0] > 0, torch.ones(batch, g, device=obs.device, dtype=torch.bool)), dim=-1)
        weights = (self.query(query)[:, :, None] * self.key(tokens)).sum(-1) / self.hidden ** 0.5
        weights = weights.masked_fill(~valid[:, None], torch.finfo(obs.dtype).min).softmax(-1)
        context = (weights[..., None] * self.value(tokens)).sum(2)
        return self.score(torch.cat((query[:, :, None].expand_as(tokens), tokens,
                                     context[:, :, None].expand_as(tokens)), dim=-1)).squeeze(-1)
