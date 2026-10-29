"""Co-MAPPO 学习组件适配：动作条件化注意力 Q 与联合动作反事实 baseline。"""

import torch
from torch import nn
from torch.nn import functional as F

from sim.marl.observation import MAX_TILES_PER_SAT


def joint_q_observations(obs, actions, action_dim):
    """每行顺序为自身、其他 agent；保留同一步联合动作，剔除自身的重复项。"""
    encoded = F.one_hot(actions.long(), action_dim).to(obs.dtype).flatten(1)
    tokens = torch.cat((obs, encoded), dim=-1)
    n = len(obs)
    order = torch.arange(n, device=obs.device)
    return torch.stack([torch.cat((tokens[i:i + 1], tokens[order != i])).flatten() for i in range(n)])


class AttentionQCritic(nn.Module):
    def __init__(self, token_dim, hidden):
        super().__init__()
        self.token_dim = token_dim
        self.hidden = hidden
        self.embedding = nn.Sequential(nn.Linear(token_dim, hidden), nn.GELU())
        self.query = nn.Linear(hidden, hidden, bias=False)
        self.key = nn.Linear(hidden, hidden, bias=False)
        self.value = nn.Sequential(nn.Linear(hidden, hidden), nn.GELU())
        self.head = nn.Sequential(nn.Linear(2 * hidden, hidden), nn.GELU(), nn.Linear(hidden, 1))

    def forward(self, critic_obs):
        tokens = critic_obs.reshape(len(critic_obs), -1, self.token_dim)
        embeddings = self.embedding(tokens)
        own, others = embeddings[:, 0], embeddings[:, 1:]
        if others.shape[1]:
            weights = (self.query(own)[:, None] * self.key(others)).sum(-1) / self.hidden ** 0.5
            context = (weights.softmax(-1)[..., None] * self.value(others)).sum(1)
        else:
            context = torch.zeros_like(own)
        return self.head(torch.cat((own, context), dim=-1))


@torch.no_grad()
def counterfactual_baseline(model, obs, action_mask, critic_obs, samples):
    """采样本星完整 tile 联合动作；固定其他星动作，重新计算含动作 query 的 Q。"""
    if samples < 1:
        raise ValueError("Counterfactual samples must be positive")
    logits, _ = model(obs)
    logits = logits.masked_fill(~action_mask, torch.finfo(logits.dtype).min)
    sampled = torch.distributions.Categorical(logits=logits).sample((samples,))
    expanded = critic_obs.unsqueeze(0).expand(samples, -1, -1).clone()
    own_start = model.in_dim
    own_end = own_start + MAX_TILES_PER_SAT * model.act_dim
    expanded[:, :, own_start:own_end] = F.one_hot(sampled, model.act_dim).to(obs.dtype).flatten(2)
    # 分块避免大量采样时出现不必要的峰值内存。
    values = torch.cat([model.central_critic(chunk) for chunk in expanded.flatten(0, 1).split(1024)])
    return values.reshape(samples, len(obs)).mean(0)
