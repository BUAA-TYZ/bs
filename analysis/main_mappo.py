"""主场景共享actor、独立全局critic和异步整图PPO。"""

import bisect
import math
import random
import torch
from torch import nn
from torch.distributions import Categorical, kl_divergence
from analysis.main_bc_interface import HierarchicalActor
from analysis.main_rl_interface import NODE_DIM, IMAGE_DIM, joint_statistics, terminal_reward
from train_marl import compute_gae

HP = dict(lr=3e-4, epochs=4, minibatch=256, clip=0.2, value_coef=0.5,
          entropy_coef=0.01, max_grad_norm=0.5, gamma=1.0, lam=1.0)


class Critic(nn.Module):
    def __init__(self):
        super().__init__()
        self.nodes = nn.Sequential(nn.Linear(NODE_DIM, 64), nn.Tanh())
        self.value = nn.Sequential(nn.Linear(64*3+4+IMAGE_DIM, 64), nn.Tanh(), nn.Linear(64, 1))

    def forward(self, record):
        c, o = record['critic_observation'], record['observation']
        x = self.nodes(torch.tensor(c['nodes'], dtype=torch.float32))
        own = self.nodes(torch.tensor(c['source_node'], dtype=torch.float32))
        return self.value(torch.cat([x.mean(0), x.max(0).values, own,
            torch.tensor(c['ground']+o['image'], dtype=torch.float32)])).squeeze()


def distribution_metrics(actor, initial, obs):
    """层次分布的精确KL/熵；按图像模式概率加权块层，而非已采样模式。"""
    im, pos = actor.logits(obs)
    ref_im, ref_pos = initial.logits(obs)
    a, b = Categorical(logits=im), Categorical(logits=ref_im)
    entropy, kl = a.entropy(), kl_divergence(a, b)
    if obs['image_mask'][0]:
        q, ref = Categorical(logits=pos), Categorical(logits=ref_pos)
        entropy = entropy + a.probs[0]*obs['block_count']*q.entropy()
        kl = kl + a.probs[0]*obs['block_count']*kl_divergence(q, ref)
    return entropy, kl


def assign_returns(records, outcomes, scored_ids):
    """同源异步决策链：奖励只落在实际终结事件所在区间，不提前泄露。"""
    by_source = {}
    for row in records:
        if row['task_id'] in scored_ids:
            by_source.setdefault(row['observation']['source'], []).append(row)
    total_events = assigned_events = 0
    for source, chain in by_source.items():
        chain.sort(key=lambda r: (r['observation']['time_seconds'], r['decision_order']))
        times = [r['observation']['time_seconds'] for r in chain]
        rewards = [0.]*len(chain)
        event_rows = [[] for _ in chain]
        for o in outcomes:
            if o['task_id'] not in scored_ids or o['source_sat_id'] != source:
                continue
            total_events += 1
            event_time = o['release_seconds']+min(o['latency_seconds'], 600) if o['status']=='completed' else o['release_seconds']+600
            # env.step产生的同刻奖励先于下一轮决策，故使用严格左区间。
            index = bisect.bisect_left(times, event_time)-1
            if index >= 0:
                reward = terminal_reward(o)
                rewards[index] += reward
                event_rows[index].append(dict(task_id=o['task_id'], time=event_time, reward=reward))
                assigned_events += 1
        values = torch.tensor([r['old_value'] for r in chain], dtype=torch.float32)
        dones = torch.zeros(len(chain), dtype=torch.bool); dones[-1] = True
        adv, returns = compute_gae(torch.tensor(rewards), values, 0., dones, gamma=HP['gamma'], lam=HP['lam'])
        for i, row in enumerate(chain):
            row.update(interval_reward=rewards[i], reward_events=event_rows[i],
                       next_decision_gap_s=times[i+1]-times[i] if i+1<len(times) else None,
                       advantage=float(adv[i]), return_target=float(returns[i]))
    rows = [r for chain in by_source.values() for r in chain]
    return rows, dict(source_reward_events=total_events, assigned_reward_events=assigned_events,
                      before_first_decision_events=total_events-assigned_events,
                      gamma=1., lam=1., note='Undiscounted source trajectories; real event intervals; no repeated image reward')


def update(actor, critic, optimizer, rows, seed):
    if not rows:
        raise ValueError('No on-policy decisions')
    # 同步batch只来自同一个策略版本，更新前逐项核对联合logp。
    with torch.no_grad():
        for row in rows:
            logits = actor.logits(row['observation'])
            logp, _ = joint_statistics(*logits, row['mode'], row['positions'])
            if not math.isclose(float(logp), row['log_probability'], abs_tol=2e-5):
                raise AssertionError('Behavior log-probability mismatch')
    advantages = torch.tensor([r['advantage'] for r in rows])
    advantages = (advantages-advantages.mean())/advantages.std(unbiased=False).clamp_min(1e-8)
    order = list(range(len(rows))); rng = random.Random(seed); history = []
    for _ in range(HP['epochs']):
        rng.shuffle(order)
        for start in range(0,len(order),HP['minibatch']):
            indices = order[start:start+HP['minibatch']]
            actor_losses, values, entropies, ratios = [], [], [], []
            for i in indices:
                r = rows[i]; logits = actor.logits(r['observation'])
                logp, _ = joint_statistics(*logits,r['mode'],r['positions'])
                ratio = (logp-r['log_probability']).exp()
                actor_losses.append(-torch.minimum(ratio*advantages[i], ratio.clamp(1-HP['clip'],1+HP['clip'])*advantages[i]))
                values.append((critic(r)-r['return_target']).square())
                im, pos = logits; dist = Categorical(logits=im)
                ent = dist.entropy()
                if r['observation']['image_mask'][0]:
                    ent = ent+dist.probs[0]*r['observation']['block_count']*Categorical(logits=pos).entropy()
                entropies.append(ent); ratios.append(ratio.detach())
            loss = torch.stack(actor_losses).mean()+HP['value_coef']*torch.stack(values).mean()-HP['entropy_coef']*torch.stack(entropies).mean()
            if not torch.isfinite(loss):raise FloatingPointError('Nonfinite PPO loss')
            optimizer.zero_grad();loss.backward()
            torch.nn.utils.clip_grad_norm_(list(actor.parameters())+list(critic.parameters()),HP['max_grad_norm'],error_if_nonfinite=True)
            optimizer.step()
            history.append(dict(loss=float(loss.detach()),entropy=float(torch.stack(entropies).mean().detach()),
                                ratio_min=float(torch.stack(ratios).min()),ratio_max=float(torch.stack(ratios).max())))
    return history
