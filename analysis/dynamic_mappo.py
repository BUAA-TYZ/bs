"""三模式联合策略PPO；原奖励、critic与超参数不变。"""
import math
import random
import torch
from torch.distributions import Categorical
from analysis.main_mappo import HP, Critic, assign_returns
from analysis.dynamic_learning_interface import joint_statistics


def expected_entropy(logits, obs):
    im, positions = logits
    image = Categorical(logits=im)
    entropy = image.entropy()
    for mode in (0, 1):
        if obs['image_mask'][mode]:
            entropy = entropy + image.probs[mode]*obs['variants'][mode]['block_count']*Categorical(logits=positions[mode]).entropy()
    return entropy


def update(actor, critic, optimizer, rows, seed):
    if not rows: raise ValueError('No on-policy decisions')
    with torch.no_grad():
        for row in rows:
            mode = row['mode']; obs = row['observation']
            assert obs['image_mask'][mode]
            assert len(row['positions']) == (obs['variants'][mode]['block_count'] if mode < 2 else 0)
            logp, _ = joint_statistics(*actor.logits(obs), mode, row['positions'])
            if not math.isclose(float(logp), row['log_probability'], abs_tol=2e-5):
                raise AssertionError('Behavior log-probability mismatch')
    advantages = torch.tensor([r['advantage'] for r in rows])
    advantages = (advantages-advantages.mean())/advantages.std(unbiased=False).clamp_min(1e-8)
    order = list(range(len(rows))); rng = random.Random(seed); history = []
    for _ in range(HP['epochs']):
        rng.shuffle(order)
        for start in range(0,len(order),HP['minibatch']):
            losses = []; values = []; entropies = []; ratios = []
            for i in order[start:start+HP['minibatch']]:
                row = rows[i]; logits = actor.logits(row['observation'])
                logp, _ = joint_statistics(*logits,row['mode'],row['positions'])
                ratio = (logp-row['log_probability']).exp()
                losses.append(-torch.minimum(ratio*advantages[i], ratio.clamp(1-HP['clip'],1+HP['clip'])*advantages[i]))
                values.append((critic(row)-row['return_target']).square())
                entropies.append(expected_entropy(logits,row['observation'])); ratios.append(ratio.detach())
            loss = torch.stack(losses).mean()+HP['value_coef']*torch.stack(values).mean()-HP['entropy_coef']*torch.stack(entropies).mean()
            if not torch.isfinite(loss): raise FloatingPointError('Nonfinite PPO loss')
            optimizer.zero_grad(); loss.backward()
            torch.nn.utils.clip_grad_norm_(list(actor.parameters())+list(critic.parameters()),HP['max_grad_norm'],error_if_nonfinite=True)
            optimizer.step()
            history.append(dict(loss=float(loss.detach()),entropy=float(torch.stack(entropies).mean().detach()),
                ratio_min=float(torch.stack(ratios).min()),ratio_max=float(torch.stack(ratios).max())))
    return history
