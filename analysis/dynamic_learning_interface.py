"""动态调度块的局部actor；窗口几何与块级计算承诺仍由公共执行器负责。"""
from dataclasses import replace

import torch
from torch import nn
from torch.distributions import Categorical, kl_divergence

from analysis import main_bc_interface as fixed
from analysis import main_rl_interface as base
from analysis.dynamic_granularity_v2 import preview
from sim.entities import ActionType
from sim.scheduler.literature import action_for, contact_plan
from sim.scheduler.input_routing import reserve_input_memory

SCHEMA = 'main-m-dynamic-4-16-ground-v1'
COUNTS = (4, 16)


def build_observation(env, state, task):
    assert state.config['local_information'] and state.config['observation_hops'] == 2
    variants = []
    for count in COUNTS:
        view = preview(env, state, task, count)
        variants.append(fixed.build_observation(view, task, view.tiles))
    common = variants[1]
    return dict(schema=SCHEMA, task_id=task, source=common['source'],
        time_seconds=common['time_seconds'], image=common['image'], variants=variants,
        image_mask=[variants[0]['image_mask'][0], variants[1]['image_mask'][0], common['image_mask'][1]])


def joint_statistics(image_logits, position_logits, mode, positions):
    image = Categorical(logits=image_logits)
    logp = image.log_prob(torch.as_tensor(mode, device=image_logits.device))
    entropy = image.entropy()
    if mode in (0, 1):
        blocks = Categorical(logits=position_logits[mode])
        logp = logp + blocks.log_prob(torch.as_tensor(positions, device=image_logits.device)).sum()
        entropy = entropy + len(positions)*blocks.entropy()
    elif mode != 2 or positions:
        raise ValueError('Unique raw-image ground mode has no block placements')
    return logp, entropy


class DynamicActor(fixed.HierarchicalActor):
    def __init__(self, hidden=64):
        super().__init__(hidden)
        self.image_head = nn.Linear(hidden*2, 3)

    def initialize_fixed(self, state):
        shared = {k: v for k, v in state.items() if not k.startswith('image_head.')}
        missing, unexpected = self.load_state_dict(shared, strict=False)
        assert set(missing) == {'image_head.weight', 'image_head.bias'} and not unexpected
        # 新4块行保留按种子初始化的随机参数；16块和原图地面继承原模型。
        with torch.no_grad():
            self.image_head.weight[1:].copy_(state['image_head.weight'])
            self.image_head.bias[1:].copy_(state['image_head.bias'])

    def _encode(self, obs):
        device = next(self.parameters()).device
        tensor = lambda key, dtype: torch.as_tensor(obs[key], dtype=dtype, device=device)
        x = torch.tanh(self.embed(tensor('nodes', torch.float32))).unsqueeze(0)
        attended, _ = self.attention(x, x, x, attn_mask=~tensor('adjacency', torch.bool),
            key_padding_mask=~tensor('node_valid', torch.bool).unsqueeze(0), need_weights=False)
        x = self.norm(x+attended)[0]
        context = torch.cat([x[0], self.image_encoder(tensor('image', torch.float32))])
        pos = self.position_head(torch.cat([x, context.expand(fixed.POSITIONS, -1)], -1)).squeeze(-1)
        return context, pos.masked_fill(~tensor('position_mask', torch.bool), -torch.inf)

    def logits(self, obs):
        assert obs['schema'] == SCHEMA
        encoded = [self._encode(v) for v in obs['variants']]
        image = self.image_head(encoded[1][0])
        mask = torch.as_tensor(obs['image_mask'], dtype=torch.bool, device=image.device)
        return image.masked_fill(~mask, -torch.inf), [e[1] for e in encoded]

    def sample(self, obs, generator):
        if not any(obs['image_mask']):
            return None
        image, positions = self.logits(obs)
        mode = int(torch.multinomial(image.softmax(-1), 1, generator=generator))
        slots = (torch.multinomial(positions[mode].softmax(-1), obs['variants'][mode]['block_count'],
                 replacement=True, generator=generator).tolist() if mode < 2 else [])
        logp, entropy = joint_statistics(image, positions, mode, slots)
        return dict(mode=mode, positions=slots, log_probability=float(logp.detach()), entropy=float(entropy.detach()))


def distribution_metrics(actor, reference, obs):
    im, pos = actor.logits(obs)
    ref_im, ref_pos = reference.logits(obs)
    a, b = Categorical(logits=im), Categorical(logits=ref_im)
    entropy, kl = a.entropy(), kl_divergence(a, b)
    for mode in (0, 1):
        if obs['image_mask'][mode]:
            q, r = Categorical(logits=pos[mode]), Categorical(logits=ref_pos[mode])
            weight = a.probs[mode]*obs['variants'][mode]['block_count']
            entropy = entropy + weight*q.entropy()
            kl = kl + weight*kl_divergence(q, r)
    return entropy, kl


def teacher_label(obs, selected, commitments, weights):
    mode = 2 if selected == 'ground' else (0 if selected.endswith('_4') else 1)
    variant = obs['variants'][1 if mode == 2 else mode]
    label = fixed.teacher_label(variant, mode == 2, commitments, weights)
    ground_slot = variant['node_ids'].index(fixed.GROUND)
    ground_represented = (label['ground_block_windows'] if mode < 2 and
        obs['image_mask'][mode] and variant['position_mask'][ground_slot] else 0.)
    return {**label, 'mode': mode, 'selected': selected,
        'ground_block_committed_windows': label['ground_block_windows'],
        'ground_block_windows': ground_represented}


def imitation_loss(actor, examples):
    losses = []
    for row in examples:
        obs, label = row['observation'], row['label']
        mode = label['mode']
        if not obs['image_mask'][mode]:
            continue
        image, positions = actor.logits(obs)
        loss = -image.log_softmax(-1)[mode]
        if mode < 2 and label['represented_windows']:
            pos = positions[mode]
            target = torch.as_tensor(label['soft_positions'], dtype=pos.dtype, device=pos.device)
            valid = target > 0
            weight = obs['variants'][mode]['block_count']*label['represented_windows']/label['total_windows']
            loss = loss-weight*(target[valid]*pos.log_softmax(-1)[valid]).sum()
        losses.append(loss)
    if not losses:
        raise ValueError('No legal teacher image action')
    return torch.stack(losses).mean()


class DynamicPolicy(base.CommittedPolicy):
    def __init__(self, actor, seed, env, plane):
        super().__init__(actor, seed)
        self.env, self.plane = env, plane

    def select_actions(self, state):
        source = state.config['observer_source']
        actions = []
        for task, tiles in base.task_groups(state):
            if task not in self.decisions:
                if not base.complete_uncommitted(state, task, tiles):
                    continue
                obs = build_observation(self.env, state, task)
                with torch.no_grad():
                    choice = self.actor.sample(obs, self.generator)
                if choice is None:
                    continue
                mode = choice['mode']
                variant = obs['variants'][1 if mode == 2 else mode]
                self.env.repack(task, 16 if mode == 2 else COUNTS[mode])
                targets = {tid: variant['node_ids'][slot]
                           for tid, slot in zip(variant['tile_ids'], choice['positions'])}
                self.decisions[task] = dict(observation=obs, **choice, targets=targets)
                # 仅自身重打包状态立即更新；其他节点仍经过原通告记录与TTL过滤。
                state = self.plane.view(self.env.export_state(), source, 2)
                tiles = {tid: row for tid, row in state.tiles.items() if row['parent_task_id'] == task}
            choice = self.decisions[task]
            if choice['mode'] == 2:
                actions.extend(base.RawImageAction(t, ActionType.OFFLOAD,
                    raw_hops=state.config['relay_max_hops']) for t in tiles)
                continue
            variant = choice['observation']['variants'][choice['mode']]
            plan = contact_plan(state)
            memory = {s: r['mem_remaining_gb'] for s, r in state.satellites.items()}
            for tid, tile in sorted(tiles.items()):
                if tile['state'] != 'QUEUED' or tile['in_transfer'] or tile['location'] != source:
                    continue
                target = choice['targets'].get(tid)
                processor = ('ground', variant['ground_id']) if target == fixed.GROUND else target
                feasible = fixed.options(state, tile, plan, memory)
                if processor not in feasible:
                    continue
                _, path = feasible[processor]
                reserve_input_memory(memory, plan, path, tile)
                plan.reserve(path, tile['data_size_mb'])
                actions.append(action_for(tid, source, processor, plan=plan, path=path,
                                          ground_relay=target == fixed.GROUND))
        return actions
