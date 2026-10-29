"""第二阶段分块地面动作；保留第一阶段冻结接口及公共执行器。"""

import math
import torch
from torch import nn
from analysis import main_rl_interface as old
from sim.entities import ActionType
from sim.scheduler.literature import action_for, contact_plan
from sim.scheduler.input_routing import current_input_options, reserve_input_memory

K = old.K
POSITIONS = K + 2
GROUND = 'ground_block'
SCHEMA = 'main-m-hierarchical-ground-v2'


def options(state, tile, plan=None, memory=None):
    plan = contact_plan(state) if plan is None else plan
    memory = {s: r['mem_remaining_gb'] for s, r in state.satellites.items()} if memory is None else memory
    return current_input_options(state, plan, tile, memory)


def build_observation(state, task, tiles):
    obs = old.build_observation(state, task, tiles)
    assert len(state.ground_stations) == 1, 'Frozen single station only'
    gid, gs = next(iter(state.ground_stations.items()))
    ground = ('ground', gid)
    plan = contact_plan(state)
    choices = [options(state, tile, plan) for tile in tiles.values()]
    available = all(ground in opts for opts in choices)
    arrival = max((opts[ground][0] for opts in choices if ground in opts), default=600.)
    row = dict(gs, mem_remaining_gb=state.config['mem_capacity_gb'],
               vram_remaining_gb=state.config['vram_capacity_gb'], running_count=gs.get('running', 0),
               advertisement_age_s=15., next_gs_windows={gid: (0., 600.)})
    feature = old.node_features(row, arrival=arrival, mem_capacity=state.config['mem_capacity_gb'],
                               vram_capacity=state.config['vram_capacity_gb'])
    obs['node_ids'].insert(1, GROUND)
    obs['nodes'].insert(1, feature)
    obs['node_valid'].insert(1, True)
    obs['position_mask'].insert(1, available)
    edges = obs['adjacency']
    for r in edges:
        r.insert(1, True)
    edges.insert(1, [True]*POSITIONS)
    obs.update(schema=SCHEMA, ground_id=gid)
    obs['image_mask'][0] = any(obs['position_mask'])
    return obs


class HierarchicalActor(old.HierarchicalActor):
    def logits(self, obs):
        assert obs['schema'] == SCHEMA
        device = next(self.parameters()).device
        tensor = lambda key, dtype: torch.as_tensor(obs[key], dtype=dtype, device=device)
        x = torch.tanh(self.embed(tensor('nodes', torch.float32))).unsqueeze(0)
        attended, _ = self.attention(x, x, x, attn_mask=~tensor('adjacency', torch.bool),
            key_padding_mask=~tensor('node_valid', torch.bool).unsqueeze(0), need_weights=False)
        x = self.norm(x+attended)[0]
        im = self.image_encoder(tensor('image', torch.float32))
        context = torch.cat([x[0], im])
        image = self.image_head(context).masked_fill(~tensor('image_mask', torch.bool), -torch.inf)
        position = self.position_head(torch.cat([x, context.expand(POSITIONS, -1)], -1)).squeeze(-1)
        return image, position.masked_fill(~tensor('position_mask', torch.bool), -torch.inf)


class CommittedPolicy(old.CommittedPolicy):
    def select_actions(self, state):
        source = state.config['observer_source']
        actions = []
        for task, tiles in old.task_groups(state):
            if task not in self.decisions:
                if not old.complete_uncommitted(state, task, tiles):
                    continue
                obs = build_observation(state, task, tiles)
                with torch.no_grad():
                    sampled = self.actor.sample(obs, self.generator)
                if sampled is None:
                    continue
                targets = {tid: obs['node_ids'][slot] for tid, slot in zip(obs['tile_ids'], sampled['positions'])}
                self.decisions[task] = {'observation': obs, **sampled, 'targets': targets}
            choice = self.decisions[task]
            if choice['mode'] == 1:
                actions.extend(old.RawImageAction(t, ActionType.OFFLOAD, raw_hops=state.config['relay_max_hops']) for t in tiles)
                continue
            plan = contact_plan(state)
            memory = {s: r['mem_remaining_gb'] for s, r in state.satellites.items()}
            for tid, tile in sorted(tiles.items()):
                if tile['state'] != 'QUEUED' or tile['in_transfer'] or tile['location'] != source:
                    continue
                target = choice['targets'].get(tid)
                processor = ('ground', choice['observation']['ground_id']) if target == GROUND else target
                feasible = options(state, tile, plan, memory)
                if processor not in feasible:
                    continue
                _, path = feasible[processor]
                reserve_input_memory(memory, plan, path, tile)
                plan.reserve(path, tile['data_size_mb'])
                actions.append(action_for(tid, source, processor, plan=plan, path=path,
                                          ground_relay=target == GROUND))
        return actions


def teacher_label(obs, raw_ground, commitments, weights):
    total = sum(weights.values())
    counts = dict(outside_topk_windows=0., masked_windows=0., uncommitted_windows=0.,
                  unsupported_windows=0., represented_windows=0.)
    mass = [0.]*POSITIONS
    mode = int(raw_ground)
    if raw_ground:
        counts['represented_windows' if obs['image_mask'][1] else 'masked_windows'] = total
    else:
        for tile, weight in weights.items():
            target = commitments.get(tile)
            if target is None:
                counts['uncommitted_windows'] += weight
            elif target not in obs['node_ids']:
                counts['outside_topk_windows' if isinstance(target, int) else 'unsupported_windows'] += weight
            elif not obs['position_mask'][obs['node_ids'].index(target)]:
                counts['masked_windows'] += weight
            else:
                mass[obs['node_ids'].index(target)] += weight
                counts['represented_windows'] += weight
    represented = counts['represented_windows']
    return {'mode': mode, 'total_windows': total, **counts,
            'soft_positions': [x/represented if represented and not raw_ground else 0. for x in mass],
            'ground_block_windows': sum(w for tid, w in weights.items() if commitments.get(tid) == GROUND)}


def imitation_loss(actor, examples):
    losses = []
    for example in examples:
        obs, label = example['observation'], example['label']
        if not obs['image_mask'][label['mode']]:
            continue
        im, pos = actor.logits(obs)
        loss = -torch.log_softmax(im, -1)[label['mode']]
        if label['mode'] == 0 and label['represented_windows']:
            target = torch.as_tensor(label['soft_positions'], dtype=pos.dtype, device=pos.device)
            valid = target > 0
            # 期望整图logp：图像项加实际块数项；零质量mask不做0*(-inf)。
            fraction = label['represented_windows']/label['total_windows']
            loss = loss-obs['block_count']*fraction*(target[valid]*torch.log_softmax(pos, -1)[valid]).sum()
        losses.append(loss)
    if not losses:
        raise ValueError('Batch has no legal teacher image action')
    return torch.stack(losses).mean()
