"""主场景两层整图策略接口；actor只接收合法局部视图，执行器保持原样。"""

from collections import defaultdict
from copy import deepcopy
from dataclasses import replace
import math

import numpy as np
import torch
from torch import nn
from torch.distributions import Categorical

from analysis.local_information_baselines import reachable_helpers
from analysis.sar_three_plan import Calendar, RawImageAction
from sim.entities import Action, ActionType
from sim.scheduler.literature import action_for, contact_plan
from sim.scheduler.input_routing import current_input_options, reserve_input_memory
from sim.topology import link_key

K = 8
NODE_DIM = 14
IMAGE_DIM = 8
SCHEMA = "main-m-hierarchical-v1"


def task_groups(state):
    groups = defaultdict(dict)
    for tid, tile in state.tiles.items():
        groups[tile["parent_task_id"]][tid] = tile
    return sorted(groups.items(), key=lambda x: (-max(t["waiting_time"] for t in x[1].values()), x[0]))


def complete_uncommitted(state, task, tiles):
    return (len(tiles) == len(state.pending_task_tiles.get(task, {}))
            and all(t["state"] == "QUEUED" and not t["in_transfer"]
                    and t["location"] == t["source_sat_id"]
                    and t["task_progress"].get("total", len(tiles)) == len(tiles)
                    and not any(t["task_progress"].get(k, 0) for k in ("failed", "done", "running", "awaiting_delivery"))
                    for t in tiles.values()))


def sat_options(state, tile, plan=None, memory=None):
    # 复用输入两跳可行性校验，但该动作头不允许分块送地面。
    view = replace(state, config={**state.config, "ground_input_relay": False})
    plan = contact_plan(view) if plan is None else plan
    memory = {sid: r["mem_remaining_gb"] for sid, r in view.satellites.items()} if memory is None else memory
    return current_input_options(view, plan, tile, memory)


def window_delay(row):
    starts = [float(v[0]) for v in row.get("next_gs_windows", {}).values() if v is not None]
    return min(starts, default=600.)


def node_features(row, *, own=False, arrival=0., hops=0., mem_capacity=1., vram_capacity=1.):
    known = bool(row.get("dynamic_known", True))
    # 固定物理量缩放，不用验证集/测试集拟合；未知由独立mask与age表达。
    return [float(own), float(known), min(1., row.get("advertisement_age_s", 15.) / 15.) if known else 1.,
            math.log1p(max(0., row.get("committed_compute_s", 0))) / math.log(601),
            min(2., row.get("mem_remaining_gb", 0) / max(mem_capacity, 1e-9)),
            min(2., row.get("vram_remaining_gb", 0) / max(vram_capacity, 1e-9)),
            math.log1p(max(0., row.get("queue_len", 0))) / math.log(101),
            min(2., row.get("running_count", 0) / 8),
            math.log1p(sum(max(0., x) for x in row.get("isl_backlog_mb", {}).values())) / math.log(10001),
            math.log1p(sum(max(0., x) for x in row.get("ground_backlog_mb", {}).values())) / math.log(10001),
            min(2., window_delay(row) / 600), min(2., arrival / 600), hops / 2,
            min(2., row["compute_rate"] / 10)]


def build_observation(state, task, tiles):
    source = state.config["observer_source"]
    assert state.config["observation_hops"] == 2 and state.config["local_information"]
    subset = replace(state, tiles=tiles)
    scope = reachable_helpers(state, source, 2, set(state.config["task_source_sat_ids"]))
    # 候选排序只用公开计划与块大小，先清除动态积压/内存估计，避免暗中按队列选Top-K。
    static_sats = {}
    for sid, row in subset.satellites.items():
        static_sats[sid] = {**row, "committed_compute_s": 0., "isl_backlog_mb": {},
                            "ground_backlog_mb": {}, "committed_result_mb": 0., "unrouted_result_mb": 0.,
                            "mem_remaining_gb": state.config["mem_capacity_gb"]}
    static = replace(subset, satellites=static_sats)
    static_plan = contact_plan(static)
    largest = max(tiles.values(), key=lambda t: t["data_size_mb"])
    options = sat_options(static, largest, static_plan)
    helpers = sorted((sid for sid in options if sid in scope), key=lambda sid: (options[sid][0], sid))[:K]
    nodes = [source, *helpers]
    plan = contact_plan(subset)
    all_options = [sat_options(subset, t, plan) for t in tiles.values()]
    mask = [all(sid in opts for opts in all_options) for sid in nodes] + [False] * (K+1-len(nodes))
    features = []
    for sid in nodes:
        path = options.get(sid, (0., ()))[1]
        features.append(node_features(state.satellites[sid], own=sid == source,
            arrival=options.get(sid, (0., ()))[0], hops=len(path),
            mem_capacity=state.config["mem_capacity_gb"], vram_capacity=state.config["vram_capacity_gb"]))
    features += [[0.] * NODE_DIM for _ in range(K+1-len(nodes))]
    adjacency = np.eye(K+1, dtype=bool)
    for i, a in enumerate(nodes):
        for j, b in enumerate(nodes):
            adjacency[i, j] = i == j or i == 0 or j == 0 or bool(state.links.get(link_key(a, b), {}).get("up", False))
    # padding query可看源节点以避免全mask softmax；padding永远不能成为key或动作。
    adjacency[:, 0] = True
    raw = [RawImageAction(tid, ActionType.OFFLOAD, raw_hops=state.config["relay_max_hops"]) for tid in tiles]
    ground_quote = Calendar(subset).quote(subset, raw, raw=True, raw_hops=state.config["relay_max_hops"])
    total_windows = sum(t["compute_cost"] for t in tiles.values())
    image = [sum(t["data_size_mb"] for t in tiles.values()) / 2000, total_windows / 100,
             len(tiles) / 16, max(t["waiting_time"] for t in tiles.values()) / 600,
             max(t["vram_req_gb"] for t in tiles.values()) / state.config["vram_capacity_gb"],
             float(ground_quote is not None), min(2., ground_quote / 600) if ground_quote is not None else 2.,
             len(helpers) / K]
    return {"schema": SCHEMA, "task_id": task, "source": source, "node_ids": nodes + [-1]*(K+1-len(nodes)),
            "nodes": features, "node_valid": [True]*len(nodes)+[False]*(K+1-len(nodes)),
            "adjacency": adjacency.tolist(), "image": image,
            "image_mask": [any(mask), ground_quote is not None], "position_mask": mask,
            "tile_ids": sorted(tiles), "window_weights": [tiles[t]["compute_cost"] for t in sorted(tiles)],
            "block_count": len(tiles), "time_seconds": state.time*state.config["dt"]}


class HierarchicalActor(nn.Module):
    """局部图注意力；一次图像选择和共享块分布的独立采样。"""

    def __init__(self, hidden=64):
        super().__init__()
        self.embed = nn.Linear(NODE_DIM, hidden)
        self.attention = nn.MultiheadAttention(hidden, 4, dropout=0., batch_first=True)
        self.norm = nn.LayerNorm(hidden)
        self.image_encoder = nn.Sequential(nn.Linear(IMAGE_DIM, hidden), nn.Tanh())
        self.image_head = nn.Linear(hidden*2, 2)
        self.position_head = nn.Sequential(nn.Linear(hidden*3, hidden), nn.Tanh(), nn.Linear(hidden, 1))

    def logits(self, obs):
        device = next(self.parameters()).device
        tensor = lambda key, dtype: torch.as_tensor(obs[key], dtype=dtype, device=device)
        x = torch.tanh(self.embed(tensor("nodes", torch.float32))).unsqueeze(0)
        valid = tensor("node_valid", torch.bool)
        edges = tensor("adjacency", torch.bool)
        attended, _ = self.attention(x, x, x, attn_mask=~edges, key_padding_mask=~valid.unsqueeze(0), need_weights=False)
        x = self.norm(x+attended)[0]
        im = self.image_encoder(tensor("image", torch.float32))
        context = torch.cat([x[0], im])
        image_logits = self.image_head(context).masked_fill(~tensor("image_mask", torch.bool), -torch.inf)
        position_logits = self.position_head(torch.cat([x, context.expand(K+1, -1)], dim=-1)).squeeze(-1)
        position_logits = position_logits.masked_fill(~tensor("position_mask", torch.bool), -torch.inf)
        return image_logits, position_logits

    def sample(self, obs, generator):
        if not any(obs["image_mask"]):
            return None
        im, pos = self.logits(obs)
        mode = int(torch.multinomial(torch.softmax(im, -1), 1, generator=generator))
        places = (torch.multinomial(torch.softmax(pos, -1), obs["block_count"], replacement=True, generator=generator).tolist()
                  if mode == 0 else [])
        logp, entropy = joint_statistics(im, pos, mode, places)
        return {"mode": mode, "positions": places, "log_probability": float(logp.detach()),
                "entropy": float(entropy.detach())}


def joint_statistics(image_logits, position_logits, mode, positions):
    image = Categorical(logits=image_logits)
    logp = image.log_prob(torch.as_tensor(mode, device=image_logits.device))
    entropy = image.entropy()
    if mode == 0:
        position = Categorical(logits=position_logits)
        logp = logp + position.log_prob(torch.as_tensor(positions, device=position_logits.device)).sum()
        entropy = entropy + len(positions)*position.entropy()
    elif positions:
        raise ValueError("Ground choice has no block actions")
    return logp, entropy


class CentralCritic(nn.Module):
    """独立全局价值网络；没有任何到actor的全局特征通路。"""
    def __init__(self, hidden=64):
        super().__init__()
        self.nodes = nn.Sequential(nn.Linear(NODE_DIM, hidden), nn.Tanh())
        self.value = nn.Sequential(nn.Linear(2*hidden+4, hidden), nn.Tanh(), nn.Linear(hidden, 1))

    def forward(self, nodes, ground):
        x = self.nodes(nodes)
        return self.value(torch.cat([x.mean(-2), x.max(-2).values, ground], dim=-1)).squeeze(-1)


def critic_observation(state):
    nodes = [node_features(row, mem_capacity=state.config["mem_capacity_gb"],
                           vram_capacity=state.config["vram_capacity_gb"]) for _, row in sorted(state.satellites.items())]
    gs = list(state.ground_stations.values())
    ground = [sum(g.get("committed_compute_s", 0) for g in gs)/600,
              sum(g.get("queue_len", 0) for g in gs)/100,
              sum(g.get("running", 0) for g in gs), state.time*state.config["dt"]/86400]
    return {"nodes": nodes, "ground": ground}


class CommittedPolicy:
    def __init__(self, actor, seed):
        self.actor = actor
        self.generator = torch.Generator().manual_seed(seed)
        self.decisions = {}

    def select_actions(self, state):
        source = state.config["observer_source"]
        actions = []
        for task, tiles in task_groups(state):
            if task not in self.decisions:
                if not complete_uncommitted(state, task, tiles):
                    continue
                obs = build_observation(state, task, tiles)
                with torch.no_grad():
                    sampled = self.actor.sample(obs, self.generator)
                if sampled is None:
                    continue
                targets = ({tid: obs["node_ids"][slot] for tid, slot in zip(obs["tile_ids"], sampled["positions"])}
                           if sampled["mode"] == 0 else {})
                self.decisions[task] = {"observation": obs, **sampled, "targets": targets}
            chosen = self.decisions[task]
            if chosen["mode"] == 1:
                actions.extend(RawImageAction(tid, ActionType.OFFLOAD, raw_hops=state.config["relay_max_hops"]) for tid in tiles)
                continue
            plan = contact_plan(state)
            memory = {sid: row["mem_remaining_gb"] for sid, row in state.satellites.items()}
            for tid, tile in sorted(tiles.items()):
                if tile["state"] != "QUEUED" or tile["in_transfer"] or tile["location"] != source:
                    continue
                target = chosen["targets"].get(tid)
                options = sat_options(state, tile, plan, memory)
                if target not in options:
                    continue
                _, path = options[target]
                reserve_input_memory(memory, plan, path, tile)
                plan.reserve(path, tile["data_size_mb"])
                actions.append(action_for(tid, source, target, plan=plan, path=path))
        return actions


def teacher_label(obs, raw_ground, commitments, weights):
    if raw_ground:
        return {"mode": 1, "soft_positions": [0.]*(K+1), "total_windows": sum(weights.values()),
                "outside_topk_windows": 0., "masked_windows": 0., "uncommitted_windows": 0.,
                "ground_block_windows": 0., "represented_windows": sum(weights.values())}
    mass = [0.]*(K+1)
    outside = masked = uncommitted = ground = 0.
    for tid, weight in weights.items():
        target = commitments.get(tid)
        if target is None:
            uncommitted += weight
        elif isinstance(target, str):
            ground += weight
        elif target not in obs["node_ids"]:
            outside += weight
        else:
            slot = obs["node_ids"].index(target)
            if not obs["position_mask"][slot]:
                masked += weight
            else:
                mass[slot] += weight
    represented = sum(mass)
    return {"mode": 0, "soft_positions": [m/represented if represented else 0. for m in mass],
            "total_windows": sum(weights.values()), "represented_windows": represented,
            "outside_topk_windows": outside, "masked_windows": masked,
            "uncommitted_windows": uncommitted, "ground_block_windows": ground}


def terminal_reward(outcome, D=60., D_max=600.):
    if outcome["status"] == "completed":
        latency = outcome["latency_seconds"]
        return -min(latency, D_max)/D_max + float(latency <= D)
    if outcome["observed_age_seconds"] < D_max:
        raise ValueError("Unfinished task lacks D_max followup")
    return -1.
