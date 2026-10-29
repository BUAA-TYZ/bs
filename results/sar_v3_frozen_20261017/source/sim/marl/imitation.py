"""可选启发式行为克隆；示范仅从训练场景采集，不作为 RL 效果证据。"""

from collections import Counter

import numpy as np
import torch

from sim.env import SimulationEnv
from sim.marl.decision import build_batch, decode_batch, decision_state, candidate_neighbors
from sim.entities import ActionType
from sim.marl.observation import MAX_NEIGHBORS
from sim.scheduler.predictive import PredictivePolicy
from sim.scheduler.window_aware import WindowAwareGreedy
from sim.scheduler.greedy import GreedyEarliestFinish
from sim.scheduler.literature import CGSComputeAdapted


def collect_demonstrations(model, cfg, device, teacher_name="predictive", learner_rollout=False):
    """同状态查询教师；DAgger 执行学生动作，但监督标签始终来自教师。"""
    teachers = {"predictive": PredictivePolicy, "window_aware": WindowAwareGreedy,
                "greedy": GreedyEarliestFinish, "cgs_compute_adapted": CGSComputeAdapted}
    if teacher_name not in teachers:
        raise ValueError("Unknown imitation teacher")
    env, teacher = SimulationEnv(cfg), teachers[teacher_name]()
    samples, counts, predictions, confusion = [], Counter(), Counter(), Counter()
    model.eval()
    try:
        for _ in range(cfg.sim_steps):
            state = env.export_state()
            obs, tile_masks, masks, ids = build_batch(state, **model.feature_options)
            chosen = {a.tile_id: a for a in teacher.select_actions(decision_state(state))}
            indices = np.zeros(tile_masks.shape, dtype=np.int64)
            for row, sid in enumerate(sorted(state.satellites)):
                neighbors = candidate_neighbors(state, sid)
                for slot, tid in enumerate(ids[sid]):
                    action = chosen[tid]
                    idx = 1 if action.action_type == ActionType.LOCAL else 0
                    if action.target_sat_id is not None:
                        idx = 2 + neighbors.index(action.target_sat_id)
                    if action.target_gs_id is not None:
                        idx = 2 + MAX_NEIGHBORS + list(state.ground_stations).index(action.target_gs_id)
                    indices[row, slot] = idx if masks[row, slot, idx] else 0
                    counts[int(indices[row, slot])] += 1
            active = tile_masks.any(axis=-1)
            if active.any():
                samples.append(tuple(torch.as_tensor(x[active]) for x in (obs, tile_masks, masks, indices)))
            with torch.no_grad():
                logits, _ = model(torch.as_tensor(obs, device=device))
                logits = logits.masked_fill(~torch.as_tensor(masks, device=device), torch.finfo(logits.dtype).min)
                predicted = logits.argmax(-1).cpu().numpy()
            for label, prediction in zip(indices[tile_masks], predicted[tile_masks]):
                predictions[int(prediction)] += 1
                confusion[f"{label}:{prediction}"] += 1
            env.step(decode_batch(predicted if learner_rollout else indices, state, ids))
        summary = env.summary()
    finally:
        env.close()
    dataset = tuple(torch.cat([s[i] for s in samples]) for i in range(4)) if samples else None
    total = sum(confusion.values())
    agreement = sum(n for pair, n in confusion.items() if pair.split(":")[0] == pair.split(":")[1])
    return dataset, {"samples": 0 if dataset is None else len(dataset[0]),
                     "action_counts": dict(counts), "prediction_counts": dict(predictions),
                     "confusion": dict(confusion), "agreement": agreement / total if total else None,
                     "rollout": "learner" if learner_rollout else "teacher", "metrics": summary}


def merge_demonstrations(datasets):
    nonempty = [data for data in datasets if data is not None]
    return tuple(torch.cat([data[i] for data in nonempty]) for i in range(4)) if nonempty else None


def fit_demonstrations(model, optimizer, dataset, device, epochs=4, minibatch=256):
    if dataset is None:
        return {"samples": 0, "loss": None, "agreement": None}
    obs, tiles, masks, actions = [x.to(device) for x in dataset]
    losses = []
    model.train()
    for _ in range(epochs):
        for idx in torch.randperm(len(obs), device=device).split(minibatch):
            logits, _ = model(obs[idx])
            logits = logits.masked_fill(~masks[idx], torch.finfo(logits.dtype).min)
            loss = torch.nn.functional.cross_entropy(logits[tiles[idx]], actions[idx][tiles[idx]])
            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 0.5)
            optimizer.step()
            losses.append(float(loss.detach()))
    model.eval()
    correct, total, class_correct, class_total = 0, 0, Counter(), Counter()
    with torch.no_grad():
        for idx in torch.arange(len(obs), device=device).split(minibatch):
            logits, _ = model(obs[idx])
            predicted = logits.masked_fill(~masks[idx], torch.finfo(logits.dtype).min).argmax(-1)[tiles[idx]]
            labels = actions[idx][tiles[idx]]
            correct += int((predicted == labels).sum())
            total += len(labels)
            for label in labels.unique().tolist():
                class_total[label] += int((labels == label).sum())
                class_correct[label] += int(((labels == label) & (predicted == labels)).sum())
    return {"samples": len(obs), "loss": float(np.mean(losses)), "agreement": correct / total,
            "class_recall": {str(k): class_correct[k] / n for k, n in class_total.items()}}


def pretrain_episode(model, optimizer, cfg, device, epochs=4, minibatch=256, teacher_name="predictive"):
    dataset, record = collect_demonstrations(model, cfg, device, teacher_name)
    record["fit"] = fit_demonstrations(model, optimizer, dataset, device, epochs, minibatch)
    record["loss"] = record["fit"]["loss"]
    return record
