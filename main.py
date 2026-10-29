from __future__ import annotations

import argparse
from dataclasses import replace
import json
from typing import Dict

from tqdm import tqdm

from sim.config import load_config
from sim.env import SimulationEnv
from sim.marl.decision import decision_state
from sim.marl.observation import MAX_TILES_PER_SAT
from sim.scheduler.greedy import GreedyEarliestFinish
from sim.scheduler.load_aware import LoadAwareResourceFit
from sim.scheduler.local import LocalOnlyPolicy
from sim.scheduler.literature import CGSComputeAdapted, HEFTAdapted
from sim.scheduler.delivery_guard import CGSDeliveryGuard
from sim.scheduler.task_gate import CGSTaskGate
from sim.scheduler.task_choice import CGSTaskChoice
from sim.scheduler.dts_tmax import DTSTMAXOneHopAdapted
from sim.scheduler.rolling_admission import RollingAdmissionPolicy
from sim.scheduler.task_quota import TaskQuotaPolicy
from sim.scheduler.task_delivery import TaskDeliveryPolicy
from sim.scheduler.arrival_calendar import ArrivalCalendarPolicy
from sim.scheduler.predictive import PredictivePolicy
from sim.scheduler.reservation import ReservationPolicy, JointReservationPolicy, ForecastReservationPolicy
from sim.scheduler.random_stub import RandomPolicy
from sim.scheduler.window_aware import WindowAwareGreedy

# make_policy 可构造的星上调度策略；地面模式不经过策略，按 pipeline_mode 执行。
POLICY_NAMES = (
    "local", "greedy", "load_aware", "random", "window_aware", "predictive",
    "reservation", "reservation_independent", "reservation_joint", "reservation_joint_sequential",
    "reservation_calendar", "reservation_guarded", "reservation_multihop", "reservation_forecast",
    "reservation_deferred", "heft_adapted", "cgs_compute_adapted", "dts_tmax_onehop", "dts_tmax_path", "dts_tmax_hybrid",
    "rolling_admission", "task_quota", "task_delivery", "arrival_calendar", "cgs_delivery_guard", "cgs_task_gate", "cgs_task_choice", "marl",
)
GROUND_MODES = ("ground_compute", "ground_relay", "ground_tiles", "ground_unique_relay")


def apply_execution_mode(cfg, policy_name: str):
    """地面模式名称按 pipeline_mode 执行（与 evaluate_policies 一致），其余名称为星上调度策略。"""
    return replace(cfg, pipeline_mode=policy_name) if policy_name in GROUND_MODES else cfg


def make_policy(name: str, seed: int, marl_checkpoint: str = ""):
    if name == "cgs_task_gate":
        return CGSTaskGate()
    if name == "cgs_task_choice":
        return CGSTaskChoice()
    if name == "cgs_delivery_guard":
        return CGSDeliveryGuard()
    if name == "arrival_calendar":
        return ArrivalCalendarPolicy()
    if name == "task_delivery":
        return TaskDeliveryPolicy()
    if name == "task_quota":
        return TaskQuotaPolicy()
    if name == "rolling_admission":
        return RollingAdmissionPolicy()
    if name in ("dts_tmax_onehop", "dts_tmax_path"):
        return DTSTMAXOneHopAdapted()
    if name == "dts_tmax_hybrid":
        return DTSTMAXOneHopAdapted(include_ground=True)
    if name == "heft_adapted":
        return HEFTAdapted()
    if name == "cgs_compute_adapted":
        return CGSComputeAdapted()
    if name == "local":
        return LocalOnlyPolicy()
    if name == "predictive":
        return PredictivePolicy()
    if name in ("reservation", "reservation_independent"):
        return ReservationPolicy(reserve=name == "reservation")
    if name in ("reservation_joint", "reservation_joint_sequential"):
        return JointReservationPolicy(beam_width=4 if name == "reservation_joint" else 1)
    if name in ("reservation_calendar", "reservation_guarded", "reservation_multihop", "reservation_forecast"):
        return ForecastReservationPolicy(guard=name in ("reservation_guarded", "reservation_forecast"),
                                         multihop=name in ("reservation_multihop", "reservation_forecast"))
    if name == "reservation_deferred":
        return ForecastReservationPolicy(multihop=True, defer=True)
    if name == "greedy":
        return GreedyEarliestFinish()
    if name == "load_aware":
        return LoadAwareResourceFit()
    if name == "random":
        return RandomPolicy(seed=seed)
    if name == "window_aware":
        return WindowAwareGreedy()
    if name == "marl":
        from sim.scheduler.marl_policy import MARLPolicy

        if not marl_checkpoint:
            raise ValueError("--marl-checkpoint must be specified for marl policy")
        return MARLPolicy(checkpoint_path=marl_checkpoint)
    raise ValueError(f"Unknown policy: {name}")


def run_once(cfg_path: str, policy_name: str, marl_checkpoint: str = "") -> Dict:
    cfg = apply_execution_mode(load_config(cfg_path), policy_name)
    return run_config(cfg, policy_name, marl_checkpoint, progress=True)


def run_config(cfg, policy_name: str, marl_checkpoint: str = "", progress: bool = False,
               observer=None) -> Dict:
    """CLI、评估脚本与工作台共用的执行流程。

    observer 只读取状态：on_start(env) 在首步前调用，on_step(env, step_idx, actions)
    在每步之后调用；抛出异常即中止运行（工作台用于取消）。
    """
    if (cfg.pipeline_mode == "distributed" and policy_name == "marl"
            and cfg.decision_tile_limit_per_sat != MAX_TILES_PER_SAT):
        raise ValueError(f"MARL requires decision_tile_limit_per_sat={MAX_TILES_PER_SAT}")
    policy = (make_policy(policy_name, cfg.seed, marl_checkpoint=marl_checkpoint)
              if cfg.pipeline_mode == "distributed" else None)
    env = SimulationEnv(cfg)
    decision_interval = max(1, cfg.decision_interval_steps)
    cached_actions = []

    total_sim_s = cfg.sim_steps * cfg.dt
    try:
        if observer is not None:
            observer.on_start(env)
        with tqdm(total=cfg.sim_steps, unit="step", desc=policy_name, disable=not progress) as pbar:
            for step_idx in range(cfg.sim_steps):
                if policy is not None and step_idx % decision_interval == 0:
                    state = decision_state(env.export_state(), cfg.decision_tile_limit_per_sat)
                    cached_actions = policy.select_actions(state)
                else:
                    cached_actions = []
                env.step(cached_actions)
                if observer is not None:
                    observer.on_step(env, step_idx, cached_actions)
                pbar.set_postfix_str(f"t={step_idx * cfg.dt:.0f}/{total_sim_s:.0f}s")
                pbar.update(1)
        summary = env.summary()
        summary["experiment"]["policy"] = policy_name if policy is not None else cfg.pipeline_mode
        summary["experiment"]["checkpoint"] = (marl_checkpoint or None) if policy is not None else None
        if isinstance(policy, (ReservationPolicy, HEFTAdapted, CGSComputeAdapted,
                               DTSTMAXOneHopAdapted, RollingAdmissionPolicy, TaskQuotaPolicy, TaskDeliveryPolicy)):
            summary["scheduler_diagnostics"] = dict(policy.stats)
        return summary
    finally:
        env.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML/JSON config")
    parser.add_argument(
        "--policy", default="greedy", help="|".join(POLICY_NAMES + GROUND_MODES)
    )
    parser.add_argument("--output", default="metrics.json")
    parser.add_argument(
        "--marl-checkpoint",
        default="",
        help="Path to MARL checkpoint (.pt) when --policy=marl",
    )
    args = parser.parse_args()

    summary = run_once(args.config, args.policy, args.marl_checkpoint)

    print(json.dumps(summary, indent=2))
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
