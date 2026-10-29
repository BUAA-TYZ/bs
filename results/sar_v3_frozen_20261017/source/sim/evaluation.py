"""完整任务配对评估，保留未完成样本以披露成功子集偏差。"""

import math


def task_age_score(outcomes, horizons_seconds, observation_end_seconds):
    """预先固定多时限等权评分；未交付任务在时延辅指标中保留为截断值。"""
    curve = task_age_delivery_curve(outcomes, horizons_seconds, observation_end_seconds)
    maximum = max(horizons_seconds)
    return {
        "delivery_curve": curve,
        "delivery_score": sum(row["delivery_rate"] for row in curve) / len(curve) if outcomes else None,
        "restricted_mean_latency_seconds": sum(
            min(row["latency_seconds"], maximum) if row["status"] == "completed" else maximum
            for row in outcomes) / len(outcomes) if outcomes else None,
    }


def task_age_delivery_curve(outcomes: list[dict], horizons_seconds: list[float],
                            observation_end_seconds: float) -> list[dict]:
    """按任务产生后的统一时限统计完整交付，拒绝观察时长不足的队列。"""
    horizons = [float(value) for value in horizons_seconds]
    if not horizons or any(not math.isfinite(value) or value <= 0 for value in horizons):
        raise ValueError("Delivery horizons must be finite and positive")
    if horizons != sorted(set(horizons)):
        raise ValueError("Delivery horizons must be unique and increasing")
    if not math.isfinite(observation_end_seconds) or observation_end_seconds < 0:
        raise ValueError("Invalid observation end time")
    if len({row["task_id"] for row in outcomes}) != len(outcomes):
        raise ValueError("Task IDs must be unique")
    latencies = []
    for row in outcomes:
        release = row["release_seconds"]
        if not math.isfinite(release) or not 0 <= release <= observation_end_seconds:
            raise ValueError("Invalid task release time")
        # 保持全部到达任务为分母，不能通过删除晚到任务处理观察不足。
        if observation_end_seconds - release < horizons[-1]:
            raise ValueError("Insufficient follow-up for all tasks; extend the observation horizon")
        if row["status"] not in ("completed", "pending", "failed"):
            raise ValueError("Unknown task status")
        latency = row["latency_seconds"]
        if row["status"] == "completed":
            if latency is None or not math.isfinite(latency) or not 0 <= latency <= observation_end_seconds - release:
                raise ValueError("Invalid completed-task latency")
            latencies.append(latency)
        elif latency is not None:
            raise ValueError("Undelivered tasks must not have a completion latency")
    total = len(outcomes)
    curve = []
    for horizon in horizons:
        delivered = sum(latency <= horizon for latency in latencies)
        curve.append({
            "horizon_seconds": horizon, "total_tasks": total,
            "delivered_within_horizon": delivered,
            "not_delivered_within_horizon": total - delivered,
            "delivery_rate": delivered / total if total else None,
        })
    return curve


def paired_task_metrics(reference: list[dict], candidate: list[dict]) -> dict:
    ref = {r["task_id"]: r for r in reference}
    cand = {r["task_id"]: r for r in candidate}
    if len(ref) != len(reference) or len(cand) != len(candidate) or ref.keys() != cand.keys():
        raise ValueError("Paired evaluation requires identical unique task IDs")
    for tid, row in ref.items():
        if any(row[key] != cand[tid][key] for key in ("source_sat_id", "release_seconds", "deadline_seconds")):
            raise ValueError("Paired evaluation requires identical task definitions")
    ref_done = {tid for tid, r in ref.items() if r["status"] == "completed"}
    cand_done = {tid for tid, r in cand.items() if r["status"] == "completed"}
    common = sorted(ref_done & cand_done)
    ref_times = [ref[tid]["latency_seconds"] for tid in common]
    cand_times = [cand[tid]["latency_seconds"] for tid in common]

    def mean(values):
        return sum(values) / len(values) if values else None

    def p95(values):
        return sorted(values)[math.ceil(0.95 * len(values)) - 1] if values else None

    return {
        "total_tasks": len(ref), "common_completed_tasks": len(common),
        "reference_only_completed_tasks": len(ref_done - cand_done),
        "candidate_only_completed_tasks": len(cand_done - ref_done),
        "neither_completed_tasks": len(ref.keys() - ref_done - cand_done),
        "reference_failed_tasks": sum(r["status"] == "failed" for r in reference),
        "candidate_failed_tasks": sum(r["status"] == "failed" for r in candidate),
        "reference_pending_tasks": sum(r["status"] == "pending" for r in reference),
        "candidate_pending_tasks": sum(r["status"] == "pending" for r in candidate),
        "latency_population": "common_completed_only; selection_bias_remains",
        "reference_latency_mean_seconds": mean(ref_times),
        "candidate_latency_mean_seconds": mean(cand_times),
        "reference_latency_p95_seconds": p95(ref_times),
        "candidate_latency_p95_seconds": p95(cand_times),
        "candidate_minus_reference_latency_mean_seconds": mean([c - r for r, c in zip(ref_times, cand_times)]),
        "ratio_of_mean_latencies_reference_over_candidate":
            sum(ref_times) / sum(cand_times) if cand_times and sum(cand_times) > 0 else None,
        "candidate_faster_tasks": sum(c < r for r, c in zip(ref_times, cand_times)),
        "candidate_slower_tasks": sum(c > r for r, c in zip(ref_times, cand_times)),
        "equal_latency_tasks": sum(c == r for r, c in zip(ref_times, cand_times)),
    }
