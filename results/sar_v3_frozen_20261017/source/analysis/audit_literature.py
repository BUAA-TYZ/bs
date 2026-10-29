"""重放文献比较的反向排名案例，记录同状态分歧、报价和生命周期。"""

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
import json
import multiprocessing
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import audit_policy_behavior as behavior
import sim.scheduler.literature as literature
from sim.config import load_config
from sim.marl.scenarios import ScenarioDistribution
from sim.scheduler.reservation import ForecastReservationPolicy


FOCUS = {3: {"task_28", "task_24", "task_12", "task_45"},
         8: {"task_19", "task_23", "task_31", "task_33", "task_58", "task_51"}}


class CalendarProbe(ForecastReservationPolicy):
    def __init__(self, focus):
        super().__init__()
        self.focus, self.quotes = focus, []

    def plan(self, ledger, tiles):
        result = super().plan(ledger, tiles)
        if any(t["parent_task_id"] in self.focus for t in tiles.values()):
            self.quotes.append({"tiles": list(tiles), "choices": result[1] if result else None})
        return result


class CGSProbe(literature.CGSComputeAdapted):
    def __init__(self, focus, future_ground=False):
        super().__init__(future_ground=future_ground)
        self.focus, self.quotes = focus, []

    def select_actions(self, state):
        original = literature.choose_opportunity
        pending = iter((tid, tile) for tid, tile in sorted(
            state.tiles.items(), key=lambda x: (-x[1]["waiting_time"], x[0]))
            if not tile["in_transfer"] and tile["state"] != "READY" and not tile["task_progress"]["failed"])

        def observed(opportunities, plan, destinations, size, **kwargs):
            tid, tile = next(pending)
            choice = original(opportunities, plan, destinations, size, **kwargs)
            if tile["parent_task_id"] in self.focus:
                candidates = []
                for ready, processor, _ in opportunities:
                    route = plan.delivery(processor, destinations, ready, size, **kwargs)
                    candidates.append({"processor": processor, "compute_ready": ready,
                                       "delivery": route[0] if route else None})
                self.quotes.append({"tile": tid, "candidates": candidates,
                                    "choice": choice[:2] if choice else None,
                                    "path": [asdict(plan.contacts[i]) for i in choice[2]] if choice else None})
            return choice

        literature.choose_opportunity = observed
        try:
            return super().select_actions(state)
        finally:
            literature.choose_opportunity = original


class ComparedPolicy:
    def __init__(self, actual, focus, stream):
        self.name, self.focus, self.stream = actual, focus, stream
        self.policies = {"reservation_calendar": CalendarProbe(focus), "cgs_compute_adapted": CGSProbe(focus)}
        if actual == "cgs_ground_wait":
            self.policies = {"cgs_compute_adapted": CGSProbe(focus), "cgs_ground_wait": CGSProbe(focus, True)}
        self.counts, self.transitions, self.last = Counter(), Counter(), {}

    def select_actions(self, state):
        outputs = {}
        for name, policy in self.policies.items():
            policy.quotes.clear()
            outputs[name] = policy.select_actions(state)
        maps = {name: {a.tile_id: behavior.signature(a) for a in actions} for name, actions in outputs.items()}
        left, right = maps.values()
        for tid in left.keys() | right.keys():
            self.counts["compared"] += 1
            if left.get(tid) != right.get(tid):
                self.counts["different"] += 1
                self.transitions[str((left.get(tid), right.get(tid)))] += 1
        focused = {tid: tile for tid, tile in state.tiles.items() if tile["parent_task_id"] in self.focus}
        changed = [tid for tid in focused if self.last.get(tid) != (left.get(tid), right.get(tid))]
        if changed:
            self.stream.write(json.dumps({"step": state.time, "tiles": focused,
                "actions": {name: {tid: action for tid, action in mapping.items() if tid in focused}
                            for name, mapping in maps.items()}, "satellites": state.satellites,
                "quotes": {name: policy.quotes for name, policy in self.policies.items()}}, allow_nan=False) + "\n")
        self.last.update({tid: (left.get(tid), right.get(tid)) for tid in focused})
        return outputs[self.name]


def run(episode, policy_name, output):
    cfg, case = ScenarioDistribution("examples/scenario_roles.json", load_config("examples/config_relay.yaml")).training_case(2026101400, episode)
    original = behavior.make_policy
    output = Path(output)
    observed = None
    with (output / f"{case['id']}_{policy_name}_decisions.jsonl").open("x") as stream:
        def factory(name, *args, **kwargs):
            nonlocal observed
            if name == policy_name:
                observed = ComparedPolicy(name, FOCUS[episode], stream)
                return observed
            return original(name, *args, **kwargs)

        behavior.make_policy = factory
        try:
            row = behavior.audit_run(cfg, policy_name, "", output / f"{case['id']}_{policy_name}")
        finally:
            behavior.make_policy = original
    folder = "ground_opportunity_probe_20261014" if policy_name == "cgs_ground_wait" else "literature_fresh_20261014"
    reference = json.loads(Path(f"results/{folder}/{case['id']}_{policy_name}.json").read_text())
    if row["metrics"]["task_outcomes"] != reference["metrics"]["task_outcomes"]:
        raise ValueError("Audited outcomes differ from the archived comparison")
    return {"case": case, "policy": policy_name, "archived_outcomes_reproduced": True,
            "same_state_counts": dict(observed.counts), "same_state_transitions": dict(observed.transitions),
            "lifecycle": {k: row[k] for k in ("event_counts", "computed_unique_tiles")}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--extension-only", action="store_true")
    args = parser.parse_args()
    output = Path(args.out_dir)
    output.mkdir(parents=True, exist_ok=False)
    rows = []
    with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context("spawn")) as pool:
        methods = ("cgs_ground_wait",) if args.extension_only else ("reservation_calendar", "cgs_compute_adapted")
        jobs = [pool.submit(run, episode, policy, str(output)) for episode in FOCUS for policy in methods]
        for job in as_completed(jobs):
            row = job.result()
            rows.append(row)
            print(json.dumps(row), flush=True)
    (output / "summary.json").write_text(json.dumps(rows, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
