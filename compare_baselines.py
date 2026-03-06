from __future__ import annotations

import argparse
from typing import Dict

from main import run_once


def format_summary(name: str, summary: Dict) -> str:
    overall = summary["overall"]
    latency = summary["latency"]
    failures = summary["failures"]
    return (
        f"{name}: tiles {overall['completed_tiles']}/{overall['total_tiles']}, "
        f"tasks {overall['completed_tasks']}/{overall['total_tasks']}, "
        f"tile_mean {latency['tile_mean']:.2f}, tile_p95 {latency['tile_p95']:.2f}, "
        f"failures {sum(failures.values())}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    greedy = run_once(args.config, "greedy")
    load_aware = run_once(args.config, "load_aware")

    print(format_summary("Greedy", greedy))
    print(format_summary("LoadAware", load_aware))


if __name__ == "__main__":
    main()
