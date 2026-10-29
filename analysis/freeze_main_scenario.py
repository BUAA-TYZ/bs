"""登记主场景和日期；只读取已有校准产物，不生成封存日期的轨道或任务。"""

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil

from analysis import run_load_pressure_six as six
from analysis import run_load_pressure_short as short
from analysis.estimate_load_pressure import OUT, PANEL, sha
from analysis.redesign_scenario import write_json

ROOT = Path("results/redesign_20261021/main_scenario")


def date_inventory():
    evidence, dates = {}, set()
    for base in (Path("results"), Path("docs"), Path("examples")):
        for path in sorted(base.rglob("*")):
            if not path.is_file() or ROOT in path.parents:
                continue
            dates.update(re.findall(r"20\d{2}-\d{2}-\d{2}", str(path)))
            name = path.name.lower()
            metadata = name in {"protocol.json", "manifest.json", "blind_dates.json", "run.json", "selection.json", "registration.json"}
            metadata |= "prereg" in name or ("config" in name and path.stat().st_size < 300000)
            if path.suffix not in (".json", ".md", ".yaml", ".toml") or not metadata or path.stat().st_size > 3000000:
                continue
            found = sorted(set(re.findall(r"20\d{2}-\d{2}-\d{2}", path.read_text(errors="replace"))))
            if found:
                evidence[str(path)] = {"sha256": sha(path), "dates": found}
                dates.update(found)
    return {"scope": "Registration/configuration metadata and filenames only; no sealed outcomes, ephemerides or events read",
            "dates": sorted(dates), "evidence": evidence}


def main():
    if (ROOT/"manifest.json").exists():
        raise FileExistsError("Main scenario already frozen")
    old = six.read(six.ROOT/"protocol.json")
    short.verify(old)
    inventory = date_inventory()
    test = [f"2026-12-{d}" for d in (21, 22, 23)]
    assert not set(test).intersection(inventory["dates"])
    ROOT.mkdir(parents=True, exist_ok=True)
    write_json(ROOT/"date_exclusion_audit.json", inventory)
    original = six.read(PANEL/"protocol.json")["jobs"]["compute_4_local"]["config"]
    config = deepcopy(original)
    config["scheduled_task_arrivals"] = []
    config["task_arrival_end_step"] = round(86400/config["dt"])
    config["sim_steps"] = round(86701/config["dt"])
    config["topology"]["start_time_utc"] = "2026-10-15T00:00:00Z"
    write_json(ROOT/"config_template.json", config)
    registration = six.read(OUT/"maritime_extension/registration.json")
    ids = set(registration["sets"]["maritime80"])
    targets = [r for r in registration["targets"] if r["target_id"] in ids]
    assert len(targets) == 80
    write_json(ROOT/"targets_maritime80.json", targets)
    write_json(ROOT/"arrival_rule.json", {
        "date_parameter": "UTC calendar date; original synthetic TLE elements/epoch stay byte-identical; propagate to requested date, never re-epoch or reset phases",
        "source_ids": list(range(24)), "duration_s": 86400, "geometry_scan_s": 10,
        "max_off_nadir_deg": 40, "incidence_range_deg": [20, 40], "sun_required": False,
        "min_capture_start_spacing_s_per_source": 60, "pass_cooldown_s": 0, "end_window_guard_s": 10,
        "formation_delay_s": 60, "deadline_origin": "formed image ready", "D_seconds": 300,
        "window_order": ["start_s", "source_sat_id", "target_id"],
        "mode_weights_in_MODES_order": [.2, .6, .2], "mode_draw": "random.Random(1701+int(YYYYMMDD)); draw mode before window fit test; draw truth seed only after acceptance",
        "accepted_start": "max(window.start_s, last_accepted_capture_start[source]+60)",
        "acceptance": "capture_end <= window.end_s-10 and capture_end+60 < 86400",
        "dispatch_blocks": 16, "daily_boundary": "Independent daily acquisition schedule, no carry-in before 00:00; same rule for all splits",
        "snippet_selection": "Largest raw-byte arrivals in [start,start+21600); earliest tie; no policy outcomes used",
        "snippet_warmup": "origin=floor((start-600)/60)*60; retain all real arrivals from origin, only main interval scored; 301s followup, no post-main arrivals",
        "limitations": "Earlier backlog not recovered; target locations are coastal monitoring proxies, not guaranteed ship counts or validated spacecraft duty cycle",
        "generator_references": ["analysis/prepare_maritime_extension.py", "analysis/prepare_load_pressure.py", "sim/acquisition.py", "analysis/redesign_scenario.py"],
        "date_adapter_status": "Rule registered; existing source_windows/arrivals hard-code 10-15. Before future data generation parameterize date and verify exact 10-15 reproduction. No future schedules generated this round."
    })
    split = {"registered_at_utc": datetime.now(timezone.utc).isoformat(), "scenario": "M",
             "calibration_dates": ["2026-10-15"],
             "training_dates": [f"2026-10-{d}" for d in range(16, 22)],
             "validation_dates": [f"2026-10-{d}" for d in range(22, 25)],
             "test_dates": test, "test_status": "SEALED_NOT_GENERATED_NOT_RUN",
             "selection": "First Monday-Wednesday of the calendar week following the latest prior sealed test (12-17); date collision metadata audit before registration",
             "window_rule": "Same busiest-raw-byte six-hour rule on each date; do not discard low-load or empty dates; paired methods use identical arrivals",
             "unseal_requires": "Frozen method/model/normalization/hyperparameters/reporting and explicit user authorization",
             "old_dates": "All old consumed/registered tests remain excluded; prior learning validation dates may be reused only as new-scenario validation, never called unseen test",
             "orbit_scope": "Synthetic orbit phase holdout, not a claim of real TLE prediction accuracy across months"}
    write_json(ROOT/"date_split.json", split)
    # 完整保留已使用的六小时配置；模板仅用于未来按登记日期派生。
    write_json(ROOT/"calibration_six_config.json", old["config"])
    evidence_paths = [six.ROOT/"protocol.json", six.ROOT/"summary.json", six.ROOT/"FINAL_AUDIT.json",
                      OUT/"hour_3600s/protocol.json", OUT/"hour_3600s/summary.json", OUT/"hour_3600s/FINAL_AUDIT.json",
                      OUT/"maritime_extension/registration.json", OUT/"maritime_extension/maritime80_arrivals.json"]
    sources = [Path(__file__), Path("analysis/prepare_maritime_extension.py"), Path("analysis/prepare_load_pressure.py"),
               Path("sim/acquisition.py"), Path("analysis/redesign_scenario.py")]
    snapshot = ROOT/"source"
    for path in sources:
        dst = snapshot/path.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, dst)
    own = [f for f in ROOT.rglob('*') if f.is_file() and f.name != 'manifest.json']
    write_json(ROOT/"manifest.json", {"scenario": "M", "frozen_at_utc": datetime.now(timezone.utc).isoformat(),
               "description": "maritime80 / 24 SAR / 144 compute / 1 station 400 Mbps / D300 / satellite and ground service x4",
               "files": {str(p): sha(p) for p in own}, "evidence_hashes": {str(p): sha(p) for p in evidence_paths},
               "source_hashes": old["frozen_protocol"]["source_hashes"], "source_entry_hashes": {str(p): sha(p) for p in sources},
               "no_new_date_generated": True, "calibration_kept_in_place": True})
    print(json.dumps({"main_scenario": str(ROOT), "manifest_sha256": sha(ROOT/"manifest.json"),
                      "date_split_sha256": sha(ROOT/"date_split.json"), "test_dates": test}))


if __name__ == "__main__":
    main()
