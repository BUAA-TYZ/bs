"""仿真工作台：字段校验、观测器只读性、worker 与 CLI 一致性、HTTP 接口。"""

import contextlib
import io
import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path

import yaml

import main
from sim.config import config_from_dict, load_config
from workbench import ROOT, catalog
from workbench.observer import RunCancelled, SnapshotObserver
from workbench.server import FINAL_STATES, RunManager, make_server
from workbench.worker import build_czml, read_json, run as worker_run

SMOKE = ROOT / "examples" / "config_smoke.yaml"
CORE_KEYS = ("overall", "latency", "failures", "resource", "network", "task_outcomes")


def smoke(**overrides):
    raw = yaml.safe_load(SMOKE.read_text(encoding="utf-8"))
    raw.update(overrides)
    return raw


def run_cli(path, policy):
    """main.run_once 即 CLI 的执行流程；按 --output 的 JSON 序列化后比较。"""
    with contextlib.redirect_stderr(io.StringIO()):
        return json.loads(json.dumps(main.run_once(str(path), policy)))


def error_fields(result):
    return {e["field"] for e in result["errors"]}


class CatalogTest(unittest.TestCase):
    def test_policy_names_follow_main(self):
        self.assertEqual([p["name"] for p in catalog.POLICIES], list(main.POLICY_NAMES + main.GROUND_MODES))

    def test_smoke_config_is_valid(self):
        result = catalog.validate(smoke(), "window_aware")
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(result["config"]["pipeline_mode"], "distributed")
        self.assertEqual(result["config"]["tile_lifecycle_log"], "")

    def test_errors_are_located_on_fields(self):
        cases = [
            ({"seed": "x"}, "seed"),
            ({"dt": 0}, "dt"),
            ({"sim_steps": 2.5}, "sim_steps"),
            ({"num_sats": 10_000}, "num_sats"),
            ({"result_relay": True}, "result_relay"),
            ({"vram_policy": "drop"}, "vram_policy"),
            ({"ground_stations": []}, "ground_stations"),
        ]
        for overrides, field in cases:
            with self.subTest(overrides=overrides):
                result = catalog.validate(smoke(**overrides), "greedy")
                self.assertFalse(result["ok"])
                self.assertIn(field, error_fields(result))
        bad_gs = smoke()
        bad_gs["ground_stations"][1]["bandwidth_mbps"] = "fast"
        self.assertIn("ground_stations.1.bandwidth_mbps", error_fields(catalog.validate(bad_gs, "greedy")))
        missing_tle = smoke()
        missing_tle["topology"]["tle_file"] = "data/tle/does_not_exist.tle"
        self.assertIn("topology.tle_file", error_fields(catalog.validate(missing_tle, "greedy")))

    def test_unknown_policy_and_marl_checkpoint(self):
        self.assertIn("policy", error_fields(catalog.validate(smoke(), "nope")))
        result = catalog.validate(smoke(), "marl", "")
        self.assertFalse(result["ok"])
        self.assertIn("checkpoint", error_fields(result))
        missing = catalog.validate(smoke(), "marl", "checkpoints/none/best.pt")
        self.assertIn("checkpoint", error_fields(missing))
        outside = catalog.inspect_checkpoint("/etc/hosts")
        self.assertFalse(outside["ok"])

    def test_ground_mode_sets_pipeline_mode(self):
        result = catalog.validate(smoke(), "ground_compute")
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(result["config"]["pipeline_mode"], "ground_compute")
        self.assertTrue(any("地面执行模式" in w for w in result["warnings"]))

    def test_normalized_config_ignores_mode_and_expands_defaults(self):
        explicit = smoke(pipeline_mode="ground_compute", relay_max_hops=8)
        explicit["topology"]["max_neighbors"] = 0
        self.assertEqual(catalog.normalized_config(smoke()), catalog.normalized_config(explicit))
        self.assertNotEqual(catalog.normalized_config(smoke()), catalog.normalized_config(smoke(seed=8)))
        flat = catalog.flatten(catalog.normalized_config(smoke()))
        self.assertEqual(flat["ground_stations.0.id"], "gs_beijing")
        self.assertIn("topology.max_range_km", flat)


class ObserverTest(unittest.TestCase):
    def setUp(self):
        self.cfg = replace(config_from_dict(smoke()), sim_steps=30)

    def test_observer_is_read_only_and_bounded(self):
        baseline = main.run_config(self.cfg, "greedy")
        observer = SnapshotObserver(max_snapshots=7)
        observed = main.run_config(self.cfg, "greedy", observer=observer)
        for key in CORE_KEYS:
            self.assertEqual(observed[key], baseline[key], key)
        export = observer.export(complete=True)
        self.assertEqual(export["stride"], 5)
        self.assertLessEqual(len(export["snapshots"]), 7)
        self.assertEqual(export["snapshots"][-1]["step"], self.cfg.sim_steps - 1)
        spans = [s["span"] for s in export["snapshots"]]
        self.assertEqual(spans[0][0], 0)
        for prev, cur in zip(spans, spans[1:]):
            self.assertEqual(cur[0], prev[1] + 1)
        last = export["snapshots"][-1]
        self.assertEqual(len(last["sats"]), self.cfg.num_sats)
        self.assertEqual(last["counters"]["completed_tasks"], observed["overall"]["completed_tasks"])
        tiles = observer.tile_table()
        self.assertEqual(tiles["time_unit"], "step")
        self.assertEqual(len(tiles["rows"]), observed["overall"]["total_tiles"])
        done = sum(1 for row in tiles["rows"] if row[2] == "DONE")
        self.assertEqual(done, observed["overall"]["completed_tiles"])

    def test_flow_rows_truncated_but_groups_complete(self):
        full = SnapshotObserver(max_snapshots=7, max_flow_rows=10**6)
        main.run_config(self.cfg, "greedy", observer=full)
        capped = SnapshotObserver(max_snapshots=7, max_flow_rows=2)
        main.run_config(self.cfg, "greedy", observer=capped)
        truncated = 0
        for a, b in zip(full.export(True)["snapshots"], capped.export(True)["snapshots"]):
            self.assertEqual(a["flows_omitted"], 0)
            self.assertEqual(b["flow_groups"], a["flow_groups"])
            self.assertEqual(len(b["flows"]) + b["flows_omitted"], len(a["flows"]))
            self.assertLessEqual(len(b["flows"]), 2)
            truncated += b["flows_omitted"] > 0
            groups = {}
            for kind, _, src, dst, sent, _, status in a["flows"]:
                g = groups.setdefault((kind, src, dst), [0, 0, 0.0, 0, 0])
                g[0] += 1
                g[1] += sent is None or sent > 1e-9
                g[2] += sent or 0.0
                g[3] += status == "done"
                g[4] += status == "aborted"
            for kind, src, dst, n, moving, sent, done, aborted in a["flow_groups"]:
                g = groups.pop((kind, src, dst))
                self.assertEqual([n, moving, done, aborted], [g[0], g[1], g[3], g[4]])
                self.assertAlmostEqual(sent, g[2], places=2)
            self.assertFalse(groups)
        self.assertGreater(truncated, 0)

    def test_cancel_stops_at_step_boundary(self):
        calls = {"n": 0}

        def cancelled():
            calls["n"] += 1
            return calls["n"] > 5

        observer = SnapshotObserver(cancelled=cancelled)
        with self.assertRaises(RunCancelled):
            main.run_config(self.cfg, "local", observer=observer)
        self.assertEqual(observer.last_step, 4)  # on_start 检查 1 次，之后每步 1 次

    def test_cli_ground_mode_matches_evaluation_semantics(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cfg.yaml"
            path.write_text(yaml.safe_dump(smoke(sim_steps=10)), encoding="utf-8")
            summary = run_cli(path, "ground_compute")
        self.assertEqual(summary["experiment"]["config"]["pipeline_mode"], "ground_compute")


class WorkerTest(unittest.TestCase):
    def test_worker_matches_cli(self):
        result = catalog.validate(smoke(sim_steps=40), "window_aware")
        self.assertTrue(result["ok"], result["errors"])
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "request.json").write_text(json.dumps({"policy": "window_aware", "checkpoint": "",
                                                              "config": result["config"]}), encoding="utf-8")
            (run_dir / "config.yaml").write_text(yaml.safe_dump(result["config"]), encoding="utf-8")
            (run_dir / "status.json").write_text("{}", encoding="utf-8")
            self.assertEqual(worker_run(run_dir), 0)
            status = read_json(run_dir / "status.json")
            self.assertEqual(status["state"], "completed", status.get("error"))
            web = read_json(run_dir / "result.json")
            cli = run_cli(run_dir / "config.yaml", "window_aware")
            snapshots = read_json(run_dir / "snapshots.json")
            czml = read_json(run_dir / "czml.json")
            events = (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
        for key in CORE_KEYS:
            self.assertEqual(web[key], cli[key], key)
        self.assertEqual(web["experiment"]["task_trace_sha256"], cli["experiment"]["task_trace_sha256"])
        self.assertTrue(snapshots["complete"])
        self.assertEqual(snapshots["dt"], 5.0)
        self.assertTrue(events)
        first = json.loads(events[0])
        self.assertAlmostEqual(first["time_seconds"], first["time"] * 5.0)
        sat_ids = sorted(p["id"] for p in czml if p["id"].startswith("sat-"))
        self.assertEqual(len(sat_ids), 8)

    def test_czml_uses_simulation_satellites_and_start_time(self):
        cfg = load_config(str(SMOKE))
        packets = build_czml(smoke(), cfg.num_sats, cfg.sim_steps, cfg.dt)
        doc = packets[0]
        self.assertEqual(doc["clock"]["interval"], "2026-03-20T17:30:00Z/2026-03-20T17:36:40Z")
        sats = [p for p in packets if p["id"].startswith("sat-")]
        self.assertEqual([p["properties"]["sat_index"] for p in sats], list(range(cfg.num_sats)))
        self.assertEqual({p["id"] for p in packets if p["id"].startswith("gs-")}, {"gs-gs_beijing", "gs-gs_urumqi"})


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.server = make_server("127.0.0.1", 0, Path(cls.tmp.name))
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.tmp.cleanup()

    def request(self, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method="POST" if body is not None else "GET",
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                kind = resp.headers.get("Content-Type", "")
                return resp.status, json.loads(raw) if kind.startswith("application/json") else raw
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def wait(self, run_id, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            _, detail = self.request(f"/api/runs/{run_id}")
            if detail["status"].get("state") in FINAL_STATES and not detail["active"]:
                return detail
            time.sleep(0.2)
        self.fail(f"run {run_id} did not finish")

    def test_meta_static_and_errors(self):
        status, meta = self.request("/api/meta")
        self.assertEqual(status, 200)
        self.assertIn("examples/config_smoke.yaml", [c["path"] for c in meta["configs"]])
        self.assertEqual(set(meta["cesium"]), {"base_url", "ion_token"})
        status, html = self.request("/")
        self.assertEqual(status, 200)
        self.assertIn(b"<html", html)
        self.assertEqual(self.request("/../main.py")[0], 404)
        self.assertEqual(self.request("/api/runs/does-not-exist")[0], 404)
        self.assertEqual(self.request("/api/config?path=../../etc/passwd")[0], 404)
        status, body = self.request("/api/validate", {"config": smoke(dt=-1), "policy": "greedy"})
        self.assertEqual(status, 200)
        self.assertFalse(body["ok"])
        self.assertIn("dt", error_fields(body))
        status, body = self.request("/api/runs", {"config": smoke(dt=-1), "policy": "greedy"})
        self.assertEqual(status, 400)
        self.assertIn("dt", error_fields(body))
        self.assertEqual(self.request("/api/compare?ids=a")[0], 400)

    def test_run_lifecycle_compare_and_downloads(self):
        body = {"config": smoke(sim_steps=30), "policy": "greedy", "base_config": "examples/config_smoke.yaml"}
        status, created = self.request("/api/runs", body)
        self.assertEqual(status, 201)
        run_id = created["id"]
        self.assertIn("--policy greedy", created["request"]["cli_command"])
        detail = self.wait(run_id)
        self.assertEqual(detail["status"]["state"], "completed", detail["status"].get("error"))
        self.assertEqual(self.request(f"/api/runs/{run_id}/cancel", {})[0], 409)
        _, result = self.request(f"/api/runs/{run_id}/result")
        self.assertEqual(result["experiment"]["policy"], "greedy")
        _, snaps = self.request(f"/api/runs/{run_id}/snapshots")
        self.assertEqual(snaps["sim_steps"], 30)
        task_id = result["task_outcomes"][0]["task_id"]
        _, events = self.request(f"/api/runs/{run_id}/events?task_id={task_id}")
        self.assertEqual(events["time_unit"], "step")
        self.assertTrue(all(e["task_id"] == task_id for e in events["events"]))
        status, csv_bytes = self.request(f"/api/runs/{run_id}/download/tasks.csv")
        self.assertEqual(status, 200)
        self.assertEqual(len(csv_bytes.decode("utf-8-sig").strip().splitlines()), len(result["task_outcomes"]) + 1)
        status, bundle = self.request(f"/api/runs/{run_id}/download/bundle.zip")
        self.assertEqual(status, 200)
        self.assertEqual(bundle[:2], b"PK")
        self.assertEqual(self.request(f"/api/runs/{run_id}/download/../../main.py")[0], 404)

        status, other = self.request("/api/runs", {**body, "policy": "local"})
        self.assertEqual(status, 201)
        self.wait(other["id"])
        _, cmp = self.request(f"/api/compare?ids={run_id},{other['id']}")
        self.assertTrue(cmp["comparable"], cmp)
        self.assertEqual(cmp["config_diffs"], [])
        self.assertEqual(len(cmp["paired"]), 1)
        self.assertEqual({r["policy"] for r in cmp["runs"]}, {"greedy", "local"})

        status, seed_run = self.request("/api/runs", {**body, "config": smoke(sim_steps=30, seed=8)})
        self.wait(seed_run["id"])
        _, cmp = self.request(f"/api/compare?ids={run_id},{seed_run['id']}")
        self.assertFalse(cmp["comparable"])
        self.assertIn("seed", [d["key"] for d in cmp["config_diffs"]])

        _, listing = self.request("/api/runs")
        hashes = {r["id"]: r["config_hash"] for r in listing["runs"]}
        self.assertEqual(hashes[run_id], hashes[other["id"]])
        self.assertNotEqual(hashes[run_id], hashes[seed_run["id"]])

    def test_single_active_run_cancel_and_rerun(self):
        body = {"config": smoke(sim_steps=5000), "policy": "local"}
        status, created = self.request("/api/runs", body)
        self.assertEqual(status, 201)
        run_id = created["id"]
        self.assertEqual(self.request("/api/runs", body)[0], 409)
        self.assertEqual(self.request(f"/api/runs/{run_id}/cancel", {})[0], 200)
        detail = self.wait(run_id, timeout=60)
        self.assertEqual(detail["status"]["state"], "cancelled")
        self.assertNotIn("result.json", detail["files"])
        self.assertEqual(self.request(f"/api/runs/{run_id}/result")[0], 404)
        status, again = self.request(f"/api/runs/{run_id}/rerun", {})
        self.assertEqual(status, 201)
        self.assertNotEqual(again["id"], run_id)
        self.assertEqual(again["request"]["rerun_of"], run_id)
        self.request(f"/api/runs/{again['id']}/cancel", {})
        self.assertEqual(self.wait(again["id"], timeout=60)["status"]["state"], "cancelled")

    def test_recover_marks_dead_runs_failed(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "stale"
            run_dir.mkdir()
            (run_dir / "request.json").write_text(json.dumps({"config": smoke(), "policy": "local"}))
            (run_dir / "status.json").write_text(json.dumps({"state": "running", "pid": 999_999_999}))
            manager = RunManager(Path(tmp))
            self.assertIsNone(manager.active())
            self.assertEqual(read_json(run_dir / "status.json")["state"], "failed")


if __name__ == "__main__":
    unittest.main()
