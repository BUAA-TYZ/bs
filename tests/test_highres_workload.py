from dataclasses import asdict, replace
import json
import math
import unittest

from sim.workload import ImageTiling


NATIVE_COMPONENTS = [
    {"name": "pan", "downsample": 1, "bands": 1, "bits_per_sample": 12},
    {"name": "ms", "downsample": 4, "bands": 6, "bits_per_sample": 12},
]


def highres(**changes):
    values = dict(width_px=46667, height_px=66667, bands=3, bits_per_sample=8,
                  window_px=1024, stride_px=824, windows_per_tile_axis=2,
                  compute_cost_per_window=0.07, result_size_mb_per_window=0.0002,
                  transport_components=NATIVE_COMPONENTS)
    values.update(changes)
    return ImageTiling.from_dict(values)


class HighResolutionWorkloadTests(unittest.TestCase):
    def test_native_scene_is_gb_scale_with_separate_inference_input(self):
        workload = highres()
        pan_bytes = math.ceil(46667 * 66667 * 12 / 8)
        ms_bytes = math.ceil(11667 * 16667 * 6 * 12 / 8)
        self.assertAlmostEqual(workload.image_size_mb, (pan_bytes + ms_bytes) / 1e6)
        self.assertTrue(6400 < workload.image_size_mb < 6430)
        self.assertEqual(workload.window_size_mb, 1024 * 1024 * 3 / 1e6)
        first = workload.tiles()[0]
        expected_window_bytes = 1024 * 1024 * 12 // 8 + 256 * 256 * 6 * 12 // 8
        self.assertAlmostEqual(first.data_size_mb, 4 * expected_window_bytes / 1e6)
        self.assertEqual(first.compute_cost, 4 * 0.07)

    def test_nondivisible_edges_include_all_native_pixels_and_packed_bits(self):
        workload = highres(width_px=10, height_px=9, window_px=4, stride_px=3,
                           windows_per_tile_axis=1)
        manifest = workload.manifest()
        self.assertAlmostEqual(workload.image_size_mb, (135 + 81) / 1e6)
        sizes = {tile.windows_xyxy[0]: round(tile.data_size_mb * 1e6) for tile in workload.tiles()}
        self.assertEqual(sizes[(0, 0, 4, 4)], 24 + 9)
        self.assertEqual(sizes[(3, 3, 7, 7)], 24 + 36)
        self.assertEqual(sizes[(6, 5, 10, 9)], 24 + 36)
        self.assertAlmostEqual(manifest["packet_input_mb"], 441 / 1e6)
        self.assertGreater(manifest["packet_input_mb"], manifest["image_size_mb"])
        self.assertEqual(manifest["transport"]["component_rasters"][1]["width_px"], 3)
        tiny = highres(width_px=3, height_px=3, window_px=3, stride_px=3)
        self.assertAlmostEqual(tiny.image_size_mb, (14 + 9) / 1e6)

    def test_compression_affects_only_transport_and_manifest_is_explicit(self):
        raw = highres(width_px=1031, height_px=1027, window_px=512, stride_px=412)
        compressed = replace(raw, transport_compression_ratio=3)
        self.assertAlmostEqual(raw.image_size_mb / 3, compressed.image_size_mb)
        self.assertEqual(raw.window_size_mb, compressed.window_size_mb)
        for a, b in zip(raw.tiles(), compressed.tiles()):
            self.assertAlmostEqual(a.data_size_mb / 3, b.data_size_mb)
            self.assertEqual((a.windows_xyxy, a.compute_cost, a.result_size_mb),
                             (b.windows_xyxy, b.compute_cost, b.result_size_mb))
        manifest = compressed.manifest()
        self.assertEqual(manifest["transport"]["format"], "native_components_packed_bitstream")
        self.assertIn("not measured", manifest["transport"]["compression_assumption"])
        self.assertIn("proxy", manifest["transport"]["compute_rule"])
        self.assertAlmostEqual(manifest["transport"]["unique_uncompressed_mb"], raw.image_size_mb)
        self.assertEqual(ImageTiling.from_dict(json.loads(json.dumps(asdict(compressed)))), compressed)

    def test_legacy_geometry_bytes_and_manifest_remain_unchanged(self):
        legacy = ImageTiling(4000, 4000, 3, 8, 1024, 824, 2, 1.0, 0.04)
        manifest = legacy.manifest()
        self.assertEqual(legacy.image_size_mb, 48)
        self.assertEqual(legacy.window_size_mb, 3.145728)
        self.assertAlmostEqual(manifest["packet_input_mb"], 78.6432)
        self.assertEqual(manifest["model"], "overlapping_window_packets_v1")
        self.assertNotIn("transport", manifest)
        self.assertNotIn("transport_components", manifest["parameters"])
        self.assertNotIn("transport_compression_ratio", manifest["parameters"])
        self.assertEqual(ImageTiling.from_dict(manifest["parameters"]), legacy)

    def test_packing_group_does_not_change_transport_or_processing(self):
        workload = highres(width_px=31, height_px=21, window_px=8, stride_px=5)
        totals = []
        for group in (1, 2, 4):
            tiles = replace(workload, windows_per_tile_axis=group).tiles()
            totals.append(tuple(math.fsum(getattr(t, key) for t in tiles)
                                for key in ("data_size_mb", "compute_cost", "result_size_mb")))
        for actual in totals[1:]:
            for a, b in zip(totals[0], actual):
                self.assertAlmostEqual(a, b)

    def test_invalid_transport_parameters_are_rejected(self):
        for ratio in (0, -1, float("nan"), float("inf"), True, "3"):
            with self.subTest(ratio=ratio), self.assertRaises(ValueError):
                highres(transport_compression_ratio=ratio)
        invalid = [[], {}, [NATIVE_COMPONENTS[0], NATIVE_COMPONENTS[0]], [None],
                   [{**NATIVE_COMPONENTS[0], "downsample": 0}],
                   [{**NATIVE_COMPONENTS[0], "downsample": 1.5}],
                   [{**NATIVE_COMPONENTS[0], "bands": True}],
                   [{**NATIVE_COMPONENTS[0], "bits_per_sample": 0}]]
        for components in invalid:
            with self.subTest(components=components), self.assertRaises(ValueError):
                highres(transport_components=components)


if __name__ == "__main__":
    unittest.main()
