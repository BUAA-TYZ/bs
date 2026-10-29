import json
from pathlib import Path
import struct
import tempfile
import unittest

import numpy as np

from analysis.calibrate_jetson_sar import (
    HEADER, RECORD, ROI_HEADER, encode_previews, load_workloads, make_candidates, polygon_nms, preview_geometry,
)


class JetsonSARCalibrationTests(unittest.TestCase):
    def test_controlled_rotated_nms_and_preview_byte_bill(self):
        candidates = make_candidates(4096, 100, 2, 800, 128, 30)
        kept = polygon_nms(candidates)
        self.assertEqual(len(kept), 2)
        payload, rois = encode_previews(np.full((4096, 4096), 127, dtype=np.uint8), kept, 64)
        self.assertEqual(len(payload), 524436)
        self.assertEqual(HEADER.unpack_from(payload), (b"SRP1", 0, 0, 0, 4096, 4096, 2))
        offset = HEADER.size
        for detection, roi in zip(kept, rois):
            offset += RECORD.size
            x0, y0, x1, y1, length = ROI_HEADER.unpack_from(payload, offset)
            offset += ROI_HEADER.size
            self.assertEqual(length, 512 * 512)
            self.assertLessEqual(x0, min(detection.xy[::2]) - 64)
            self.assertLessEqual(y0, min(detection.xy[1::2]) - 64)
            self.assertGreaterEqual(x1, max(detection.xy[::2]) + 64)
            self.assertGreaterEqual(y1, max(detection.xy[1::2]) + 64)
            preview = np.frombuffer(payload, dtype=np.uint8, count=length, offset=offset).reshape(512, 512)
            resized, padding = preview_geometry(x1 - x0, y1 - y0)
            self.assertEqual(list(resized), roi["resized_wh"])
            self.assertEqual(list(padding), roi["padding_left_top"])
            self.assertEqual(preview[256, 256], 127)
            self.assertEqual(preview[0, 0], 0)
            offset += length
        self.assertEqual(offset, len(payload))

    def test_empty_window_still_has_completion_header(self):
        self.assertEqual(make_candidates(4096, 0, 0, 800, 128, 30), [])
        payload, rois = encode_previews(np.zeros((32, 32), dtype=np.uint8), [], 64)
        self.assertEqual(len(payload), 28)
        self.assertEqual(rois, [])
        self.assertEqual(struct.unpack("<4s6I", payload)[-1], 0)

    def test_preview_contains_entire_long_ship_crop(self):
        kept = polygon_nms(make_candidates(4096, 100, 2, 1600, 246, 30))
        payload, rois = encode_previews(np.zeros((4096, 4096), dtype=np.uint8), kept, 64)
        self.assertEqual(len(payload), 524436)
        self.assertTrue(all(roi["source_box_xyxy"][2] - roi["source_box_xyxy"][0] > 512 for roi in rois))
        self.assertTrue(all(max(roi["resized_wh"]) == 512 for roi in rois))

    def test_fixture_controls_target_count_origin_and_full_roi(self):
        source = {"windows": [{"window_id": 7, "x0": 2304, "y0": 4608, "records": [{
            "object_id": 42, "class_id": 1, "score": 0.9,
            "vertices": [[100, 100], [300, 100], [300, 200], [100, 200]],
            "roi_xyxy": [36, 36, 364, 264]}]}]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.json"
            path.write_text(json.dumps(source))
            workload = load_workloads(path, 4096, 100)[0]
        kept = polygon_nms(workload["candidates"])
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0].object_id, 42)
        payload, rois = encode_previews(np.zeros((4096, 4096), dtype=np.uint8), kept, 999,
                                       window_id=workload["window_id"], origin=workload["origin"],
                                       roi_by_object=workload["rois"])
        self.assertEqual(HEADER.unpack_from(payload), (b"SRP1", 7, 2304, 4608, 4096, 4096, 1))
        self.assertEqual(len(payload), 262232)
        self.assertEqual(rois[0]["source_box_xyxy"], [36, 36, 364, 264])


if __name__ == "__main__":
    unittest.main()
