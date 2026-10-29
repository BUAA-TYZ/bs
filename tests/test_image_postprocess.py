import unittest

from workload_benchmark.postprocess import (
    Detection, HEADER, RECORD, deserialize_detections, label_diagnostic,
    polygon_iou, polygon_nms, restore_detection, serialize_detections, validate_detection,
)


def rectangle(window=0, item=0, cls=0, score=0.8, x=0):
    return Detection(window, item, cls, score, (x, 0, x + 10, 0, x + 10, 10, x, 10))


class ImagePostprocessTests(unittest.TestCase):
    def test_coordinate_restore_and_binary_roundtrip(self):
        detection = restore_detection(3, 0, 2, .75, rectangle().xy, (824, 56))
        payload = serialize_detections([detection], 3, (824, 56, 1848, 1080))
        self.assertEqual(len(payload), 28 + 40)
        box, decoded = deserialize_detections(payload)
        self.assertEqual(box, (824, 56, 1848, 1080))
        self.assertEqual(decoded, [detection])

    def test_empty_packet_is_explicit_completion(self):
        payload = serialize_detections([], 7, (0, 0, 1024, 1024))
        self.assertEqual(len(payload), HEADER.size)
        self.assertEqual(RECORD.size, 40)
        self.assertEqual(deserialize_detections(payload)[1], [])

    def test_nms_class_awareness_and_stable_ties(self):
        detections = [rectangle(2), rectangle(1), rectangle(0, cls=1)]
        forward = polygon_nms(detections)
        backward = polygon_nms(list(reversed(detections)))
        self.assertEqual(forward, backward)
        self.assertEqual([(d.window_id, d.class_id) for d in forward], [(0, 1), (1, 0)])

    def test_rotated_polygon_iou_not_axis_aligned_surrogate(self):
        diamond = Detection(0, 0, 0, .8, (5, 0, 10, 5, 5, 10, 0, 5))
        iou = polygon_iou(validate_detection(diamond), validate_detection(rectangle()))
        self.assertAlmostEqual(iou, 0.5)
        self.assertEqual(len(polygon_nms([diamond, rectangle(1)], .6)), 2)
        self.assertEqual(len(polygon_nms([diamond, rectangle(1)], .4)), 1)

    def test_single_parent_types_deduplicate_across_windows(self):
        detections = [rectangle(0, cls=1, score=.7), rectangle(1, cls=2, score=.9),
                      rectangle(2, cls=1, score=.8, x=50)]
        result = polygon_nms(detections, .5, class_agnostic=True)
        self.assertEqual(result, [detections[1], detections[2]])
        self.assertEqual(result, polygon_nms(list(reversed(detections)), .5,
                                             class_agnostic=True))
        self.assertEqual(len(polygon_nms(detections, .5)), 3)

    def test_cross_type_ties_ignore_arrival_order(self):
        detections = [rectangle(2, cls=1), rectangle(1, cls=2)]
        self.assertEqual(polygon_nms(detections, .5, class_agnostic=True),
                         polygon_nms(list(reversed(detections)), .5,
                                     class_agnostic=True))

    def test_invalid_polygon_and_framing_fail(self):
        with self.assertRaises(ValueError):
            polygon_nms([Detection(0, 0, 0, .9, (0, 0, 10, 10, 10, 0, 0, 10))])
        with self.assertRaises(ValueError):
            deserialize_detections(b"OBB1")
        with self.assertRaises(ValueError):
            polygon_nms([rectangle(), rectangle()])

    def test_quality_matching_counts_duplicate_predictions_as_false_positive(self):
        values = label_diagnostic([rectangle(), rectangle(1), rectangle(2, cls=1)], [rectangle()])
        self.assertEqual((values["tp"], values["fp"], values["fn"]), (1, 2, 0))
        self.assertIsNone(values["mAP"])

    def test_difficult_labels_ignore_matching_predictions(self):
        values = label_diagnostic([rectangle(), rectangle(1, x=50)], [rectangle()],
                                  ignored_labels=[rectangle(x=50)])
        self.assertEqual((values["tp"], values["fp"], values["fn"]), (1, 0, 0))
        self.assertEqual(values["ignored_predictions"], 1)


if __name__ == "__main__":
    unittest.main()
