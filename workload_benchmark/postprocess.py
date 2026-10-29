"""以 Shapely/GEOS 计算旋转框 IoU，按固定全局顺序做逐类 NMS。"""

from dataclasses import dataclass
import math
import struct

from shapely.geometry import Polygon


HEADER = struct.Struct("<4s6I")
RECORD = struct.Struct("<9fI")


@dataclass(frozen=True)
class Detection:
    window_id: int
    detection_id: int
    class_id: int
    score: float
    xy: tuple[float, ...]


def restore_detection(window_id, detection_id, class_id, score, xy, origin):
    values = tuple(float(v) + origin[i % 2] for i, v in enumerate(xy))
    detection = Detection(window_id, detection_id, int(class_id), float(score), values)
    validate_detection(detection)
    return detection


def validate_detection(detection):
    if len(detection.xy) != 8 or not all(math.isfinite(v) for v in detection.xy):
        raise ValueError("Expected four finite polygon vertices")
    if not 0 <= detection.score <= 1 or detection.class_id < 0:
        raise ValueError("Invalid confidence or class")
    polygon = Polygon(list(zip(detection.xy[::2], detection.xy[1::2])))
    if not polygon.is_valid or polygon.area <= 0:
        raise ValueError("Invalid or empty predicted quadrilateral")
    return polygon


def polygon_iou(left, right):
    intersection = left.intersection(right).area
    union = left.area + right.area - intersection
    return intersection / union if union > 0 else 0.0


def polygon_nms(detections, iou_threshold=0.1, *, class_agnostic=False):
    """单一父类的细分类任务可跨类别去重；阈值仍须在验证集上确定。"""
    if not 0 <= iou_threshold <= 1:
        raise ValueError("NMS IoU must be between zero and one")
    # 固定置信度及原始窗口/框序号，禁止按包或到达顺序提前做 NMS。
    ordered = sorted(detections, key=lambda d: (-d.score, d.window_id, d.detection_id))
    ids = [(d.window_id, d.detection_id) for d in ordered]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate window/detection identity")
    polygons = [validate_detection(d) for d in ordered]
    kept = []
    for index, candidate in enumerate(ordered):
        if all((not class_agnostic and candidate.class_id != ordered[j].class_id) or
               polygon_iou(polygons[index], polygons[j]) <= iou_threshold for j in kept):
            kept.append(index)
    return [ordered[i] for i in kept]


def serialize_detections(detections, window_id, box):
    """28 B 头含窗口身份、原图坐标和数量；空输出同样显式确认完成。"""
    x0, y0, x1, y1 = box
    payload = bytearray(HEADER.pack(b"OBB1", window_id, x0, y0, x1 - x0, y1 - y0,
                                    len(detections)))
    for detection in detections:
        validate_detection(detection)
        payload.extend(RECORD.pack(*detection.xy, detection.score, detection.class_id))
    return bytes(payload)


def deserialize_detections(payload):
    if len(payload) < HEADER.size:
        raise ValueError("Truncated OBB message")
    magic, window_id, x, y, width, height, count = HEADER.unpack_from(payload)
    if magic != b"OBB1" or len(payload) != HEADER.size + count * RECORD.size:
        raise ValueError("Invalid OBB message framing")
    detections = []
    for index in range(count):
        values = RECORD.unpack_from(payload, HEADER.size + index * RECORD.size)
        detection = Detection(window_id, index, values[-1], values[-2], tuple(values[:8]))
        validate_detection(detection)
        detections.append(detection)
    return (x, y, x + width, y + height), detections


def label_diagnostic(detections, labels, threshold=0.5, ignored_labels=()):
    """固定阈值的小样本逐类一对一匹配；不是 mAP 或正式 DOTA 评测。"""
    all_labels = list(labels) + list(ignored_labels)
    ground_truth = [validate_detection(label) for label in all_labels]
    unmatched = set(range(len(labels)))
    matched, ignored_predictions = 0, 0
    for detection in sorted(detections, key=lambda d: (-d.score, d.window_id, d.detection_id)):
        polygon = validate_detection(detection)
        candidates = [(polygon_iou(polygon, ground_truth[i]), i) for i in range(len(all_labels))
                      if all_labels[i].class_id == detection.class_id]
        if candidates:
            iou, index = max(candidates, key=lambda value: (value[0], -value[1]))
            if iou >= threshold:
                if index >= len(labels):
                    ignored_predictions += 1
                elif index in unmatched:
                    matched += 1
                    unmatched.remove(index)
    evaluated_predictions = len(detections) - ignored_predictions
    return {"iou_threshold": threshold, "tp": matched, "fp": evaluated_predictions - matched,
            "fn": len(labels) - matched, "label_count": len(labels),
            "ignored_label_count": len(ignored_labels), "ignored_predictions": ignored_predictions,
            "precision": matched / evaluated_predictions if evaluated_predictions else None,
            "recall": matched / len(labels) if labels else None,
            "mAP": None, "scope": "fixed-threshold smoke diagnostic, not independent DOTA evaluation"}
