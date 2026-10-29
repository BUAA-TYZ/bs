"""训练续跑、策略执行与分析工具共用的观测协议校验。"""

from sim.marl.features import observation_version_for, validate_feature_workload


def validate_checkpoint_protocol(checkpoint):
    if not isinstance(checkpoint, dict) or not isinstance(checkpoint.get("model_config"), dict):
        raise ValueError("Legacy/incompatible checkpoint: missing model configuration")
    feature_set = checkpoint["model_config"].get("feature_set", "base")
    if checkpoint.get("observation_version") != observation_version_for(feature_set):
        raise ValueError("Legacy/incompatible checkpoint: observation version does not match feature semantics")
    if "config" in checkpoint:
        validate_feature_workload(checkpoint["config"].get("image_tiling"), feature_set)
