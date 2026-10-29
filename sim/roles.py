"""任务数据权限；源星是起点，计算层承担全部跨星存转。"""


def sources(config):
    get = config.get if isinstance(config, dict) else lambda key, default=None: getattr(config, key, default)
    return set(get("task_source_sat_ids", ()) or ()) if get("sar_source_isolation", False) else set()


def can_receive(config, satellite_id):
    return satellite_id not in sources(config)


def can_compute(config, satellite_id, source_id):
    return satellite_id == source_id or can_receive(config, satellite_id)
