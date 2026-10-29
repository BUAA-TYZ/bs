"""共享链路容量与地面站接入；所有调度策略使用同一传输规则。"""

from collections import defaultdict


def fair_bytes(demands: list[float], capacity: float) -> list[float]:
    """流体公平分配：短传输剩余的份额在本步内重新分配。"""
    allocated = [0.0] * len(demands)
    active = {i for i, demand in enumerate(demands) if demand > 0}
    remaining = max(0.0, capacity)
    while active and remaining > 1e-12:
        share = remaining / len(active)
        finished = {i for i in active if demands[i] - allocated[i] <= share}
        if not finished:
            for i in active:
                allocated[i] += share
            break
        for i in finished:
            amount = demands[i] - allocated[i]
            allocated[i] += amount
            remaining -= amount
        active -= finished
    return allocated


def advance_ground_transfers(env, transfers: list) -> None:
    """每站按卫星轮转接入；每颗卫星最多占用一个站的一个通道。"""
    by_station = defaultdict(lambda: defaultdict(list))
    for transfer in transfers:
        if env.vis_cache.is_visible(transfer.src_sat, transfer.gs_id, env.ground_stations):
            by_station[transfer.gs_id][transfer.src_sat].append(transfer)
    assigned_sats = set()
    station_ids = sorted(env.ground_stations)
    if station_ids:
        offset = env.time % len(station_ids)
        station_ids = station_ids[offset:] + station_ids[:offset]
    for gs_id in station_ids:
        gs = env.ground_stations[gs_id]
        candidates = sorted(set(by_station[gs_id]) - assigned_sats)
        last = env._ground_rr_last.get(gs_id, -1)
        ordered = [s for s in candidates if s > last] + [s for s in candidates if s <= last]
        selected = ordered[:gs.receive_channels]
        capacity = gs.bandwidth_mbps / 8.0 * env.cfg.dt
        sent_total = 0.0
        for sat_id in selected:
            flows = by_station[gs_id][sat_id]
            allocations = fair_bytes([tr.remaining_mb for tr in flows], capacity)
            for tr, amount in zip(flows, allocations):
                tr.remaining_mb = max(0.0, tr.remaining_mb - amount)
                sent_total += amount
            assigned_sats.add(sat_id)
        if selected:
            env._ground_rr_last[gs_id] = selected[-1]
        env.metrics.record_network(f"ground:{gs_id}", sent_total, capacity * gs.receive_channels)
