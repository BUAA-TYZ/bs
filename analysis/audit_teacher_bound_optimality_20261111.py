"""独立标签修正法复算时间图最早到达，不使用运行器的Dijkstra实现。"""
from collections import defaultdict, deque
import gzip
import json
import math
from pathlib import Path
import time

from analysis import run_teacher_analysis_20261111 as a


def node(value):
    return tuple(value) if isinstance(value, list) else value


def earliest(records, source, release, service, ground):
    outgoing = defaultdict(list)
    grouped = defaultdict(list)
    for origin, destination, start, end in records:
        grouped[node(origin), node(destination)].append((start, end))
    for (origin, destination), intervals in grouped.items():
        outgoing[origin].append((destination, sorted(intervals)))
    labels = {(source, 0): release}
    pending = deque([(source, 0)])
    queued = {(source, 0)}
    while pending:
        current = pending.popleft()
        queued.remove(current)
        origin, phase = current
        ready = labels[current]
        proposals = [((origin, 1), ready + service)] if phase == 0 else []
        for destination, intervals in outgoing[origin]:
            arrival = next((max(start, ready) for start, end in intervals if end >= ready), None)
            if arrival is not None:
                proposals.append(((destination, phase), arrival))
        # 持续修正到不动点，不用优先队列，也不在首次到地面时提前结束。
        for target, arrival in proposals:
            if arrival < labels.get(target, math.inf):
                labels[target] = arrival
                if target not in queued:
                    queued.add(target)
                    pending.append(target)
    best = min((labels.get((g, 1), math.inf) for g in ground), default=math.inf)
    return None if math.isinf(best) else best


def main():
    started = time.monotonic()
    registration = a.read(a.ROOT / 'OPTIMALITY_AUDIT_REGISTRATION.json')
    assert a.sha(__file__) == registration['source_sha256']
    ground = {('ground', 0)}
    assert earliest([(0, ['ground', 0], 10, 20)], 0, 0, 2, ground) == 10
    assert earliest([(0, ['ground', 0], 0, 1)], 0, 0, 2, ground) == 2
    assert earliest([], 0, 0, 2, ground) is None
    rows = []
    for date, protocol in zip(a.DATES, a.read(a.ROOT / 'manifest.json')['protocols']):
        result = a.read(a.ROOT / 'bounds' / f'{date}.json')
        assert a.sha(result['contacts_path']) == result['contacts_sha256']
        contacts = json.loads(gzip.decompress(Path(result['contacts_path']).read_bytes()))
        config = a.read(protocol)['config']
        ground = {('ground', g['id']) for g in config['ground_stations']}
        errors = []
        for row in result['lower']:
            event = config['scheduled_task_arrivals'][int(row['task_id'].split('_')[-1])]
            arrival = earliest(contacts, event['source_sat_id'], row['release_seconds'],
                               row['service_floor_s'], ground)
            actual = None if arrival is None else arrival - row['release_seconds'] + row['fixed_merge_s']
            if actual is None:
                assert row['latency_seconds'] is None
            else:
                error = abs(actual - row['latency_seconds'])
                assert error < 1e-8, (date, row['task_id'], error)
                errors.append(error)
        rows.append(dict(date=date,images=len(result['lower']),max_absolute_error_s=max(errors,default=0)))
    a.write(a.ROOT / 'OPTIMALITY_AUDIT.json', dict(status='PASS',at=a.now(),days=rows,
        algorithm='Independent FIFO label correction to fixed point; no heap or early exit',
        deterministic_examples_passed=3,source_sha256=a.sha(__file__),
        wall_seconds=time.monotonic()-started))
    print({'status':'PASS','images':sum(r['images'] for r in rows),'wall_seconds':time.monotonic()-started})


if __name__ == '__main__':
    main()
