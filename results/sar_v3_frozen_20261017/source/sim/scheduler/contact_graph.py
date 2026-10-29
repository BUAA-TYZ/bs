"""CGS 的机会枚举和交付搜索；完整分片时延是本仿真的显式适配。"""

from dataclasses import dataclass
from collections import Counter
import heapq
from itertools import count
import math


@dataclass
class Contact:
    source: object
    target: object
    start: float
    end: float
    rate: float
    volume: float
    delay: float = 0


def quantize(value, dt):
    return math.ceil(max(0, value) / dt - 1e-12) * dt if dt else value


def arrival(contact, ready, size, dt=0):
    if contact.rate <= 0 or contact.volume + 1e-9 < size:
        return None
    start = max(quantize(ready, dt), quantize(contact.start, dt))
    service = max(dt, quantize(size / contact.rate, dt))
    if start + service > contact.end + 1e-9:
        return None
    return start + service + contact.delay


class ContactPlan:
    def __init__(self, contacts):
        self.contacts = contacts
        self.outgoing = {}
        for index, contact in enumerate(contacts):
            self.outgoing.setdefault(contact.source, []).append(index)

    def delivery(self, source, destinations, ready, size, *, dt=0, max_hops=100):
        """FIFO 接触模型的最早完整到达；保留跳数标签，支持等待未来接触。"""
        serial = count()
        queue = [(ready, 0, next(serial), source, ())]
        best = {(source, 0): ready}
        while queue:
            time, hops, _, node, path = heapq.heappop(queue)
            if best[node, hops] != time:
                continue
            if node in destinations:
                return time, path
            if hops >= max_hops:
                continue
            for index in self.outgoing.get(node, []):
                contact = self.contacts[index]
                end = arrival(contact, time, size, dt)
                if end is None or any(best.get((contact.target, h), math.inf) <= end
                                      for h in range(hops + 2)):
                    continue
                best[contact.target, hops + 1] = end
                heapq.heappush(queue, (end, hops + 1, next(serial), contact.target, path + (index,)))
        return None

    def reserve(self, path, size):
        # CGS 扣减接触总量，不等于给数据流分配独占的精确时间片。
        amounts = Counter(path)
        for index, uses in amounts.items():
            if self.contacts[index].volume + 1e-9 < uses * size:
                raise ValueError("Contact volume overbooked")
        for index, uses in amounts.items():
            self.contacts[index].volume -= uses * size


def choose_opportunity(opportunities, plan, destinations, size, *, dt=0, max_hops=100):
    """CGS Algorithm 1：先枚举最早产出机会，比较其后续最早交付。"""
    best = None
    for ready, node, label in sorted(opportunities, key=lambda x: x[0]):
        if best is not None and ready >= best[0]:
            break
        route = plan.delivery(node, destinations, ready, size, dt=dt, max_hops=max_hops)
        if route is not None and (best is None or route[0] < best[0]):
            best = route[0], label, route[1]
    return best
