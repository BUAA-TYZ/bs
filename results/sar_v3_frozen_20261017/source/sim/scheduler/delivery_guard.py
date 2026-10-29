"""CGS + 接触边界余量：分配计算前即排除执行器不会接纳的末端窗口。

余量取公开接触扫描周期，不引入为得分调出的常数。此候选不修改执行规则，
也不预知计算后的真实结果大小。接触容量仍是同轮预约，非跨轮带宽保证。
"""

from sim.scheduler.literature import CGSComputeAdapted, contact_plan


class CGSDeliveryGuard(CGSComputeAdapted):
    def _contact_plan(self, state):
        plan = contact_plan(state)
        guard = state.config.get("contact_scan_s", 0) if state.config.get("relay_contact_guard") else 0
        for contact in plan.contacts:
            if contact.target not in state.satellites:
                continue
            occupied = max(0.0, (contact.end - contact.start) * contact.rate - contact.volume)
            contact.end = max(contact.start, contact.end - guard)
            contact.volume = max(0.0, (contact.end - contact.start) * contact.rate - occupied)
        self.stats["guarded_plans"] += 1
        return plan
