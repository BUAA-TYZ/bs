import copy
import unittest

from sim.scheduler.delivery_guard import CGSDeliveryGuard
from sim.scheduler.literature import contact_plan
from test_reservation import contention_state


class DeliveryGuardTests(unittest.TestCase):
    def test_guard_removes_tail_capacity_without_erasing_existing_backlog(self):
        state = contention_state()
        state.config.update(relay_contact_guard=True, contact_scan_s=3)
        for link in state.links.values():
            link["contact_windows"] = [(0, 10), (20, 2)]
        baseline = contact_plan(state)
        original = copy.deepcopy(state)
        guarded = CGSDeliveryGuard()._contact_plan(state)
        self.assertEqual(state, original)
        for before, after in zip(baseline.contacts, guarded.contacts):
            if before.target not in state.satellites:
                self.assertEqual(before, after)
            else:
                self.assertEqual(after.end, max(before.start, before.end - 3))
                self.assertAlmostEqual(after.volume, max(0, before.volume - 3 * before.rate))

    def test_disabled_guard_preserves_cgs_contact_plan(self):
        state = contention_state()
        state.config["relay_contact_guard"] = False
        self.assertEqual(CGSDeliveryGuard()._contact_plan(state).contacts, contact_plan(state).contacts)


if __name__ == "__main__":
    unittest.main()
