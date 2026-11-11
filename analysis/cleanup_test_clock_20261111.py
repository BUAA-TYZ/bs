"""仅pytest显式加载：旧用例的时钟夹具，不修改冻结源码或真实任务截止。"""
from contextlib import ExitStack
from datetime import datetime, timezone
import importlib
from unittest.mock import patch

import pytest

MODULES = (
    'analysis.run_dynamic_learning',
    'analysis.run_ground_scaling_20261029',
    'analysis.run_m_formal_20261027',
    'analysis.run_l_reduced_validation_20261028',
    'analysis.run_s_formal_20261029',
    'analysis.run_scale_validation_20261027',
)
TARGETS = {
    'test_dynamic_mappo.py': {'test_training_rollout_records_real_terminal_credit', 'test_validation_records_all_deadlines_and_passive_resources'},
    'test_ground_distributed_20261030.py': {'test_failed_batch_drains_without_next_batch', 'test_stop_control_prevents_dispatch'},
    'test_ground_distributed_v2_20261030.py': {'test_failed_batch_drains_without_next_batch', 'test_stop_control_prevents_dispatch'},
    'test_ground_scaling_20261029.py': {'test_failed_batch_drains_and_does_not_prefetch'},
    'test_l_formal_20261028.py': {'test_failed_batch_drains_without_prefetch', 'test_real_toy_formal_records_all_policy_families', 'test_second_open_rejected'},
    'test_l_reduced_validation_20261028.py': {'test_failure_drains_only_current_batch', 'test_real_toy_worker_all_retained_policy_families'},
    'test_m_formal_20261027.py': {'test_real_toy_execution_all_new_families'},
    'test_s_formal_20261029.py': {'test_all20_real_toy_and60_synthetic_report_with_independent_verifier', 'test_failed_batch_drains_no_prefetch', 'test_open_gate_and_second_open_rejected'},
    'test_scale_validation_20261027.py': {'test_worker_record_resources_and_followup_for_all_policy_families'},
}


class ValidationClock(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 10, 25, tzinfo=timezone.utc)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    with ExitStack() as stack:
        if item.name in TARGETS.get(item.path.name, set()):
            for name in MODULES:
                stack.enter_context(patch.object(importlib.import_module(name), 'datetime', ValidationClock))
        yield
