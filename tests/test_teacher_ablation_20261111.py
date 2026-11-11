from copy import deepcopy
from dataclasses import replace
from unittest.mock import patch

from analysis.teacher_ablation_20261111 import heft_with_plan, HeftCooperativeThreePlan
from analysis.dynamic_granularity_v2 import DynamicThreePlan
from analysis.sar_three_plan import Calendar
from sim.scheduler.literature import HEFTAdapted, CGSComputeAdapted, contact_plan
from analysis.run_sar_frozen import invariants
from test_dynamic_granularity_v2 import fixture


def test_bound_plan_matches_original_heft_and_does_not_change_globals():
    env = fixture()
    try:
        state = env.export_state()
        assert heft_with_plan(state, contact_plan(state)) == HEFTAdapted().select_actions(state)
        assert HEFTAdapted.select_actions.__globals__['contact_plan'] is contact_plan
    finally: env.close()


def test_heft_uses_supplied_plan_not_fresh_geometry():
    env = fixture()
    try:
        state = env.export_state(); plan = contact_plan(state)
        for c in plan.contacts: c.volume = 0
        with patch('sim.scheduler.literature.contact_plan', side_effect=AssertionError('regenerated')):
            heft_with_plan(state, plan)
    finally: env.close()


def test_local_and_ground_quotes_and_selector_unchanged():
    a, b = fixture(), fixture()
    try:
        pa, pb = DynamicThreePlan(a), HeftCooperativeThreePlan(b)
        pa.select_actions(a.export_state()); pb.select_actions(b.export_state())
        for key in ('local_4','local_16','ground'):
            assert pa.decisions[0]['quotes_seconds'][key] == pb.decisions[0]['quotes_seconds'][key]
        assert DynamicThreePlan.select_actions is HeftCooperativeThreePlan.select_actions
    finally: a.close(); b.close()


def test_committed_and_partial_fallback_stays_cgs():
    env = fixture()
    try:
        state = env.export_state(); p = HeftCooperativeThreePlan(env)
        p.decided.add('task_0')
        assert p._select_actions_with_plan(state, contact_plan(state)) == CGSComputeAdapted()._select_actions_with_plan(state, contact_plan(state))
        p.decided.clear()
        partial = replace(state, tiles=dict(list(state.tiles.items())[:2]))
        assert p._select_actions_with_plan(partial, contact_plan(partial)) == CGSComputeAdapted()._select_actions_with_plan(partial, contact_plan(partial))
    finally: env.close()


def test_heft_whole_image_executes_and_conserves():
    env = fixture()
    try:
        policy = HeftCooperativeThreePlan(env)
        for _ in range(80): env.step(policy.select_actions(env.export_state()))
        assert invariants(env)['passed']
        assert env.tasks['task_0'].completion_time is not None
    finally: env.close()


def test_supplied_calendar_mutation_is_isolated():
    env = fixture()
    try:
        state = env.export_state(); calendar = Calendar(state); before = deepcopy(calendar)
        heft_with_plan(state, deepcopy(calendar.plan))
        assert calendar.plan.contacts == before.plan.contacts
        assert calendar.busy == before.busy
    finally: env.close()
