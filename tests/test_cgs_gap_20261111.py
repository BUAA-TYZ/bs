from copy import deepcopy
import gzip
import itertools
import json
import math
from types import SimpleNamespace

import pytest

from analysis import cgs_gap_20261111 as gap
from analysis.cgs_gap_capture_20261111 import capturing_policy, save_snapshot
from analysis.nonuniform_granularity_20261111 import NonuniformThreePlan
from analysis.sar_three_plan import Calendar
from sim.entities import Action, ActionType
from test_dynamic_granularity_v2 import fixture


def test_four_enumerates_every_assignment():
    values=list(gap.assignments(4,3))
    assert values==list(itertools.product(range(3),repeat=4))
    assert len(set(values))==3**4


def test_sixteen_count_vectors_are_complete_and_fixed():
    for k in (1,2,3,5):
        counts=list(gap.compositions(16,k))
        values=list(gap.assignments(16,k))
        assert len(counts)==len(values)==math.comb(16+k-1,k-1)
        assert len(set(values))==len(values)
        assert all(len(v)==16 and tuple(sorted(v))==v for v in values)
        assert all(tuple(v.count(i) for i in range(k))==c for c,v in zip(counts,values))
    assert sum(1 for _ in gap.compositions(16,8))==245157


def test_observer_preserves_original_actions_decisions_and_snapshot(tmp_path):
    left,right=fixture(),fixture()
    try:
        original=NonuniformThreePlan(left,enabled=False)
        saved=[]
        sample=dict(id='toy',task_id='task_0',selected='cgs_4',actual=4,
                    time_seconds=right.time*right.cfg.dt)
        captured=capturing_policy(right,[sample],saved.append)
        assert captured.select_actions.__func__.__code__ is original.select_actions.__func__.__code__
        assert original.select_actions(left.export_state())==captured.select_actions(right.export_state())
        assert original.decisions==captured.decisions
        assert len(saved)==1
        snapshot=saved[0]
        assert type(snapshot['calendar']) is Calendar
        assert deepcopy(snapshot['calendar']).quote(snapshot['state'],snapshot['actions'])==snapshot['calendar_quote']
        record=save_snapshot(tmp_path/'snapshot.pkl.gz',snapshot)
        restored=gap.load_snapshot(record['path'],record['sha256'])
        assert deepcopy(restored['calendar']).quote(restored['state'],restored['actions'])==snapshot['calendar_quote']
        for _ in range(60):
            left.step(original.select_actions(left.export_state()))
            right.step(captured.select_actions(right.export_state()))
        assert left.summary()['task_outcomes']==right.summary()['task_outcomes']
    finally:
        left.close();right.close()


class DummyCalendar:
    plan=None
    memory={}
    def quote(self,state,actions):return 200.


@pytest.mark.parametrize('used,expected',[(3,'ELIGIBLE'),(8,'ELIGIBLE'),(9,'OVER_CAP')])
def test_candidates_keep_all_actual_positions_and_cap(monkeypatch,used,expected):
    tiles={f'task_0_tile_{i}':dict(location=99,source_sat_id=99,parent_task_id='task_0') for i in range(16)}
    from dataclasses import make_dataclass
    State=make_dataclass('State',['tiles','pending_task_tiles','config'])
    state=State(tiles,{},dict(ground_input_relay=True))
    snapshot=dict(state=state,calendar=DummyCalendar(),calendar_quote=200.,
        actions=[Action(t,ActionType.OFFLOAD,target_sat_id=i%used) for i,t in enumerate(tiles)])
    monkeypatch.setattr(gap,'input_options',lambda *args:{i:(0,()) for i in range(12)})
    monkeypatch.setattr(gap,'quote',lambda *args:200.)
    plan=gap.candidates(snapshot)
    assert plan['status']==expected and plan['used_positions']==used
    assert set(range(used))<=set(plan['positions'])
    if expected=='ELIGIBLE':assert len(plan['positions'])<=8
    else:assert plan['planned_quotes']==0


def test_exact_raw_quotes_and_minimum(tmp_path,monkeypatch):
    monkeypatch.setattr(gap,'quote',lambda snapshot,positions,ordered=None:float(sum(positions)))
    plan=dict(status='ELIGIBLE',blocks=4,positions=[0,1],cgs=[1]*4,cgs_quote=4.,ordered=list('abcd'),planned_quotes=16)
    raw=tmp_path/'raw.gz';r=gap.enumerate_snapshot({},plan,raw)
    rows=[json.loads(line) for line in gzip.open(raw,'rt')]
    assert len(rows)==16 and r['best_quote']==0 and r['improvement_s']==4
    assert rows==[[i,float(sum(v))] for i,v in enumerate(itertools.product(range(2),repeat=4))]


def test_approximation_reports_incumbent_outside_canonical_mapping(tmp_path,monkeypatch):
    cgs=[0,1]*8
    monkeypatch.setattr(gap,'quote',lambda snapshot,positions,ordered=None:0. if positions==cgs else 1.)
    plan=dict(status='ELIGIBLE',blocks=16,positions=[0,1],cgs=cgs,cgs_quote=0.,ordered=list(range(16)),planned_quotes=17)
    r=gap.enumerate_snapshot({},plan,tmp_path/'raw.gz')
    assert r['canonical_best_quote']==1 and r['best_quote']==0
    assert r['incumbent_better_than_canonical'] and not r['exact_assignment_space']


def test_moves_swaps_and_local_search_converge(tmp_path,monkeypatch):
    assert (1,0) in gap.neighbors((0,1),2)
    assert (0,0) in gap.neighbors((0,1),2)
    monkeypatch.setattr(gap,'quote',lambda snapshot,positions,ordered=None:float(sum(positions)))
    cgs=list(range(9))+[0]*7
    plan=dict(status='OVER_CAP',cgs=cgs,cgs_quote=float(sum(cgs)),local_search_positions=list(range(9)),ordered=list(range(16)))
    r=gap.local_search({},plan,tmp_path/'raw.gz')
    assert r['best_quote']==0 and r['stop_reason']=='NO_IMPROVEMENT'
    assert r['iterations_accepted']<=20


def test_quote_preserves_original_calendar():
    env=fixture()
    try:
        saved=[]
        sample=dict(task_id='task_0',selected='cgs_4',actual=4,time_seconds=env.time*env.cfg.dt)
        capturing_policy(env,[sample],saved.append).select_actions(env.export_state())
        s=saved[0];before=deepcopy(s['calendar'].__dict__)
        p=gap.candidates(s)
        assert p['status']=='ELIGIBLE'
        gap.quote(s,p['cgs'])
        assert s['calendar'].plan.contacts==before.pop('plan').contacts
        assert {k:v for k,v in s['calendar'].__dict__.items() if k!='plan'}==before
    finally:env.close()
