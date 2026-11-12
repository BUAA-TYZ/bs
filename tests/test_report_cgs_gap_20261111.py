import gzip
import itertools
import json

import pytest

from analysis import report_cgs_gap_20261111 as report
from analysis import cgs_gap_20261111 as core
from analysis import run_cgs_gap_20261111 as runner
from analysis.run_cgs_gap_shards_20261111 import quote_range


@pytest.mark.parametrize('values',[[],[0.],[0.,1.,5.,6.],[.2,.5,9.,12.,14.,55.]])
def test_statistics_independently_verified(values):
    summary=report.stats(values);report.verify_stats(values,summary)
    if not values:assert summary['mean'] is None
    else:assert summary['within_1s']==sum(v<=1+1e-9 for v in values)/len(values)


@pytest.mark.parametrize('blocks,k',[(4,3),(16,1),(16,2),(16,3),(16,5)])
def test_stars_and_bars_matches_registered_recursive_order(blocks,k):
    assert list(report.independent_assignments(blocks,k))==list(core.assignments(blocks,k))


def test_full_coverage_and_best_requote(tmp_path,monkeypatch):
    monkeypatch.setattr(core,'quote',lambda snapshot,positions,ordered=None:float(sum(positions)))
    monkeypatch.setattr(core,'load_snapshot',lambda *args:{})
    plan=dict(blocks=4,positions=[0,1],ordered=list(range(4)),planned_quotes=16,cgs_quote=4.)
    entry=dict(plan=plan,snapshot=dict(path='unused',sha256='unused'))
    results=[]
    for start,end in ((0,7),(7,16)):
        path=tmp_path/f'{start}.gz';result=quote_range({},plan,start,end,path)
        result=json.loads(json.dumps(result));result.update(raw_path=str(path),raw_sha256=runner.sha(path))
        results.append(result)
    merged=report.audit_enumeration(entry,results)
    assert merged['best_quote']==0 and merged['evaluated']==16 and merged['improvement_s']==4
    with pytest.raises(AssertionError):report.audit_enumeration(entry,results[1:])


def test_local_search_raw_trace_audit(tmp_path,monkeypatch):
    monkeypatch.setattr(core,'quote',lambda snapshot,positions,ordered=None:float(sum(positions)))
    monkeypatch.setattr(core,'load_snapshot',lambda *args:{})
    plan=dict(status='OVER_CAP',blocks=16,cgs=list(range(9))+[0]*7,cgs_quote=36.,
              local_search_positions=list(range(9)),ordered=list(range(16)))
    path=tmp_path/'local.gz';result=core.local_search({},plan,path)
    result=json.loads(json.dumps(result));result.update(raw_path=str(path),raw_sha256=runner.sha(path))
    value=report.audit_search(dict(plan=plan,snapshot=dict(path='unused',sha256='unused')),result)
    assert value['improvement_s']==36 and value['stop_reason']=='NO_IMPROVEMENT'
