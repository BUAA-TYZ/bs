import gzip
import json
import pytest

from analysis import cgs_gap_20261111 as core
from analysis.run_cgs_gap_shards_20261111 import ranges,quote_range


@pytest.mark.parametrize('total',[1,4999,5000,5001,245157])
def test_partition_has_exact_coverage_without_overlap(total):
    chunks=ranges(total)
    assert chunks[0][0]==0 and chunks[-1][1]==total
    assert all(left[1]==right[0] for left,right in zip(chunks,chunks[1:]))
    assert sum(end-start for start,end in chunks)==total
    assert all(0<end-start<=5000 for start,end in chunks)


@pytest.mark.parametrize('blocks,total',[(4,16),(16,17)])
def test_shard_merge_matches_unsharded_reference(tmp_path,monkeypatch,blocks,total):
    monkeypatch.setattr(core,'quote',lambda snapshot,positions,ordered=None:
        None if all(p==1 for p in positions) else float(sum((i+1)*p for i,p in enumerate(positions))))
    plan=dict(blocks=blocks,positions=[0,1],ordered=list(range(blocks)),planned_quotes=total)
    rows=[];best=[]
    for start,end in ranges(total,5):
        path=tmp_path/f'{start}.gz';result=quote_range({},plan,start,end,path)
        rows.extend(json.loads(line) for line in gzip.open(path,'rt'))
        if result['canonical_best_quote'] is not None:best.append(result['canonical_best_quote'])
    expected=[[i,core.quote({},list(v))] for i,v in enumerate(core.assignments(blocks,2))]
    assert rows==expected
    assert min(best)==min(value for _,value in expected if value is not None)
