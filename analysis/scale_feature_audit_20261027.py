"""冻结训练输入范围与旁路越界计数；不改变actor输入或采样。"""
from collections import Counter
from copy import deepcopy
import math

TOLERANCE = 1e-6
NODE_NAMES = ['own','known','advertisement_age','committed_compute','memory','vram',
              'queue','running','isl_backlog','ground_backlog','ground_window','arrival','hops','compute_rate']
IMAGE_NAMES = ['bytes','compute','blocks','waiting','vram','raw_feasible','raw_quote','helpers']


def feature_rows(obs):
    for v in obs.get('variants', [obs]):
        count = v['block_count']; prefix = f'g{count}'
        yield prefix+'/image', v['image'], (2,5), {6:2.}
        for sid,valid,row in zip(v['node_ids'],v['node_valid'],v['nodes'],strict=True):
            if not valid: continue
            assert sid != -1
            role = 'source' if sid == v['source'] else 'ground' if sid == 'ground_block' else 'compute'
            known = 'known' if row[1] else 'unknown'
            yield f'{prefix}/{role}/{known}', row, (0,1), {2:1.,4:2.,5:2.,7:2.,10:2.,11:2.,13:2.}


def add_ranges(ranges, obs):
    for key,row,categorical,_ in feature_rows(obs):
        assert all(math.isfinite(v) for v in row)
        r = ranges.setdefault(key,dict(count=0,minimum=list(row),maximum=list(row),
            categorical={str(i):[] for i in categorical}))
        r['count'] += 1
        for i,value in enumerate(row):
            r['minimum'][i] = min(r['minimum'][i],value); r['maximum'][i] = max(r['maximum'][i],value)
            if i in categorical and value not in r['categorical'][str(i)]:
                r['categorical'][str(i)].append(value); r['categorical'][str(i)].sort()


def merge_ranges(left, right):
    out = deepcopy(left)
    for key,r in right.items():
        if key not in out: out[key] = deepcopy(r); continue
        a = out[key]; a['count'] += r['count']
        a['minimum'] = [min(x,y) for x,y in zip(a['minimum'],r['minimum'],strict=True)]
        a['maximum'] = [max(x,y) for x,y in zip(a['maximum'],r['maximum'],strict=True)]
        for i,values in r['categorical'].items(): a['categorical'][i] = sorted(set(a['categorical'][i]+values))
    return out


class FeatureAudit:
    def __init__(self, reference):
        self.reference = reference; self.groups = {}; self.decisions = 0; self.outside_decisions = 0
        self.new_category_decisions = 0; self.candidates = {}; self.task_ids = []

    def observe(self, obs, candidates):
        self.decisions += 1; self.task_ids.append(obs['task_id']); outside = novel = False
        for key,row,categorical,caps in feature_rows(obs):
            r = self.reference.get(key)
            a = self.groups.setdefault(key,dict(rows=0,continuous_cells=0,outside_cells=0,
                outside_by_dimension=[0]*len(row),new_categories_by_dimension=[0]*len(row),
                saturation_by_dimension=[0]*len(row),missing_reference_rows=0))
            a['rows'] += 1; a['missing_reference_rows'] += r is None
            for i,value in enumerate(row):
                assert math.isfinite(value)
                if i in categorical:
                    new = r is None or value not in r['categorical'][str(i)]
                    a['new_categories_by_dimension'][i] += new; novel |= new
                else:
                    bad = r is None or value < r['minimum'][i]-TOLERANCE or value > r['maximum'][i]+TOLERANCE
                    a['continuous_cells'] += 1; a['outside_cells'] += bad
                    a['outside_by_dimension'][i] += bad; outside |= bad
                if i in caps: a['saturation_by_dimension'][i] += value >= caps[i]-TOLERANCE
        self.outside_decisions += outside; self.new_category_decisions += novel
        for count,values in candidates.items():
            for field,value in values.items():
                self.candidates.setdefault(f'g{count}/{field}',Counter())[int(value)] += 1

    def result(self):
        total = sum(v['continuous_cells'] for v in self.groups.values())
        outside = sum(v['outside_cells'] for v in self.groups.values())
        nodes = sum(v['rows'] for k,v in self.groups.items() if not k.endswith('/image'))
        unknown = sum(v['rows'] for k,v in self.groups.items() if k.endswith('/unknown'))
        return dict(decisions=self.decisions,task_ids=self.task_ids,groups=self.groups,
            continuous_cells=total,outside_cells=outside,outside_cell_fraction=outside/total if total else None,
            outside_decisions=self.outside_decisions,outside_decision_fraction=self.outside_decisions/self.decisions if self.decisions else None,
            new_category_decisions=self.new_category_decisions,unknown_node_fraction=unknown/nodes if nodes else None,
            candidates={k:histogram_summary(v) for k,v in self.candidates.items()},tolerance=TOLERANCE,
            space='Encoded actor input; historical pre-clipping values unavailable')


def histogram_summary(counts):
    n = sum(counts.values()); values = sorted(counts)
    def quantile(q):
        at = (n-1)*q
        def pick(rank):
            seen = 0
            for v in values:
                seen += counts[v]
                if rank < seen: return v
        lo,hi = math.floor(at),math.ceil(at)
        return pick(lo)+(pick(hi)-pick(lo))*(at-lo)
    return dict(histogram=dict(sorted(counts.items())),count=n,
        mean=sum(k*v for k,v in counts.items())/n,P50=quantile(.5),P90=quantile(.9),maximum=max(values))
