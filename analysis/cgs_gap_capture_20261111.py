"""仅观察原教师的候选报价入口，保存同一调用前的状态和日历。"""
from copy import deepcopy
import gzip
import hashlib
from pathlib import Path
import pickle
from types import FunctionType

from analysis.nonuniform_granularity_20261111 import NonuniformThreePlan
from analysis.sar_three_plan import Calendar


def capturing_policy(env, samples, save):
    selected={row['task_id']:row for row in samples}
    calls={}

    class ObservedCalendar(Calendar):
        def quote(self, state, actions, **kwargs):
            task=next(iter(state.tiles.values()))['parent_task_id'] if state.tiles else None
            saved=None
            if task in selected:
                now=state.time*state.config['dt']
                expected=selected[task]
                assert now==expected['time_seconds']
                key=(task,now);calls[key]=calls.get(key,0)+1
                index=2 if expected['selected']=='cgs_4' else 4
                if calls[key]==index:
                    plain=object.__new__(Calendar)
                    plain.__dict__=deepcopy(self.__dict__)
                    saved=dict(state=deepcopy(state),calendar=plain,actions=deepcopy(actions),sample=expected)
            value=super().quote(state,actions,**kwargs)
            if saved is not None:
                saved['calendar_quote']=value
                assert len(saved['state'].tiles)==selected[task]['actual']
                save(saved)
            return value

    original=NonuniformThreePlan.select_actions
    namespace=dict(original.__globals__,Calendar=ObservedCalendar)
    select=FunctionType(original.__code__,namespace,original.__name__,original.__defaults__,original.__closure__)

    class CapturingPolicy(NonuniformThreePlan):
        select_actions=select

    policy=CapturingPolicy(env,enabled=False)
    assert policy.select_actions.__func__.__code__ is original.__code__
    return policy


def save_snapshot(path, snapshot):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    data=gzip.compress(pickle.dumps(snapshot,protocol=5),mtime=0)
    with path.open('xb') as output:output.write(data)
    return dict(path=str(path),sha256=hashlib.sha256(data).hexdigest(),bytes=len(data),
                sample=snapshot['sample'],calendar_quote=snapshot['calendar_quote'])
