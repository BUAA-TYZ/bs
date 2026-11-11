"""代表基线补充评估；复用冻结物理与队列，不覆盖原147项。"""
import argparse
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
import platform
from pathlib import Path
import subprocess
import sys
import time
import traceback
from unittest.mock import patch

from analysis import run_ground_scaling_20261029 as original
from analysis import run_ground_queue_20261030 as queue
from analysis import run_m_formal_20261027 as formal
from analysis import scaled_ground_adapter_20261029 as adapter
from analysis import sar_three_plan as three
from sim.env import SimulationEnv

ROOT = original.ROOT/'representatives_20261031'
REG = ROOT/'REGISTRATION.json'
SPEC = ROOT/'EXPERIMENT.json'
MODULE = 'analysis.run_ground_representatives_20261031'
EXPERIMENT = 'ground_representatives_20261031'
METHODS = ('ground_16','local_16','local_heft_16','local_cgs_16','global_heft_16','global_cgs_16')
STAGES = {'L_single_representatives': ('L_single_completion', ('local_16','global_heft_16','global_cgs_16')),
          'L_scaled_representatives': ('L_scaled', METHODS), 'M_scaled_representatives': ('M_scaled', METHODS)}
read, sha, write = original.read, original.sha, original.write_json


class GroundOnlyPipeline(adapter.SharedPipeline):
    def route_raw_tiles(self, links):
        # 自动原图分支也使用已登记的任务级公开选站和原图到齐屏障。
        env = self.env
        pending = [t for t in env.tasks.values() if t.task_id not in self.raw_tasks and t.failure_reason is None]
        state = SimulationEnv.export_state(env) if pending else None
        for task in sorted(pending, key=lambda t:(t.release_time,t.task_id)):
            tiles = {tid: state.tiles[tid] for tid in task.tile_ids if tid in state.tiles}
            if len(tiles) != len(task.tile_ids) or not tiles:
                continue
            if not all(t['state'] in ('QUEUED','READY') and not t['in_transfer'] for t in tiles.values()):
                continue
            from dataclasses import replace
            gid = adapter.choose_station(replace(state,tiles=tiles),raw=True,hops=env.cfg.relay_max_hops)
            if gid is None:
                continue
            self.raw_stations[task.task_id] = gid
            self.raw_tasks[task.task_id] = env.cfg.relay_max_hops
            self.routing_views.setdefault(env.cfg.relay_max_hops,three.RoutingView(env,env.cfg.relay_max_hops))
        self.route_unique(links)


class GroundOnlyEnv(adapter.SharedEnv):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self._dist_pipeline = GroundOnlyPipeline(self)


@contextmanager
def installed(method, scaled):
    engine = original.engine
    keys = ('ROOT','VALID','METHODS','LEARNED','BASELINES','EXTRAS','GranularityEnv','policy_set',
            'ThreePlanChoice','ControlPlane','check_time','verify_manifest')
    saved = {k:getattr(engine,k) for k in keys}
    try:
        formal.install_worker_adapter(method)
        with ExitStack() as stack:
            if scaled:
                stack.enter_context(adapter.installed(engine))
                if method == 'ground_16':
                    stack.enter_context(patch.object(engine,'GranularityEnv',GroundOnlyEnv))
            else:
                stack.enter_context(patch.object(engine,'TrafficAudit',adapter.StationTraffic))
                stack.enter_context(patch.object(engine,'resources',adapter.resources))
            yield engine
    finally:
        for k,v in saved.items():
            setattr(engine,k,v)


def verify():
    m = read(REG)
    for path,h in m['hashes'].items():
        assert sha(path)==h,path
    original.verify_manifest()
    spec = read(SPEC)
    assert len(spec['jobs'])==len({j['id'] for j in spec['jobs']})==45
    assert spec['deadline']==original.END.timestamp()
    return spec


def deployment_check():
    spec=verify()
    checked=[]
    for path in sorted({j['job'][1] for j in spec['jobs']}):
        original.engine.verify(read(path));checked.append(dict(path=path,sha256=sha(path)))
    return dict(status='PASS',host=platform.node(),python=sys.version,root=str(Path.cwd().resolve()),
                registration_sha256=sha(REG),protocols=checked)


def probe():
    with ProcessPoolExecutor(max_workers=1,mp_context=multiprocessing.get_context('spawn')) as pool:
        print(json.dumps(pool.submit(deployment_check).result()))


def execute(item):
    stage,path,method = item['job']
    try:
        verify(); original.check_time()
        assert not Path(item['output']).exists() and not Path(item['output']).with_suffix('.progress.json').exists()
        assert item in read(SPEC)['jobs']
        with installed(method, '_scaled_' in stage) as engine, ExitStack() as stack:
            for k,v in dict(ROOT=ROOT/stage,VALID=original.DATES,METHODS=METHODS,
                            check_time=original.check_time,verify_manifest=lambda:read(REG)).items():
                stack.enter_context(patch.object(engine,k,v))
            short = engine.simulate((path,method))
        if short['status']!='PASS':
            return dict(short,status='FAILED',job=item['job'])
        row = read(short['path'])
        row.update(stage=stage,scale=stage[0],setting='scaled' if '_scaled_' in stage else 'single',
                   tests_sealed=False,registration_sha256=sha(REG),original_opening_sha256=sha(original.ROOT/'TEST_OPENED.json'),
                   adapter_sha256=sha(adapter.__file__),supplement_adapter_sha256=sha(__file__))
        write(Path(short['path']),row)
        short['sha256']=sha(short['path'])
        write(Path(short['path']).with_suffix('.progress.json'),short)
        return short
    except Exception:
        return dict(status='FAILED',job=item['job'],traceback=traceback.format_exc())


def register():
    original.check_time(); assert not REG.exists()
    assert read(ROOT/'PREFLIGHT.json')['status']=='PASS'
    hashes=dict(original.verify_manifest()['hashes'])
    jobs=[]
    for stage,(source,methods) in STAGES.items():
        protocols=read(original.ROOT/source/'run_manifest.json')['protocols']
        run=dict(protocols=protocols,methods=list(methods),hashes={p:sha(p) for p in protocols})
        queue.old.exclusive(ROOT/stage/'run_manifest.json',run)
        hashes[str(ROOT/stage/'run_manifest.json')]=sha(ROOT/stage/'run_manifest.json')
        hashes.update(run['hashes'])
        for method in methods:
            for protocol in protocols:
                job=[stage,protocol,method]
                out=ROOT/stage/'validation'/read(protocol)['date']/f'{method}.json'
                assert not out.exists()
                jobs.append(dict(id=queue.job_id(job),job=job,output=str(out),legacy_host='local',hosts=list(queue.HOSTS)))
    queue.old.exclusive(SPEC,dict(id=EXPERIMENT,jobs=jobs,hosts=dict(local=9,devbox1=6,devbox2=6,devbox3=6),
        deadline=original.END.timestamp(),dispatch='free slots, all four hosts; no retries; failure stops new claims'))
    paths=[Path(__file__).resolve(),Path('analysis/report_ground_representatives_20261031.py'),
           Path('analysis/verify_ground_representatives_20261031.py'),Path('tests/test_ground_representatives_20261031.py'),
           Path('analysis/preflight_ground_representatives_20261031.py'),Path(formal.__file__),
           Path(queue.__file__),Path('analysis/evaluation_queue.py'),ROOT/'SUPPLEMENT.md',ROOT/'PREFLIGHT.json',SPEC,
           original.ROOT/'report/FINAL_AUDIT.json',original.ROOT/'report/REPORT_HASHES.json',
           original.MAIN/'m_formal_test_20261027/FINAL_AUDIT.json',
           original.MAIN/'l_formal_test_20261028/independent_audit_20261028/FINAL_AUDIT.json',
           original.ROOT/'manifest.json',original.ROOT/'TEST_OPENED.json',
           original.ROOT/'http_queue_20261030/EXPERIMENT.json',
           original.ROOT/'distributed_20261030_v2/RUNTIME_ENVIRONMENTS.json']
    # 环境登记沿用旧路径。
    paths[-1]=original.ROOT/'distributed_20261030/RUNTIME_ENVIRONMENTS.json' if not paths[-1].exists() else paths[-1]
    for p in paths:hashes[str(p)]=sha(p)
    queue.old.exclusive(REG,dict(at=queue.now(),hashes=hashes,models={},methods=METHODS,
        original_manifest_sha256=sha(original.ROOT/'manifest.json'),deadline=original.END.isoformat(),
        authorization='User explicitly approved 36 scaled plus missing L single representatives; evaluation only',
        approximation='Teacher edge-based capacity quote unchanged; station contention not fully predicted'))
    verify()
    print(json.dumps(dict(status='REGISTERED',jobs=45,sha256=sha(REG))))


def publish():
    spec=verify()
    previous=queue.api('/status',dict(exp='ground_scaling_20261029'),retry=False)
    assert previous['complete'] and not previous['blocked']
    print(queue.api('/experiments',spec,retry=False))
    print(queue.api('/release',dict(exp=EXPERIMENT,host='local',completed={},evidence={str(REG):sha(REG)}),retry=False))


def collect(state):
    verify(); ownership={}; results=[]
    for item in state['jobs']:
        assert item['state']=='PASS'
        short=item['result'];out=Path(item['spec']['output'])
        assert short['path']==str(out)
        out.parent.mkdir(parents=True,exist_ok=True)
        if not out.exists():
            name=hashlib.sha256((EXPERIMENT+'/'+item['id']).encode()).hexdigest()
            source=queue.old.REMOTE_ROOT+'/'+str(original.ROOT/'http_queue_20261030/server/artifacts'/name)
            temp=out.with_suffix('.download')
            while True:
                original.check_time()
                try:
                    subprocess.run(['scp','-q','-o','ConnectTimeout=10',f'devbox3:{source}',str(temp)],check=True,timeout=120)
                    break
                except (subprocess.CalledProcessError,subprocess.TimeoutExpired):
                    time.sleep(30)
            assert sha(temp)==short['sha256'];temp.replace(out)
        assert sha(out)==short['sha256']
        row=read(out)
        assert [row['stage'],row['protocol'],row['method']]==item['spec']['job']
        write(out.with_suffix('.progress.json'),short)
        ownership[str(out)]=dict(owner=item['owner'],sha256=short['sha256'],job_id=item['id'])
        results.append(short)
    assert len(results)==len(ownership)==45
    write(ROOT/'DISTRIBUTED_AUDIT.json',dict(status='PASS',records=ownership,registration_sha256=sha(REG)))
    write(ROOT/'final_status.json',dict(status='COMPLETE',results=results,at=queue.now()))
    from analysis.report_ground_representatives_20261031 import main as report
    from analysis.verify_ground_representatives_20261031 import main as audit
    report();audit()
    write(ROOT/'REPORT_COMPLETE.json',dict(status='PASS',at=queue.now(),
        report_hashes_sha256=sha(ROOT/'report/HASHES.json'),audit_sha256=sha(ROOT/'report/INDEPENDENT_AUDIT.json')))


def configure_queue():
    queue.ROOT=ROOT;queue.REG=REG;queue.SPEC=SPEC;queue.MODULE=MODULE;queue.EXPERIMENT=EXPERIMENT
    queue.verify=verify;queue.execute=execute;queue.collect=collect
    queue.release_legacy=lambda host,spec: None


def coordinate(role):
    verify()
    while True:
        original.check_time()
        state=queue.api('/status',dict(exp=EXPERIMENT))
        write(ROOT/'QUEUE_STATUS.json',dict(at=queue.now(),**state))
        if state['blocked']:
            write(ROOT/role/'final_status.json',dict(status='BLOCKED',reason=state['blocked'],at=queue.now()))
            return
        if state['complete']:
            collect(state)
            write(ROOT/role/'final_status.json',dict(status='COMPLETE',total=45,at=queue.now()))
            return
        time.sleep(30)


def coordinate_guarded(role):
    try:
        coordinate(role)
    except Exception:
        write(ROOT/role/'final_status.json',dict(status='BLOCKED',at=queue.now(),traceback=traceback.format_exc()))
        raise


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=('register','verify','probe','publish','launch','supervise','worker','coordinate'))
    parser.add_argument('role',nargs='?',choices=(*queue.HOSTS,'coordinator'))
    args=parser.parse_args();configure_queue()
    if args.mode in ('register','verify','probe','publish'):globals()[args.mode]()
    elif args.mode=='coordinate':coordinate_guarded(args.role)
    else:getattr(queue,args.mode)(args.role)
