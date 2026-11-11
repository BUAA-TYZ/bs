"""两层无争用分析的三个验证日离线任务，沿用HTTP原子领取。"""
import argparse
from copy import deepcopy
import gzip
import json
from pathlib import Path
import time
import traceback

from analysis import run_nonuniform_gate1_20261111 as queue
from analysis import run_teacher_analysis_20261111 as ablation

ROOT = ablation.ROOT/'bound_queue'
EXPERIMENT = 'teacher_bounds_20261111'
DOC = Path('docs/thesis/TEACHER_BOUND_IMPLEMENTATION_20261111.md')
read,write,sha,now = queue.read,queue.write,queue.sha,queue.now


def verify_manifest():
    registration = read(ROOT/'REGISTRATION.json'); m = read(ROOT/'manifest.json')
    assert sha(ROOT/'manifest.json') == registration['manifest_sha256']
    for path,value in m['hashes'].items(): assert sha(path) == value,path
    return m


def prepare():
    base = ablation.verify_manifest()
    assert read(ROOT/'TESTS_PASS.json')['status'] == 'PASS'
    hashes = dict(base['hashes'])
    for path in (DOC,Path(__file__).relative_to(Path.cwd()) if Path(__file__).is_absolute() else Path(__file__),
                 Path('analysis/teacher_bounds_20261111.py'),Path('tests/test_teacher_bounds_20261111.py'),
                 ROOT/'TESTS_PASS.json',ROOT/'tests.log',ablation.ROOT/'REGISTRATION.json'):
        hashes[str(path)] = sha(path)
    jobs = [dict(id=f'{date}_bounds',job=['TEACHER_BOUNDS',path,'bounds'],
        output=str(ablation.ROOT/'bounds'/f'{date}.json'),legacy_host='local',hosts=['local'])
        for date,path in zip(ablation.DATES,base['protocols'])]
    write(ROOT/'manifest.json',dict(hashes=hashes,protocols=base['protocols'],jobs=jobs))
    write(ROOT/'REGISTRATION.json',dict(at=now(),manifest_sha256=sha(ROOT/'manifest.json'),
        spec=dict(id=EXPERIMENT,deadline=time.time()+7*86400,hosts={'local':3},jobs=jobs),
        final_tests_sealed=True,no_retry=True,estimated_run_hours=[0.5,1.0]))
    print({'registration_sha256':sha(ROOT/'REGISTRATION.json'),'files':len(hashes),'tasks':3})


def execute(job,host):
    started = time.time(); env = None
    try:
        import torch
        from analysis.dynamic_granularity_v2 import GranularityEnv
        from analysis.teacher_bounds_20261111 import geometry_and_reference,TemporalGraph,certified_image_bound
        from sim.config import config_from_dict
        from analysis.run_load_pressure_short import verify
        torch.set_num_threads(1)
        m = verify_manifest(); assert job in m['jobs'] and host == 'local'
        path = job['job'][1]; p = read(path); verify(p)
        config = deepcopy(p['config']); config['scheduled_task_arrivals'] = []
        env = GranularityEnv(config_from_dict(config))
        def progress(value):
            write(ROOT/'progress'/f"{p['date']}.json",dict(at=now(),wall_seconds=time.time()-started,**value),exclusive=False)
        contacts,reference,geometry = geometry_and_reference(env,p,progress)
        graph = TemporalGraph(contacts); wanted = set(p['scored_task_ids']); lower = []
        for index,event in enumerate(p['config']['scheduled_task_arrivals']):
            task_id = f'task_{index}'
            if task_id in wanted:
                assert (event['release_step']*env.cfg.dt+600 <= p['sim_duration_s']+1e-8)
                lower.append(certified_image_bound(graph,env.cfg,event,task_id))
        graph_path = ablation.ROOT/'bounds'/f"{p['date']}_contacts.json.gz"
        graph_path.parent.mkdir(parents=True,exist_ok=True)
        with graph_path.open('xb') as output:
            output.write(gzip.compress(json.dumps(contacts,separators=(',',':')).encode(),mtime=0))
        result = dict(status='PASS',stage='TEACHER_BOUNDS',protocol=path,method='bounds',date=p['date'],
            host=host,job_id=job['id'],reference=reference,lower=lower,geometry=geometry,
            contacts_path=str(graph_path),contacts_sha256=sha(graph_path),runtime=queue.runtime(),
            registration_sha256=sha(ROOT/'REGISTRATION.json'),manifest_sha256=sha(ROOT/'manifest.json'),
            finished_utc=now(),wall_seconds=time.time()-started)
        verify_manifest(); write(job['output'],result)
        progress(dict(phase='COMPLETE',images=len(lower)))
        return dict(status='PASS',path=job['output'],sha256=sha(job['output']))
    except Exception:
        failure = dict(status='FAILED',job=job['id'],traceback=traceback.format_exc())
        write(ROOT/'failures'/f"{job['id']}.json",failure)
        return failure
    finally:
        if env is not None: env.close()


def configure():
    queue.ROOT = ROOT; queue.EXPERIMENT = EXPERIMENT; queue.HOSTS = {'local':3}
    queue.verify_manifest = verify_manifest; queue.execute = execute


if __name__ == '__main__':
    parser = argparse.ArgumentParser();parser.add_argument('mode',choices=['prepare','publish','worker','verify','status'])
    args = parser.parse_args();configure()
    if args.mode == 'prepare':prepare()
    elif args.mode == 'publish':queue.publish()
    elif args.mode == 'worker':queue.worker('local')
    elif args.mode == 'verify':print({'files':len(verify_manifest()['hashes'])})
    else:print(queue.api('/status',exp=EXPERIMENT))
