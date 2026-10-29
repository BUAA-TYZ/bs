"""M报告后续跑L精简验证；复用9项，单池按批补33项。"""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import multiprocessing
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

from analysis import run_scale_validation_20261027 as engine
from analysis.run_dynamic_learning import write_json
from analysis.run_dynamic_heuristics_v2 import read, sha, digest, configuration
from analysis.run_m_formal_20261027 import L_METHODS, MAPPO, EmptyPolicy
from analysis.sar_three_plan import ThreePlanChoice
from analysis.local_information_baselines import LocalInformationPlane
from sim.control import ControlPlane

MAIN = engine.MAIN
ROOT = MAIN/'l_reduced_validation_20261028'
OLD = MAIN/'scale_validation_20261027'
MROOT = MAIN/'m_formal_test_20261027'
PROTOCOL = MAIN/'L_REDUCED_VALIDATION_PROTOCOL_20261028.md'
DATES = ('2026-10-22','2026-10-23','2026-10-24')
METHODS = L_METHODS
END = datetime(2026,10,30,16,tzinfo=timezone.utc)


def check_time():
    if datetime.now(timezone.utc) >= END:
        raise TimeoutError('L validation cutoff 10-31 00:00 Beijing')


def verify_manifest():
    m=read(ROOT/'manifest.json')
    for path,h in m['hashes'].items():
        assert sha(path)==h,path
    return m


def validate_record(short, models):
    assert short['status']=='PASS' and sha(short['path'])==short['sha256']
    row=read(short['path']);p=read(row['protocol']);engine.verify(p)
    assert p['date'] in DATES and row['date']==p['date'] and row['method'] in METHODS
    assert p['score_end_s']-p['score_start_s']==3600
    assert p['config']['num_sats']==336 and len(p['config']['task_source_sat_ids'])==48
    assert row['config_sha256']==digest(configuration(p,row['method']))
    engine.check_record(row,p)
    if row['method'] in models:
        assert row['checkpoint']==models[row['method']]
        assert row['checkpoint_sha256']==sha(row['checkpoint'])
    return row


def pending_jobs(protocols, reused):
    known={(read(r['path'])['method'],read(r['path'])['date']) for r in reused}
    return [(p,n) for n in METHODS for p in protocols if (n,read(p)['date']) not in known]


def reuse_records(pause):
    return [dict(status=r['status'],path=r['output'],sha256=r['sha256'],reused=True)
            for r in pause['results']]


def prepare():
    assert not ROOT.exists(),'No overwrite'
    check_time();engine.verify_main()
    assert read(MROOT/'M_NOTIFIED.json')['status']=='NOTIFIED'
    assert read(MROOT/'FOLLOWUP_AUDIT_20261028.json')['status']=='PASS'
    assert not (MAIN/'L_TEST_OPENED.json').exists()
    assert sha(OLD/'manifest.json')=='fa47bba78bcf777bb758505a6736e244652fc8a5eadab7f6c2b0467d300ba497'
    old=read(OLD/'manifest.json');run=read(OLD/'run_manifest.json');pause=read(OLD/'PAUSED_BY_USER.json')
    assert pause['old_coordinator_must_not_resume'] and pause['queued_tasks_cancelled'] and not pause['unexpected_progress']
    results=reuse_records(pause);assert len(results)==9
    keys=set()
    for short in results:
        row=validate_record(short,old['models']);keys.add((row['method'],row['date']))
    assert keys=={(f'fixed_{s}',d) for s in (1701,1702,1703) for d in DATES}
    assert len(pending_jobs(run['protocols'],results))==33
    hashes={**old['hashes'],**run['hashes'],**{r['path']:r['sha256'] for r in results}}
    for path in [Path(__file__),PROTOCOL,OLD/'PAUSED_BY_USER.json',OLD/'run_manifest.json',
                 MROOT/'M_NOTIFIED.json',MROOT/'FOLLOWUP_AUDIT_20261028.json',
                 Path('analysis/run_m_formal_20261027.py'),Path('tests/test_l_reduced_validation_20261028.py'),
                 MAIN/'M_TEST_PRIORITY_AMENDMENT_20261027.md']:
        hashes[str(path)]=sha(path)
    for path,h in hashes.items():
        assert sha(path)==h,path
    manifest=dict(hashes=hashes,models=old['models'],ranges=old['ranges'],dates=DATES,methods=METHODS,
        protocols=run['protocols'],reused=results,workers=9,torch_threads=1,no_retry=True,zero_shot=True,
        l_tests_sealed=True,deadline=END.isoformat(),comparison_scopes_status='INCOMPLETE_NOT_DETERMINABLE',
        finetune_diagnostic_rules=['minload_2_4','minload_2_16','zero_2_16'],finetuning_scheduled=False)
    write_json(ROOT/'manifest.json',manifest)
    write_json(ROOT/'run_manifest.json',dict(protocols=run['protocols'],methods=METHODS,
        hashes={**run['hashes'],str(ROOT/'manifest.json'):sha(ROOT/'manifest.json')},l_tests_sealed=True))
    print('REGISTERED',sha(ROOT/'manifest.json'))


def install_adapter(method):
    assert method in METHODS
    engine.ROOT=ROOT;engine.VALID=DATES;engine.METHODS=METHODS;engine.LEARNED=MAPPO
    engine.verify_manifest=verify_manifest;engine.check_time=check_time
    # 每次任务重置，防止同一worker先跑强地面后污染其他方法。
    engine.ThreePlanChoice=(lambda hops:EmptyPolicy()) if method=='ground_16' else ThreePlanChoice
    engine.ControlPlane=LocalInformationPlane if method=='ground_16' else ControlPlane


def simulate(args):
    path,method=args
    try:
        assert read(path)['date'] in DATES,'L validation dates only; test remains sealed'
        install_adapter(method)
        return engine.simulate(args)
    except Exception:
        return dict(status='FAILED',protocol=path,method=method,traceback=traceback.format_exc())


def execute_batches(pool, jobs, reused):
    results=list(reused)
    for offset in range(0,len(jobs),9):
        check_time()
        batch=[pool.submit(simulate,args) for args in jobs[offset:offset+9]]
        failed=False
        for future in as_completed(batch):
            try:short=future.result()
            except Exception:short=dict(status='FAILED',traceback=traceback.format_exc())
            results.append(short);failed=failed or short['status']!='PASS'
            write_json(ROOT/'progress.json',dict(stage='validation',done=len(results),total=42,
                reused=9,new_done=len(results)-9,batch=offset//9+1,results=results,l_tests_sealed=True))
        if failed:
            break
    return results


def panel():
    started=time.perf_counter()
    try:
        check_time();m=verify_manifest();assert not (ROOT/'progress.json').exists()
        jobs=pending_jobs(m['protocols'],m['reused']);assert len(jobs)==33
        write_json(ROOT/'progress.json',dict(stage='validation',done=9,total=42,new_done=0,reused=9,l_tests_sealed=True))
        with ProcessPoolExecutor(max_workers=9,mp_context=multiprocessing.get_context('spawn')) as pool:
            results=execute_batches(pool,jobs,m['reused'])
        verify_manifest();passed=len(results)==42 and all(r['status']=='PASS' for r in results)
        if passed:
            for short in results:validate_record(short,m['models'])
            write_json(ROOT/'COMPLETION_CHECK.json',dict(status='PASS',records=42,reused=9,new=33,
                hashes={r['path']:r['sha256'] for r in results},independent_report_pending=True))
        write_json(ROOT/'final_status.json',dict(status='COMPLETE' if passed else 'BLOCKED',results=results,
            l_tests_sealed=True,independent_report_pending=passed,total_wall_seconds=time.perf_counter()-started))
    except Exception:
        write_json(ROOT/'final_status.json',dict(status='BLOCKED',traceback=traceback.format_exc(),l_tests_sealed=True))


def supervise():
    verify_manifest();command=[sys.executable,'-B','-m','analysis.run_l_reduced_validation_20261028','panel']
    with (ROOT/'panel.log').open('xb') as log:
        child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    awake=subprocess.Popen(['/usr/bin/caffeinate','-i','-w',str(child.pid)],start_new_session=True)
    identity=subprocess.check_output(['ps','-p',str(child.pid),'-o','pid=,lstart=,command='],text=True).strip()
    write_json(ROOT/'launch.json',dict(pid=child.pid,supervisor_pid=os.getpid(),caffeinate_pid=awake.pid,command=command,
        identity=identity,started_at=datetime.now(timezone.utc).isoformat(),manifest_sha256=sha(ROOT/'manifest.json')))
    timed_out=False
    while child.poll() is None:
        if datetime.now(timezone.utc)>=END:
            timed_out=True;os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=10)
            except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait()
            break
        time.sleep(2)
    if timed_out or child.returncode or not (ROOT/'final_status.json').exists():
        write_json(ROOT/'supervisor_failure.json',dict(status='BLOCKED',timeout=timed_out,returncode=child.returncode,
            at=datetime.now(timezone.utc).isoformat()))


def launch():
    check_time();verify_manifest();assert not (ROOT/'launch_request.json').exists()
    assert not (ROOT/'progress.json').exists()
    assert not (MAIN/'L_TEST_OPENED.json').exists()
    processes=subprocess.check_output(['ps','-axo','command='],text=True).splitlines()
    assert not any((' -m analysis.run_' in c and c.rstrip().endswith((' panel',' supervise'))) or
                   'multiprocessing.spawn import spawn_main' in c for c in processes),'Another experiment pool is active'
    with (ROOT/'supervisor.log').open('xb') as log:
        child=subprocess.Popen([sys.executable,'-B','-m','analysis.run_l_reduced_validation_20261028','supervise'],
            stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    write_json(ROOT/'launch_request.json',dict(supervisor_pid=child.pid,at=datetime.now(timezone.utc).isoformat()))
    print('SUPERVISOR',child.pid)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['prepare','launch','panel','supervise'])
    globals()[parser.parse_args().mode]()
