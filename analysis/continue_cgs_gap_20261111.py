"""本次已授权分析的持久接续；每阶段只执行一次，不重试科学任务。"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import shlex
import statistics
import subprocess
import sys
import time
import traceback

from analysis import run_cgs_gap_20261111 as r

REPO=Path('/Users/bytedance/Projects/bs')
PYTHON=str(REPO/'.venv/bin/python')
HOSTS=('devbox1','devbox2','devbox3')
ENV=dict(os.environ,OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',
         VECLIB_MAXIMUM_THREADS='1',PYTHONDONTWRITEBYTECODE='1')


def state(stage,**values):
    r.write(r.ROOT/'continuation/status.json',dict(at=r.now(),pid=os.getpid(),stage=stage,**values),exclusive=False)


def run(module,*args):
    log=r.ROOT/'continuation'/f'{module.rsplit(".",1)[-1]}_{"_".join(args) or "main"}.log'
    log.parent.mkdir(parents=True,exist_ok=True)
    started=time.time()
    with log.open('xb') as output:
        proc=subprocess.run([PYTHON,'-B','-m',module,*args],cwd=REPO,env=ENV,stdout=output,stderr=subprocess.STDOUT)
    assert proc.returncode==0,(module,args,proc.returncode,str(log))
    return dict(module=module,args=args,log=str(log),wall_seconds=time.time()-started)


def estimate():
    manifest=r.read(r.folder('enumeration')/'manifest.json')
    pilots=[r.read(p) for p in (r.ROOT/'timing').glob('*.json')]
    rates={b:statistics.median(p['seconds_per_quote'] for p in pilots if p['plan']['blocks']==b) for b in (4,16)}
    reference=next(p['seconds_per_quote'] for p in pilots if p['snapshot']['sample']['id']=='2026-10-23_task_9')
    speed={'local':1.}
    for host in HOSTS:
        speed[host]=reference/r.read(r.ROOT/'deployment'/f'HOST_PREFLIGHT_{host}.json')['seconds_per_quote']
    entries={e['snapshot']['sample']['id']:e for e in r.read(r.ROOT/'CANDIDATES.json')['samples']}
    seconds=sum(j['quote_budget_upper']*rates[entries[j['sample_id']]['plan']['blocks']] for j in manifest['jobs'])
    ideal=seconds/sum(6*v for v in speed.values())
    tail=max(j['quote_budget_upper']*rates[entries[j['sample_id']]['plan']['blocks']] for j in manifest['jobs'])/min(speed.values())
    lower=1.3*ideal+tail
    upper=2.0*ideal+tail+1800
    now=datetime.now(timezone.utc)
    value=dict(at=r.now(),tasks=len(manifest['jobs']),quote_budget_upper=sum(j['quote_budget_upper'] for j in manifest['jobs']),
        mac_median_seconds_per_quote=rates,relative_per_slot_speed=speed,slots={h:6 for h in ('local',*HOSTS)},
        ideal_parallel_seconds=ideal,estimated_enumeration_hours=[lower/3600,upper/3600],
        estimated_report_finish_utc=[(now+timedelta(seconds=v)).isoformat() for v in (lower,upper)],
        assumptions='Measured single-slot rates; 30%-100% throughput allowance plus slowest whole shard tail and up to 30 minutes final collection/audit. Local search uses registered worst-case 20-round budget. Not a completion guarantee.',
        over_two_hours_warning_already_given=True)
    r.write(r.ROOT/'RESOURCE_ESTIMATE.json',value)
    return value


def deploy(host,files):
    started=time.time()
    proc=subprocess.run(['rsync','-a','--checksum','--stats','--files-from=-','-e',
        'ssh -o BatchMode=yes -o ConnectTimeout=15','./',f'{host}:{REPO}/'],
        input='\n'.join(files)+'\n',text=True,capture_output=True,cwd=REPO,timeout=600)
    value=dict(at=r.now(),host=host,returncode=proc.returncode,stdout=proc.stdout,stderr=proc.stderr,wall_seconds=time.time()-started)
    r.write(r.ROOT/'deployment'/f'{host}_enumeration.json',value)
    assert proc.returncode==0,(host,proc.stderr)
    cmd=f'cd {shlex.quote(str(REPO))} && {shlex.quote(PYTHON)} -B -m analysis.run_cgs_gap_shards_20261111 verify'
    check=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15',host,cmd],capture_output=True,text=True,timeout=120)
    r.write(r.ROOT/'deployment'/f'{host}_enumeration_verify.json',dict(at=r.now(),returncode=check.returncode,stdout=check.stdout,stderr=check.stderr))
    assert check.returncode==0,(host,check.stderr)
    return host


def launch(host):
    program=f'''
import json,os,subprocess
from pathlib import Path
os.chdir({str(REPO)!r})
root=Path({str(r.folder('enumeration'))!r})
log=root/'workers'/{host!r}/'stdout.log';log.parent.mkdir(parents=True,exist_ok=True)
env=dict(os.environ,OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',VECLIB_MAXIMUM_THREADS='1',PYTHONDONTWRITEBYTECODE='1')
command=[{PYTHON!r},'-B','-m','analysis.run_cgs_gap_shards_20261111','worker','--host',{host!r}]
with log.open('xb') as output:
 p=subprocess.Popen(command,env=env,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
print(json.dumps(dict(host={host!r},pid=p.pid,command=command)))
'''
    if host=='local':cmd=[PYTHON,'-B','-']
    else:cmd=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15',host,f'{shlex.quote(PYTHON)} -B -']
    proc=subprocess.run(cmd,input=program,text=True,capture_output=True,timeout=90)
    assert proc.returncode==0,(host,proc.stderr)
    value=json.loads(proc.stdout);value['at']=r.now()
    if host=='local':value['caffeinate_pid']=subprocess.Popen(['caffeinate','-i','-w',str(value['pid'])],start_new_session=True).pid
    r.write(r.folder('enumeration')/f'LAUNCH_{host}.json',value)
    return value


def main():
    started=time.time();state('WAITING_REPLAY')
    while True:
        path=r.folder('replay')/'workers/local/final_status.json'
        failures=list((r.folder('replay')/'failures').glob('*.json'))
        assert not failures,failures
        if path.exists():
            assert r.read(path)['status']=='COMPLETE',r.read(path)
            break
        time.sleep(30)
    steps=[run('analysis.plan_cgs_gap_20261111','pilot')]
    state('PREPARING_ENUMERATION');steps.append(run('analysis.run_cgs_gap_shards_20261111','prepare'))
    value=estimate();state('DEPLOYING_ENUMERATION',estimate=value)
    manifest=r.read(r.folder('enumeration')/'manifest.json');files=set()
    for name in manifest['hashes']:
        p=Path(name);files.add(str(p.relative_to(REPO) if p.is_absolute() else p))
    files.update(str(r.folder('enumeration')/name) for name in ('manifest.json','REGISTRATION.json'))
    with ThreadPoolExecutor(max_workers=3) as pool:list(pool.map(lambda host:deploy(host,sorted(files)),HOSTS))
    steps.append(run('analysis.run_cgs_gap_shards_20261111','publish'))
    launches=[launch(host) for host in ('local',*HOSTS)]
    r.write(r.ROOT/'continuation/ENUMERATION_LAUNCHED.json',dict(at=r.now(),launches=launches,estimate=value))
    while True:
        try:queue=r.queue.api('/status',exp=r.experiment('enumeration'),retry=False)
        except Exception as error:
            state('STATUS_UNAVAILABLE',error=type(error).__name__,note='No task restart, lease reclaim or failure inference')
            time.sleep(30);continue
        state('ENUMERATING',counts=queue['counts'],blocked=queue['blocked'],estimate=value)
        r.write(r.folder('enumeration')/'QUEUE_STATUS.json',queue,exclusive=False)
        assert not queue['blocked'],queue['blocked']
        if queue['complete']:break
        time.sleep(30)
    state('COLLECTING');steps.append(run('analysis.collect_cgs_gap_20261111'))
    state('AUDITING');steps.append(run('analysis.report_cgs_gap_20261111'))
    r.write(r.ROOT/'continuation/FINAL.json',dict(status='PASS',at=r.now(),wall_seconds=time.time()-started,steps=steps))
    state('REPORT_COMPLETE_PENDING_REVIEW_AND_PUSH')


if __name__=='__main__':
    try:main()
    except Exception:
        r.write(r.ROOT/'continuation/FAILURE.json',dict(at=r.now(),traceback=traceback.format_exc()))
        state('BLOCKED');raise
