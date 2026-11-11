"""独立核对跨机验证输出，不读取正式测试表现来接受或选择机器。"""
import math
from numbers import Real
from pathlib import Path
import sys
from analysis.run_dynamic_heuristics_v2 import read, sha
from analysis.run_dynamic_learning import write_json

ROOT = Path('results/redesign_20261021/main_scenario/ground_scaling_20261029/distributed_20261030')


def compare(a,b,path=''):
    if isinstance(a,dict):
        assert isinstance(b,dict) and set(a)==set(b),path
        for k in a: compare(a[k],b[k],f'{path}/{k}')
    elif isinstance(a,list):
        assert isinstance(b,list) and len(a)==len(b),path
        for i,(x,y) in enumerate(zip(a,b)): compare(x,y,f'{path}/{i}')
    elif isinstance(a,Real) and not isinstance(a,bool):
        if 'observation' in path or isinstance(a,int):
            assert a==b,(path,a,b)
        else:
            prob = '_prob' in path
            assert math.isclose(a,b,rel_tol=1e-4 if prob else 1e-9,abs_tol=1e-5 if prob else 1e-6),(path,a,b)
    else:
        assert a==b,(path,a,b)


def main():
    reference=read(ROOT/'preflight_mac.json'); checked={}
    for host in ('devbox1','devbox2','devbox3'):
        p=ROOT/f'preflight_{host}.json'; row=read(p)
        assert row['status']==reference['status']=='PASS'
        assert row['validation_sha256']==reference['validation_sha256']
        assert row['manifest_sha256']==reference['manifest_sha256']
        assert row['source_sha256']==reference['source_sha256']
        for pkg,version in reference['environment']['packages'].items():
            assert row['environment']['packages'][pkg].split('+')[0]==version.split('+')[0],(host,pkg)
        assert row['environment']['python'].split()[0]==reference['environment']['python'].split()[0]
        compare(reference['geometry'],row['geometry'],'geometry')
        compare(reference['toy'],row['toy'],'toy')
        checked[host]=dict(sha256=sha(p),environment=row['environment'],wall_seconds=row['wall_seconds'])
    write_json(ROOT/'PREFLIGHT_AUDIT.json',dict(status='PASS',reference_sha256=sha(ROOT/'preflight_mac.json'),
        hosts=checked,real_validation_cases_per_host=20,toy_followup600_cases_per_host=20,
        rules='Float32 actor inputs/masks/discrete actions/toy traces exact; probabilities atol1e-5/rtol1e-4; other physical floats atol1e-6/rtol1e-9',
        limitation='Validation smoke and toy equality are not proof of bitwise equality for all full test trajectories'))
    print('PREFLIGHT_AUDIT PASS',flush=True)


if __name__=='__main__': main()
