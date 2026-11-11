"""在真实spawn子进程检查完整冻结校验链，不创建仿真环境。"""
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import os
from pathlib import Path
import sys
from analysis import run_ground_scaling_20261029 as r


def check():
    assert os.getcwd()=='/Users/bytedance/Projects/bs'
    assert Path.cwd().resolve()==Path('/Users/bytedance/Projects/bs')
    from analysis.run_calibration_capacity import implementation_hashes,source_hashes
    r.verify_manifest()
    protocols=r.read(r.ROOT/'M_scaled/run_manifest.json')['protocols']
    for path in protocols:
        p=r.read(path)
        r.engine.verify(p)
        assert source_hashes()==p['frozen_protocol']['source_hashes']
        assert implementation_hashes()==p['frozen_protocol']['implementation_hashes']
    return dict(status='PASS',pid=os.getpid(),cwd=os.getcwd(),module=r.__file__,
        protocols={p:r.sha(p) for p in protocols},implementation_hashes=implementation_hashes(),
        source_hashes=source_hashes(),manifest_sha256=r.sha(r.ROOT/'manifest.json'),
        no_simulation_environment_created=True)


if __name__=='__main__':
    with ProcessPoolExecutor(max_workers=1,mp_context=multiprocessing.get_context('spawn')) as pool:
        result=pool.submit(check).result()
    dest=r.ROOT/'distributed_20261030_v2'/f'PATH_PREFLIGHT_{sys.argv[1]}.json'
    assert not dest.exists()
    r.write_json(dest,result)
    print(dest,flush=True)
