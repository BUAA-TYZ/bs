"""只读取所选M模型实际训练谱系，冻结规模泛化编码空间参考范围。"""
from copy import deepcopy
import gzip
import json
from pathlib import Path

from analysis.run_dynamic_learning import write_json
from analysis.run_scale_probe_20261027 import MAIN, read, sha
from analysis.scale_feature_audit_20261027 import add_ranges, merge_ranges, NODE_NAMES, IMAGE_NAMES

ROOT = MAIN/'m_training_ranges_20261027'
PROTOCOL = MAIN/'SCALE_VALIDATION_PROTOCOL_20261027.md'
FIXED_END = {'1701':352,'1702':176,'1703':176}
DYNAMIC_END = {'1701':88,'1702':88,'1703':176}


def rollout_sources(family, seed, limit):
    root = MAIN/('mappo_20261022' if family=='fixed' else 'dynamic_learning_20261026')
    folders = [root/'updates'/seed]
    if family=='fixed': folders.append(root/'resume_20261025/updates'/seed)
    rows = {}; hashes = {}
    for folder in folders:
        for path in sorted(folder.glob('*.json')):
            if int(path.stem)>limit: continue
            u = read(path); assert u['status']=='PASS'; hashes[str(path)] = sha(path)
            for name,h in u['rollout_hashes'].items():
                number = int(Path(name).name.split('.')[0]); assert number<=limit
                assert number not in rows or rows[number]==(name,h)
                rows[number]=(name,h)
    assert sorted(rows)==list(range(1,limit+1)),(family,seed,len(rows),limit)
    hashes.update(dict(rows.values())); return [rows[i][0] for i in sorted(rows)],hashes


def prepare():
    assert not ROOT.exists()
    hashes = {str(p):sha(p) for p in (Path(__file__),Path('analysis/scale_feature_audit_20261027.py'),PROTOCOL,
        MAIN/'scale_probe_20261027/independent_audit_20261027/RESOURCE_DECISION.json',
        MAIN/'fixed_mappo_frozen_20261025/manifest.json',MAIN/'dynamic_learning_20261026/BEST_CHECKPOINTS.json')}
    bc = read(MAIN/'bc_stage2_20261022/manifest.json'); sources={}; models={}
    for family in ('fixed','dynamic'):
        teacher_root = MAIN/('bc_stage2_20261022/teachers' if family=='fixed' else 'dynamic_teachers_20261026/episodes')
        teachers=[]
        for p in bc['train']:
            protocol=read(p); assert '2026-10-16'<=protocol['date']<='2026-10-21'
            name=Path(p).parent.parent.name+'_'+Path(p).parent.name
            path=teacher_root/name/'teacher.json'; teachers.append(dict(path=str(path),protocol=p))
            hashes[str(path)]=sha(path); hashes[p]=sha(p)
        sources[family+'_bc']=teachers
    best=read(MAIN/'dynamic_learning_20261026/BEST_CHECKPOINTS.json')
    for seed in FIXED_END:
        models['fixed_'+seed]=str(MAIN/'fixed_mappo_frozen_20261025'/f'mappo_{seed}.pt')
        models['dynamic_'+seed]=best[seed]['checkpoint']
        for family,limit in (('fixed',FIXED_END[seed]),('dynamic',DYNAMIC_END[seed])):
            paths,hs=rollout_sources(family,seed,limit); sources[family+'_'+seed]=paths;hashes.update(hs)
    for path in models.values():hashes[path]=sha(path)
    for path,h in hashes.items(): assert sha(path)==h,path
    write_json(ROOT/'registration.json',dict(hashes=hashes,sources=sources,models=models,
        fixed_limits=FIXED_END,dynamic_limits=DYNAMIC_END,tests_sealed=True,
        reference='BC plus actually updated MAPPO through selected checkpoint, including fixed ancestors for dynamic',
        categorical_node=[0,1],categorical_image=[2,5],tolerance=1e-6,
        node_names=NODE_NAMES,image_names=IMAGE_NAMES,padding_excluded=True,
        preclip_values='Unavailable historically; encoded-space audit only',no_simulation=True))


def build():
    reg=read(ROOT/'registration.json'); assert not (ROOT/'ranges.json').exists()
    for path,h in reg['hashes'].items():assert sha(path)==h,path
    base={};counts={};ranges={}
    for family in ('fixed','dynamic'):
        base[family]={};count=0
        for item in reg['sources'][family+'_bc']:
            data=read(item['path']);p=read(item['protocol']);assert data['status']=='PASS'
            for task in p['scored_task_ids']:
                if task not in data['labels']:continue
                row=data['labels'][task];obs=row['observation']
                if not obs['image_mask'][row['label']['mode']]:continue
                add_ranges(base[family],obs);count+=1
        counts[family+'_bc']=count
    for seed in FIXED_END:
        for family in ('fixed','dynamic'):
            key=family+'_'+seed
            r=(deepcopy(base['fixed']) if family=='fixed' else merge_ranges(ranges['fixed_'+seed],base['dynamic']))
            count=0
            for path in reg['sources'][key]:
                with gzip.open(path,'rt') as stream:data=json.load(stream)
                assert data['status']=='PASS' and data['checks']['passed']
                job=data['job'];assert job['kind']=='train' and job['seed']==int(seed)
                p=read(job['protocol']);assert '2026-10-16'<=p['date']<='2026-10-21'
                wanted=set(p['scored_task_ids'])
                for row in data['records']:
                    assert row['task_id'] in wanted
                    add_ranges(r,row['observation']);count+=1
            ranges[key]=r;counts[key]=count
            write_json(ROOT/'progress.json',dict(status='RUNNING',completed=list(ranges),observations=counts))
    for path,h in reg['hashes'].items():assert sha(path)==h,path
    write_json(ROOT/'ranges.json',dict(status='PASS',ranges=ranges,counts=counts,
        registration_sha256=sha(ROOT/'registration.json'),tests_sealed=True,
        inherited_fixed_observations_in_dynamic=True,tolerance=1e-6))
    write_json(ROOT/'HASHES.json',{str(p):sha(p) for p in (ROOT/'registration.json',ROOT/'ranges.json')})
    write_json(ROOT/'progress.json',dict(status='COMPLETE',observations=counts))
    print({'status':'PASS','counts':counts,'ranges_sha256':sha(ROOT/'ranges.json')})


if __name__=='__main__':
    prepare();build()
