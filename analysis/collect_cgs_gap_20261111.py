"""完成后只读核验队列服务器原字节，取回自包含的报价原始记录。"""
import base64
import hashlib
import json
from pathlib import Path
import subprocess

from analysis import run_cgs_gap_20261111 as r

REMOTE = r'''
import base64,hashlib,json,sqlite3
from collections import Counter
from pathlib import Path
p=Path('/Users/bytedance/Projects/bs/results/redesign_20261021/main_scenario/ground_scaling_20261029/http_queue_20261030/server/queue.sqlite')
db=sqlite3.connect('file:'+str(p)+'?mode=ro',uri=True)
exp='cgs_gap_enumeration_20261111'
claims=Counter();finishes=Counter();owners={}
for kind,body in db.execute('SELECT kind,body FROM events WHERE experiment=? ORDER BY rowid',(exp,)):
 e=json.loads(body)
 if kind=='CLAIM':
  for jid in e['ids']:claims[jid]+=1;owners[jid]=e['owner']
 if kind=='FINISH':
  assert e['status']=='PASS';finishes[e['id']]+=1;assert owners[e['id']]==e['owner']
records=[]
for jid,state,owner,result in db.execute('SELECT id,state,owner,result FROM jobs WHERE experiment=? ORDER BY rowid',(exp,)):
 assert state=='PASS' and claims[jid]==finishes[jid]==1 and owner==owners[jid]
 short=json.loads(result)
 data=(p.parent/'artifacts'/hashlib.sha256((exp+'/'+jid).encode()).hexdigest()).read_bytes()
 assert hashlib.sha256(data).hexdigest()==short['sha256']
 records.append(dict(id=jid,owner=owner,sha256=short['sha256'],bytes=len(data),blob=base64.b64encode(data).decode()))
print(json.dumps(dict(status='PASS',records=records)))
'''


def preserve(path,data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():assert path.read_bytes()==data,path
    else:
        with path.open('xb') as output:output.write(data)


def main():
    manifest=r.verify_manifest('enumeration')
    final=r.queue.api('/status',exp=r.experiment('enumeration'))
    assert final['complete'] and final['counts']=={'PASS':len(manifest['jobs'])} and not final['blocked']
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','devbox3',
        '/Users/bytedance/Projects/bs/.venv/bin/python -B -'],input=REMOTE,text=True,capture_output=True,timeout=120)
    assert result.returncode==0,result.stderr
    remote=json.loads(result.stdout);jobs={j['id']:j for j in manifest['jobs']};audit=[]
    assert {row['id'] for row in remote['records']}==set(jobs)
    for row in remote['records']:
        job=jobs[row['id']];data=base64.b64decode(row.pop('blob'));record=json.loads(data)
        assert hashlib.sha256(data).hexdigest()==row['sha256']
        assert [record['stage'],record['protocol'],record['method']]==job['job']
        assert record['job_id']==job['id'] and record['sample_id']==job['sample_id']
        assert record['host'] in job['hosts'] and row['owner'].startswith(record['host']+':')
        preserve(job['output'],data)
        raw=base64.b64decode(record['raw_gzip_b64'])
        assert hashlib.sha256(raw).hexdigest()==record['raw_sha256']
        expected=r.ROOT/'enumeration/raw'/f"{job['id']}.jsonl.gz"
        assert Path(record['raw_path'])==expected
        preserve(expected,raw)
        audit.append(dict(**row,raw_sha256=record['raw_sha256'],claim_count=1,finish_count=1))
    r.write(r.folder('enumeration')/'QUEUE_FINAL.json',final)
    r.write(r.ROOT/'SERVER_ARTIFACT_AUDIT.json',dict(status='PASS',at=r.now(),tasks=len(audit),jobs=audit,
        access='Read-only SQLite and original artifact bytes; embedded raw gzip decoded and hash checked'))
    print(dict(status='PASS',tasks=len(audit),artifacts_downloaded=True))


if __name__=='__main__':main()
