"""持久化评估队列；断联不回收任务，失败停止派发，结果提交幂等。"""
import argparse
import base64
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import sqlite3
import time


def packed(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def checksum(data):
    return hashlib.sha256(data).hexdigest()


class Queue:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS experiments (
                    id TEXT PRIMARY KEY, spec TEXT NOT NULL, blocked TEXT);
                CREATE TABLE IF NOT EXISTS jobs (
                    experiment TEXT, id TEXT, spec TEXT NOT NULL,
                    state TEXT NOT NULL, owner TEXT, heartbeat REAL, result TEXT,
                    PRIMARY KEY (experiment, id));
                CREATE TABLE IF NOT EXISTS releases (
                    experiment TEXT, host TEXT, body TEXT NOT NULL,
                    PRIMARY KEY (experiment, host));
                CREATE TABLE IF NOT EXISTS claims (
                    experiment TEXT, request TEXT, body TEXT, response TEXT,
                    PRIMARY KEY (experiment, request));
                CREATE TABLE IF NOT EXISTS events (
                    at REAL, experiment TEXT, kind TEXT, body TEXT);
            ''')

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def event(db, exp, kind, body):
        db.execute('INSERT INTO events VALUES (?,?,?,?)', (time.time(), exp, kind, packed(body)))

    def create(self, spec):
        exp = spec['id']
        assert len({j['id'] for j in spec['jobs']}) == len(spec['jobs'])
        with self.transaction() as db:
            old = db.execute('SELECT spec FROM experiments WHERE id=?', (exp,)).fetchone()
            if old:
                assert old['spec'] == packed(spec), 'Experiment already registered with different content'
                return {'status': 'EXISTS'}
            assert spec['deadline'] > time.time()
            db.execute('INSERT INTO experiments VALUES (?,?,NULL)', (exp, packed(spec)))
            for job in spec['jobs']:
                db.execute('INSERT INTO jobs VALUES (?,?,?,?,NULL,NULL,NULL)',
                           (exp, job['id'], packed(job), 'RESERVED'))
            self.event(db, exp, 'CREATE', {'jobs': len(spec['jobs'])})
        return {'status': 'CREATED'}

    def release(self, exp, host, completed, evidence):
        # 旧执行器已退出后才解除它的保留权；幂等重复只接受完全相同的证据。
        body = packed({'completed': completed, 'evidence': evidence})
        with self.transaction() as db:
            old = db.execute('SELECT body FROM releases WHERE experiment=? AND host=?', (exp, host)).fetchone()
            if old:
                assert old['body'] == body, 'Conflicting handoff'
                return {'status': 'ALREADY_RELEASED'}
            rows = db.execute('SELECT * FROM jobs WHERE experiment=?', (exp,)).fetchall()
            own = {r['id']: r for r in rows if json.loads(r['spec'])['legacy_host'] == host}
            assert own and set(completed) <= own.keys()
            for jid, row in own.items():
                assert row['state'] == 'RESERVED'
                result = completed.get(jid)
                if result:
                    assert result['status'] == 'PASS' and len(result['sha256']) == 64
                db.execute('UPDATE jobs SET state=?,owner=?,result=? WHERE experiment=? AND id=?',
                           ('PASS' if result else 'PENDING', 'legacy:' + host if result else None,
                            packed(result) if result else None, exp, jid))
            db.execute('INSERT INTO releases VALUES (?,?,?)', (exp, host, body))
            self.event(db, exp, 'RELEASE', {'host': host, 'completed': len(completed), 'evidence': evidence})
        return {'status': 'RELEASED'}

    def claim(self, exp, host, owner, capacity, request):
        with self.transaction() as db:
            experiment = db.execute('SELECT * FROM experiments WHERE id=?', (exp,)).fetchone()
            spec = json.loads(experiment['spec'])
            assert host in spec['hosts'] and 0 < capacity <= spec['hosts'][host]
            assert owner.startswith(host + ':')
            body = packed([host, owner, capacity])
            # 响应丢失只能重取同一次领取；其他空闲进程仍可独立领新任务。
            previous = db.execute('SELECT * FROM claims WHERE experiment=? AND request=?', (exp, request)).fetchone()
            if previous:
                assert previous['body'] == body
                return json.loads(previous['response'])
            if experiment['blocked'] or time.time() >= spec['deadline']:
                return {'jobs': [], 'blocked': experiment['blocked'] or 'DEADLINE'}
            rows = db.execute('SELECT * FROM jobs WHERE experiment=? ORDER BY rowid', (exp,)).fetchall()
            active = sum(r['state'] == 'RUNNING' and (r['owner'] or '').startswith(host + ':') for r in rows)
            capacity = min(capacity, spec['hosts'][host] - active)
            unfinished_l = any(json.loads(r['spec'])['job'][0].startswith('L_') and r['state'] != 'PASS' for r in rows)
            selected = []
            for row in rows:
                if len(selected) >= capacity:
                    break
                job = json.loads(row['spec'])
                if row['state'] != 'PENDING' or host not in job['hosts']:
                    continue
                if host == 'local' and unfinished_l and job['job'][0] == 'M_scaled':
                    continue
                selected.append(job)
            for job in selected:
                db.execute("UPDATE jobs SET state='RUNNING',owner=?,heartbeat=? WHERE experiment=? AND id=?",
                           (owner, time.time(), exp, job['id']))
            if selected:
                self.event(db, exp, 'CLAIM', {'host': host, 'owner': owner, 'ids': [j['id'] for j in selected]})
            response = {'jobs': selected}
            db.execute('INSERT INTO claims VALUES (?,?,?,?)', (exp, request, body, packed(response)))
            return response

    def heartbeat(self, exp, owner):
        with self.transaction() as db:
            db.execute("UPDATE jobs SET heartbeat=? WHERE experiment=? AND owner=? AND state='RUNNING'",
                       (time.time(), exp, owner))
        return {'status': 'OK'}

    def finish(self, exp, jid, owner, short, blob=None):
        assert short['status'] in ('PASS', 'FAILED')
        with self.transaction() as db:
            row = db.execute('SELECT * FROM jobs WHERE experiment=? AND id=?', (exp, jid)).fetchone()
            assert row and row['owner'] == owner, 'Wrong task owner'
            job = json.loads(row['spec'])
            if row['state'] in ('PASS', 'FAILED'):
                assert row['result'] == packed(short), 'Conflicting result'
                return {'status': 'ALREADY_SAVED'}
            assert row['state'] == 'RUNNING'
            if short['status'] == 'PASS':
                data = base64.b64decode(blob, validate=True)
                assert checksum(data) == short['sha256'], 'Result hash mismatch'
                record = json.loads(data)
                assert record['status'] == 'PASS'
                assert [record['stage'], record['protocol'], record['method']] == job['job']
                assert short['path'] == job['output']
                dest = Path(self.path).parent / 'artifacts' / checksum((exp + '/' + jid).encode())
                dest.parent.mkdir(exist_ok=True)
                if dest.exists():
                    assert checksum(dest.read_bytes()) == short['sha256']
                else:
                    with dest.open('xb') as f:
                        f.write(data)
                        f.flush()
                        import os
                        os.fsync(f.fileno())
            else:
                db.execute('UPDATE experiments SET blocked=? WHERE id=?', (packed(short), exp))
            db.execute('UPDATE jobs SET state=?,result=? WHERE experiment=? AND id=?',
                       (short['status'], packed(short), exp, jid))
            self.event(db, exp, 'FINISH', {'id': jid, 'owner': owner, 'status': short['status']})
        return {'status': 'SAVED'}

    def block(self, exp, reason):
        with self.transaction() as db:
            db.execute('UPDATE experiments SET blocked=COALESCE(blocked,?) WHERE id=?', (reason, exp))
            self.event(db, exp, 'BLOCK', reason)
        return {'status': 'BLOCKED'}

    def status(self, exp):
        with self.transaction() as db:
            experiment = db.execute('SELECT * FROM experiments WHERE id=?', (exp,)).fetchone()
            rows = db.execute('SELECT * FROM jobs WHERE experiment=? ORDER BY rowid', (exp,)).fetchall()
            counts = {}
            jobs = []
            for row in rows:
                item = dict(row)
                item['spec'] = json.loads(item['spec'])
                item['result'] = json.loads(item['result']) if item['result'] else None
                item['heartbeat_stale'] = item['state'] == 'RUNNING' and time.time() - item['heartbeat'] > 180
                counts[item['state']] = counts.get(item['state'], 0) + 1
                jobs.append(item)
            return {'counts': counts, 'blocked': experiment['blocked'], 'jobs': jobs,
                    'complete': bool(jobs) and all(j['state'] == 'PASS' for j in jobs)}


def serve(db_path, token_path, bind, port):
    queue = Queue(db_path)
    token = Path(token_path).read_text().strip()
    assert len(token) >= 32

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            try:
                if not secrets.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token):
                    self.send_error(401)
                    return
                size = int(self.headers.get('Content-Length', '0'))
                assert 0 < size <= 128 * 1024 * 1024
                body = json.loads(self.rfile.read(size))
                routes = {'/experiments': queue.create, '/release': queue.release, '/claim': queue.claim,
                          '/heartbeat': queue.heartbeat, '/finish': queue.finish,
                          '/status': queue.status, '/block': queue.block}
                result = routes[self.path](**body)
                data = packed(result).encode()
                self.send_response(200)
            except (AssertionError, KeyError, ValueError, TypeError) as exc:
                data = packed({'error': str(exc)}).encode()
                self.send_response(409)
            except Exception as exc:
                data = packed({'error': type(exc).__name__}).encode()
                self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer((bind, port), Handler)
    print(packed({'status': 'LISTENING', 'bind': bind, 'port': port}), flush=True)
    server.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--token-file', required=True)
    parser.add_argument('--bind', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=18765)
    args = parser.parse_args()
    serve(args.db, args.token_file, args.bind, args.port)
