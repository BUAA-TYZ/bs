import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from analysis.evaluation_queue import Queue, packed
from analysis import run_ground_queue_20261030 as runner


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'queue.sqlite'
        self.q = Queue(self.path)
        jobs = []
        for i in range(10):
            jobs.append(dict(id=str(i), job=['M_scaled', 'protocol', 'method' + str(i)],
                             output=f'results/{i}.json', legacy_host='old', hosts=['local', 'devbox1']))
        self.spec = dict(id='exp', jobs=jobs, hosts={'local': 2, 'devbox1': 2}, deadline=time.time()+300)
        self.q.create(self.spec)

    def release(self):
        self.q.release('exp', 'old', {}, {'drained': True})

    def claim(self, host='local', request='a', capacity=2):
        return self.q.claim('exp', host, host+':worker', capacity, host + ':' + request)

    def finish(self, item, host='local'):
        stage, protocol, method = item['job']
        data = packed(dict(status='PASS', stage=stage, protocol=protocol, method=method)).encode()
        short = dict(status='PASS', path=item['output'], sha256=hashlib.sha256(data).hexdigest())
        return self.q.finish('exp', item['id'], host+':worker', short, base64.b64encode(data).decode())

    def test_reserved_not_claimable(self):
        self.assertEqual(self.claim()['jobs'], [])

    def test_create_is_idempotent_not_overwritable(self):
        self.assertEqual(self.q.create(self.spec)['status'], 'EXISTS')
        with self.assertRaises(AssertionError):
            self.q.create(dict(self.spec, deadline=time.time()+400))

    def test_release_is_idempotent_not_overwritable(self):
        self.release()
        self.release()
        with self.assertRaises(AssertionError):
            self.q.release('exp', 'old', {}, {'different': True})

    def test_concurrent_claim_is_unique(self):
        self.release()
        with ThreadPoolExecutor(2) as pool:
            batches = list(pool.map(lambda host: self.claim(host=host), ['local', 'devbox1']))
        ids = [j['id'] for batch in batches for j in batch['jobs']]
        self.assertEqual(len(ids), 4)
        self.assertEqual(len(set(ids)), 4)

    def test_lost_claim_response_returns_same_assignment(self):
        self.release()
        first = self.claim()
        self.assertEqual(first, self.claim())
        self.assertEqual(self.q.status('exp')['counts']['RUNNING'], 2)

    def test_global_host_capacity_includes_other_owner(self):
        self.release()
        self.claim()
        other = self.q.claim('exp', 'local', 'local:other', 2, 'b')
        self.assertEqual(other['jobs'], [])

    def test_free_slot_claims_without_waiting_slow_peer(self):
        self.release()
        first = self.claim()['jobs']
        self.finish(first[0])
        second = self.claim(request='b', capacity=1)['jobs']
        self.assertEqual(len(second), 1)
        self.assertNotIn(second[0]['id'], [j['id'] for j in first])
        self.assertEqual(self.q.status('exp')['counts']['RUNNING'], 2)

    def test_failure_stops_dispatch_but_running_can_finish(self):
        self.release()
        first = self.claim()['jobs']
        self.q.finish('exp', first[0]['id'], 'local:worker', {'status': 'FAILED', 'traceback': 'test'})
        self.assertTrue(self.claim('devbox1')['blocked'])
        self.finish(first[1])
        self.assertEqual(self.q.status('exp')['counts']['PASS'], 1)

    def test_stale_heartbeat_never_requeues(self):
        self.release()
        self.claim()
        with self.q.transaction() as db:
            db.execute('UPDATE jobs SET heartbeat=0 WHERE state="RUNNING"')
        state = self.q.status('exp')
        self.assertEqual(sum(j['heartbeat_stale'] for j in state['jobs']), 2)
        self.assertEqual(self.claim(request='b')['jobs'], [])

    def test_server_restart_preserves_claims(self):
        self.release()
        first = self.claim()
        self.q = Queue(self.path)
        self.assertEqual(first, self.claim())

    def test_wrong_owner_and_bad_result_hash_rejected(self):
        self.release()
        item = self.claim()['jobs'][0]
        short = dict(status='PASS', path=item['output'], sha256='0'*64)
        with self.assertRaises(AssertionError):
            self.q.finish('exp', item['id'], 'devbox1:worker', short, 'e30=')
        with self.assertRaises(AssertionError):
            self.q.finish('exp', item['id'], 'local:worker', short, 'e30=')
        self.assertEqual(self.q.status('exp')['counts']['RUNNING'], 2)

    def test_finish_duplicate_is_idempotent(self):
        self.release()
        item = self.claim()['jobs'][0]
        self.finish(item)
        self.assertEqual(self.finish(item)['status'], 'ALREADY_SAVED')

    def test_expired_deadline_stops_new_claims(self):
        self.release()
        with patch('analysis.evaluation_queue.time.time', return_value=self.spec['deadline']+1):
            self.assertEqual(self.claim()['blocked'], 'DEADLINE')

    def test_local_l_priority_and_remote_m_parallel(self):
        # 既有实验规则：L留在Mac，M可先由远端领取。
        with self.q.transaction() as db:
            job = dict(self.spec['jobs'][0], job=['L_scaled', 'p', 'm'], hosts=['local'])
            db.execute('UPDATE jobs SET spec=? WHERE id="0"', (packed(job),))
        self.release()
        self.assertEqual(len(self.claim()['jobs']), 1)
        self.assertEqual(len(self.claim('devbox1')['jobs']), 2)
        self.finish(job)
        self.assertEqual(len(self.claim(request='b')['jobs']), 2)

    def test_failed_legacy_record_cannot_be_reissued(self):
        with self.assertRaises(AssertionError):
            self.q.release('exp', 'old', {'0': {'status': 'FAILED', 'sha256': '0'*64}}, {})
        self.assertEqual(self.q.status('exp')['counts']['RESERVED'], 10)

    def test_orphan_legacy_progress_blocks_handoff(self):
        root = Path(self.temp.name)
        olddir = root / 'old'
        newdir = root / 'new'
        host = 'devbox1'
        (olddir / host).mkdir(parents=True)
        runner.r.write_json(olddir / host / 'launch.json', {'pid': 1, 'supervisor_pid': 2})
        runner.r.write_json(olddir / host / 'final_status.json', {'status': 'BLOCKED', 'results': []})
        runner.r.write_json(olddir / host / 'STOP_AFTER_BATCH.json', {})
        out = root / 'output.json'
        runner.r.write_json(out.with_suffix('.progress.json'), {'status': 'RUNNING'})
        spec = {'jobs': [dict(job=['M_scaled', 'p', 'm'], id='a', legacy_host=host, output=str(out))]}
        with patch.object(runner, 'ROOT', newdir), patch.object(runner.old, 'DIST', olddir), \
             patch.object(runner, 'process_active', return_value=False):
            with self.assertRaises(AssertionError):
                runner.release_legacy(host, spec)

    def test_real_http_auth_claim_and_result_roundtrip(self):
        token = Path(self.temp.name) / 'token'
        token.write_text('test-only-token-' * 4)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        process = subprocess.Popen([sys.executable, '-B', '-m', 'analysis.evaluation_queue',
            '--db', str(self.path), '--token-file', str(token), '--port', str(port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        def cleanup():
            process.terminate()
            process.wait(timeout=5)
        self.addCleanup(cleanup)
        def post(route, body, authorized=True):
            headers = {'Authorization': 'Bearer ' + token.read_text()} if authorized else {}
            req = Request(f'http://127.0.0.1:{port}' + route, json.dumps(body).encode(), headers)
            with urlopen(req, timeout=5) as response:
                return json.load(response)
        for _ in range(100):
            try:
                post('/status', {'exp': 'exp'})
                break
            except URLError:
                time.sleep(.02)
        with self.assertRaises(HTTPError) as raised:
            post('/status', {'exp': 'exp'}, authorized=False)
        self.assertEqual(raised.exception.code, 401)
        post('/release', dict(exp='exp', host='old', completed={}, evidence={'test': True}))
        item = post('/claim', dict(exp='exp', host='local', owner='local:http', capacity=1, request='http1'))['jobs'][0]
        stage, protocol, method = item['job']
        data = packed(dict(status='PASS', stage=stage, protocol=protocol, method=method)).encode()
        short = dict(status='PASS', path=item['output'], sha256=hashlib.sha256(data).hexdigest())
        post('/finish', dict(exp='exp', jid=item['id'], owner='local:http', short=short, blob=base64.b64encode(data).decode()))
        self.assertEqual(post('/status', {'exp': 'exp'})['counts']['PASS'], 1)


if __name__ == '__main__':
    unittest.main()
