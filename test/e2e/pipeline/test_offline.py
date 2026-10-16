"""Runner safety and real process fixture checks. No cluster is contacted."""
import json
import os
import pathlib
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from run import CASES, DEFERRED, KNOWN, write_reports


class RunnerSafety(unittest.TestCase):
    def invoke(self, *args):
        with tempfile.TemporaryDirectory() as tmp:
            marker = pathlib.Path(tmp) / 'called'
            binary = pathlib.Path(tmp) / 'kubectl'
            binary.write_text('#!/bin/sh\ntouch "' + str(marker) + '"\nexit 99\n')
            binary.chmod(0o755)
            env = dict(os.environ, PATH=tmp + os.pathsep + os.environ.get('PATH', ''))
            result = subprocess.run([sys.executable, str(ROOT / 'run.py'), *args], env=env, text=True, capture_output=True, timeout=10)
            self.assertFalse(marker.exists(), 'Read-only discovery must never invoke kubectl')
            return result

    def test_list_has_no_cluster_side_effects(self):
        result = self.invoke('--list')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('D17\tEXECUTABLE', result.stdout)
        self.assertIn('E10\tDEFERRED', result.stdout)

    def test_execution_requires_explicit_local_opt_in(self):
        result = self.invoke('--case', 'A01')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('--execute-local', result.stderr)

    def test_unknown_case_rejected_before_cluster_access(self):
        result = self.invoke('--execute-local', '--case', 'Typo')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Unknown case', result.stderr)

    def test_all_planned_case_ids_are_accounted_for(self):
        ids = set(CASES) | set(DEFERRED)
        for group, n in [('A', 12), ('B', 10), ('C', 14), ('D', 17), ('E', 10), ('F', 8)]:
            for i in range(1, n + 1):
                name = f'{group}{i:02}'
                self.assertTrue(name in ids or any(x.startswith(name + '-') for x in ids), name)
        self.assertFalse(set(CASES) & set(DEFERRED))
        self.assertTrue(set(KNOWN) <= set(CASES))

    def test_junit_never_counts_known_or_deferred_as_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = [{'id': 'A01', 'status': 'PASS'}, {'id': 'D17', 'status': 'XFAIL', 'detail': 'known'},
                    {'id': 'E10', 'status': 'SKIP', 'detail': 'runtime'}, {'id': 'A02', 'status': 'FAIL', 'detail': 'bad'}]
            write_reports(pathlib.Path(tmp), rows)
            root = ET.parse(pathlib.Path(tmp) / 'junit.xml').getroot()
            self.assertEqual(root.attrib['failures'], '1')
            self.assertEqual(root.attrib['skipped'], '2')
            self.assertEqual(json.loads((pathlib.Path(tmp) / 'results.json').read_text()), rows)


class WorkerProtocol(unittest.TestCase):
    def worker(self, mode):
        p = subprocess.Popen([sys.executable, '-u', str(ROOT / 'runtime_worker.py')],
                             env=dict(os.environ, MODE=mode), stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=False, start_new_session=True)
        self.addCleanup(self.cleanup, p)
        buf = b''
        until = time.monotonic() + 5
        while time.monotonic() < until:
            if select.select([p.stdout], [], [], .1)[0]:
                buf += os.read(p.stdout.fileno(), 4096)
                if b'READY mode=' in buf and (mode != 'child' or b'CHILD_READY' in buf):
                    return p, buf.decode()
        self.fail('worker not ready: ' + buf.decode())

    @staticmethod
    def cleanup(p):
        if p.poll() is None:
            os.killpg(p.pid, signal.SIGKILL)
        p.wait(timeout=5)
        p.stdout.close()

    def test_graceful_modes_stop_before_finishing_work(self):
        for mode, expected in [('idle', 'EXIT'), ('short', 'COMMIT_RESULT'), ('long', 'RELEASE_LEASE'), ('child', 'CHILD_REAPED')]:
            with self.subTest(mode=mode):
                p, initial = self.worker(mode)
                p.terminate()
                tail, _ = p.communicate(timeout=5)
                output = initial + tail.decode()
                self.assertEqual(p.returncode, 0, output)
                self.assertLess(output.index('SIGNAL 15'), output.index('STOP_FETCH'))
                self.assertLess(output.index('STOP_FETCH'), output.index(expected))
                self.assertIn('EXIT', output)

    def test_ignoring_term_needs_kill(self):
        p, initial = self.worker('ignore')
        p.terminate()
        with self.assertRaises(subprocess.TimeoutExpired):
            p.wait(timeout=.3)
        p.kill()
        tail, _ = p.communicate(timeout=5)
        self.assertEqual(p.returncode, -signal.SIGKILL)
        self.assertNotIn('EXIT', initial + tail.decode())



class DeletionGuard(unittest.TestCase):
    def test_simulator_delete_is_bound_to_uid(self):
        from harness import Cluster
        c = Cluster()
        calls = []
        c.k = lambda *args, **kwargs: calls.append((args, kwargs)) or '{}'
        self.assertTrue(c.complete_deletion({'metadata': {'name': 'worker', 'namespace': 'worker-type-prototype', 'uid': 'old-uid'}}))
        self.assertEqual(calls[0][1]['obj']['preconditions']['uid'], 'old-uid')
        self.assertEqual(calls[0][1]['obj']['gracePeriodSeconds'], 0)

    def test_concurrent_delete_or_replacement_is_not_simulator_failure(self):
        from harness import Cluster
        for reason in ['NotFound', 'Conflict']:
            c = Cluster()
            def rejected(*args, **kwargs):
                raise RuntimeError('Error from server (' + reason + ')')
            c.k = rejected
            self.assertFalse(c.complete_deletion({'metadata': {'name': 'worker', 'namespace': 'worker-type-prototype', 'uid': 'old-uid'}}))

class FaultConfigIsolation(unittest.TestCase):
    def test_reset_explicitly_clears_previous_fault_fields(self):
        import json
        from harness import Cluster
        c = Cluster()
        captured = []
        def reply(*args, **kwargs):
            value = json.loads(args[-1])
            captured.append(value)
            return json.dumps(value)
        c.k = reply
        c.fault('reject', target='journal-clear', repeat=1000, statusCode=503)
        c.fault('before')
        self.assertEqual(captured[-1]['target'], '')
        self.assertEqual(captured[-1]['repeat'], 0)
        self.assertEqual(captured[-1]['statusCode'], 0)

    def test_stale_proxy_configuration_is_rejected(self):
        from harness import Cluster
        c = Cluster()
        c.k = lambda *args, **kwargs: '{}'
        with self.assertRaisesRegex(AssertionError, 'did not replace configuration'):
            c.fault()


class ManagedBaseline(unittest.TestCase):
    def test_managed_workers_records_baseline_before_returning(self):
        from cases import managed_workers
        from harness import NODES, TYPE
        pods = [{'metadata': {'name': 'worker-' + str(i), 'uid': str(i), 'labels': {TYPE: 'A'}},
                 'spec': {'nodeName': NODES[0]}} for i in range(6)]
        class API:
            def create(self, obj): pass
            def patch(self, *args, **kwargs): pass
            def get(self, kind, name, ns):
                return {'status': {'currentHealthy': 6}} if kind == 'pdb' else next(p for p in pods if p['metadata']['name'] == name)
        class Fixture:
            c = API()
            initial = {}
            recorded = False
            def pods(self): return pods
            def pdb(self, minimum): pass
            def wait_pdb_baseline(self): self.recorded = True
        f = Fixture()
        managed_workers(f, 5)
        self.assertTrue(f.recorded, 'managed_workers returned after live GET without waiting for recorded baseline')


class StaticPluginSafety(unittest.TestCase):
    def spec(self):
        manifest = json.loads((ROOT / 'infrastructure.json').read_text())
        return next(x for x in manifest['items'] if x['kind'] == 'Deployment')['spec']['template']['spec']

    def test_static_fixture_allowed(self):
        from harness import static_runtime_valid
        self.assertTrue(static_runtime_valid(self.spec()))

    def test_disabled_or_dynamic_fixture_rejected(self):
        from harness import static_runtime_valid
        for mutation in ('disabled', 'dynamic', 'wrong-image'):
            with self.subTest(mutation=mutation):
                spec = self.spec(); c = spec['containers'][0]
                if mutation == 'disabled': c['args'].remove('--enable-worker-type-preemption=true')
                elif mutation == 'dynamic': c['args'].append('--plugins-dir=/plugins')
                else: c['image'] = 'untrusted:latest'
                self.assertFalse(static_runtime_valid(spec))


if __name__ == '__main__':
    unittest.main()
