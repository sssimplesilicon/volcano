"""Real kubelet/CPU container Eviction tests; independent of GPU selection tests."""
import json
import pathlib
import subprocess
import time
from cases import case
from harness import KCFG, NS, OWNER, require, wait


@case('E01,E02,E03,E04,E05,E06', 'real CPU process grace period and signal handling')
def graceful(f, ident):
    mode = {'E01': 'idle', 'E02': 'short', 'E03': 'long', 'E04': 'idle', 'E05': 'child', 'E06': 'ignore'}[ident]
    source = pathlib.Path(__file__).with_name('runtime_worker.py').read_text()
    f.c.create({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'e2e-runtime', 'namespace': NS}, 'data': {'worker.py': source}})
    # Only the kubelet of the dedicated local k3s cluster; no synthetic GPU resource.
    real = next(n['metadata']['name'] for n in f.c.get('nodes')['items'] if 'k3s' in n.get('status', {}).get('nodeInfo', {}).get('kubeletVersion', '') and not n['metadata']['name'].startswith('wt-node-'))
    container = {'name': 'worker', 'image': 'python:3.12-alpine', 'command': ['python', '-u', '/test/worker.py'],
                 'env': [{'name': 'MODE', 'value': mode}], 'resources': {'requests': {'cpu': '10m', 'memory': '32Mi'}, 'limits': {'memory': '128Mi'}},
                 'volumeMounts': [{'name': 'script', 'mountPath': '/test', 'readOnly': True}]}
    if ident == 'E04':
        container['lifecycle'] = {'preStop': {'exec': {'command': ['sh', '-c', 'echo PRESTOP > /proc/1/fd/1']}}}
    if ident == 'E05':
        container['command'] = ['sh', '-c', 'exec python -u /test/worker.py']
    name = 'runtime-worker'
    f.c.create({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': NS, 'finalizers': ['test.simplesilicon.com/retain-evidence']},
                'spec': {'nodeName': real, 'schedulerName': 'default-scheduler', 'restartPolicy': 'Never', 'terminationGracePeriodSeconds': 5,
                         'containers': [container], 'volumes': [{'name': 'script', 'configMap': {'name': 'e2e-runtime'}}]}})
    stream = None
    log_file = (f.evidence / "worker.log").open("w")
    try:
        wait(lambda: f.c.get('pod', name, NS).get('status', {}).get('phase') == 'Running', 120, 'real Python container Running')
        # Attach before eviction: kubelet may remove the container log immediately
        # after termination even while the Pod is retained by a finalizer.
        stream = subprocess.Popen(['kubectl', '--kubeconfig', str(KCFG), 'logs', '-f', name, '-n', NS],
                                  stdout=log_file, stderr=subprocess.STDOUT)
        def log():
            return (f.evidence / 'worker.log').read_text()
        wait(lambda: 'READY mode=' in log(), description='worker signal handler installed')
        if mode == 'child':
            wait(lambda: 'CHILD_READY' in log())
        uid = f.c.get('pod', name, NS)['metadata']['uid']
        started = time.monotonic()
        f.c.k('create', '--raw', '/api/v1/namespaces/' + NS + '/pods/' + name + '/eviction', '-f', '-',
              obj={'apiVersion': 'policy/v1', 'kind': 'Eviction', 'metadata': {'name': name, 'namespace': NS}, 'deleteOptions': {'preconditions': {'uid': uid}}})
        def terminated():
            statuses = f.c.get('pod', name, NS).get('status', {}).get('containerStatuses', [])
            return statuses and statuses[0].get('state', {}).get('terminated')
        state = wait(terminated, 40, 'real kubelet termination')
        elapsed = time.monotonic() - started
        stream.wait(timeout=15)
        require(stream.returncode == 0, 'Log stream failed: ' + log())
        output = log()
        (f.evidence / 'termination.json').write_text(json.dumps({'state': state, 'elapsed': elapsed}, indent=2))
        require('SIGNAL 15' in output, 'SIGTERM not observed')
        if mode == 'ignore':
            require(state['exitCode'] == 137 and elapsed >= 4, 'Expected forced termination after grace period')
            require('EXIT' not in output, 'Ignoring worker unexpectedly exited gracefully')
        else:
            require(state['exitCode'] == 0 and 'STOP_FETCH' in output and 'EXIT' in output, 'Graceful exit failed')
            for token in {'short': ['COMMIT_RESULT'], 'long': ['CHECKPOINT', 'RELEASE_LEASE'], 'child': ['CHILD_REAPED']}.get(mode, []):
                require(output.index('STOP_FETCH') < output.index(token) < output.index('EXIT'), 'Wrong drain order')
            if ident == 'E04':
                require(output.index('PRESTOP') < output.index('SIGNAL 15'), 'preStop must precede TERM')
    finally:
        if stream is not None and stream.poll() is None:
            stream.terminate()
            stream.wait(timeout=5)
        log_file.close()
        f.c.patch('pod', name, {'metadata': {'finalizers': None}}, NS)
