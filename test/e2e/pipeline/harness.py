"""Real local API/scheduler; synthetic GPU nodes. Never uses current context."""
import copy
import os
import datetime
import json
import pathlib
import subprocess
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parents[3]
KCFG = pathlib.Path(os.environ.get('PIPELINE_E2E_KUBECONFIG', '/nonexistent/pipeline-e2e-kubeconfig'))
NS = 'worker-type-prototype'
OWNER = 'test.simplesilicon.com/suite'
TYPE = 'scheduling.simplesilicon.com/worker-type'
ALLOW = 'scheduling.simplesilicon.com/preemptible'
JOURNAL = 'worker-type-test-recovery'
NODES = ['wt-node-a', 'wt-node-b']


def static_runtime_valid(spec):
    containers = {c['name']: c for c in spec.get('containers', [])}
    scheduler = containers.get('scheduler', {})
    return (scheduler.get('image') == os.environ.get('PIPELINE_E2E_IMAGE', 'volcano-ss:v1.15.3-ss.1-local')
        and scheduler.get('command') == ['/vc-scheduler']
        and '--enable-worker-type-preemption=true' in scheduler.get('args', [])
        and not any(x.startswith('--plugins-dir') for x in scheduler.get('args', []))
        and not spec.get('initContainers')
        and not any(x.get('mountPath') == '/plugins' for x in scheduler.get('volumeMounts', []))
        and containers.get('fault-proxy', {}).get('image') == 'worker-type-prototype:v7-e2e-final'
        and containers.get('budget-controller', {}).get('image') == scheduler['image'])


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def wait(fn, seconds=60, description='condition'):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(.3)
    raise AssertionError('timeout: ' + description)


class Cluster:
    def k(self, *args, obj=None):
        p = subprocess.run(['kubectl', '--kubeconfig', str(KCFG), *args],
                           input=None if obj is None else json.dumps(obj),
                           text=True, capture_output=True, timeout=40)
        if p.returncode:
            raise RuntimeError(p.stderr)
        return p.stdout

    def get(self, kind, name=None, ns=None):
        args = ['get', kind] + ([name] if name else [])
        if ns:
            args += ['-n', ns]
        return json.loads(self.k(*args, '-o', 'json'))

    def patch(self, kind, name, body, ns=None, status=False):
        args = ['patch', kind, name, '--type=merge', '-p', json.dumps(body)]
        if ns:
            args += ['-n', ns]
        if status:
            args += ['--subresource=status']
        return self.k(*args)

    def create(self, obj):
        obj = copy.deepcopy(obj)
        obj['metadata'].setdefault('labels', {})[OWNER] = 'preemption-e2e'
        return self.k('create', '-f', '-', obj=obj)

    def complete_deletion(self, pod):
        """Simulated kubelet completion must not touch a replacement with the same name."""
        meta = pod['metadata']
        try:
            self.k('delete', '--raw', '/api/v1/namespaces/' + meta['namespace'] + '/pods/' + meta['name'], '-f', '-',
                   obj={'apiVersion': 'v1', 'kind': 'DeleteOptions', 'gracePeriodSeconds': 0,
                        'preconditions': {'uid': meta['uid']}})
            return True
        except RuntimeError as exc:
            if '(NotFound)' in str(exc) or '(Conflict)' in str(exc):
                return False  # Another actor completed deletion or replaced this exact Pod.
            raise

    def journal(self):
        return json.loads(self.get('cm', JOURNAL, NS)['data']['intents.json'])

    def logs(self, container, since):
        return self.k('logs', '-n', 'volcano-system', 'deployment/worker-type-test',
                      '-c', container, '--since-time=' + since)

    def fault(self, mode='', nth=1, **extra):
        desired = {'mode': mode, 'nth': nth, 'delaySeconds': 15,
                   'target': '', 'repeat': 0, 'statusCode': 0, **extra}
        response = self.k('exec', '-n', 'volcano-system', 'deployment/worker-type-test',
                         '-c', 'fault-proxy', '--', 'curl', '--fail', '--silent',
                         '-X', 'PUT', 'http://127.0.0.1:18080/__fault/config',
                         '-d', json.dumps(desired))
        actual = json.loads(response)
        require(all(actual.get(k) == v for k, v in desired.items()),
                'Fault proxy did not replace configuration: ' + response)
        return response

    def preflight(self):
        cfg = json.loads(self.k('config', 'view', '--minify', '-o', 'json'))
        require(cfg['clusters'][0]['cluster']['server'] == 'https://127.0.0.1:16473',
                'Only the dedicated local k3s endpoint is allowed')
        nodes = self.get('nodes')['items']
        require(any('k3s' in n['status']['nodeInfo']['kubeletVersion'] for n in nodes),
                'Expected local k3s')
        d = self.get('deployment', 'worker-type-test', 'volcano-system')
        require(d['spec']['replicas'] == 1 and d.get('status', {}).get('readyReplicas') == 1,
                'Dedicated single-active test scheduler must be ready')
        containers = d['spec']['template']['spec']['containers']
        require(any(c['name'] == 'fault-proxy' for c in containers), 'v4 test proxy required')
        scheduler = next(c for c in containers if c['name'] == 'scheduler')
        require(static_runtime_valid(d['spec']['template']['spec']), 'Expected explicitly enabled static local test image')
        require('--scheduler-name=worker-type-test' in scheduler['args'], 'Wrong scheduler scope')
        conf = self.get('cm', 'worker-type-test', 'volcano-system')['data']['volcano-scheduler.conf']
        actions = conf.split('actions:', 1)[1].splitlines()[0].strip().strip('"').split(',')
        require('worker-type-preempt' in [x.strip() for x in actions], 'Guarded action missing')
        require(not {'preempt', 'reclaim'} & {x.strip() for x in actions}, 'Unsafe alternate preemption action')
        require(not self.journal(), 'Outstanding journal: recover before starting tests; never erase it')
        for kind in ['pods', 'podgroups.scheduling.volcano.sh', 'pdb', 'replicasets']:
            require(not self.get(kind, ns=NS)['items'], 'Dedicated test namespace must have no workloads')
        names = {n['metadata']['name'] for n in nodes}
        require(not names.intersection(NODES), 'Synthetic test nodes already exist; inspect first')
        for kind, names in [('queue', ['wt-base', 'wt-shared']), ('priorityclass', ['wt-high'])]:
            for name in names:
                require(not self.k('get', kind, name, '--ignore-not-found').strip(),
                        'Reserved test resource already exists: ' + name)


class Fixture:
    def __init__(self, cluster, evidence):
        self.c, self.evidence = cluster, pathlib.Path(evidence)
        self.evidence.mkdir(parents=True, exist_ok=False)
        self.stop = threading.Event()
        self.errors, self.timeline = [], []
        self.victims = []
        self.initial = {}
        self.extra_namespaces = []
        self.capacity = {n: {'cpu': '64', 'memory': '128Gi', 'pods': '100', 'nvidia.com/gpu': '8'} for n in NODES}
        self.finish_deletions = True
        self.since = datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')

    def setup(self):
        for n in NODES:
            self.c.create({'apiVersion': 'v1', 'kind': 'Node', 'metadata': {'name': n, 'labels': {'worker-type-test': 'true', 'test.local/slot': n[-1]}},
                           'spec': {'taints': [{'key': 'worker-type-test', 'effect': 'NoSchedule'}]}})
        for q in ['wt-base', 'wt-shared']:
            self.c.create({'apiVersion': 'scheduling.volcano.sh/v1beta1', 'kind': 'Queue', 'metadata': {'name': q}, 'spec': {'weight': 1, 'reclaimable': False}})
        self.c.create({'apiVersion': 'scheduling.k8s.io/v1', 'kind': 'PriorityClass', 'metadata': {'name': 'wt-high'}, 'value': 100, 'globalDefault': False})
        self.c.fault()
        self.tick()
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()

    def tick(self):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
        for n in NODES:
            self.c.patch('node', n, {'status': {'capacity': self.capacity[n], 'allocatable': self.capacity[n], 'conditions': [{'type': 'Ready', 'status': 'True', 'lastHeartbeatTime': now, 'reason': 'SyntheticE2E'}]}}, status=True)
        pods = self.pods()
        self.timeline.append({'time': now, 'pods': pods, 'pdbs': self.c.get('pdb', ns=NS)['items']})
        for p in pods:
            if p['metadata'].get('labels', {}).get(OWNER) != 'preemption-e2e':
                raise AssertionError('Unowned Pod appeared in fixture namespace')
            if p['metadata'].get('deletionTimestamp') and self.finish_deletions and p['spec'].get('nodeName') in NODES:
                self.victims.append({'name': p['metadata']['name'], 'uid': p['metadata']['uid'], 'time': now})
                # Fake nodes have no kubelet. Only complete deletion ALREADY accepted by API.
                self.c.complete_deletion(p)

    def loop(self):
        while not self.stop.wait(.5):
            try:
                self.tick()
            except Exception as exc:
                self.errors.append(str(exc))
                return

    def pods(self):
        return self.c.get('pods', ns=NS)['items']

    def pod(self, name, typ='A', queue='wt-shared', node='wt-node-a', gpu=1, priority=0, allow=True, cpu='100m', memory='32Mi', selector=None, gate=False, pg=None, ready=True, tolerates=True, pipeline=None, namespace=NS, target_namespace=None):
        if pg is None:
            pg = name
            self.c.create({'apiVersion': 'scheduling.volcano.sh/v1beta1', 'kind': 'PodGroup', 'metadata': {'name': pg, 'namespace': namespace},
                           'spec': {'queue': queue, 'minMember': 1, **({'priorityClassName': 'wt-high'} if priority else {})}})
        labels = {ALLOW: str(allow).lower()}
        if pipeline:
            labels['pipeline.simplesilicon.com/name'] = pipeline
        if target_namespace:
            labels['pipeline.simplesilicon.com/namespace'] = target_namespace
        if typ is not None:
            labels[TYPE] = typ
        spec = {'schedulerName': 'worker-type-test', 'terminationGracePeriodSeconds': 10,
                'nodeSelector': selector or {'worker-type-test': 'true'},
                'containers': [{'name': 'worker', 'image': 'busybox:1.36', 'command': ['sleep', '3600'],
                                'resources': {'requests': {'cpu': cpu, 'memory': memory, 'nvidia.com/gpu': str(gpu)}, 'limits': {'nvidia.com/gpu': str(gpu)}}}]}
        if tolerates:
            spec['tolerations'] = [{'key': 'worker-type-test', 'operator': 'Exists', 'effect': 'NoSchedule'}]
        if node:
            spec['nodeName'] = node
        if priority:
            spec['priorityClassName'] = 'wt-high'
        if gate:
            spec['schedulingGates'] = [{'name': 'test.local/hold'}]
        self.c.create({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': namespace, 'labels': labels, 'annotations': {'scheduling.k8s.io/group-name': pg}}, 'spec': spec})
        if node:
            self.c.patch('pod', name, {'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True' if ready else 'False'}]}}, namespace, True)
            self.initial[name] = self.c.get('pod', name, namespace)

    def train(self, name='training', **kwargs):
        self.pod(name, typ=None, node=None, priority=100, **kwargs)

    def bound(self, name='training'):
        return self.c.get('pod', name, NS)['spec'].get('nodeName')

    def observe(self, seconds=12):
        # At least four 3s scheduler cycles. Safety assertions are never accepted on empty observations.
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            require(not self.errors, str(self.errors))
            time.sleep(.3)

    def deleted(self):
        live = {p['metadata']['uid'] for p in self.pods() if not p['metadata'].get('deletionTimestamp')}
        return {n for n, p in self.initial.items() if p['metadata']['uid'] not in live}

    def expect(self, count, bound=True, training='training'):
        wait(lambda: len(self.deleted()) == count and bool(self.bound(training)) == bound,
             description='victim count and training binding')
        self.observe()
        require(len(self.deleted()) == count, 'Unexpected extra victims: ' + str(self.deleted()))
        require(bool(self.bound(training)) == bound, 'Unexpected binding')

    def pdb(self, minimum=5, name='type-a', maximum=None):
        spec = {'selector': {'matchLabels': {TYPE: 'A'}}}
        spec.update({'minAvailable': minimum} if maximum is None else {'maxUnavailable': maximum})
        self.c.create({'apiVersion': 'policy/v1', 'kind': 'PodDisruptionBudget', 'metadata': {'name': name, 'namespace': NS}, 'spec': spec})

    def six(self, minimum=5):
        for slot in ['a', 'b']:
            n = 'wt-node-' + slot
            self.pod('base-' + slot, queue='wt-base', node=n, gpu=2)
            self.pod('filler-' + slot, typ='FILLER', queue='wt-base', node=n, gpu=4)
            for i in range(2):
                self.pod('shared-' + slot + '-' + str(i), node=n)
        self.pdb(minimum)
        wait(lambda: self.c.get('pdb', 'type-a', NS).get('status', {}).get('currentHealthy') == 6,
             description='real PDB controller convergence')
        self.wait_pdb_baseline()

    def wait_pdb_baseline(self):
        # Establish a recorded pre-eviction baseline, not merely a separate GET
        # that may run between sampler ticks.
        wait(lambda: any(any(b['metadata']['name'] == 'type-a' and
                            b.get('status', {}).get('currentHealthy') == 6
                            for b in sample['pdbs']) for sample in self.timeline),
             description='recorded converged PDB baseline')

    def floor(self, minimum):
        samples = [s for s in self.timeline if any(b['metadata']['name'] == 'type-a' and b.get('status', {}).get('currentHealthy') == 6 for b in s['pdbs'])]
        require(samples, 'No converged PDB sample')
        first = self.timeline.index(samples[0])
        healthy = [sum(p['metadata'].get('labels', {}).get(TYPE) == 'A' and not p['metadata'].get('deletionTimestamp') and any(c['type'] == 'Ready' and c['status'] == 'True' for c in p.get('status', {}).get('conditions', [])) for p in s['pods']) for s in self.timeline[first:]]
        require(min(healthy) >= minimum, 'Observed healthy floor breached: ' + str(min(healthy)))

    def protected(self):
        live = {p['metadata']['name']: p for p in self.pods()}
        require(self.timeline, 'No observations collected')
        for sample in self.timeline:
            for node in NODES:
                used = sum(int(c.get('resources', {}).get('requests', {}).get('nvidia.com/gpu', '0'))
                           for p in sample['pods'] if p['spec'].get('nodeName') == node
                           and p.get('status', {}).get('phase') not in ['Succeeded', 'Failed']
                           for c in p['spec'].get('containers', []))
                require(used <= int(self.capacity[node]['nvidia.com/gpu']), 'Observed GPU request overcommit on ' + node)
        for name, p in self.initial.items():
            if name.startswith(('base-', 'filler-')):
                require(name in live and live[name]['metadata']['uid'] == p['metadata']['uid'] and live[name]['spec'] == p['spec'] and not live[name]['metadata'].get('deletionTimestamp'), 'Protected Pod changed: ' + name)

    def finish(self):
        # Save before cleanup, including failures. Never hide simulator errors.
        self.stop.set()
        if hasattr(self, 'thread'):
            self.thread.join(45)
        for key, value in [('timeline', self.timeline), ('victims', self.victims), ('initial', self.initial), ('simulator-errors', self.errors)]:
            (self.evidence / (key + '.json')).write_text(json.dumps(value, indent=2))
        for container in ['scheduler', 'fault-proxy']:
            try:
                (self.evidence / (container + '.log')).write_text(self.c.logs(container, self.since))
            except Exception as exc:
                (self.evidence / (container + '-error.txt')).write_text(str(exc))
        (self.evidence / 'after.json').write_text(json.dumps(self.pods(), indent=2))
        (self.evidence / 'journal.json').write_text(json.dumps(self.c.journal(), indent=2))
        # Evidence is complete. Stop owned controllers before removing their Pods.
        self.c.fault()
        self.c.k('delete', 'replicasets', '-n', NS, '-l', OWNER + '=preemption-e2e', '--cascade=orphan', '--wait=true', '--timeout=30s')
        for kind in ['pods', 'podgroups.scheduling.volcano.sh', 'pdb', 'configmaps']:
            args = ['delete', kind, '-n', NS, '-l', OWNER + '=preemption-e2e', '--wait=false']
            if kind == 'pods':
                args += ['--force', '--grace-period=0']
            self.c.k(*args)
        for namespace in self.extra_namespaces:
            self.c.k('delete', 'pods', '-n', namespace, '-l', OWNER + '=preemption-e2e', '--force', '--grace-period=0', '--wait=false')
            self.c.k('delete', 'namespace', namespace, '--wait=false')
        wait(lambda: not self.c.journal(), description='journal drains after cleanup')
        for kind in ['nodes', 'queues.scheduling.volcano.sh', 'priorityclasses']:
            self.c.k('delete', kind, '-l', OWNER + '=preemption-e2e', '--wait=true', '--timeout=30s')
        require(not self.errors, 'Simulator error: ' + str(self.errors))
