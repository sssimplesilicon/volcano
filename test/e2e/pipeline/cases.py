"""Black-box scenarios. Assertions describe outcomes, not internal Go callbacks."""
import concurrent.futures
import json
import time
from harness import NS, NODES, TYPE, ALLOW, JOURNAL, require, wait

CASES = {}


def case(ids, title):
    def register(fn):
        for ident in ids.split(','):
            CASES[ident] = (title, fn)
        return fn
    return register


def full(f, typ='A', allow=True, priority=0, queue='wt-shared', wide=False):
    for slot in ['a', 'b']:
        node = 'wt-node-' + slot
        f.pod('base-' + slot, typ='BASE', queue='wt-base', node=node, gpu=4)
        for i in range(1 if wide else 4):
            f.pod('shared-' + slot + '-' + str(i), typ=typ, queue=queue, node=node,
                  gpu=4 if wide else 1, allow=allow, priority=priority)


@case('A01', 'free capacity: no victims')
def free(f, _):
    f.pod('base-a', queue='wt-base', gpu=4)
    f.train()
    f.expect(0)


@case('A02,C11', 'higher priority preempts shared; no PDB means no replica guarantee')
def simple(f, _):
    full(f)
    f.train()
    f.expect(1)
    require(all(n.startswith('shared-') for n in f.deleted()), 'Unexpected victim')


@case('A03,A03-lower', 'equal or lower priority cannot preempt')
def equal(f, ident):
    full(f, priority=100)
    if ident == 'A03-lower':
        f.pod('training', typ=None, node=None, priority=0)
    else:
        f.train()
    f.observe()
    require(not f.deleted() and not f.bound(), 'Equal-priority worker was evicted')


@case('A04', 'base cannot be victim')
def base(f, _):
    for slot in ['a', 'b']:
        f.pod('base-' + slot, queue='wt-base', node='wt-node-' + slot, gpu=8)
    f.train()
    f.observe()
    require(not f.deleted() and not f.bound(), 'Base protection failed')


@case('A05-type,A05-permission', 'missing worker-type or permission is protected')
def labels(f, ident):
    full(f, typ=None if ident.endswith('type') else 'A', allow=ident.endswith('type'))
    f.train()
    f.observe()
    require(not f.deleted() and not f.bound(), 'Unlabelled/unapproved worker evicted')


@case('A06,D13', 'terminating victim is not resubmitted; ordinary allocation continues')
def terminating(f, _):
    full(f)
    f.finish_deletions = False
    f.train()
    wait(lambda: f.c.journal(), description='accepted intent')
    f.observe()
    require(len(f.deleted()) == 1 and not f.bound(), 'Expected one terminating victim')
    proxy = f.c.logs('fault-proxy', f.since)
    require(proxy.count('EVICTION_REQUEST') == 1, 'Repeated Eviction for terminating victim')
    # CPU-only allocation requires no GPU resources and must not wait for recovery.
    f.train('training-cpu', gpu=0)
    wait(lambda: f.bound('training-cpu'), description='ordinary allocate continues')
    f.finish_deletions = True
    wait(lambda: f.bound() and not f.c.journal())


@case('A07', 'preemptor outside shared cannot use guarded preemption')
def other_queue(f, _):
    full(f)
    f.train(queue='wt-base')
    f.observe()
    require(not f.deleted() and not f.bound(), 'Cross-queue preemption occurred')


@case('A08,A09,A12', 'aggregate type across PodGroups and rerank A,A,B,A')
def rerank(f, _):
    f.pod('base-a', typ='BASE', queue='wt-base', gpu=3)
    f.pod('base-b', typ='BASE', queue='wt-base', node=NODES[1], gpu=4)
    for i in range(4):
        f.pod('a-' + str(i), typ='A')
    f.pod('a-4', typ='A', node=NODES[1])
    f.pod('b-local', typ='B')
    for i in range(3):
        f.pod('b-' + str(i), typ='B', node=NODES[1])
    f.train(gpu=4)
    f.expect(4)
    # The proxy observes actual API submissions, not tentative selection logging.
    lines = [x for x in f.c.logs('fault-proxy', f.since).splitlines() if 'EVICTION_REQUEST' in x]
    names = [x.split('/pods/')[1].split('/eviction')[0] for x in lines]
    types = [f.initial[n]['metadata']['labels'][TYPE] for n in names]
    require(types == ['A', 'A', 'B', 'A'], 'Wrong actual order: ' + str(types))


@case('A11', 'largest type is protected; choose eligible smaller type')
def protected_type(f, _):
    f.pod('base-a', typ='BASE', queue='wt-base', gpu=4)
    f.pod('a', gpu=4, allow=False)
    f.pod('base-b', typ='BASE', queue='wt-base', node=NODES[1], gpu=6)
    f.pod('b', typ='B', node=NODES[1], gpu=2)
    f.train()
    f.expect(1)
    require(f.deleted() == {'b'}, 'Protected type selected')


@case('B01', 'free GPUs on different nodes are not pooled for one Pod')
def split(f, _):
    for slot in ['a', 'b']:
        f.pod('base-' + slot, queue='wt-base', node='wt-node-' + slot, gpu=7)
    f.train(gpu=2)
    f.observe()
    require(not f.bound() and not f.deleted(), 'Cross-node GPUs incorrectly combined')


@case('B02,B03', 'CPU fragmentation chooses feasible node only')
def fragmentation(f, _):
    full(f)
    # CPU requests on already-created Pods are immutable: set up the constraint
    # through node allocatable instead; simulator keeps this reduced capacity.
    f.capacity[NODES[0]]['cpu'] = '1'
    f.tick()
    f.train(gpu=2, cpu='2')
    f.expect(2)
    require(f.bound() == NODES[1], 'Bound to CPU-infeasible node')
    require(all(n.startswith('shared-b-') for n in f.deleted()), 'Evicted on infeasible node')


@case('B04,B05-taint,B05-cordon,B06', 'impossible placement does not evict')
def infeasible(f, ident):
    full(f)
    kwargs = {}
    if ident == 'B04':
        kwargs['selector'] = {'test.local/slot': 'missing'}
    elif ident == 'B05-taint':
        kwargs['tolerates'] = False
    elif ident == 'B05-cordon':
        for n in NODES:
            f.c.patch('node', n, {'spec': {'unschedulable': True}})
    else:
        kwargs['gpu'] = 5  # Each node has 4 protected base GPUs.
    f.train(**kwargs)
    f.observe()
    require(not f.deleted() and not f.bound(), 'Infeasible request caused eviction')


@case('B07', 'infeasible trial releases PDB reservation for smaller request')
def rollback(f, _):
    f.six(5)
    f.train(gpu=2)
    f.observe()
    require(not f.deleted() and not f.bound(), 'Trial unexpectedly committed')
    f.train('training-small')
    f.expect(1, training='training-small')
    require(not f.bound(), 'Large request unexpectedly bound')
    f.floor(5)


@case('B08', 'indivisible four-GPU victim may free more than one requested GPU')
def wide(f, _):
    full(f, wide=True)
    f.train()
    f.expect(1)
    require(next(iter(f.deleted())).startswith('shared-'), 'Wrong victim')


@case('C01,C02,C03,C04,F01', 'PDB budget across single, sequential and concurrent demands')
def budget(f, ident):
    f.six(5)
    count = {'C01': 1, 'C02': 2, 'C03': 2, 'C04': 4, 'F01': 2}[ident]
    for i in range(count):
        f.train('training-' + str(i), gate=True, selector={'test.local/slot': 'a' if i % 2 == 0 else 'b'})
    if ident == 'C02':
        f.c.patch('pod', 'training-0', {'spec': {'schedulingGates': None}}, NS)
        wait(lambda: f.bound('training-0'))
        f.c.patch('pod', 'training-1', {'spec': {'schedulingGates': None}}, NS)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
            list(pool.map(lambda i: f.c.patch('pod', 'training-' + str(i), {'spec': {'schedulingGates': None}}, NS), range(count)))
    wait(lambda: sum(bool(f.bound('training-' + str(i))) for i in range(count)) == 1)
    f.observe()
    require(len(f.deleted()) == 1, 'Budget overspent')
    require(sum(bool(f.bound('training-' + str(i))) for i in range(count)) == 1, 'Unexpected bound count')
    f.floor(5)


@case('C06,C07', 'base satisfies floor or zero floor permits shared eviction')
def low_floor(f, ident):
    minimum = 2 if ident == 'C06' else 0
    f.six(minimum)
    f.train(gpu=2)
    f.expect(2)
    f.floor(minimum)


@case('C10', 'overlapping PDBs conservatively prevent eviction')
def overlap(f, _):
    f.six(5)
    f.pdb(5, name='also-a')
    wait(lambda: f.c.get('pdb', 'also-a', NS).get('status', {}).get('currentHealthy') == 6)
    f.train()
    f.observe()
    require(not f.deleted() and not f.bound(), 'Overlapping PDBs did not protect victim')


@case('C13', 'NotReady worker consumes healthy budget')
def unhealthy(f, _):
    f.six(5)
    f.c.patch('pod', 'shared-a-0', {'status': {'conditions': [{'type': 'Ready', 'status': 'False'}]}}, NS, True)
    wait(lambda: f.c.get('pdb', 'type-a', NS)['status'].get('disruptionsAllowed') == 0)
    f.train()
    f.observe()
    require(not f.deleted() and not f.bound(), 'Unhealthy worker protection was bypassed')


@case('D04,D05,D08,D09', 'timeout, lost response, partial success and scheduler restart')
def recovery(f, ident):
    partial = ident in ('D08', 'D09')
    minimum = 4 if partial else 5
    f.six(minimum)
    mode = 'after' if ident == 'D05' else 'before'
    f.c.fault(mode, 2 if partial else 1)
    f.train(gpu=2 if partial else 1, selector={'test.local/slot': 'a'})
    wait(lambda: 'FAULT_' + mode.upper() in f.c.logs('fault-proxy', f.since), description='fault triggered')
    require(f.c.journal(), 'Eviction without durable intent')
    (f.evidence / 'fault-intent.json').write_text(json.dumps(f.c.journal(), indent=2))
    if ident == 'D09':
        (f.evidence / 'proxy-before-restart.log').write_text(f.c.logs('fault-proxy', f.since))
        (f.evidence / 'scheduler-before-restart.log').write_text(f.c.logs('scheduler', f.since))
        old = f.c.get('pods', ns='volcano-system')['items']
        old = {p['metadata']['uid'] for p in old if p['metadata'].get('labels', {}).get('app') == 'worker-type-test'}
        f.c.k('rollout', 'restart', 'deployment/worker-type-test', '-n', 'volcano-system')
        f.c.k('rollout', 'status', 'deployment/worker-type-test', '-n', 'volcano-system', '--timeout=30s')
        new = {p['metadata']['uid'] for p in f.c.get('pods', ns='volcano-system')['items'] if p['metadata'].get('labels', {}).get('app') == 'worker-type-test'}
        require(old.isdisjoint(new), 'Scheduler did not restart')
    f.expect(2 if partial else 1)
    wait(lambda: not f.c.journal())
    f.floor(minimum)


@case('D11', 'recovery cancels retry after victim permission removed')
def cancel_protection(f, _):
    f.six(5)
    f.c.fault('before')
    f.train()
    wait(lambda: f.c.journal())
    intent = next(iter(f.c.journal().values()))
    # Protect every candidate so fresh planning cannot select another worker.
    for p in f.pods():
        if p['metadata']['name'].startswith('shared-'):
            f.c.patch('pod', p['metadata']['name'], {'metadata': {'labels': {ALLOW: 'false'}}}, NS)
    wait(lambda: not f.c.journal())
    f.observe()
    require(not f.deleted() and not f.bound(), 'Protected victim retried')
    require(f.c.get('pod', intent['name'], NS)['metadata']['uid'] == intent['uid'], 'UID changed')


@case('D14', 'corrupt journal blocks preemption and recovers when restored')
def corrupt(f, _):
    full(f)
    f.c.patch('cm', JOURNAL, {'data': {'intents.json': 'not-json'}}, NS)
    try:
        f.train()
        f.observe()
        require(not f.deleted() and not f.bound(), 'Corrupt journal did not fail closed')
    finally:
        # Restores the originally empty journal, before any Eviction was allowed.
        f.c.patch('cm', JOURNAL, {'data': {'intents.json': '{}'}}, NS)
    f.expect(1)


@case('D17,D17-restart,D17-replaced,D17-partial', 'canceled training must not cause deferred eviction')
def canceled_training(f, ident):
    partial = ident == 'D17-partial'
    f.six(4 if partial else 5)
    f.c.fault('before', nth=2 if partial else 1)
    f.train(gpu=2 if partial else 1, selector={'test.local/slot': 'a'})
    wait(lambda: 'FAULT_BEFORE' in f.c.logs('fault-proxy', f.since))
    require(f.c.journal(), 'Fault did not leave a recovery intent')
    (f.evidence / 'fault-intent.json').write_text(json.dumps(f.c.journal(), indent=2))
    f.c.k('delete', 'pod', 'training', '-n', NS, '--wait=false')
    if ident == 'D17-replaced':
        wait(lambda: not f.c.k('get', 'pod', 'training', '-n', NS, '--ignore-not-found', '-o', 'name').strip())
        # Same name, new UID and no GPU demand: must not inherit the old intent.
        f.c.create({'apiVersion':'v1','kind':'Pod','metadata':{'name':'training','namespace':NS},
                    'spec':{'schedulerName':'test-replacement-unhandled','containers':[{'name':'worker','image':'busybox:1.36'}]}})
    if ident == 'D17-restart':
        (f.evidence / 'proxy-before-restart.log').write_text(f.c.logs('fault-proxy', f.since))
        f.c.k('rollout','restart','deployment/worker-type-test','-n','volcano-system')
        f.c.k('rollout','status','deployment/worker-type-test','-n','volcano-system','--timeout=60s')
    wait(lambda: not f.c.journal())
    f.observe()
    require(len(f.deleted()) == (1 if partial else 0), 'Canceled training caused unnecessary worker eviction')
    requests = f.c.logs('fault-proxy', f.since).count('EVICTION_REQUEST')
    require(requests == (0 if ident == 'D17-restart' else 2 if partial else 1), 'Eviction retried after cancellation')
    f.floor(4 if partial else 5)


@case('C05', 'replacement readiness restores real PDB budget')
def replacement(f, _):
    f.six(5)
    f.train()
    f.expect(1)
    victim = next(iter(f.deleted()))
    old = f.initial[victim]
    f.c.k('delete', 'pod', 'training', '-n', NS, '--force', '--grace-period=0', '--wait=false')
    wait(lambda: not f.c.journal())
    # A new PodGroup name avoids re-creating the existing original PodGroup.
    replacement_name = 'replacement-a'
    f.pod(replacement_name, node=old['spec']['nodeName'])
    wait(lambda: f.c.get('pdb', 'type-a', NS)['status'].get('disruptionsAllowed') == 1)
    f.train('training-next')
    wait(lambda: f.bound('training-next'))
    f.observe()
    require(len(f.deleted()) == 2, 'Expected one additional eviction after readiness restored')
    f.floor(5)


@case('C12', 'external API Eviction and plugin share native PDB budget')
def external_eviction(f, _):
    f.six(5)
    f.train(gate=True)
    pod = f.c.get('pod', 'shared-b-0', NS)
    def external():
        try:
            f.c.k('create', '--raw', '/api/v1/namespaces/' + NS + '/pods/shared-b-0/eviction', '-f', '-',
                  obj={'apiVersion': 'policy/v1', 'kind': 'Eviction', 'metadata': {'name': 'shared-b-0', 'namespace': NS}, 'deleteOptions': {'preconditions': {'uid': pod['metadata']['uid']}}})
            return 'accepted'
        except RuntimeError as exc:
            require('TooManyRequests' in str(exc) or 'disruption budget' in str(exc).lower(), 'Unexpected external API error: ' + str(exc))
            return 'PDB-rejected'
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(external)
        b = pool.submit(f.c.patch, 'pod', 'training', {'spec': {'schedulingGates': None}}, NS)
        outcome = a.result()
        b.result()
    wait(lambda: f.bound())
    f.observe()
    require(len(f.deleted()) == 1, 'External and plugin Evictions overspent budget')
    f.floor(5)
    (f.evidence / 'external-eviction.txt').write_text(outcome)


@case('B09', 'infeasible gang does not cause partial worker loss')
def gang(f, _):
    full(f)
    f.c.create({'apiVersion': 'scheduling.volcano.sh/v1beta1', 'kind': 'PodGroup', 'metadata': {'name': 'training-gang', 'namespace': NS},
                'spec': {'queue': 'wt-shared', 'minMember': 2, 'priorityClassName': 'wt-high'}})
    for i in range(2):
        # Both require all four reclaimable GPUs on node a. Only one can fit.
        f.train('training-' + str(i), pg='training-gang', gpu=4, selector={'test.local/slot': 'a'}, gate=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda i: f.c.patch('pod', 'training-' + str(i), {'spec': {'schedulingGates': None}}, NS), range(2)))
    f.observe(18)
    require(not f.deleted(), 'Infeasible gang caused partial eviction')
    require(not f.bound('training-0') and not f.bound('training-1'), 'Infeasible gang partially bound')


@case('F03', 'new training arrivals cannot bypass unresolved eviction')
def arrivals(f, _):
    f.six(5)
    f.finish_deletions = False
    f.train('training-0')
    wait(lambda: f.c.journal())
    for i in range(1, 5):
        f.train('training-' + str(i))
        time.sleep(.5)
    f.observe()
    require(len(f.deleted()) == 1, 'Additional victims while intent unresolved')
    require(f.c.logs('fault-proxy', f.since).count('EVICTION_REQUEST') == 1, 'Duplicate/extra eviction submitted')
    f.finish_deletions = True
    wait(lambda: not f.c.journal())
    f.observe()
    require(len(f.deleted()) == 1, 'Burst exceeded floor after recovery')
    f.floor(5)


@case('D10', 'same name with new UID is never retried as old eviction')
def uid_replacement(f, _):
    f.six(5)
    f.c.fault('before')
    f.train()
    wait(lambda: 'FAULT_BEFORE' in f.c.logs('fault-proxy', f.since))
    intent = next(iter(f.c.journal().values()))
    # Prevent subsequent new plans from choosing a different victim.
    f.c.k('delete', 'pod', 'training', '-n', NS, '--wait=false')
    old = f.c.get('pod', intent['name'], NS)
    f.c.k('delete', 'pod', intent['name'], '-n', NS, '--force', '--grace-period=0', '--wait=true', '--timeout=10s')
    new = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': intent['name'], 'namespace': NS,
           'labels': {TYPE: 'A', ALLOW: 'false'}, 'annotations': old['metadata'].get('annotations', {})}, 'spec': old['spec']}
    f.c.create(new)
    current = f.c.get('pod', intent['name'], NS)
    require(current['metadata']['uid'] != intent['uid'], 'Replacement must have different UID')
    wait(lambda: not f.c.journal())
    f.observe()
    after = f.c.get('pod', intent['name'], NS)
    require(after['metadata']['uid'] == current['metadata']['uid'] and not after['metadata'].get('deletionTimestamp'), 'Replacement incorrectly evicted')
    # Require the proxy to have seen no second Eviction at all, not merely a rejection.
    require(f.c.logs('fault-proxy', f.since).count('EVICTION_REQUEST') == 1, 'Retried a completed old intent')


@case('D01,D02', 'journal write rejection or conflict prevents any Eviction')
def journal_failure(f, ident):
    full(f)
    f.c.fault('reject', target='journal-write', statusCode=403 if ident == 'D01' else 409, repeat=1000)
    f.train()
    wait(lambda: 'FAULT_REJECT' in f.c.logs('fault-proxy', f.since))
    f.observe()
    require('EVICTION_REQUEST' not in f.c.logs('fault-proxy', f.since), 'Eviction submitted without durable write')
    require(not f.deleted() and not f.bound(), 'Journal failure did not fail closed')
    f.c.fault()
    f.expect(1)


@case('D03', 'restart after durable journal write but before Eviction POST')
def restart_write(f, _):
    f.six(5)
    f.c.fault('after', target='journal-write')
    f.train()
    wait(lambda: 'FAULT_AFTER' in f.c.logs('fault-proxy', f.since))
    require(f.c.journal(), 'Journal write was not accepted')
    log = f.c.logs('fault-proxy', f.since)
    require('EVICTION_REQUEST' not in log, 'Crash window missed: POST already sent')
    (f.evidence / 'proxy-before-restart.log').write_text(log)
    f.c.k('rollout', 'restart', 'deployment/worker-type-test', '-n', 'volcano-system')
    f.c.k('rollout', 'status', 'deployment/worker-type-test', '-n', 'volcano-system', '--timeout=30s')
    f.expect(1)
    wait(lambda: not f.c.journal())
    f.floor(5)


@case('D06-403,D06-429,D07', 'Eviction rejection or ambiguous 5xx and eventual recovery')
def eviction_failure(f, ident):
    f.six(5)
    code = 503 if ident == 'D07' else int(ident[-3:])
    f.c.fault('reject', statusCode=code, repeat=1000 if code != 503 else 1)
    f.train()
    wait(lambda: 'FAULT_REJECT' in f.c.logs('fault-proxy', f.since))
    if code != 503:
        f.observe()
        require(not f.deleted() and not f.bound(), 'Rejected Eviction released resources')
        f.c.fault()
    f.expect(1)
    wait(lambda: not f.c.journal())
    f.floor(5)


@case('D15', 'repeated timeouts back off and eventually resolve same UID')
def repeated_timeout(f, _):
    f.six(5)
    f.c.fault('before', repeat=3)
    f.train()
    wait(lambda: f.c.journal())
    original_uid = next(iter(f.c.journal()))
    wait(lambda: any(x['attempts'] >= 3 for x in f.c.journal().values()), description='three persisted attempts')
    require(set(f.c.journal()) == {original_uid}, 'Retry selected a different UID')
    f.expect(1)
    wait(lambda: not f.c.journal())
    f.floor(5)


@case('D16', 'journal clear failure cannot trigger duplicate eviction')
def clear_failure(f, _):
    f.six(5)
    f.c.fault('reject', target='journal-clear', statusCode=503, repeat=1000)
    f.train()
    wait(lambda: 'FAULT_REJECT target=journal-clear' in f.c.logs('fault-proxy', f.since))
    require(f.c.journal(), 'Expected durable unresolved record')
    f.observe()
    require(len(f.deleted()) == 1, 'Repeated victim during journal cleanup failure')
    require(f.c.logs('fault-proxy', f.since).count('EVICTION_REQUEST') == 1, 'Duplicate Eviction')
    f.c.fault()
    wait(lambda: not f.c.journal())
    f.expect(1)
    f.floor(5)


@case('F02', 'higher priority training wins when both requests become eligible together')
def priorities(f, _):
    full(f)
    f.pdb(minimum=7)  # Eight healthy shared A workers permit only one victim.
    wait(lambda: f.c.get('pdb', 'type-a', NS).get('status', {}).get('disruptionsAllowed') == 1)
    f.c.create({'apiVersion': 'scheduling.k8s.io/v1', 'kind': 'PriorityClass', 'metadata': {'name': 'wt-medium'}, 'value': 50, 'globalDefault': False})
    # Hold both requests behind a scheduler restart, so API arrival order cannot
    # legitimately allow the medium task to allocate before the high task exists.
    f.c.k('scale', 'deployment/worker-type-test', '-n', 'volcano-system', '--replicas=0')
    try:
        wait(lambda: not f.c.k('get', 'pods', '-n', 'volcano-system', '-l', 'app=worker-type-test', '-o', 'name').strip())
        f.train('training-high')
        f.c.create({'apiVersion': 'scheduling.volcano.sh/v1beta1', 'kind': 'PodGroup', 'metadata': {'name': 'training-medium', 'namespace': NS},
                    'spec': {'queue': 'wt-shared', 'minMember': 1, 'priorityClassName': 'wt-medium'}})
        spec = f.c.get('pod', 'training-high', NS)['spec']
        spec['priorityClassName'] = 'wt-medium'
        spec.pop('priority', None)
        f.c.create({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'training-medium', 'namespace': NS,
                    'annotations': {'scheduling.k8s.io/group-name': 'training-medium'}}, 'spec': spec})
    finally:
        f.c.k('scale', 'deployment/worker-type-test', '-n', 'volcano-system', '--replicas=1')
        f.c.k('rollout', 'status', 'deployment/worker-type-test', '-n', 'volcano-system', '--timeout=30s')
    f.expect(1, training='training-high')
    require(not f.bound('training-medium'), 'Lower priority consumed the only disruption budget')


@case('F04', 'disabling custom action stops new eviction but preserves allocation')
def disable_action(f, _):
    full(f)
    cm = f.c.get('cm', 'worker-type-test', 'volcano-system')
    original = cm['data']['volcano-scheduler.conf']
    changed = original.replace('allocate, worker-type-preempt, backfill', 'allocate, backfill')
    require(changed != original, 'Unexpected scheduler config')
    try:
        f.c.patch('cm', 'worker-type-test', {'data': {'volcano-scheduler.conf': changed}}, 'volcano-system')
        f.c.k('rollout', 'restart', 'deployment/worker-type-test', '-n', 'volcano-system')
        f.c.k('rollout', 'status', 'deployment/worker-type-test', '-n', 'volcano-system', '--timeout=30s')
        f.train()
        f.train('training-cpu', gpu=0)
        wait(lambda: f.bound('training-cpu'))
        f.observe()
        require(not f.deleted() and not f.bound(), 'Disabled action still evicted worker')
    finally:
        f.c.patch('cm', 'worker-type-test', {'data': {'volcano-scheduler.conf': original}}, 'volcano-system')
        f.c.k('rollout', 'restart', 'deployment/worker-type-test', '-n', 'volcano-system')
        f.c.k('rollout', 'status', 'deployment/worker-type-test', '-n', 'volcano-system', '--timeout=30s')
    f.expect(1)


@case('A10', 'same type in another namespace does not inflate local ranking')
def namespaces(f, _):
    other = 'worker-type-e2e-other'
    require(not f.c.k('get', 'ns', other, '--ignore-not-found').strip(), 'Secondary namespace already exists')
    f.c.create({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': other}})
    f.extra_namespaces.append(other)
    f.pod('base-a', typ='BASE', queue='wt-base', gpu=5)
    f.pod('a-local', typ='A')
    f.pod('b-local', typ='B', gpu=2)
    f.pod('base-b', typ='BASE', queue='wt-base', node=NODES[1], gpu=4)
    f.c.create({'apiVersion': 'scheduling.volcano.sh/v1beta1', 'kind': 'PodGroup', 'metadata': {'name': 'a-other', 'namespace': other}, 'spec': {'queue': 'wt-shared', 'minMember': 1}})
    spec = f.c.get('pod', 'a-local', NS)['spec']
    spec['nodeName'] = NODES[1]
    spec['containers'][0]['resources']['requests']['nvidia.com/gpu'] = '4'
    spec['containers'][0]['resources']['limits']['nvidia.com/gpu'] = '4'
    f.c.create({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'a-other', 'namespace': other,
                'labels': {TYPE: 'A', ALLOW: 'true'}, 'annotations': {'scheduling.k8s.io/group-name': 'a-other'}}, 'spec': spec})
    f.c.patch('pod', 'a-other', {'status': {'phase': 'Running'}}, other, True)
    uid = f.c.get('pod', 'a-other', other)['metadata']['uid']
    f.train(selector={'test.local/slot': 'a'})
    f.expect(1)
    require(f.deleted() == {'b-local'}, 'Namespace/type aggregation incorrectly selected A')
    pod = f.c.get('pod', 'a-other', other)
    require(pod['metadata']['uid'] == uid and not pod['metadata'].get('deletionTimestamp'), 'Other namespace modified')


def managed_workers(f, minimum):
    from harness import OWNER
    f.c.create({'apiVersion': 'scheduling.volcano.sh/v1beta1', 'kind': 'PodGroup', 'metadata': {'name': 'managed', 'namespace': NS},
                'spec': {'queue': 'wt-shared', 'minMember': 1}})
    labels = {TYPE: 'A', ALLOW: 'true', OWNER: 'preemption-e2e', 'test.local/controller': 'managed'}
    f.c.create({'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {'name': 'managed', 'namespace': NS}, 'spec': {'replicas': 6,
                'selector': {'matchLabels': {'test.local/controller': 'managed'}},
                'template': {'metadata': {'labels': labels, 'annotations': {'scheduling.k8s.io/group-name': 'managed'}},
                             'spec': {'schedulerName': 'worker-type-test', 'terminationGracePeriodSeconds': 10,
                                      'nodeSelector': {'test.local/slot': 'a'},
                                      'tolerations': [{'key': 'worker-type-test', 'operator': 'Exists', 'effect': 'NoSchedule'}],
                                      'containers': [{'name': 'worker', 'image': 'busybox:1.36', 'command': ['sleep', '3600'],
                                                      'resources': {'requests': {'cpu': '100m', 'memory': '32Mi', 'nvidia.com/gpu': '1'}, 'limits': {'nvidia.com/gpu': '1'}}}]}}}})
    wait(lambda: len([p for p in f.pods() if p['spec'].get('nodeName') == NODES[0]]) == 6)
    for p in f.pods():
        name = p['metadata']['name']
        f.c.patch('pod', name, {'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}}, NS, True)
        f.initial[name] = f.c.get('pod', name, NS)
    f.pdb(minimum)
    wait(lambda: f.c.get('pdb', 'type-a', NS).get('status', {}).get('currentHealthy') == 6)
    f.wait_pdb_baseline()


@case('C14', 'percentage PDB uses actual ReplicaSet scale')
def percentage(f, _):
    managed_workers(f, '50%')
    wait(lambda: f.c.get('pdb', 'type-a', NS)['status'].get('disruptionsAllowed') == 3)
    f.train(gpu=5, selector={'test.local/slot': 'a'})
    f.expect(3)
    f.floor(3)
    require(f.c.get('pdb', 'type-a', NS)['status']['desiredHealthy'] == 3, 'Wrong percentage floor')


@case('E09', 'ReplicaSet replacement waits during training and binds after training removal')
def controller_restore(f, _):
    managed_workers(f, 5)
    f.train(gpu=3, selector={'test.local/slot': 'a'})
    f.expect(1)
    old_uids = {p['metadata']['uid'] for p in f.initial.values()}
    def replacements():
        return [p for p in f.pods() if p['metadata'].get('labels', {}).get(TYPE) == 'A' and p['metadata']['uid'] not in old_uids]
    wait(lambda: replacements())
    require(all(not p['spec'].get('nodeName') for p in replacements()), 'Replacement overcommitted training node')
    f.c.k('delete', 'pod', 'training', '-n', NS, '--force', '--grace-period=0', '--wait=false')
    wait(lambda: any(p['spec'].get('nodeName') == NODES[0] for p in replacements()))
    for p in replacements():
        f.c.patch('pod', p['metadata']['name'], {'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}}, NS, True)
    wait(lambda: f.c.get('pdb', 'type-a', NS)['status'].get('currentHealthy') == 6)
    f.floor(5)


@case('F08', 'explicit external delete is not attributed to plugin Eviction')
def external_delete(f, _):
    full(f)
    f.c.k('delete', 'pod', 'shared-a-0', '-n', NS, '--force', '--grace-period=0', '--wait=false')
    f.observe()
    require(f.deleted() == {'shared-a-0'}, 'Unexpected additional removal')
    require('EVICTION_REQUEST' not in f.c.logs('fault-proxy', f.since), 'Plugin evicted without demand')
    require(not f.c.journal(), 'External deletion generated plugin intent')


@case('B03-memory', 'memory fragmentation chooses feasible node only')
def memory_fragmentation(f, _):
    full(f)
    f.capacity[NODES[0]]['memory'] = '512Mi'
    f.tick()
    f.train(gpu=2, memory='512Mi')
    f.expect(2)
    require(f.bound() == NODES[1], 'Bound to memory-infeasible node')
    require(all(n.startswith('shared-b-') for n in f.deleted()), 'Evicted on memory-infeasible node')


@case('C14-maxUnavailable', 'maxUnavailable uses actual ReplicaSet scale')
def maximum_unavailable(f, _):
    managed_workers(f, 5)
    f.c.patch('pdb', 'type-a', {'spec': {'minAvailable': None, 'maxUnavailable': 1}}, NS)
    wait(lambda: f.c.get('pdb', 'type-a', NS)['status'].get('observedGeneration') == f.c.get('pdb', 'type-a', NS)['metadata']['generation'])
    f.train(gpu=3, selector={'test.local/slot': 'a'})
    f.expect(1)
    f.floor(5)


def cpu_memory_pressure(f, resource='both'):
    for slot in ['a', 'b']:
        node = 'wt-node-' + slot
        f.capacity[node]['cpu'] = '4'
        f.capacity[node]['memory'] = '4Gi'
    f.tick()
    for slot in ['a', 'b']:
        f.pod('shared-' + slot, node='wt-node-' + slot, gpu=1,
              cpu='4' if resource in ('both', 'cpu') else '1',
              memory='4Gi' if resource in ('both', 'memory') else '1Gi')


@case('H01,H02', 'CPU-only high priority cannot evict GPU workers for CPU or memory')
def cpu_cannot_preempt(f, ident):
    cpu_memory_pressure(f, 'cpu' if ident == 'H01' else 'memory')
    f.train(gpu=0, cpu='1', memory='1Gi')
    f.observe()
    require(not f.deleted() and not f.bound(), 'CPU-only training evicted GPU worker')
    require('EVICTION_REQUEST' not in f.c.logs('fault-proxy', f.since), 'CPU-only Eviction submitted')


@case('H03', 'GPU training can preempt when CPU/memory rather than GPU is exhausted')
def gpu_with_cpu_pressure(f, _):
    cpu_memory_pressure(f)
    f.train(gpu=1, cpu='1', memory='1Gi')
    f.expect(1)


@case('H04,H05,H06,H07', 'admitted resource requests determine CPU/memory accounting')
def request_defaulting(f, ident):
    cpu_memory_pressure(f)
    if ident == 'H07':
        f.c.create({'apiVersion':'v1','kind':'LimitRange','metadata':{'name':'e2e-defaults','namespace':NS},
                    'spec':{'limits':[{'type':'Container','defaultRequest':{'cpu':'250m','memory':'256Mi'},
                                      'default':{'cpu':'500m','memory':'512Mi'}}]}})
    try:
        resources = {} if ident == 'H06' else {'limits':{'nvidia.com/gpu':'1'}}
        if ident == 'H05':resources['limits'].update({'cpu':'1','memory':'1Gi'})
        f.c.create({'apiVersion':'scheduling.volcano.sh/v1beta1','kind':'PodGroup',
                    'metadata':{'name':'training','namespace':NS},'spec':{'queue':'wt-shared','minMember':1,'priorityClassName':'wt-high'}})
        f.c.create({'apiVersion':'v1','kind':'Pod','metadata':{'name':'training','namespace':NS,
                    'annotations':{'scheduling.k8s.io/group-name':'training'}},
                    'spec':{'schedulerName':'worker-type-test','priorityClassName':'wt-high',
                            'nodeSelector':{'worker-type-test':'true'},
                            'tolerations':[{'key':'worker-type-test','operator':'Exists','effect':'NoSchedule'}],
                            'containers':[{'name':'worker','image':'busybox:1.36','resources':resources}]}})
        p=f.c.get('pod','training',NS)
        (f.evidence/'admitted-training.json').write_text(json.dumps(p,indent=2))
        req=p['spec']['containers'][0].get('resources',{}).get('requests',{})
        if ident in ('H04','H06'):
            require('cpu' not in req and 'memory' not in req, 'Unexpected defaults in test namespace')
            f.expect(0)
        elif ident == 'H05':
            require(req.get('cpu')=='1' and req.get('memory')=='1Gi' and req.get('nvidia.com/gpu')=='1', 'Limits not defaulted to requests')
            f.expect(1)
        else:
            require(req.get('cpu')=='250m' and req.get('memory')=='256Mi' and req.get('nvidia.com/gpu')=='1', 'LimitRange defaults missing')
            f.expect(1)
    finally:
        if ident == 'H07':f.c.k('delete','limitrange','e2e-defaults','-n',NS,'--ignore-not-found')
