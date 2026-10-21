"""Queue admission is open; pipeline-specific eviction authorization remains enforced."""
from cases import case
from harness import require, wait
from access_cases import setup, cleanup, train, bound, NSS
from access_cases import concurrent_namespaces, revoke, recovery
from pipeline_cases import combined_concurrency
from harness import NODES


def without_acl(f):
    conf = f.c.get('cm', 'worker-type-test', 'volcano-system')['data']['volcano-scheduler.conf']
    require('name: queue-access' not in conf and 'queueAccess: true' not in conf,
            'This regression requires queue ACL disabled')
    require(not f.c.k('get', 'validatingwebhookconfiguration', 'wt-queue-access',
                     '--ignore-not-found').strip(), 'Queue ACL admission must be absent')


@case('P24', 'without queue ACL an unlisted namespace uses idle shared GPU but cannot gain eviction rights')
def idle_queue(f, _):
    without_acl(f)
    try:
        setup(f)
        # setup deliberately leaves legacy QueueAccessPolicy denying NSS[2].
        # No victim exists: normal allocation must ignore that obsolete ACL.
        train(f, NSS[2])
        wait(lambda: bound(f, NSS[2]), description='idle capacity admitted despite legacy queue ACL')
        f.observe(4)
        require(not f.deleted(), 'Idle allocation unexpectedly evicted a worker')
        require('EVICTION_REQUEST' not in f.c.logs('fault-proxy', f.since), 'Unexpected Eviction')
    finally:
        cleanup(f)


@case('P25', 'without queue ACL concurrent remote demands share GPU and PDB floors; base protected')
def concurrent_remote(f, ident):
    without_acl(f)
    concurrent_namespaces(f, ident)


@case('P26', 'without queue ACL revocation blocks eviction and reauthorization resumes pending demand')
def revoke_remote(f, _):
    without_acl(f)
    revoke(f, 'P21')


@case('P27', 'without queue ACL revoked demand cannot continue ambiguous recovery evictions')
def revoked_recovery(f, ident):
    without_acl(f)
    recovery(f, ident)


@case('P28', 'capacity/nodegroup/binpack composition preserves concurrent pipeline GPU/PDB floors')
def production_plugins(f, ident):
    without_acl(f)
    conf = f.c.get('cm', 'worker-type-test', 'volcano-system')['data']['volcano-scheduler.conf']
    require(all('name: '+name in conf for name in ['capacity', 'nodegroup', 'binpack']),
            'Production plugin composition required')
    for node in NODES:
        f.c.patch('node', node, {'metadata': {'labels': {'volcano.sh/nodegroup-name': 'wt-pipeline'}}})
    for queue in ['wt-base', 'wt-shared']:
        f.c.patch('queue', queue, {'spec': {
            'deserved': {'nvidia.com/gpu': '8'},
            'capability': {'nvidia.com/gpu': '8'},
            'affinity': {'nodeGroupAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': ['wt-pipeline']}}}})
    combined_concurrency(f, ident)
