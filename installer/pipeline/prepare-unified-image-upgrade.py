#!/usr/bin/env python3
"""Prepare a guarded FIRST-STAGE backend image upgrade. Kubernetes reads only.

Does not transfer Arena admission, edit policies/quotas/ACLs, or enable preemption.
The rollback is image-only and valid only before subsequent admission handoff.
"""
import argparse
import copy
import json
import pathlib
import re
import subprocess


def require(ok, message):
    if not ok:
        raise ValueError(message)


def patch_image(deployment, image):
    spec = deployment['spec']
    require(spec.get('replicas', 1) >= 2, 'workload-policy must have at least two replicas')
    require(spec.get('strategy', {}).get('type', 'RollingUpdate') == 'RollingUpdate',
            'expected RollingUpdate strategy')
    pod = spec['template']['spec']
    require(pod.get('schedulerName', 'default-scheduler') == 'default-scheduler',
            'admission service must use default-scheduler')
    matches = [i for i, c in enumerate(pod['containers']) if c['name'] == 'webhook']
    require(len(matches) == 1, 'expected exactly one webhook container')
    i = matches[0]
    c = pod['containers'][i]
    require(c.get('command') in (None, [], ['/workload-routing-webhook'], ['/workload-policy-webhook']),
            'unexpected explicit command; review before upgrade')
    require(c.get('readinessProbe'), 'readinessProbe required')
    require(c['image'] != image, 'image already installed; do not overwrite rollback snapshot')
    target = copy.deepcopy(spec)
    target['template']['spec']['containers'][i]['image'] = image
    path = f'/spec/template/spec/containers/{i}/image'
    def patch(old_spec, new_image):
        return [
            {'op': 'test', 'path': '/metadata/uid', 'value': deployment['metadata']['uid']},
            {'op': 'test', 'path': '/spec', 'value': old_spec},
            {'op': 'replace', 'path': path, 'value': new_image},
        ]
    return patch(spec, image), patch(target, c['image'])


def check_split(webhooks):
    """Never issue an image-only rollback promise if PTJ roots already use v2 backend."""
    policy_routes = 0
    arena_routes = 0
    for obj in webhooks['items']:
        for w in obj.get('webhooks', []):
            svc = w.get('clientConfig', {}).get('service', {})
            if svc.get('namespace') != 'volcano-system':
                continue
            if svc.get('name') == 'workload-routing':
                arena_routes += 1
            if svc.get('name') != 'workload-policy':
                continue
            if svc.get('path') not in ('/mutate', '/mutate-workload', '/validate-workload'):
                continue
            policy_routes += 1
            for rule in w.get('rules', []):
                if set(rule.get('apiGroups', [])) & {'*', 'kubeflow.org'}:
                    require(not set(rule.get('resources', [])) & {'*', '*/*', 'pytorchjobs', 'pytorchjobs/*'},
                            'PyTorch already handled by workload-policy; use reviewed handoff rollback instead')
    require(policy_routes > 0 and arena_routes > 0,
            'expected existing separate ordinary and Arena webhook backends')


def unchanged_projection(obj):
    if 'webhooks' in obj:
        result = copy.deepcopy(obj['webhooks'])
        # Certificate rotation is independent of routing and must not cause a false drift alarm.
        for w in result:
            w.get('clientConfig', {}).pop('caBundle', None)
        # Validation entries have no mutation ordering semantics. Kyverno can
        # reconcile the same named entries in a different list order. Preserve
        # every entry and field; only normalize this outer validation list.
        # Never extend this to mutating webhooks, whose order can matter.
        if obj.get('kind') == 'ValidatingWebhookConfiguration':
            result.sort(key=lambda w: w['name'])
        return result
    return obj.get('spec', obj.get('data', {}))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['prepare', 'verify'])
    p.add_argument('--context', required=True)
    p.add_argument('--state', type=pathlib.Path, required=True)
    p.add_argument('--image')
    a = p.parse_args()
    def get(resource, name=None, namespace=None):
        args = ['kubectl', '--context', a.context, '--request-timeout=30s', 'get', resource]
        if name:
            args += [name]
        if namespace:
            args += ['-n', namespace]
        return json.loads(subprocess.check_output(args + ['-o', 'json'], text=True))
    def write(name, value):
        (a.state / name).write_text(json.dumps(value, indent=2) + '\n')
    def read(name):
        return json.loads((a.state / name).read_text())
    if a.action == 'prepare':
        require(a.image and re.fullmatch(r'[^\s@]+@sha256:[0-9a-f]{64}', a.image),
                '--image must be a digest-pinned registry image')
        a.state.mkdir(mode=0o700, parents=True, exist_ok=False)
        write('context.json', {'context': a.context, 'image': a.image, 'stage': 'backend-image-only'})
        dep = get('deployment', 'workload-policy', 'volcano-system')
        write('deployment-before.json', dep)
        webhooks = get('mutatingwebhookconfigurations,validatingwebhookconfigurations')
        write('webhooks-before.json', webhooks)
        check_split(webhooks)
        policies = get('workloadroutingpolicies.scheduling.simplesilicon.com,nodepoolpolicies.scheduling.simplesilicon.com')
        write('policies-before.json', policies)
        write('kyverno-before.json', get('clusterpolicies.kyverno.io'))
        acl_crd = subprocess.check_output(
            ['kubectl', '--context', a.context, '--request-timeout=30s', 'get', 'crd',
             'queueaccesspolicies.scheduling.simplesilicon.com', '--ignore-not-found', '-o', 'name'], text=True)
        write('queue-access-before.json', get('queueaccesspolicies.scheduling.simplesilicon.com')
              if acl_crd.strip() else {'items': []})
        write('queues-before.json', get('queues.scheduling.volcano.sh'))
        write('arena-backend-before.json', get('deployment', 'workload-routing', 'volcano-system'))
        schedulers = []
        configs = {}
        for name in ['volcano-scheduler', 'volcano-infra-scheduler']:
            d = get('deployment', name, 'volcano-system')
            schedulers.append(d)
            for v in d['spec']['template']['spec'].get('volumes', []):
                cm = v.get('configMap', {}).get('name')
                if not cm or cm in configs:
                    continue
                o = get('configmap', cm, 'volcano-system')
                configs[cm] = o
                for value in o.get('data', {}).values():
                    for action in re.findall(r'^actions:\s*(.*)$', value, re.M):
                        tokens = {s.strip(' \"\'') for s in action.split(',')}
                        require(not tokens & {'preempt', 'reclaim', 'worker-type-preempt'},
                                f'{cm}: preemption action already enabled; stop and review')
        write('schedulers-before.json', {'items': schedulers})
        write('active-configmaps-before.json', {'items': list(configs.values())})
        forward, rollback = patch_image(dep, a.image)
        write('upgrade-image.json', forward)
        write('rollback-image.json', rollback)
        write('plan.json', {
            'deployment': 'volcano-system/workload-policy',
            'oldImage': next(c['image'] for c in dep['spec']['template']['spec']['containers'] if c['name'] == 'webhook'),
            'newImage': a.image,
            'changes': ['webhook container image only'],
            'unchanged': ['Arena backend and routing', 'namespace scopes', 'system exclusions',
                          'queue ACL enforcement', 'scheduler configs', 'preemption actions',
                          'business workload specs'],
            'nextStageRequires': ['fresh Arena v1 owner registry and Trainer identities',
                                  'Arena poolRef/acceptedRoutes', 'descendant webhook gates',
                                  'guarded Arena service handoff and separate ACL retirement'],
            'rollbackBoundary': 'image-only rollback is for this stage before Arena handoff',
        })
        print(f'PREPARED {a.state}: reads only; review plan.json. No production changes.')
    else:
        state = read('context.json')
        require(state['context'] == a.context, 'context differs from saved snapshot')
        current = get('deployment', 'workload-policy', 'volcano-system')
        old = read('deployment-before.json')
        expected = copy.deepcopy(old['spec'])
        for c in expected['template']['spec']['containers']:
            if c['name'] == 'webhook':
                c['image'] = state['image']
        require(current['metadata']['uid'] == old['metadata']['uid'], 'Deployment UID changed')
        require(current['spec'] == expected, 'Deployment differs from image-only plan')
        status = current.get('status', {})
        replicas = expected['replicas']
        require(status.get('observedGeneration', 0) >= current['metadata']['generation'] and
                status.get('updatedReplicas', 0) == replicas and
                status.get('availableReplicas', 0) == replicas and
                status.get('replicas', 0) == replicas, 'rollout not complete')
        for filename in ['webhooks-before.json', 'policies-before.json', 'kyverno-before.json',
                         'queue-access-before.json', 'queues-before.json', 'schedulers-before.json',
                         'active-configmaps-before.json', 'arena-backend-before.json']:
            saved = read(filename)
            for o in saved.get('items', [saved]):
                meta = o['metadata']
                now = get(o['kind'], meta['name'], meta.get('namespace'))
                require(now['metadata']['uid'] == meta['uid'] and
                        unchanged_projection(now) == unchanged_projection(o),
                        f"configuration changed: {o['kind']}/{meta['name']}")
        print('PASS: backend rollout complete; snapshotted policies/webhook gates, Arena backend, queues and active scheduler configs unchanged.')
        print('This is image staging only. Arena handoff and queue namespace ACL retirement have NOT been performed.')


if __name__ == '__main__':
    main()
