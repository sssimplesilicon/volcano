#!/usr/bin/env python3
"""Guarded Arena admission handoff. prepare/verify read the cluster; only enable/rollback write.

No workload deletion, quota change, or preemption action. Rollback restores root
CREATE ingress only; v2 descendant protection and retired namespace ACLs remain.
"""
import argparse
import copy
import hashlib
import json
import pathlib
import re
import subprocess

DKEY = 'scheduling.simplesilicon.com/workload-decision'
INJECT = 'cert-manager.io/inject-ca-from'
IMAGE = 'ss-container-lib-registry.cn-shanghai.cr.aliyuncs.com/infra/volcano@sha256:3d431a973b35cf54bb9d96fb88d27e628c1dcd613d80f65c48996dad3c006436'
NS = 'volcano-system'
EXTRA = 'workload-policy-arena-descendants'


def require(ok, message):
    if not ok:
        raise ValueError(message)


def ptr(key):
    return key.replace('~', '~0').replace('/', '~1')


def at(obj, path):
    for key in path.strip('/').split('/'):
        key = key.replace('~1', '/').replace('~0', '~')
        obj = obj[int(key)] if isinstance(obj, list) else obj[key]
    return obj


def marked(obj='object'):
    return f"(has({obj}.metadata.annotations) && '{DKEY}' in {obj}.metadata.annotations && {obj}.metadata.annotations['{DKEY}'] != '')"


def check_actions(text):
    actions = re.findall(r'^actions:\s*[\"\']?([^\n]*?)\s*$', text, re.M)
    require(len(actions) == 1, 'expected one actions line')
    require([x.strip(' \"\'') for x in actions[0].split(',')] == ['enqueue', 'allocate', 'backfill'],
            'actions differ from non-preempting baseline')


def strip_acl(text):
    # Deliberately accept only the known simple YAML block, not arbitrary YAML.
    check_actions(text)
    require(len(re.findall(r'(?m)^\s*- name: queue-access\s*$', text)) == 1, 'expected exactly one queue-access plugin')
    lines = text.splitlines(keepends=True)
    i = next(i for i, s in enumerate(lines) if re.fullmatch(r'\s*- name: queue-access\s*', s))
    indent = len(lines[i]) - len(lines[i].lstrip())
    j = i + 1
    while j < len(lines) and (not lines[j].strip() or len(lines[j]) - len(lines[j].lstrip()) > indent):
        j += 1
    return ''.join(lines[:i] + lines[j:])


def scope_union(a, b):
    require(a.get('include') == ['*'] and b.get('include') == ['*'], 'expected existing all-business scopes')
    return {'include': ['*'], 'exclude': sorted(set(a.get('exclude', [])) & set(b.get('exclude', [])))}


def policy_targets(arena, pool, user):
    a, p = copy.deepcopy(arena['spec']), copy.deepcopy(pool['spec'])
    require({r['name'] for r in a['rules']} == {'ego-explicit', 'ego-legacy'}, 'unexpected Arena rules')
    require(a['schedulerName'] == 'volcano-infra', 'unexpected scheduler')
    require(a['fallback'] == {'queue': 'default', 'nodeGroup': 'general'}, 'unexpected fallback')
    a.update(mode='Enforce', workloadKinds=['PyTorchJob'])
    require(p.get('mode') == 'Enforce', 'NodePoolPolicy must already be Enforce')
    a['controllerUsers'] = sorted(set(a.get('controllerUsers', []) + [user]))
    for r in a['rules']:
        require(r['target'] == {'nodeGroup': 'infra-ego-pipeline', 'queue': 'infra-ego-pipeline-shared'}, 'unexpected Arena target')
        r['target']['poolRef'] = pool['metadata']['name']
    # Legacy refs remain only for the standby old router on rollback. Unified engine ignores them.
    p['namespaceScope'] = scope_union(p['namespaceScope'], a['namespaceScope'])
    p['controllerUsers'] = sorted(set(p.get('controllerUsers', []) + [user]))
    require(not any(x['policyRef'] == arena['metadata']['name'] for x in p['acceptedRoutes']), 'Arena pool acceptance already exists; do not overwrite original snapshot')
    p['acceptedRoutes'].append({'policyRef': arena['metadata']['name'], 'ruleNames': ['ego-explicit', 'ego-legacy']})
    return a, p


def descendant_hooks(root, client, injection):
    w = root['webhooks'][0]
    owners = "has(object.metadata.ownerReferences) && object.metadata.ownerReferences.exists(o, o.apiVersion == 'kubeflow.org/v1' && o.kind == 'PyTorchJob' && has(o.controller) && o.controller)"
    child = f"({owners}) && ({marked()} || (request.operation == 'UPDATE' && {marked('oldObject')}))"
    roots = f"{marked()} || (request.operation == 'UPDATE' && {marked('oldObject')})"
    def hook(name, path, group, version, resources, operations, expression, mutate):
        h = {k: copy.deepcopy(w[k]) for k in ['admissionReviewVersions', 'namespaceSelector', 'matchPolicy', 'failurePolicy', 'sideEffects', 'timeoutSeconds']}
        h.update(name=name+'.simplesilicon.com', clientConfig=copy.deepcopy(client), objectSelector={},
                 rules=[dict(apiGroups=[group], apiVersions=[version], operations=operations, resources=resources, scope='Namespaced')],
                 matchConditions=copy.deepcopy(w.get('matchConditions', []))+[{'name': 'v2-arena-only', 'expression': expression}])
        h['clientConfig']['service']['path'] = path
        if mutate:
            h['reinvocationPolicy'] = 'IfNeeded'
        return h
    m = [hook('arena-descendant-route', '/mutate-workload', '', 'v1', ['pods'], ['CREATE'], child, True),
         hook('arena-descendant-isolation', '/mutate-isolation', '', 'v1', ['pods'], ['CREATE'], child, True)]
    v = [hook('arena-root-validation', '/validate-workload', 'kubeflow.org', 'v1', ['pytorchjobs'], ['CREATE','UPDATE'], roots, False),
         hook('arena-descendant-validation', '/validate-workload', '', 'v1', ['pods'], ['CREATE','UPDATE'], child, False),
         hook('arena-descendant-isolation', '/validate-isolation', '', 'v1', ['pods'], ['CREATE'], child, False)]
    return [dict(apiVersion='admissionregistration.k8s.io/v1', kind=kind,
                 metadata={'name': EXTRA, 'annotations': {INJECT: injection}}, webhooks=hooks)
            for kind, hooks in [('MutatingWebhookConfiguration',m),('ValidatingWebhookConfiguration',v)]]


def projection(obj):
    """Only scheduling/identity fields; never persist environment variables or credentials."""
    def pod(s):
        return {k:s.get(k) for k in ['schedulerName','nodeSelector','affinity','tolerations','priorityClassName','schedulingGates']}
    m = obj['metadata']; s = obj['spec']
    q = {'kind':obj['kind'], 'name':m['name'], 'namespace':m['namespace'], 'uid':m['uid'],
         'decision':m.get('annotations',{}).get(DKEY)}
    if obj['kind']=='Pod':
        q['scheduling']=pod(s)
        q['queue']=m.get('annotations',{}).get('scheduling.volcano.sh/queue-name')
        q['node']=s.get('nodeName')
    else:
        q['scheduling']={k:pod(v['template']['spec']) for k,v in s['pytorchReplicaSpecs'].items()}
        q['queue']=s.get('runPolicy',{}).get('schedulingPolicy',{}).get('queue')
    return q


class Run:
    def __init__(self, args):
        self.a=args; self.state=args.state
    def k(self,*args,obj=None):
        r=subprocess.run(['kubectl','--context',self.a.context,'--request-timeout=45s',*args],
                         input=json.dumps(obj) if obj is not None else None, text=True,capture_output=True)
        require(r.returncode==0,r.stderr.strip() or r.stdout.strip())
        return r.stdout
    def get(self,kind,name=None,ns=None):
        return json.loads(self.k('get',kind,*([name] if name else []),*(['-n',ns] if ns else []),'-o','json'))
    def save(self,name,obj):
        (self.state/(name+'.json')).write_text(json.dumps(obj,indent=2)+'\n')
    def read(self,name):
        return json.loads((self.state/(name+'.json')).read_text())
    def patch_entry(self,o,fields):
        return {'kind':o['kind'],'name':o['metadata']['name'],'namespace':o['metadata'].get('namespace'),
                'uid':o['metadata']['uid'],'fields':[{'path':path,'before':at(o,path),'after':value} for path,value in fields.items()]}
    def patch(self,e,dry=False,reverse=False):
        o=self.get(e['kind'],e['name'],e['namespace']); require(o['metadata']['uid']==e['uid'],'UID drift: '+e['name'])
        changes=[]
        for f in e['fields']:
            before,after=(f['after'],f['before']) if reverse else (f['before'],f['after'])
            now=at(o,f['path'])
            if now==after: continue
            require(now==before,'field drift: '+e['name']+' '+f['path'])
            changes.append({'op':'replace','path':f['path'],'value':after})
        if not changes: return
        guards=[{'op':'test','path':'/metadata/uid','value':e['uid']},
                {'op':'test','path':'/metadata/resourceVersion','value':o['metadata']['resourceVersion']}]
        self.k('patch',e['kind'],e['name'],*(['-n',e['namespace']] if e['namespace'] else []),
               '--type=json','-p',json.dumps(guards+changes),*(['--dry-run=server'] if dry else []))
        print(('PREFLIGHT ' if dry else 'PATCHED ')+e['kind']+'/'+e['name'],flush=True)
    def create(self,o,dry=False,must_exist=False):
        ns=o['metadata'].get('namespace'); name=o['metadata']['name']
        existing=self.k('get',o['kind'],name,*(['-n',ns] if ns else []),'--ignore-not-found','-o','json')
        if existing:
            cur=json.loads(existing)
            require(cur['metadata'].get('annotations',{}).get('scheduling.simplesilicon.com/handoff-id')==self.read('plan')['id'], 'refuse preexisting resource '+name)
            def subset(x,y):
                if isinstance(x,dict):return isinstance(y,dict) and all(k in y and subset(v,y[k]) for k,v in x.items() if k!='caBundle')
                if isinstance(x,list):return isinstance(y,list) and len(x)==len(y) and all(subset(a,b) for a,b in zip(x,y))
                return x==y
            require(subset(o,cur),'created resource drift: '+name)
            return
        require(not must_exist,'required migration resource missing: '+o['kind']+'/'+name)
        self.k('create','-f','-',*(['--dry-run=server'] if dry else []),obj=o)
        print(('PREFLIGHT ' if dry else 'CREATED ')+o['kind']+'/'+name,flush=True)
    def ready(self,name):
        d=self.get('deployment',name,NS);s=d.get('status',{});n=d['spec'].get('replicas',1)
        require(n>=1 and s.get('observedGeneration',0)>=d['metadata']['generation'] and s.get('updatedReplicas',0)==n and s.get('availableReplicas',0)==n and s.get('replicas',0)==n,'rollout incomplete: '+name)
        return d
    def prepare(self):
        self.state.mkdir(parents=True,exist_ok=False,mode=0o700)
        dep=self.ready('workload-policy');self.ready('workload-routing')
        require(next(c['image'] for c in dep['spec']['template']['spec']['containers'] if c['name']=='webhook')==IMAGE,'unverified unified backend image')
        require(dep['spec'].get('replicas',1)>=2,'need two admission replicas')
        ar=self.get('workloadroutingpolicy','arena-pytorch');pool=self.get('nodepoolpolicy','ego-pipeline')
        root=self.get('mutatingwebhookconfiguration','workload-routing'); ordinary=self.get('mutatingwebhookconfiguration','workload-policy')
        require(len(root['webhooks'])==1,'expected one Arena root hook')
        w=root['webhooks'][0]
        require(w['clientConfig']['service']=={'namespace':NS,'name':'workload-routing','path':'/mutate','port':443},'Arena already switched or unexpected service')
        require(w['rules']==[dict(apiGroups=['kubeflow.org'],apiVersions=['v1'],operations=['CREATE'],resources=['pytorchjobs'],scope='Namespaced')],'unexpected Arena rules')
        require(next(c['expression'] for c in w['matchConditions'] if c['name']=='rollout-enabled')=='true','Arena gate is not enabled')
        require(w['namespaceSelector']=={'matchExpressions':[{'key':'kubernetes.io/metadata.name','operator':'NotIn','values':ar['spec']['namespaceScope']['exclude']}]},'Arena namespace selector differs from policy')
        require(not w.get('objectSelector'), 'unexpected Arena object selector')
        tns,tname=self.a.trainer.split('/')
        trainer=self.ready(tname) if tns==NS else self.get('deployment',tname,tns)
        sa=trainer['spec']['template']['spec'].get('serviceAccountName','default')
        user=f'system:serviceaccount:{tns}:{sa}'
        client=ordinary['webhooks'][0]['clientConfig']
        require(client['service']['name']=='workload-policy' and client['caBundle'] and client['caBundle']==w['clientConfig']['caBundle'],'backend CA mismatch or empty')
        injection=ordinary['metadata']['annotations'][INJECT]
        sa_new=dep['spec']['template']['spec'].get('serviceAccountName','default')
        for verb,resource in [('get','pytorchjobs.kubeflow.org'),('list','workloadroutingpolicies.scheduling.simplesilicon.com'),('list','nodepoolpolicies.scheduling.simplesilicon.com')]:
            answer=self.k('auth','can-i',verb,resource,'--as',f'system:serviceaccount:{NS}:{sa_new}',*(['-n','infra-group'] if resource.startswith('pytorchjobs.') else []))
            require(answer.strip()=='yes','unified backend lacks '+verb+' '+resource)
        newa,newp=policy_targets(ar,pool,user)
        acl=self.get('validatingwebhookconfiguration','ego-queue-access')
        sched=self.ready('volcano-infra-scheduler'); old=self.get('deployment','volcano-scheduler',NS)
        old_configs=[]
        for v in old['spec']['template']['spec'].get('volumes',[]):
            if v.get('configMap'):
                oldcm=self.get('configmap',v['configMap']['name'],NS)
                old_configs.append(oldcm)
                for text in oldcm.get('data',{}).values():
                    if re.search(r'^actions:',text,re.M):check_actions(text)
        volumes=sched['spec']['template']['spec']['volumes']; matches=[(i,v) for i,v in enumerate(volumes) if v['name']=='scheduler-config']
        require(len(matches)==1,'scheduler-config volume not found')
        idx,volume=matches[0];cm=self.get('configmap',volume['configMap']['name'],NS)
        data=copy.deepcopy(cm['data']);data['volcano-scheduler.conf']=strip_acl(data['volcano-scheduler.conf'])
        cmname='volcano-infra-no-queue-acl-'+hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()[:10]
        token=hashlib.sha256(str(self.state.resolve()).encode()).hexdigest()[:16]
        newcm={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':cmname,'namespace':NS},'immutable':True,'data':data}
        hooks=descendant_hooks(root,client,injection)
        for o in [newcm]+hooks:o['metadata'].setdefault('annotations',{})['scheduling.simplesilicon.com/handoff-id']=token
        ky=self.get('clusterpolicies.kyverno.io')
        for name,key in [('business-independent-routing',DKEY),('business-pods-use-volcano',DKEY),('pipeline-nodepool-isolation',DKEY),('arena-pytorch-independent-routing','scheduling.simplesilicon.com/workload-routing-policy')]:
            p=next((x for x in ky['items'] if x['metadata']['name']==name),None);require(p is not None,'missing compatibility policy '+name)
            for r in p['spec']['rules']:
                conditions=r.get('preconditions',{}).get('all',[])
                require(any(key in c.get('key','') and c.get('operator')=='Equals' and (c.get('value') is False if key==DKEY else c.get('value')=='') for c in conditions),'missing v2 skip: '+name+'/'+r['name'])
        patches=[self.patch_entry(ar,{'/spec':newa}),self.patch_entry(pool,{'/spec':newp})]
        aclfields={f'/webhooks/{i}/matchConditions':[{'name':'namespace-acl-retired','expression':'false'}] for i in range(len(acl['webhooks']))}
        patches += [self.patch_entry(acl,aclfields), self.patch_entry(sched,{f'/spec/template/spec/volumes/{idx}/configMap/name':cmname})]
        rootpatch=self.patch_entry(root,{'/webhooks/0/clientConfig/service':{**client['service'],'path':'/mutate-workload'},'/metadata/annotations/'+ptr(INJECT):injection})
        plan={'context':self.a.context,'id':token,'trainerUser':user,'patches':patches,'creates':[newcm]+hooks,'rootPatch':rootpatch,
              'namespaceScope':newa['namespaceScope'],'poolScopeBefore':pool['spec']['namespaceScope'],'poolScopeAfter':newp['namespaceScope'],
              'rollback':'Root CREATE endpoint only; keep v2 descendants/pool acceptance and namespace ACL retirement for already-enrolled jobs.',
              'effects':['All business PyTorch roots use unified backend','Remove independent scheduler queue-access plugin and disable ego-queue-access','Pending jobs previously blocked only by ACL may now schedule','No quota, preemption or business workload mutation']}
        self.save('plan',plan)
        self.save('protected',{'objects':[old,ordinary,self.get('validatingwebhookconfiguration','workload-policy'),dep,trainer]+old_configs+ky['items']+self.get('queues.scheduling.volcano.sh')['items'], 'scheduler':sched, 'configmap':cm, 'root':root, 'acl':acl})
        workloads=json.loads(self.k('get','pytorchjobs.kubeflow.org,pods','-A','-o','json'))
        self.save('workloads-before',[projection(o) for o in workloads['items']])
        print('PREPARED: cluster reads only. Review plan.json. No production changes.')
    def check_protected(self):
        saved=self.read('protected')
        for o in saved['objects']+[saved['configmap']]:
            m=o['metadata'];cur=self.get(o['kind'],m['name'],m.get('namespace'))
            def proj(x):
                if 'webhooks' in x:
                    v=copy.deepcopy(x['webhooks'])
                    for h in v:h['clientConfig'].pop('caBundle',None)
                    return sorted(v,key=lambda h:h['name']) if x['kind']=='ValidatingWebhookConfiguration' else v
                return x.get('spec',x.get('data',{}))
            require(cur['metadata']['uid']==m['uid'] and proj(cur)==proj(o),'protected config changed: '+o['kind']+'/'+m['name'])
        plan=self.read('plan')
        # Endpoint edits must never mask concurrent selector/rule changes.
        for key,entry in [('root',plan['rootPatch']),('acl',plan['patches'][2])]:
            before=copy.deepcopy(saved[key]);cur=self.get(before['kind'],before['metadata']['name'])
            require(cur['metadata']['uid']==before['metadata']['uid'],'webhook UID drift')
            for f in entry['fields']:
                require(at(cur,f['path']) in [f['before'],f['after']], 'unexpected webhook field drift')
                parts=f['path'].strip('/').split('/'); parent=cur
                for part in parts[:-1]:
                    part=part.replace('~1','/').replace('~0','~')
                    parent=parent[int(part)] if isinstance(parent,list) else parent[part]
                last=parts[-1].replace('~1','/').replace('~0','~')
                parent[int(last) if isinstance(parent,list) else last]=copy.deepcopy(f['before'])
            require(proj(cur)==proj(before),'webhook scope/rules drift: '+key)
        scheduler=self.get('deployment','volcano-infra-scheduler',NS)
        expected=copy.deepcopy(saved['scheduler']['spec'])
        for v in expected['template']['spec']['volumes']:
            if v['name']=='scheduler-config':v['configMap']['name']=plan['creates'][0]['metadata']['name']
        require(scheduler['metadata']['uid']==saved['scheduler']['metadata']['uid'] and scheduler['spec'] in [expected,saved['scheduler']['spec']], 'scheduler changed beyond ACL config volume')
    def probe(self):
        plan=self.read('plan')
        for ns in ['infra-group','data-group','intern-group']:
            for batch,queue in [('allocentric','infra-ego-pipeline-shared'),('old-5880','infra-ego-pipeline-shared'),('new-5880','default')]:
                # Simulate Arena metadata only for server-side dry-run. No Helm release or job is created.
                obj={'apiVersion':'kubeflow.org/v1','kind':'PyTorchJob','metadata':{'generateName':'arena-unified-preview-','namespace':ns,'labels':{'safeguard':'arena-test','routing.example.org/controller':'new','heritage':'Helm','release':'arena-unified-preview-'+batch}},
                     'spec':{'runPolicy':{'suspend':True},'pytorchReplicaSpecs':{'Master':{'replicas':1,'restartPolicy':'Never','template':{'metadata':{},'spec':{'nodeSelector':{'batch':batch},'containers':[{'name':'pytorch','image':'busybox:1.36','command':['sh','-c','echo admission-preview'],'resources':{'requests':{'cpu':'100m','memory':'64Mi'}}}]}}}}}}
                out=json.loads(self.k('create','--dry-run=server','-f','-','-o','json',obj=obj))
                decision=json.loads(out['metadata'].get('annotations',{}).get(DKEY,'{}'))
                require(decision.get('target',{}).get('queue')==queue,'probe route failed: '+ns+'/'+batch)
                t=out['spec']['pytorchReplicaSpecs']['Master']['template']
                require(t['spec']['schedulerName']=='volcano-infra' and t['spec']['nodeSelector']['batch']==batch and t['metadata']['annotations'][DKEY]==out['metadata']['annotations'][DKEY],'template route mismatch')
                print('PASS dry-run '+ns+'/'+batch+' -> '+queue)
        for f in plan['rootPatch']['fields']:
            require(at(self.get('mutatingwebhookconfiguration','workload-routing'),f['path'])==f['after'],'root ingress not unified')
    def verify(self):
        self.check_protected();self.ready('workload-policy');self.ready('volcano-infra-scheduler')
        plan=self.read('plan')
        for e in plan['patches']:
            cur=self.get(e['kind'],e['name'],e['namespace'])
            require(cur['metadata']['uid']==e['uid'] and all(at(cur,f['path'])==f['after'] for f in e['fields']),'planned config mismatch: '+e['name'])
        for o in plan['creates']:self.create(o,dry=True,must_exist=True)
        self.probe()
        now={x['metadata']['uid']:projection(x) for x in json.loads(self.k('get','pytorchjobs.kubeflow.org,pods','-A','-o','json'))['items']}
        gone=bound=0
        for before in self.read('workloads-before'):
            cur=now.get(before['uid'])
            if cur is None:gone+=1;continue
            old=copy.deepcopy(before);cur=copy.deepcopy(cur)
            if old['kind']=='Pod' and not old.get('node') and cur.get('node'):
                old['node']=cur['node'];bound+=1
            require(old==cur,'surviving old workload scheduling/decision changed: '+before['namespace']+'/'+before['name'])
        print(f'PASS surviving old object scheduling unchanged; {gone} baseline objects disappeared/recreated (lifecycle cause not inferred); {bound} formerly unbound Pods scheduled.')
        print('PASS: root routing, ACL retirement, protected configs and preemption-off verified. Dry-run only; no GPU workload submitted.')
    def main(self):
        if self.a.action=='prepare':return self.prepare()
        plan=self.read('plan');require(plan['context']==self.a.context,'context differs from snapshot')
        if self.a.action=='rollback':
            self.ready('workload-routing')
            self.patch(plan['rootPatch'],reverse=True)
            print('ROLLED BACK root CREATE ingress. V2 descendant handlers and ACL retirement retained; no business objects deleted. Do not downgrade workload-policy image.')
            return
        self.check_protected()
        if self.a.action=='verify-rollback':
            current=self.get('mutatingwebhookconfiguration','workload-routing')
            require(all(at(current,f['path'])==f['before'] for f in plan['rootPatch']['fields']),'root rollback incomplete')
            self.ready('workload-routing')
            print('PASS root endpoint restored and protected configs unchanged. Retain unified backend and descendant hooks for v2 jobs.')
            return
        if self.a.action in ['preflight','enable']:
            for o in plan['creates']:self.create(o,dry=True)
            for e in plan['patches']:self.patch(e,dry=True)
            self.patch(plan['rootPatch'],dry=True)
            if self.a.action=='preflight':return
            for e in plan['patches'][:2]:self.patch(e)
            for o in plan['creates']:self.create(o)
            # Verify CA injection and backend readiness before exposing roots.
            self.ready('workload-policy')
            for o in plan['creates'][1:]:
                cur=self.get(o['kind'],o['metadata']['name'])
                ca=self.get('mutatingwebhookconfiguration','workload-policy')['webhooks'][0]['clientConfig']['caBundle']
                require(ca and all(h['clientConfig']['caBundle']==ca for h in cur['webhooks']),'new hook CA mismatch; retry after injection')
            for e in plan['patches'][2:]:self.patch(e)
            self.k('-n',NS,'rollout','status','deployment/volcano-infra-scheduler','--timeout=180s')
            self.ready('volcano-infra-scheduler')
            root_ca=self.get('mutatingwebhookconfiguration','workload-routing')['webhooks'][0]['clientConfig']['caBundle']
            new_ca=self.get('mutatingwebhookconfiguration','workload-policy')['webhooks'][0]['clientConfig']['caBundle']
            require(root_ca and root_ca==new_ca,'CA changed before root handoff; stop and review')
            self.patch(plan['rootPatch'])
            print('ENABLED. Run verify next. If it fails, retain state and use rollback; no automatic workload deletion.')
        elif self.a.action=='verify':self.verify()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['prepare','preflight','enable','verify','rollback','verify-rollback'])
    p.add_argument('--context',required=True);p.add_argument('--state',type=pathlib.Path,required=True)
    p.add_argument('--trainer',default='trainer-system/training-operator')
    Run(p.parse_args()).main()

if __name__=='__main__': main()
