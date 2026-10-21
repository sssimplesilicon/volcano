import copy
import importlib.util
import pathlib
import unittest
import types

ROOT=pathlib.Path(__file__).resolve().parents[3]
s=importlib.util.spec_from_file_location('handoff',ROOT/'installer/pipeline/arena-unified-handoff.py')
h=importlib.util.module_from_spec(s);s.loader.exec_module(h)

class HandoffTests(unittest.TestCase):
    def root(self):
        return {'webhooks':[dict(admissionReviewVersions=['v1'],namespaceSelector={'matchExpressions':[{'key':'kubernetes.io/metadata.name','operator':'NotIn','values':['kube-system']}]},matchPolicy='Equivalent',failurePolicy='Fail',sideEffects='None',timeoutSeconds=15,matchConditions=[{'name':'rollout-enabled','expression':'true'}])]}
    def hooks(self):
        return h.descendant_hooks(self.root(),{'caBundle':'CA','service':{'name':'workload-policy','namespace':'volcano-system','port':443}},'volcano-system/workload-policy-serving')
    def test_remove_only_acl(self):
        prefix='actions: "enqueue, allocate, backfill"\ntiers:\n- plugins:\n'
        rest='  - name: priority\n  - name: capacity\n  - name: binpack\n    arguments:\n      binpack.weight: 10\n'
        self.assertEqual(h.strip_acl(prefix+'  - name: queue-access\n    arguments:\n      foo: bar\n'+rest),prefix+rest)
    def test_preemption_rejected(self):
        for action in ['preempt','reclaim','worker-type-preempt']:
            with self.assertRaises(ValueError):h.strip_acl('actions: "enqueue, allocate, '+action+'"\n  - name: queue-access\n')
    def test_missing_or_duplicate_acl_rejected(self):
        for text in ['', '  - name: queue-access\n  - name: queue-access\n']:
            with self.assertRaises(ValueError):h.strip_acl('actions: "enqueue, allocate, backfill"\n'+text)
    def test_union_preserves_system_exclusions(self):
        self.assertEqual(h.scope_union({'include':['*'],'exclude':['argo','kube-system']},{'include':['*'],'exclude':['kube-system']}),{'include':['*'],'exclude':['kube-system']})
    def test_new_hooks_no_unmarked_descendants(self):
        for o in self.hooks():
            for w in o['webhooks']:
                self.assertEqual(w['namespaceSelector'],self.root()['webhooks'][0]['namespaceSelector'])
                self.assertIn(h.DKEY,w['matchConditions'][-1]['expression'])
                if w['rules'][0]['resources']==['pods']:
                    self.assertIn("o.kind == 'PyTorchJob'",w['matchConditions'][-1]['expression'])
                    self.assertIn('o.controller',w['matchConditions'][-1]['expression'])
    def test_marker_removal_update_validated(self):
        for w in self.hooks()[1]['webhooks']:
            if 'UPDATE' in w['rules'][0]['operations']:
                self.assertIn('oldObject.metadata.annotations',w['matchConditions'][-1]['expression'])
    def test_no_old_root_mutation_on_update(self):
        self.assertTrue(all(w['rules'][0]['resources']==['pods'] and w['rules'][0]['operations']==['CREATE'] for w in self.hooks()[0]['webhooks']))
    def test_policy_preserves_legacy_rollback_refs_and_business_routes(self):
        a={'metadata':{'name':'arena-pytorch'},'spec':{'namespaceScope':{'include':['*'],'exclude':['kube-system']},'schedulerName':'volcano-infra','fallback':{'queue':'default','nodeGroup':'general'},'rules':[{'name':n,'match':{'batchValues':['allocentric','old-5880']},'queueAccessPolicyRef':'legacy','target':{'nodeGroup':'infra-ego-pipeline','queue':'infra-ego-pipeline-shared'}} for n in ['ego-explicit','ego-legacy']]}}
        p={'metadata':{'name':'ego-pipeline'},'spec':{'mode':'Enforce','namespaceScope':{'include':['*'],'exclude':['kube-system','argo']},'controllerUsers':['old-user'],'acceptedRoutes':[{'policyRef':'business-routing','ruleNames':['base']} ]}}
        original=copy.deepcopy((a,p));na,np=h.policy_targets(a,p,'trainer-user')
        self.assertEqual((a,p),original)
        self.assertEqual(np['acceptedRoutes'][0],p['spec']['acceptedRoutes'][0])
        self.assertEqual(na['workloadKinds'],['PyTorchJob'])
        self.assertIn('trainer-user',np['controllerUsers'])
        self.assertEqual(na['rules'][0]['queueAccessPolicyRef'],'legacy')
        self.assertTrue(all(r['target']['poolRef']=='ego-pipeline' for r in na['rules']))
    def test_projection_omits_secrets(self):
        o={'kind':'Pod','metadata':{'name':'x','namespace':'n','uid':'u'},'spec':{'containers':[{'env':[{'name':'secret','value':'never-save-me'}]}],'schedulerName':'old'}}
        self.assertNotIn('never-save-me',str(h.projection(o)))
    def test_patch_guards_and_idempotency(self):
        r=h.Run(types.SimpleNamespace(context='local',state=None))
        current={'metadata':{'uid':'u','resourceVersion':'7'},'spec':{'value':'old'}}
        calls=[];r.get=lambda *args:copy.deepcopy(current)
        r.k=lambda *args,**kw:calls.append(args)
        entry={'kind':'ConfigMap','name':'x','namespace':'n','uid':'u','fields':[{'path':'/spec/value','before':'old','after':'new'}]}
        r.patch(entry)
        import json
        ops=json.loads(calls[0][calls[0].index('-p')+1])
        self.assertEqual(ops[:2],[{'op':'test','path':'/metadata/uid','value':'u'},{'op':'test','path':'/metadata/resourceVersion','value':'7'}])
        current['spec']['value']='new';r.patch(entry);self.assertEqual(len(calls),1)
        current['spec']['value']='other'
        with self.assertRaises(ValueError):r.patch(entry)
        current['spec']['value']='old';current['metadata']['uid']='recreated'
        with self.assertRaises(ValueError):r.patch(entry)
    def test_root_handoff_last_and_rollout_failure_stops(self):
        plan={'context':'local','patches':[{'name':n} for n in ['arena','pool','acl','scheduler']],
              'creates':[{'kind':'ConfigMap','metadata':{'name':'cm'}},{'kind':'MutatingWebhookConfiguration','metadata':{'name':'extra'}}],
              'rootPatch':{'name':'root'}}
        def execute(fail=False):
            r=h.Run(types.SimpleNamespace(context='local',state=None,action='enable'));events=[]
            r.read=lambda name:plan;r.check_protected=lambda:None;r.ready=lambda name:None
            r.create=lambda o,dry=False:events.append(('create',o['metadata']['name'],dry))
            r.patch=lambda e,dry=False:events.append(('patch',e['name'],dry))
            r.get=lambda *args:{'webhooks':[{'clientConfig':{'caBundle':'CA'}}]}
            def k(*args):
                events.append(('rollout',))
                if fail:raise ValueError('test rollout failed')
            r.k=k
            if fail:
                with self.assertRaises(ValueError):r.main()
            else:r.main()
            return events
        ok=execute();self.assertEqual(ok[-1],('patch','root',False))
        self.assertLess(ok.index(('patch','acl',False)),ok.index(('rollout',)))
        self.assertNotIn(('patch','root',False),execute(True))
    def test_probe_meets_arena_policy_and_remains_dry_run(self):
        import json
        r=h.Run(types.SimpleNamespace(context='local',state=None));calls=[]
        r.read=lambda _: {'rootPatch':{'fields':[]}}
        def admit(*args,obj=None):
            self.assertIn('--dry-run=server',args)
            self.assertEqual(args[0],'create')
            labels=obj['metadata']['labels']
            # Exact conditions of require-arena-submit/pytorchjob-must-be-arena-managed.
            if labels.get('heritage','')!='Helm' or labels.get('release','')=='':
                raise ValueError('require-arena-submit: missing heritage=Helm or release')
            self.assertTrue(obj['spec']['runPolicy']['suspend'])
            template=obj['spec']['pytorchReplicaSpecs']['Master']['template']
            for c in template['spec']['containers']:
                for field in ['requests','limits']:
                    self.assertEqual(str(c.get('resources',{}).get(field,{}).get('nvidia.com/gpu','0')),'0')
            batch=template['spec']['nodeSelector']['batch']
            queue='default' if batch=='new-5880' else 'infra-ego-pipeline-shared'
            result=copy.deepcopy(obj);d=json.dumps({'target':{'queue':queue}})
            result['metadata']['annotations']={h.DKEY:d}
            t=result['spec']['pytorchReplicaSpecs']['Master']['template']
            t['spec']['schedulerName']='volcano-infra';t['metadata']['annotations']={h.DKEY:d}
            calls.append((obj['metadata']['namespace'],batch));return json.dumps(result)
        r.k=admit;r.probe()
        self.assertEqual(len(calls),9)
        self.assertEqual({ns for ns,_ in calls},{'infra-group','data-group','intern-group'})
    def test_json_pointer(self):
        self.assertEqual(h.at({'metadata':{'annotations':{'a/b':'c'}}},'/metadata/annotations/'+h.ptr('a/b')),'c')

if __name__=='__main__':unittest.main()
