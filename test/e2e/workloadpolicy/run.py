#!/usr/bin/env python3
"""Real admission/controller tests. Refuses any endpoint except local k3s :16473.
All test workloads use a nonexistent node selector and request no GPU.
"""
import atexit,base64,copy,json,os,pathlib,subprocess,tempfile,time
from concurrent.futures import ThreadPoolExecutor
ROOT=pathlib.Path(__file__).resolve().parents[3]
KCFG=os.environ.get('WORKLOAD_POLICY_E2E_KUBECONFIG','/nonexistent/local-only')
OUT=pathlib.Path(os.environ.get('WORKLOAD_POLICY_E2E_OUTPUT','/tmp/workload-policy-e2e'));OUT.mkdir(parents=True,exist_ok=True)
NAME='wp-e2e';NS='wp-e2e-system';BUS='wp-e2e-business';OTHER='wp-e2e-other';SECOND='wp-e2e-business-two';IMAGE='workload-policy:local';DKEY='scheduling.simplesilicon.com/workload-decision';RKEY='scheduling.simplesilicon.com/route'
results=[]
(OUT/'results.json').unlink(missing_ok=True)
def k(*args,obj=None,ok=True):
 r=subprocess.run(['kubectl','--kubeconfig',KCFG,*args],input=json.dumps(obj) if obj is not None else None,text=True,capture_output=True,timeout=90)
 if ok and r.returncode:raise RuntimeError(r.stderr)
 return r
cfg=json.loads(k('config','view','--minify','-o','json').stdout)
assert cfg['clusters'][0]['cluster']['server']=='https://127.0.0.1:16473','LOCAL K3S ONLY'
def get(kind,name=None,ns=None):return json.loads(k('get',kind,*([name] if name else []),*(['-n',ns] if ns else []),'-o','json').stdout)
def apply(o):
 if o['kind'].endswith('WebhookConfiguration'):
  current=k('get',o['kind'],o['metadata']['name'],'--ignore-not-found','-o','json').stdout
  if current:o['metadata']['resourceVersion']=json.loads(current)['metadata']['resourceVersion']
 return k('apply','-f','-',obj=o)
def check(name,ok,detail=''):
 if not ok:raise AssertionError(name+': '+str(detail))
 results.append(name);print('PASS',name,flush=True);(OUT/'results.json').write_text(json.dumps(results,indent=2)+'\n')
def wait(fn,seconds=75):
 until=time.monotonic()+seconds
 while time.monotonic()<until:
  v=fn()
  if v:return v
  time.sleep(1)
 raise AssertionError('timeout')
def preview(o,update=False):
 r=k('replace' if update else 'create','--dry-run=server','-f','-','-o','json',obj=o,ok=False)
 return (json.loads(r.stdout), '') if not r.returncode else (None,r.stderr)
def meta(n,ns=None):return {'name':n,**({'namespace':ns} if ns else {})}
def podspec():return {'nodeSelector':{'workload-policy.test/nonexistent':'true'},'containers':[{'name':'worker','image':'busybox:1.36','command':['sh','-c','echo test'],'resources':{'requests':{'cpu':'10m','memory':'16Mi'}}}]}
def workload(kind,name='probe',ns=BUS,route=None):
 m=meta(name,ns);m['labels']={'app':name,'safeguard':'workload-policy-test'}
 if route:m['labels'][RKEY]=route
 template={'metadata':{'labels':{'app':name}},'spec':podspec()}
 api='apps/v1';spec={'replicas':1,'selector':{'matchLabels':{'app':name}},'template':template}
 if kind=='Pod':api='v1';spec=podspec()
 if kind=='StatefulSet':spec['serviceName']='wp-headless'
 if kind=='Job':api='batch/v1';spec={'template':template};template['spec']['restartPolicy']='Never'
 if kind=='CronJob':api='batch/v1';template['spec']['restartPolicy']='Never';spec={'schedule':'* * * * *','jobTemplate':{'spec':{'template':template}}}
 if kind=='PyTorchJob':api='kubeflow.org/v1';spec={'runPolicy':{'suspend':True},'pytorchReplicaSpecs':{'Master':{'replicas':1,'restartPolicy':'Never','template':template}}}
 if kind=='Workflow':api='argoproj.io/v1alpha1';spec={'entrypoint':'worker','nodeSelector':{'workload-policy.test/nonexistent':'true'},'templates':[{'name':'worker','container':{'image':'busybox:1.36','command':['sh','-c','echo test']}}]}
 return {'apiVersion':api,'kind':kind,'metadata':m,'spec':spec}
legacy=k('get','mutatingwebhookconfiguration','iristack-pytorch-migration','--ignore-not-found','-o','json').stdout
if legacy:
 old_hooks=json.loads(legacy)['webhooks'];new_hooks=copy.deepcopy(old_hooks)
 for w in new_hooks:w.setdefault('matchConditions',[]).append({'name':'workload-policy-e2e-only','expression':f"request.namespace != '{BUS}' && request.namespace != '{SECOND}'"})
 k('patch','mutatingwebhookconfiguration','iristack-pytorch-migration','--type=json','-p',json.dumps([{'op':'test','path':'/webhooks','value':old_hooks},{'op':'replace','path':'/webhooks','value':new_hooks}]))
 def restore_adapter():
  r=k('patch','mutatingwebhookconfiguration','iristack-pytorch-migration','--type=json','-p',json.dumps([{'op':'test','path':'/webhooks','value':new_hooks},{'op':'replace','path':'/webhooks','value':old_hooks}]),ok=False)
  if r.returncode:print('ADAPTER RESTORE FAILED:',r.stderr,flush=True)
 atexit.register(restore_adapter)
for ns in [NS,BUS,OTHER,SECOND]:apply({'apiVersion':'v1','kind':'Namespace','metadata':meta(ns)})
# Preserve preexisting routing configuration and scheduler Deployments as evidence.
scheduler_before={x['metadata']['namespace']+'/'+x['metadata']['name']:x['spec'] for x in json.loads(k('get','deployments','-A','-o','json').stdout)['items'] if 'scheduler' in x['metadata']['name']}

def scheduler_configs():
 configs={}
 for key,spec in scheduler_before.items():
  ns=key.split('/')[0]
  for volume in spec['template']['spec'].get('volumes',[]):
   if 'configMap' in volume:
    name=volume['configMap']['name'];configs[ns+'/'+name]=get('configmap',name,ns).get('data',{})
 return configs
scheduler_configs_before=scheduler_configs()
for data in scheduler_configs_before.values():
 for text in data.values():
  for line in text.splitlines():
   if line.strip().startswith('actions:'):
    actions={x.strip().strip('\"').strip("'") for x in line.split(':',1)[1].strip().strip('\"').strip("'").split(',')}
    assert not actions & {'preempt','reclaim','worker-type-preempt'}, 'Local active scheduler must not run preemption in this suite'
before=get('workloadroutingpolicies');(OUT/'routing-before.json').write_text(json.dumps(before))
for f in ['workload-routing-crd.json','nodepool-policy-crd.json']:k('apply','-f',str(ROOT/'installer/pipeline'/f))
k('wait','--for=condition=Established','crd/nodepoolpolicies.scheduling.simplesilicon.com','--timeout=60s')
with tempfile.TemporaryDirectory() as td:
 cert=pathlib.Path(td)/'tls.crt';key=pathlib.Path(td)/'tls.key'
 subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','2','-subj',f'/CN={NAME}.{NS}.svc','-addext',f'subjectAltName=DNS:{NAME}.{NS}.svc'],check=True,capture_output=True)
 ca=base64.b64encode(cert.read_bytes()).decode()
 apply({'apiVersion':'v1','kind':'Secret','metadata':meta(NAME,NS),'type':'kubernetes.io/tls','data':{'tls.crt':ca,'tls.key':base64.b64encode(key.read_bytes()).decode()}})
users=['system:kube-controller-manager','system:serviceaccount:trainer-routed-system:training-operator','system:serviceaccount:kube-system:deployment-controller','system:serviceaccount:kube-system:replicaset-controller','system:serviceaccount:kube-system:statefulset-controller','system:serviceaccount:kube-system:job-controller','system:serviceaccount:kube-system:cronjob-controller','system:serviceaccount:argo:argo','system:serviceaccount:argo:argo-workflow-controller',f'system:serviceaccount:{BUS}:argo']
policy={'apiVersion':'scheduling.simplesilicon.com/v1alpha1','kind':'WorkloadRoutingPolicy','metadata':meta(NAME),'spec':{'namespaceScope':{'include':[BUS,SECOND]},'workloadKinds':['Pod','Deployment','StatefulSet','Job','CronJob','Workflow','PyTorchJob'],'controllerUsers':users,'schedulerName':'default-scheduler','fallback':{'queue':'default','nodeGroup':'general'},'rules':[
 {'name':'base','match':{'labels':{RKEY:'base'},'absentLabels':['pipeline.simplesilicon.com/name']},'target':{'queue':'wp-ego-base','nodeGroup':'ego','poolRef':'wp-ego'}},
 {'name':'shared','match':{'labels':{RKEY:'shared'}},'target':{'queue':'wp-ego-shared','nodeGroup':'ego','poolRef':'wp-ego'}},
 {'name':'video','match':{'labels':{'pipeline.simplesilicon.com/name':'video'}},'target':{'queue':'wp-video','nodeGroup':'video','poolRef':'wp-video'}},
 {'name':'cpu','match':{'labels':{RKEY:'cpu'}},'target':{'queue':'wp-cpu','nodeGroup':'general','cpuOnly':True}},
 {'name':'legacy','match':{'batchValues':['old-5880','allocentric'],'legacyBatch':True},'target':{'queue':'wp-ego-shared','nodeGroup':'ego','poolRef':'wp-ego'}}]}}
pools={'apiVersion':'v1','kind':'List','items':[]}
for n,v,rs in [('wp-ego','ego',['base','shared','legacy']),('wp-video','video',['video'])]:
 pools['items'].append({'apiVersion':'scheduling.simplesilicon.com/v1alpha1','kind':'NodePoolPolicy','metadata':meta(n),'spec':{'namespaceScope':{'include':[BUS,SECOND]},'nodeSelector':{'workload-policy.test/pool':v},'acceptedRoutes':[{'policyRef':NAME,'ruleNames':rs}],'controllerUsers':users,'legacyRegistry':{'namespace':NS,'name':'grandfather'}}})
apply({'apiVersion':'v1','kind':'ConfigMap','metadata':meta('grandfather',NS),'data':{'legacyOwners':'[]'}})
with tempfile.TemporaryDirectory() as td:
 p=pathlib.Path(td)/'policy.json';p.write_text(json.dumps(policy));q=pathlib.Path(td)/'pools.json';q.write_text(json.dumps(pools))
 rendered=json.loads(subprocess.check_output(['python3',str(ROOT/'installer/pipeline/render-workload-policy.py'),'--policy',str(p),'--pools',str(q),'--image',IMAGE,'--name',NAME,'--namespace',NS,'--tls-secret',NAME],text=True))
 for o in rendered['items']:
  if o['kind']=='Deployment':
   pod=o['spec']['template']['spec'];pod['nodeSelector']={'node-role.kubernetes.io/control-plane':'true'};pod.pop('affinity',None);pod['containers'][0]['imagePullPolicy']='Never';o['spec']['replicas']=1
  if o['kind'].endswith('WebhookConfiguration'):
   o['metadata'].pop('annotations',None)
   for w in o['webhooks']:w['clientConfig']['caBundle']=ca
  apply(o)
k('rollout','restart','deployment/'+NAME,'-n',NS);k('rollout','status','deployment/'+NAME,'-n',NS,'--timeout=90s')
def gates(enabled):
 for kind in ['mutatingwebhookconfiguration','validatingwebhookconfiguration']:
  o=get(kind,NAME)
  for w in o['webhooks']:w['matchConditions']=[{'name':'rollout-enabled','expression':'true' if enabled else 'false'}]
  apply(o)
def cleanup():
 try:gates(False)
 except Exception as e:print('CLEANUP GATES:',e)
 for ns in [BUS,SECOND]:
  for kind in ['deployments','statefulsets','jobs','cronjobs','workflows','pytorchjobs','pods','configmaps']:
   k('delete',kind,'--all','-n',ns,'--wait=false',ok=False)
 for resource,names in [('workloadroutingpolicy',[NAME,NAME+'-overlap']),('nodepoolpolicy',['wp-ego','wp-video','wp-overlap'])]:
  for name in names:k('delete',resource,name,'--ignore-not-found',ok=False)
atexit.register(cleanup)
# The only admitted backend must have no access to the namespace ACL API.
rbac=get('clusterrole',NAME)
check('no-queue-access-RBAC',all('queueaccesspolicies' not in r.get('resources',[]) for r in rbac['rules']))
# Real Trainer creates a PodGroup before Pods; its queue must exist.
assert not k('get','queue.scheduling.volcano.sh','wp-ego-shared','--ignore-not-found','-o','name').stdout,'Refusing to overwrite an existing test queue'
apply({'apiVersion':'scheduling.volcano.sh/v1beta1','kind':'Queue','metadata':meta('wp-ego-shared'),'spec':{'weight':1,'reclaimable':False}})
def cleanup_queue():
 # Workloads and their PodGroups are removed by cleanup before this callback.
 k('delete','queue.scheduling.volcano.sh','wp-ego-shared','--ignore-not-found','--wait=false',ok=False)
 k('wait','--for=delete','queue.scheduling.volcano.sh/wp-ego-shared','--timeout=45s',ok=False)
atexit.unregister(cleanup)
atexit.register(cleanup_queue)
atexit.register(cleanup)
# Historical Deployment before enrollment: worker stays on its old scheduler/template.
old=workload('Deployment','historical');old['spec']['template']['spec']['nodeSelector']['workload-policy.test/pool']='ego';k('create','-f','-',obj=old)
old=get('deployment','historical',BUS)
def arena(name,ns=BUS,batch='allocentric',legacy=False):
 obj=workload('PyTorchJob',name,ns)
 obj['metadata']['labels']['routing.example.org/controller']='new'
 obj['spec']['runPolicy']={'suspend':False,'cleanPodPolicy':'None'}
 t=obj['spec']['pytorchReplicaSpecs']['Master']['template']
 t['spec']['containers'][0]['name']='pytorch'
 t['spec']['nodeSelector']['batch']=batch
 if legacy:
  obj['metadata']['labels']['scheduling.simplesilicon.com/training-routing-version']='v1'
  obj['spec']['runPolicy']['schedulingPolicy']={'queue':'wp-ego-shared'}
  t['metadata'].setdefault('annotations',{})['scheduling.volcano.sh/queue-name']='wp-ego-shared'
  t['metadata']['labels']['scheduling.simplesilicon.com/training-routing-version']='v1'
  t['spec']['schedulerName']='default-scheduler'
  t['spec']['nodeSelector']['workload-policy.test/pool']='ego'
 return obj
legacy_arena=arena('historical-arena',legacy=True);k('create','-f','-',obj=legacy_arena)
legacy_arena=get('pytorchjob','historical-arena',BUS)

apply({'apiVersion':'v1','kind':'ConfigMap','metadata':meta('grandfather',NS),'data':{'legacyOwners':json.dumps([BUS+'|'+old['metadata']['uid'],BUS+'|'+legacy_arena['metadata']['uid']])}})
# Independent namespaced Argo controller, cloned only from the existing LOCAL demo.
for kind,name in [('serviceaccount','argo'),('role','argo-role'),('rolebinding','argo-binding'),('configmap','workflow-controller-configmap'),('deployment','workflow-controller')]:
 source=get(kind,name,'argo-volcano-demo');source.pop('status',None);source['metadata']={'name':name,'namespace':BUS}
 if kind=='rolebinding':
  for subject in source.get('subjects',[]):
   if subject.get('kind')=='ServiceAccount':subject['namespace']=BUS
 if kind=='deployment':
  source['spec']['template']['spec']['nodeSelector']={'node-role.kubernetes.io/control-plane':'true'}
  source['spec']['template']['spec']['schedulerName']='default-scheduler'
 apply(source)
k('rollout','status','deployment/workflow-controller','-n',BUS,'--timeout=75s')
gates(True)
wait(lambda:preview(workload('Pod','ready'))[0] is not None)
for kind in ['Pod','Deployment','StatefulSet','Job','CronJob','Workflow','PyTorchJob']:
 o,err=preview(workload(kind,'probe-'+kind.lower(),route='base'));check('route-'+kind,o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-base',err)
# Simulate redirecting the old Arena URL to the one backend; no second router runs.
compat=get('mutatingwebhookconfiguration',NAME);original_compat=copy.deepcopy(compat)
compat['webhooks'][0]['clientConfig']['service']['path']='/mutate';apply(compat)
o,err=preview(arena('compatibility-url'))
check('old-URL-unified-engine',o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-shared',err)
apply(original_compat)
# Arena uses the same handler and label rules in both business namespaces.
for ns in [BUS,SECOND]:
 for batch,expected in [('allocentric','wp-ego-shared'),('old-5880','wp-ego-shared'),('new-5880','default')]:
  obj=arena('arena-'+batch,ns,batch);o,err=preview(obj)
  check('arena-'+ns+'-'+batch,o is not None and o['spec']['runPolicy']['schedulingPolicy']['queue']==expected,err)
obj=arena('arena-conflict');rep=copy.deepcopy(obj['spec']['pytorchReplicaSpecs']['Master']);rep['template']['spec']['nodeSelector']['batch']='old-5880';obj['spec']['pytorchReplicaSpecs']['Worker']=rep
o,err=preview(obj);check('arena-replica-conflict-denied',o is None and 'identical' in err,err)
# Legacy references are accepted with an explicit warning for a staged service handoff.
stale=copy.deepcopy(policy);stale['spec']['rules'][0]['queueAccessPolicyRef']='wp-ego-base';apply(stale)
o,err=preview(workload('Pod','stale-acl',route='base'));check('legacy-acl-reference-does-not-block',o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-base',err);apply(policy)
o,err=preview(workload('Pod','outside',OTHER));check('outside-namespace-unchanged',o is not None and DKEY not in o['metadata'].get('annotations',{}),err)
for route,expected in [('default','default'),('cpu','wp-cpu'),('shared','wp-ego-shared')]:
 o,err=preview(workload('Pod','case-'+route,route=route));check('route-'+route,o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']==expected,err)
for label,value,expected in [('batch','old-5880','wp-ego-shared'),('pipeline.simplesilicon.com/name','video','wp-video')]:
 obj=workload('Pod','multi');obj['metadata']['labels'][label]=value;o,err=preview(obj);check('multi-pipeline-'+value,o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']==expected,err)
obj=workload('Pod','explicit-video',route='base');obj['metadata']['labels']['pipeline.simplesilicon.com/name']='video';o,err=preview(obj);check('explicit-pipeline-beats-compatibility',o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']=='wp-video',err)
obj=workload('Pod','cpu-gpu',route='cpu');obj['spec']['containers'][0]['resources']['limits']={'nvidia.com/gpu':'1'};o,err=preview(obj);check('cpu-gpu-denied',o is None and 'CPU route' in err,err)
obj=workload('Pod','binding');obj['spec']['nodeName']='fake';o,err=preview(obj);check('nodeName-denied',o is None and 'nodeName' in err,err)
obj=workload('Pod','conflict');obj['spec']['nodeSelector']['workload-policy.test/pool']='ego';o,err=preview(obj);check('unauthorized-pool-selector-denied',o is None and 'conflict' in err,err)
obj=workload('Pod','or');obj['spec']['affinity']={'nodeAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':{'nodeSelectorTerms':[{'matchExpressions':[{'key':'zone','operator':'In','values':['a']}]},{'matchExpressions':[{'key':'zone','operator':'In','values':['b']}]}]}}};o,err=preview(obj);terms=o['spec']['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'] if o else [];check('every-or-branch-excluded',len(terms)==2 and all(any(x['key']=='workload-policy.test/pool' and set(x['values'])=={'ego','video'} for x in t['matchExpressions']) for t in terms),err)
# Ambiguous policies and pool definitions must fail deterministically.
dup=copy.deepcopy(policy);dup['metadata']['name']=NAME+'-overlap';apply(dup)
o,err=preview(workload('Pod','overlap'));check('overlapping-policy-denied',o is None and 'overlapping' in err,err)
k('delete','workloadroutingpolicy',NAME+'-overlap')
conflict=copy.deepcopy(policy);conflict['spec']['rules'].append({'name':'conflict','match':{'labels':{RKEY:'base'}},'target':{'queue':'another','nodeGroup':'other'}});apply(conflict)
o,err=preview(workload('Pod','ambiguous',route='base'));check('ambiguous-rule-denied',o is None and 'conflicting' in err,err);apply(policy)
dup=copy.deepcopy(pools['items'][0]);dup['metadata']['name']='wp-overlap';apply(dup)
o,err=preview(workload('Pod','overlap-pool'));check('overlapping-pools-denied',o is None and 'distinct values' in err,err);k('delete','nodepoolpolicy','wp-overlap')
for pool in pools['items']:k('delete','nodepoolpolicy',pool['metadata']['name'])
o,err=preview(workload('Pod','missing-pool',route='base'));check('missing-referenced-pool-denied',o is None and 'node pool' in err,err)
for pool in pools['items']:apply(pool)
preview_policy=copy.deepcopy(policy);preview_policy['spec']['mode']='Preview';apply(preview_policy)
o,err=preview(workload('Deployment','preview-mode',route='base'));check('preview-mode-no-mutation',o is not None and DKEY not in o['metadata'].get('annotations',{}),err);apply(policy)
def concurrent_probe(i):
 obj=workload('Pod','parallel-'+str(i),route='base' if i%2 else 'cpu');o,err=preview(obj)
 return o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']==('wp-ego-base' if i%2 else 'wp-cpu'),err
with ThreadPoolExecutor(max_workers=4) as executor:parallel=list(executor.map(concurrent_probe,range(12)))
check('concurrent-routing-isolation',all(ok for ok,_ in parallel),parallel)
# A referenced namespaced Argo template is checked, including forbidden overrides.
wt={'apiVersion':'argoproj.io/v1alpha1','kind':'WorkflowTemplate','metadata':meta('referenced',BUS),'spec':{'entrypoint':'worker','templates':[{'name':'worker','container':{'image':'busybox:1.36','command':['sh','-c','echo test']}}]}}
apply(wt);obj=workload('Workflow','reference');obj['spec']={'workflowTemplateRef':{'name':'referenced'}};o,err=preview(obj);check('workflow-template-reference',o is not None,err)
wt['spec']['templates'][0]['podSpecPatch']='{}';apply(wt);o,err=preview(obj);check('workflow-reference-override-denied',o is None and 'podSpecPatch' in err,err)
k('delete','workflowtemplate','referenced','-n',BUS)
# Real Trainer children, including a historical v1 job created before enrollment.
def trainer_pods(name):
 return [p for p in get('pods',ns=BUS)['items'] if any(r.get('kind')=='PyTorchJob' and r['name']==name for r in p['metadata'].get('ownerReferences',[]))]
old_training_pod=wait(lambda:next(iter(trainer_pods('historical-arena')),None))
check('legacy-arena-not-enrolled',DKEY not in old_training_pod['metadata'].get('annotations',{}),old_training_pod)
k('delete','pod',old_training_pod['metadata']['name'],'-n',BUS,'--wait=false')
old_training_replacement=wait(lambda:next((p for p in trainer_pods('historical-arena') if p['metadata']['uid']!=old_training_pod['metadata']['uid']),None))
check('legacy-arena-recreated-unchanged',DKEY not in old_training_replacement['metadata'].get('annotations',{}) and old_training_replacement['spec']['schedulerName']==old_training_pod['spec']['schedulerName'] and old_training_replacement['spec']['nodeSelector']==old_training_pod['spec']['nodeSelector'])
obj=arena('new-arena');k('create','-f','-',obj=obj)
training_pod=wait(lambda:next(iter(trainer_pods('new-arena')),None))
check('real-arena-child-routed',json.loads(training_pod['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-shared',training_pod)
k('delete','pod',training_pod['metadata']['name'],'-n',BUS,'--wait=false')
training_replacement=wait(lambda:next((p for p in trainer_pods('new-arena') if p['metadata']['uid']!=training_pod['metadata']['uid']),None))
check('real-arena-recreated',training_replacement['metadata']['annotations'][DKEY]==training_pod['metadata']['annotations'][DKEY])
# Real controller, real replacement Pod, no resources allocated.
obj=workload('Deployment','new-worker',route='base');k('create','-f','-',obj=obj)
def pods(app):return json.loads(k('get','pods','-n',BUS,'-l','app='+app,'-o','json').stdout)['items']
first=wait(lambda:next(iter(pods('new-worker')),None));check('real-deployment-child-routed',DKEY in first['metadata'].get('annotations',{}) and first['spec']['nodeSelector'].get('workload-policy.test/pool')=='ego',first)
k('delete','pod',first['metadata']['name'],'-n',BUS,'--wait=false');second=wait(lambda:next((x for x in pods('new-worker') if x['metadata']['uid']!=first['metadata']['uid']),None));check('replacement-retains-routing',second['metadata']['annotations'][DKEY]==first['metadata']['annotations'][DKEY])
# Real StatefulSet, Job, CronJob and Argo controller descendants.
for kind in ['StatefulSet','Job','CronJob','Workflow']:
 name='controller-'+kind.lower();obj=workload(kind,name,route='shared');k('create','-f','-',obj=obj)
 def descendants():
  allpods=get('pods',ns=BUS)['items']
  return [x for x in allpods if x['metadata'].get('labels',{}).get('app')==name or x['metadata'].get('labels',{}).get('workflows.argoproj.io/workflow')==name]
 child=wait(lambda:next(iter(descendants()),None),85)
 check('real-'+kind+'-child',DKEY in child['metadata'].get('annotations',{}) and json.loads(child['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-shared',child)
 if kind=='CronJob':k('patch','cronjob',name,'-n',BUS,'--type=merge','-p','{"spec":{"suspend":true}}')
# Actual Arena CLI submissions use the pinned local kubeconfig, never current-context.
for batch,expected in [('allocentric','wp-ego-shared'),('old-5880','wp-ego-shared'),('new-5880','default')]:
 name='arena-cli-'+batch
 result=subprocess.run([os.environ.get('WORKLOAD_POLICY_E2E_ARENA','arena'),'--config',KCFG,'--namespace',BUS,'submit','pytorch','--name',name,'--image','busybox:1.36','--workers','1','--gpus','0','--cpu','10m','--memory','32Mi','--share-memory','16Mi','--retry','0','--clean-task-policy','None','--label','routing.example.org/controller=new','--label','safeguard=workload-policy-test','--selector','workload-policy.test/nonexistent=true','--selector','batch='+batch,'--','echo unified-routing-test'],text=True,capture_output=True,timeout=90)
 check('arena-cli-submit-'+batch,result.returncode==0,result.stderr+result.stdout)
 job=get('pytorchjob',name,BUS)
 check('arena-cli-route-'+batch,job['spec']['runPolicy']['schedulingPolicy']['queue']==expected and DKEY in job['metadata'].get('annotations',{}),job)
 child=wait(lambda:next(iter(trainer_pods(name)),None))
 check('arena-cli-child-'+batch,child['metadata']['annotations'].get('scheduling.volcano.sh/queue-name')==expected and DKEY in child['metadata'].get('annotations',{}),child)
# Parent policy edits do not re-route enrolled workers.
policy['spec']['rules'][0]['target']['queue']='wp-ego-base-v2';apply(policy);k('scale','deployment/new-worker','-n',BUS,'--replicas=2');newpods=wait(lambda:pods('new-worker') if len(pods('new-worker'))==2 else None);check('scale-retains-frozen-queue',all(json.loads(x['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-base' for x in newpods))
# Historical recreate uses the real ancestor UID, not just a submitted label.
firstold=wait(lambda:next(iter(pods('historical')),None));k('delete','pod',firstold['metadata']['name'],'-n',BUS,'--wait=false');secondold=wait(lambda:next((x for x in pods('historical') if x['metadata']['uid']!=firstold['metadata']['uid']),None));check('historical-recreate-preserved',DKEY not in secondold['metadata'].get('annotations',{}) and secondold['spec']['nodeSelector']['workload-policy.test/pool']=='ego')
obj=workload('Pod','fake-owner');obj['metadata']['ownerReferences']=[{'apiVersion':'apps/v1','kind':'Deployment','name':old['metadata']['name'],'uid':old['metadata']['uid'],'controller':True}];obj['spec']['nodeSelector']['workload-policy.test/pool']='ego';o,err=preview(obj);check('forged-grandfather-owner-denied',o is None,err)
# Updates preserve routing identity while allowing image/replica changes.
dep=get('deployment','new-worker',BUS);bad=copy.deepcopy(dep);bad['spec']['template']['spec']['schedulerName']='another';o,err=preview(bad,True);check('update-scheduler-denied',o is None and 'scheduler' in err,err)
good=copy.deepcopy(dep);good['spec']['template']['spec']['containers'][0]['image']='busybox:1.37';o,err=preview(good,True);check('image-update-allowed',o is not None,err)
# Real Kyverno policies, scoped to these local namespaces only.
legacy_names=['business-independent-routing','business-pods-use-volcano','pipeline-nodepool-isolation']
for n in legacy_names:assert not k('get','clusterpolicy',n,'--ignore-not-found','-o','name').stdout,'Refusing to overwrite an existing policy'
def remove_legacy():
 for n in legacy_names:k('delete','clusterpolicy',n,'--ignore-not-found',ok=False)
atexit.register(remove_legacy)
fixtures=json.loads((pathlib.Path(__file__).parent/'legacy-kyverno.json').read_text())
for item in fixtures['items']:apply(item)
for n in legacy_names:k('wait','--for=condition=Ready','clusterpolicy/'+n,'--timeout=60s')
with tempfile.TemporaryDirectory() as td:
 td=pathlib.Path(td);ps={'apiVersion':'v1','kind':'List','items':[get('clusterpolicy',n) for n in legacy_names]}
 (td/'policies.json').write_text(json.dumps(ps));(td/'workloads.json').write_text(json.dumps({'items':[get('deployment','historical',BUS)]}));(td/'registry.json').write_text(json.dumps(get('configmap','grandfather',NS)))
 subprocess.run(['python3',str(ROOT/'installer/pipeline/prepare-workload-handoff.py'),'--policies',str(td/'policies.json'),'--workloads',str(td/'workloads.json'),'--registry',str(td/'registry.json'),'--namespace',BUS,'--protected-queue','wp-ego-base','--output',str(td/'patches')],check=True)
 for n in legacy_names:k('patch','clusterpolicy',n,'--type=json','--patch-file',str(td/'patches'/f'{n}-forward.json'))
 for n in legacy_names:k('wait','--for=condition=Ready','clusterpolicy/'+n,'--timeout=60s')
 obj=workload('Deployment','handoff',route='base');o,err=preview(obj);check('kyverno-handoff-preserves-new-decision',o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-base-v2',err)
 obj=workload('Pod','legacy-outside',OTHER,route='base');obj['metadata']['annotations']={DKEY:'untrusted-marker'};o,err=preview(obj)
 check('handoff-marker-does-not-bypass-outside-scope',o is not None and o['metadata']['labels']['scheduling.simplesilicon.com/routing-version']=='v1' and o['spec']['schedulerName']=='default-scheduler',err)
 # Guarded patches refuse stale replay, including registry state.
 r=k('patch','clusterpolicy',legacy_names[0],'--type=json','--patch-file',str(td/'patches'/f'{legacy_names[0]}-forward.json'),ok=False);check('handoff-patch-stale-replay-rejected',r.returncode!=0,r.stderr)
for kind in ['Pod','Deployment','PyTorchJob']:
 obj=workload(kind,'system-probe','kube-system')
 if kind=='PyTorchJob':obj['spec'].pop('runPolicy',None)  # compatible with the pre-existing old-operator admission
 o,err=preview(obj)
 check('system-'+kind+'-unchanged',o is not None and DKEY not in o['metadata'].get('annotations',{}),err)
# Failure is scoped, then rollback gates stop new enrollment.
k('scale','deployment/'+NAME,'-n',NS,'--replicas=0');wait(lambda:not get('pods',ns=NS)['items'],30)
o,err=preview(workload('Pod','outage'));check('scoped-outage-denied',o is None,err)
o,err=preview(workload('Pod','outage-system','kube-system'));check('outage-system-unaffected',o is not None and DKEY not in o['metadata'].get('annotations',{}),err)
o,err=preview(workload('Pod','outage-outside',OTHER));check('outage-outside-unaffected',o is not None,err)
k('scale','deployment/'+NAME,'-n',NS,'--replicas=1');k('rollout','status','deployment/'+NAME,'-n',NS,'--timeout=60s')
policy['spec']['mode']='Disabled';apply(policy)
o,err=preview(workload('Pod','rollback'));check('rollback-new-enrollment-disabled',o is not None and DKEY not in o['metadata'].get('annotations',{}),err)
prior_ids={p['metadata']['uid'] for p in pods('new-worker')};k('delete','pod',pods('new-worker')[0]['metadata']['name'],'-n',BUS,'--wait=false')
replacement=wait(lambda:next((x for x in pods('new-worker') if x['metadata']['uid'] not in prior_ids),None))
check('rollback-enrolled-recreation-retained',json.loads(replacement['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-base')
k('delete','pod',training_replacement['metadata']['name'],'-n',BUS,'--wait=false')
after_stop=wait(lambda:next((p for p in trainer_pods('new-arena') if p['metadata']['uid']!=training_replacement['metadata']['uid']),None))
check('rollback-enrolled-arena-recreation-retained',after_stop['metadata']['annotations'][DKEY]==training_replacement['metadata']['annotations'][DKEY])
check('rollback-existing-template-unchanged',get('deployment','new-worker',BUS)['spec']==dep['spec'])
after=get('workloadroutingpolicies');prior={x['metadata']['uid']:x['spec'] for x in before['items'] if x['metadata']['name']!=NAME};check('other-routing-policies-unchanged',all(prior.get(x['metadata']['uid'])==x['spec'] for x in after['items'] if x['metadata']['name']!=NAME))
scheduler_after={x['metadata']['namespace']+'/'+x['metadata']['name']:x['spec'] for x in json.loads(k('get','deployments','-A','-o','json').stdout)['items'] if 'scheduler' in x['metadata']['name']}
check('existing-schedulers-unchanged',scheduler_before==scheduler_after)
check('active-scheduler-configs-unchanged',scheduler_configs_before==scheduler_configs())
for ns in [BUS,SECOND]:
 for pod in get('pods',ns=ns)['items']:
  for container in pod['spec'].get('containers',[])+pod['spec'].get('initContainers',[]):
   for field in ['requests','limits']:
    assert str(container.get('resources',{}).get(field,{}).get('nvidia.com/gpu','0'))=='0','test unexpectedly requests GPU'
check('no-GPU-requests-in-real-test-pods',True)
print(str(len(results))+' local API/controller checks passed; no GPU allocated.',flush=True)
