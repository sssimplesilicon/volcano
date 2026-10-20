#!/usr/bin/env python3
"""Real admission tests, explicitly pinned to local k3s. No production context fallback."""
from concurrent.futures import ThreadPoolExecutor
import atexit,base64,copy,json,os,pathlib,subprocess,tempfile,time
ROOT=pathlib.Path(__file__).resolve().parents[3]
KCFG=os.environ.get('ROUTING_E2E_KUBECONFIG','/nonexistent/local-only')
OUT=pathlib.Path(os.environ.get('ROUTING_E2E_OUTPUT','/tmp/workload-routing-e2e'))
OUT.mkdir(parents=True,exist_ok=True)
(OUT/'results.json').unlink(missing_ok=True)
NAME='wr-e2e'; NS='wr-e2e-system'; ALLOW='wr-e2e-allow'; DENY='wr-e2e-deny'; OUTSIDE='wr-e2e-outside'
IMAGE='workload-routing:local'
results=[]
def k(*args,obj=None,ok=True):
 p=subprocess.run(['kubectl','--kubeconfig',KCFG,*args],input=json.dumps(obj) if obj is not None else None,text=True,capture_output=True,timeout=90)
 if ok and p.returncode:raise RuntimeError(p.stderr)
 return p

def get(kind,name,ns=None):return json.loads(k('get',kind,name,*(['-n',ns] if ns else []),'-o','json').stdout)
def apply(o):return k('apply','-f','-',obj=o)
def check(name,condition,detail=''):
 if not condition:raise AssertionError(name+': '+detail)
 results.append(name);print('PASS',name,flush=True)
def wait(fn,seconds=90):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  v=fn()
  if v:return v
  time.sleep(1)
 raise AssertionError('timeout')
def meta(name,ns=None):return dict(name=name,**({'namespace':ns} if ns else {}))
def access(name,namespaces):return {'apiVersion':'scheduling.simplesilicon.com/v1alpha1','kind':'QueueAccessPolicy','metadata':meta(name),'spec':{'allowedNamespaces':namespaces}}
def job(name,ns=ALLOW,batch='old-5880',pipeline=None,selector=False,old=False):
 labels={'app':'pytorchjob','safeguard':'routing-local-test','routing.example.org/controller':'old' if old else 'new'}
 if not selector and batch:labels['batch']=batch
 if pipeline:labels['pipeline.simplesilicon.com/name']=pipeline
 podspec={'containers':[{'name':'pytorch','image':'busybox:1.36','command':['sh','-c','echo local-routing-test'],'resources':{'requests':{'cpu':'10m','memory':'16Mi'}}}],'tolerations':[{'key':'user-kept','operator':'Exists'}]}
 if selector:podspec['nodeSelector']={'batch':batch}
 return {'apiVersion':'kubeflow.org/v1','kind':'PyTorchJob','metadata':meta(name,ns)|{'labels':labels},'spec':{'runPolicy':{'suspend':True,'cleanPodPolicy':'None'},'pytorchReplicaSpecs':{'Master':{'replicas':1,'restartPolicy':'Never','template':{'metadata':{'labels':{'safeguard':'routing-local-test'}},'spec':podspec}}}}}
def preview(o):
 p=k('create','--dry-run=server','--validate=strict','-f','-','-o','json',obj=o,ok=False)
 if p.returncode:return None,p.stderr
 return json.loads(p.stdout),''
def queue(o):return o['spec'].get('runPolicy',{}).get('schedulingPolicy',{}).get('queue')
def scopewebhook(enabled=True):
 return {'apiVersion':'admissionregistration.k8s.io/v1','kind':'MutatingWebhookConfiguration','metadata':meta(NAME),'webhooks':[{'name':'routing.e2e.simplesilicon.com','admissionReviewVersions':['v1'],'sideEffects':'None','failurePolicy':'Fail','reinvocationPolicy':'Never','timeoutSeconds':10,'namespaceSelector':{'matchLabels':{'routing-e2e':'true'}},'matchConditions':[{'name':'enabled','expression':'true' if enabled else 'false'}],'clientConfig':{'service':{'namespace':NS,'name':NAME,'path':'/mutate'},'caBundle':ca},'rules':[{'operations':['CREATE'],'apiGroups':['kubeflow.org'],'apiVersions':['v1'],'resources':['pytorchjobs'],'scope':'Namespaced'}]}]}
config=json.loads(k('config','view','--minify','-o','json').stdout)
assert config['clusters'][0]['cluster']['server']=='https://127.0.0.1:16473','Local k3s endpoint required'
assert '127.0.0.1' in config['clusters'][0]['cluster']['server']
# The unrelated local legacy adapter defaults new test namespaces to old ownership.
# Exclude only the three dedicated test namespaces, and restore it even on failure.
legacy=k('get','mutatingwebhookconfiguration','iristack-pytorch-migration','--ignore-not-found','-o','json').stdout
if legacy:
 before=json.loads(legacy)['webhooks']; after=copy.deepcopy(before)
 for w in after:
  w.setdefault('matchConditions',[]).append({'name':'workload-routing-e2e-isolation','expression':f"!(request.namespace in ['{ALLOW}', '{DENY}', '{OUTSIDE}'])"})
 k('patch','mutatingwebhookconfiguration','iristack-pytorch-migration','--type=json','-p',json.dumps([{'op':'test','path':'/webhooks','value':before},{'op':'replace','path':'/webhooks','value':after}]))
 def restore_legacy():
  p=k('patch','mutatingwebhookconfiguration','iristack-pytorch-migration','--type=json','-p',json.dumps([{'op':'test','path':'/webhooks','value':after},{'op':'replace','path':'/webhooks','value':before}]),ok=False)
  if p.returncode: print('RESTORE FAILED:',p.stderr,flush=True)
 atexit.register(restore_legacy)
for ns in [NS,ALLOW,DENY,OUTSIDE]:apply({'apiVersion':'v1','kind':'Namespace','metadata':meta(ns)|{'labels':{'routing-e2e':'true'} if ns in [ALLOW,DENY] else {}}})
# Exercise the real legacy Kyverno handoff, confined to dedicated test namespaces.
legacy_policy_name='arena-pytorch-independent-routing'
assert not k('get','clusterpolicy',legacy_policy_name,'--ignore-not-found','-o','name').stdout,'Unrelated legacy routing policy exists; refusing overwrite'
legacy_fixture=json.loads((pathlib.Path(__file__).parent/'legacy-kyverno.json').read_text())
apply(legacy_fixture)
def remove_legacy_fixture():
 p=k('delete','clusterpolicy',legacy_policy_name,'--ignore-not-found',ok=False)
 if p.returncode: print('LEGACY CLEANUP FAILED:',p.stderr,flush=True)
atexit.register(remove_legacy_fixture)
k('wait','--for=condition=Ready','clusterpolicy/'+legacy_policy_name,'--timeout=60s')
with tempfile.TemporaryDirectory() as td:
 snapshot=pathlib.Path(td)/'legacy.json';snapshot.write_text(json.dumps(get('clusterpolicy',legacy_policy_name)))
 subprocess.run(['python3',str(ROOT/'installer/pipeline/prepare-routing-handoff.py'),'--legacy',str(snapshot),'--output',td],check=True)
 k('patch','clusterpolicy',legacy_policy_name,'--type=json','--patch-file',str(pathlib.Path(td)/'legacy-handoff-forward.json'))
 # Keep the original guarded reverse patch to validate rollback as well.
 legacy_reverse=json.loads((pathlib.Path(td)/'legacy-handoff-rollback.json').read_text())
# Keep the existing local scheduler and workload controllers untouched.

for filename in ['queue-access-crd.json','workload-routing-crd.json']:k('apply','-f',str(ROOT/'installer/pipeline'/filename))
k('wait','--for=condition=Established','crd/workloadroutingpolicies.scheduling.simplesilicon.com','--timeout=60s')
# Catch accidental interference from policies outside this isolated suite.
existing=json.loads(k('get','workloadroutingpolicies','-o','json').stdout)['items']
assert all(p['metadata']['name'].startswith(NAME) for p in existing),'Unrelated routing policy present; use isolated cluster'
with tempfile.TemporaryDirectory() as td:
 cert,key=pathlib.Path(td)/'tls.crt',pathlib.Path(td)/'tls.key'
 subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','2','-subj',f'/CN={NAME}.{NS}.svc','-addext',f'subjectAltName=DNS:{NAME}.{NS}.svc'],check=True,capture_output=True)
 ca=base64.b64encode(cert.read_bytes()).decode()
 apply({'apiVersion':'v1','kind':'Secret','metadata':meta(NAME,NS),'type':'kubernetes.io/tls','data':{'tls.crt':ca,'tls.key':base64.b64encode(key.read_bytes()).decode()}})
apply({'apiVersion':'v1','kind':'ServiceAccount','metadata':meta(NAME,NS)})
apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':meta(NAME),'rules':[{'apiGroups':['scheduling.simplesilicon.com'],'resources':['queueaccesspolicies','workloadroutingpolicies'],'verbs':['get','list']}]})
apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRoleBinding','metadata':meta(NAME),'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':NAME},'subjects':[{'kind':'ServiceAccount','name':NAME,'namespace':NS}]})
apply({'apiVersion':'v1','kind':'Service','metadata':meta(NAME,NS),'spec':{'selector':{'app':NAME},'ports':[{'port':443,'targetPort':8443}]}})
apply({'apiVersion':'apps/v1','kind':'Deployment','metadata':meta(NAME,NS),'spec':{'replicas':1,'selector':{'matchLabels':{'app':NAME}},'template':{'metadata':{'labels':{'app':NAME}},'spec':{'serviceAccountName':NAME,'nodeSelector':{'node-role.kubernetes.io/control-plane':'true'},'containers':[{'name':'webhook','image':IMAGE,'imagePullPolicy':'Never','command':['/workload-routing-webhook'],'volumeMounts':[{'name':'tls','mountPath':'/tls'}],'readinessProbe':{'httpGet':{'scheme':'HTTPS','path':'/healthz','port':8443}}}],'volumes':[{'name':'tls','secret':{'secretName':NAME}}]}}}})
k('rollout','restart','deployment/'+NAME,'-n',NS)
k('rollout','status','deployment/'+NAME,'-n',NS,'--timeout=80s')
policy=json.loads((ROOT/'installer/pipeline/workload-routing-example.json').read_text());policy['metadata']['name']=NAME
policy['spec']['namespaceScope']={'include':[ALLOW,DENY],'exclude':[OUTSIDE]}
policy['spec']['schedulerName']='default-scheduler'
for r in policy['spec']['rules']:r['queueAccessPolicyRef']=r['target']['queue']='wr-e2e-ego';r['target']['nodeGroup']='wr-e2e-ego'
policy['spec']['rules'].append({'name':'video','match':{'labels':{'pipeline.simplesilicon.com/name':'video-pipeline'}},'queueAccessPolicyRef':'wr-e2e-video','target':{'queue':'wr-e2e-video','nodeGroup':'wr-e2e-video'}})
apply(access('wr-e2e-ego',[ALLOW]));apply(access('wr-e2e-video',[ALLOW]));apply(policy)
apply(scopewebhook(False))
def disable_test_webhook():
 p=k('patch','mutatingwebhookconfiguration',NAME,'--type=json','-p',json.dumps([{'op':'replace','path':'/webhooks/0/matchConditions','value':[{'name':'enabled','expression':'false'}]}]),ok=False)
 if p.returncode: print('DISABLE FAILED:',p.stderr,flush=True)
atexit.register(disable_test_webhook)
# Historical root is created BEFORE enabling the new router, without its marker.
k('delete','pytorchjob','historical-root','-n',ALLOW,'--ignore-not-found')
historical=job('historical-root');historical['spec']['pytorchReplicaSpecs']['Master']['template']['spec']['nodeSelector']={'kubernetes.io/hostname':'wr-e2e-nonexistent'}
k('create','-f','-',obj=historical)
historical_saved=get('pytorchjob','historical-root',ALLOW)
apply(scopewebhook())
# Register first so even the first successful result proves API-server TLS admission.
wait(lambda:queue(preview(job('ready'))[0] or {'spec':{}})=='wr-e2e-ego',seconds=30)
for name,ns,batch,pipeline,selector,expected in [
 ('legacy-label',ALLOW,'old-5880',None,False,'wr-e2e-ego'),
 ('legacy-selector',ALLOW,'allocentric',None,True,'wr-e2e-ego'),
 ('ego-explicit',ALLOW,'allocentric','ego-pipeline',False,'wr-e2e-ego'),
 ('video-explicit',ALLOW,'old-5880','video-pipeline',False,'wr-e2e-video'),
 ('unknown-explicit',ALLOW,'old-5880','unknown',False,'default'),
 ('unauthorized',DENY,'old-5880',None,False,'default'),
 ('new-batch',ALLOW,'new-5880',None,False,'default'),
 ('no-batch',ALLOW,None,None,False,'default'),
]:
 o,e=preview(job(name,ns,batch,pipeline,selector));check(name,o is not None and queue(o)==expected,e or str(o))
 check(name+'-preserves-tolerations',o['spec']['pytorchReplicaSpecs']['Master']['template']['spec']['tolerations']==[{'key':'user-kept','operator':'Exists'}])
o,e=preview(job('outside',OUTSIDE));check('namespace-outside-not-routed',o is not None and 'scheduling.simplesilicon.com/workload-routing-policy' not in o['metadata'].get('annotations',{}),e)
check('legacy-continues-outside-new-scope',queue(o)=='legacy-shared',str(o))
o,e=preview(job('old-controller',old=True));check('old-controller-not-routed',o is not None and 'scheduling.simplesilicon.com/workload-routing-policy' not in o['metadata'].get('annotations',{}),e)
# Queue authorization is independent per pipeline, even within the same scope.
apply(access('wr-e2e-video',[DENY]))
o,e=preview(job('video-not-authorized',ALLOW,pipeline='video-pipeline'));check('per-pipeline-denial-fallback',o is not None and queue(o)=='default',e)
o,e=preview(job('video-other-ns',DENY,pipeline='video-pipeline'));check('per-pipeline-other-namespace-allowed',o is not None and queue(o)=='wr-e2e-video',e)
apply(access('wr-e2e-video',[ALLOW]))
def concurrent_case(i):
 ns=ALLOW if i%2==0 else DENY
 pipeline='video-pipeline' if i%3==0 else None
 o,e=preview(job('concurrent-'+str(i),ns,pipeline=pipeline))
 expected=('wr-e2e-video' if pipeline else 'wr-e2e-ego') if ns==ALLOW else 'default'
 return o is not None and queue(o)==expected,e
with ThreadPoolExecutor(max_workers=4) as pool: concurrent=list(pool.map(concurrent_case,range(12)))
check('concurrent-pipelines-and-namespaces',all(x[0] for x in concurrent),str(concurrent))
k('annotate','pytorchjob','historical-root','-n',ALLOW,'test-update=router-enabled','--overwrite')
historical_after=get('pytorchjob','historical-root',ALLOW)
check('preexisting-unmarked-root-unchanged',historical_after['metadata']['uid']==historical_saved['metadata']['uid'] and historical_after['spec']==historical_saved['spec'])
historical_template=copy.deepcopy(historical_saved['spec']['pytorchReplicaSpecs']['Master']['template']);historical_template['metadata'].setdefault('labels',{})['app']='wr-e2e-historical';historical_template['spec']['restartPolicy']='Always'
apply({'apiVersion':'apps/v1','kind':'ReplicaSet','metadata':meta('wr-e2e-historical',ALLOW),'spec':{'replicas':1,'selector':{'matchLabels':{'app':'wr-e2e-historical'}},'template':historical_template}})
def historical_pods():return json.loads(k('get','pods','-n',ALLOW,'-l','app=wr-e2e-historical','-o','json').stdout)['items']
hp=wait(lambda:next(iter(historical_pods()),None));k('delete','pod',hp['metadata']['name'],'-n',ALLOW,'--wait=false');hp2=wait(lambda:next((p for p in historical_pods() if p['metadata']['uid']!=hp['metadata']['uid']),None))
check('preexisting-unmarked-child-recreated-unchanged',hp2['spec']['schedulerName']==hp['spec']['schedulerName'] and hp2['spec']['nodeSelector']==hp['spec']['nodeSelector'] and 'scheduling.simplesilicon.com/workload-routing-policy' not in hp2['metadata'].get('annotations',{}))
conflict=copy.deepcopy(policy);r=copy.deepcopy(conflict['spec']['rules'][1]);r['name']='duplicate-match';conflict['spec']['rules'].append(r);apply(conflict)
o,e=preview(job('conflict'));check('ambiguous-rules-rejected',o is None and 'ambiguous routing rules' in e,e);apply(policy)
overlap=copy.deepcopy(policy);overlap['metadata']['name']=NAME+'-overlap';apply(overlap)
o,e=preview(job('overlap'));check('overlapping-policies-rejected',o is None and 'multiple routing policies' in e,e);k('delete','workloadroutingpolicy',NAME+'-overlap')
bad=copy.deepcopy(policy);bad['spec']['rules'][0]['queueAccessPolicyRef']='wrong'
p=k('apply','--dry-run=server','-f','-',obj=bad,ok=False);check('schema-ref-mismatch-rejected',p.returncode!=0 and 'queueAccessPolicyRef' in p.stderr,p.stderr)
# No historical policy decisions are recalculated on UPDATE or child recreation.
oldjob=job('old-root');k('create','-f','-',obj=oldjob)
saved=get('pytorchjob','old-root',ALLOW);original=copy.deepcopy(saved['spec']);apply(access('wr-e2e-ego',[]))
o,e=preview(job('revoked'));check('authorization-revocation-fallback',o is not None and queue(o)=='default',e)
k('annotate','pytorchjob','old-root','-n',ALLOW,'test-update=after-revocation','--overwrite')
check('existing-root-update-preserves-spec',get('pytorchjob','old-root',ALLOW)['spec']==original)
# A real ReplicaSet creates a child from the saved immutable route template.
# Pending is intentional: synthetic nodegroup labels prevent running any GPU workload.
template=copy.deepcopy(saved['spec']['pytorchReplicaSpecs']['Master']['template']);template['metadata'].setdefault('labels',{})['app']='wr-e2e-rebuild'
template['spec']['restartPolicy']='Always'
rs={'apiVersion':'apps/v1','kind':'ReplicaSet','metadata':meta('wr-e2e-rebuild',ALLOW),'spec':{'replicas':1,'selector':{'matchLabels':{'app':'wr-e2e-rebuild'}},'template':template}}
apply(rs)
def pods():return json.loads(k('get','pods','-n',ALLOW,'-l','app=wr-e2e-rebuild','-o','json').stdout)['items']
first=wait(lambda:next(iter(pods()),None));k('delete','pod',first['metadata']['name'],'-n',ALLOW,'--wait=false')
second=wait(lambda:next((p for p in pods() if p['metadata']['uid']!=first['metadata']['uid']),None))
check('old-child-recreated-after-revocation',second['metadata']['annotations']['scheduling.volcano.sh/queue-name']=='wr-e2e-ego' and second['spec']['nodeSelector']==first['spec']['nodeSelector'])
apply(access('wr-e2e-ego',[ALLOW]))
# Missing authorization is distinct from an intentionally empty authorization list.
k('delete','queueaccesspolicy','wr-e2e-ego');o,e=preview(job('missing'));check('missing-access-fails-closed',o is None and 'missing QueueAccessPolicy' in e,e);apply(access('wr-e2e-ego',[ALLOW]))
badjob=job('inconsistent');worker=copy.deepcopy(badjob['spec']['pytorchReplicaSpecs']['Master']);worker['template']['spec']['nodeSelector']={'batch':'other'};badjob['spec']['pytorchReplicaSpecs']['Worker']=worker
o,e=preview(badjob);check('replica-selector-conflict-rejected',o is None and 'must be identical' in e,e)
# Outage scope: unenrolled namespace remains usable; enrolled namespace fails closed.
k('scale','deployment/'+NAME,'-n',NS,'--replicas=0')
wait(lambda:not json.loads(k('get','pods','-n',NS,'-l','app='+NAME,'-o','json').stdout)['items'])
o,e=preview(job('outage-in'));check('enrolled-outage-fails-closed',o is None and 'routing.e2e.simplesilicon.com' in e,e)
o,e=preview(job('outage-out',OUTSIDE));check('outside-outage-unaffected',o is not None,e)
apply(scopewebhook(False));time.sleep(2)
o,e=preview(job('rollback'));check('disabled-rollback-admits-new',o is not None and 'scheduling.simplesilicon.com/workload-routing-policy' not in o['metadata'].get('annotations',{}),e)
check('rollback-retains-old-spec',get('pytorchjob','old-root',ALLOW)['spec']==original)
check('disabled-new-router-restores-legacy-result',queue(o)=='legacy-shared',str(o))
k('patch','clusterpolicy',legacy_policy_name,'--type=json','-p',json.dumps(legacy_reverse))
o,e=preview(job('legacy-restored'));check('guarded-legacy-rollback',o is not None and queue(o)=='legacy-shared',e)
k('scale','deployment/'+NAME,'-n',NS,'--replicas=1');k('rollout','status','deployment/'+NAME,'-n',NS,'--timeout=80s')
(OUT/'results.json').write_text(json.dumps({'passed':results,'count':len(results),'productionTouched':False},indent=2)+'\n')
k('delete','replicaset','wr-e2e-rebuild','wr-e2e-historical','-n',ALLOW)
k('delete','pytorchjob','old-root','historical-root','-n',ALLOW)
print(f'{len(results)} checks passed; webhook left disabled; evidence {OUT}',flush=True)
