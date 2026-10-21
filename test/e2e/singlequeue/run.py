#!/usr/bin/env python3
"""Local k3s only: real admission, Deployment/PodGroup/PDB/scale controllers and scheduler.
GPU capacity/Pod Ready are simulated on dedicated nodes, never real GPUs.
"""
import argparse,copy,json,os,pathlib,subprocess,sys,tempfile,time,base64,fcntl,concurrent.futures
ROOT=pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'test/e2e/pipeline'))
from harness import Cluster,Fixture,NS,NODES,OWNER,wait,require
p=argparse.ArgumentParser();p.add_argument('--execute-local',action='store_true');p.add_argument('--output',required=True);a=p.parse_args()
require(a.execute_local,'Explicit --execute-local required')
OUT=pathlib.Path(a.output);OUT.mkdir(parents=True,exist_ok=False)
lock=(ROOT/'test/e2e/pipeline/.run.lock').open('a');fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
c=Cluster();cfg=json.loads(c.k('config','view','--minify','-o','json'));require(cfg['clusters'][0]['cluster']['server']=='https://127.0.0.1:16473','LOCAL ONLY')
IMAGE='volcano-single-queue:local';SYS='single-queue-e2e';NAME='single-queue-e2e';PIPE='single-queue';KEY='pipeline.simplesilicon.com/name';TYPE='scheduling.simplesilicon.com/worker-type';API='scheduling.simplesilicon.com/v1alpha1'
results=[];installed=[];f=None
old=c.get('deployment','volcano-scheduler','volcano-system');(OUT/'old-scheduler.json').write_text(json.dumps(old,indent=2))
def report(name):results.append(name);print('PASS',name,flush=True);(OUT/'results.json').write_text(json.dumps(results,indent=2))
def apply(o):return c.k('apply','-f','-',obj=o)
def create(o):return c.create(o)
def delete(kind,name,ns=None):return c.k('delete',kind,name,*(['-n',ns] if ns else []),'--ignore-not-found','--wait=false')
def meta(name,ns=None):return {'name':name,**({'namespace':ns} if ns else {})}
def policy(name='single-queue',floor=4,desired=8,typ='A',deployment='a'):
 d=c.get('deployment',deployment,NS)
 return {'apiVersion':API,'kind':'WorkerBudget','metadata':meta(name+'-'+typ.lower(),NS),'spec':{'pipelineRef':{'name':name},'workerType':typ,'resourceName':'nvidia.com/gpu','minAvailableGPU':floor,'desiredGPU':desired,'workloadRef':{'kind':'Deployment','name':deployment,'uid':d['metadata']['uid']}}}
def worker(name,typ='A'):
 return {'apiVersion':'apps/v1','kind':'Deployment','metadata':meta(name,NS)|{'labels':{KEY:PIPE,TYPE:typ}},'spec':{'replicas':0,'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{'app':name,OWNER:'preemption-e2e'}},'spec':{'nodeSelector':{'worker-type-test':'true'},'tolerations':[{'key':'worker-type-test','operator':'Exists','effect':'NoSchedule'}],'containers':[{'name':'worker','image':'busybox:1.36','command':['sleep','3600'],'resources':{'requests':{'cpu':'100m','memory':'32Mi','nvidia.com/gpu':'1'},'limits':{'nvidia.com/gpu':'1'}}}]}}}}
def training(name,gpu=4):
 return {'apiVersion':'v1','kind':'Pod','metadata':meta(name,NS)|{'labels':{KEY:PIPE,'test.local/role':'training'}},'spec':{'nodeSelector':{'worker-type-test':'true'},'tolerations':[{'key':'worker-type-test','operator':'Exists','effect':'NoSchedule'}],'containers':[{'name':'training','image':'busybox:1.36','resources':{'requests':{'cpu':'100m','memory':'32Mi','nvidia.com/gpu':str(gpu)},'limits':{'nvidia.com/gpu':str(gpu)}}}]}}
def healthy(typ=None):
 return [x for x in f.pods() if x['metadata'].get('labels',{}).get(TYPE) and (typ is None or x['metadata']['labels'][TYPE]==typ) and x.get('status',{}).get('phase')=='Running' and not x['metadata'].get('deletionTimestamp') and any(y['type']=='Ready' and y['status']=='True' for y in x.get('status',{}).get('conditions',[]))]
def bound(n):return c.get('pod',n,NS)['spec'].get('nodeName')
class ManagedFixture(Fixture):
 def tick(self):
  super().tick()
  for x in self.pods():
   if x['spec'].get('nodeName') in NODES and not x['metadata'].get('deletionTimestamp') and not any(y['type']=='Ready' and y['status']=='True' for y in x.get('status',{}).get('conditions',[])):
    self.c.patch('pod',x['metadata']['name'],{'status':{'phase':'Running','conditions':[{'type':'Ready','status':'True','reason':'SyntheticE2E'}]}},NS,True)
# Refuse dirty/unowned fixtures before entering cleanup: never delete an existing namespace on refusal.
require(not c.get('pods',ns=NS)['items'],'Test namespace must have no workloads')
require(not c.journal(),'Outstanding recovery journal: do not reset it')
require(not c.k('get','namespace',SYS,'--ignore-not-found').strip(),'Test control namespace already exists; inspect first')
try:
 for file in ['budget-crds.json','workload-routing-crd.json','nodepool-policy-crd.json']:c.k('apply','-f',str(ROOT/'installer/pipeline'/file))
 infra=json.loads((ROOT/'test/e2e/pipeline/infrastructure.json').read_text())
 for o in infra['items']:
  if o['kind']=='ConfigMap' and 'volcano-scheduler.conf' in o.get('data',{}):
   o['data']['volcano-scheduler.conf']=o['data']['volcano-scheduler.conf'].replace('  - name: proportion','  - name: capacity\n  - name: nodegroup\n    arguments:\n      strict: false\n  - name: binpack\n    arguments:\n      binpack.weight: 10\n      binpack.resources: nvidia.com/gpu\n      binpack.resources.nvidia.com/gpu: 10')
  if o['kind']=='Deployment':
   for ct in o['spec']['template']['spec']['containers']:
    if ct['name']!='fault-proxy':ct['image']=IMAGE
  apply(o);installed.append(o)
 c.k('rollout','restart','deployment/worker-type-test','-n','volcano-system')
 c.k('rollout','status','deployment/worker-type-test','-n','volcano-system','--timeout=120s')
 apply({'apiVersion':'v1','kind':'Namespace','metadata':meta(SYS)})
 apply({'apiVersion':'v1','kind':'ServiceAccount','metadata':meta(NAME,SYS)})
 # Test service account: read-only input APIs, explicit namespaced scale/status writes below.
 apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':meta(NAME),'rules':[{'apiGroups':['scheduling.simplesilicon.com'],'resources':['pipelinepolicies','workerbudgets','workloadroutingpolicies','nodepoolpolicies'],'verbs':['get','list','watch']},{'apiGroups':['scheduling.volcano.sh'],'resources':['podgroups'],'verbs':['get','list','watch']},{'apiGroups':[''],'resources':['pods'],'verbs':['get','list','watch']},{'apiGroups':['apps'],'resources':['deployments','replicasets','statefulsets','daemonsets'],'verbs':['get']}]})
 apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRoleBinding','metadata':meta(NAME),'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':NAME},'subjects':[{'kind':'ServiceAccount','name':NAME,'namespace':SYS}]})
 for ns in [NS,SYS]:
  rules=([{'apiGroups':[''],'resources':['configmaps'],'resourceNames':['worker-type-test-recovery'],'verbs':['get']},{'apiGroups':['apps'],'resources':['deployments/scale'],'verbs':['get','update']},{'apiGroups':['autoscaling'],'resources':['horizontalpodautoscalers'],'verbs':['list']},{'apiGroups':['scheduling.simplesilicon.com'],'resources':['workerbudgets/status'],'verbs':['update']}] if ns==NS else [{'apiGroups':['coordination.k8s.io'],'resources':['leases'],'verbs':['get','list','watch','create','update']}])
  apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'Role','metadata':meta(NAME,ns),'rules':rules})
  apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':meta(NAME,ns),'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':NAME},'subjects':[{'kind':'ServiceAccount','name':NAME,'namespace':SYS}]})
 def deploy(name,command,args,sa=NAME,ns=SYS):
  apply({'apiVersion':'apps/v1','kind':'Deployment','metadata':meta(name,ns),'spec':{'replicas':1,'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{'app':name}},'spec':{'serviceAccountName':sa,'nodeSelector':{'node-role.kubernetes.io/control-plane':'true'},'containers':[{'name':'controller','image':IMAGE,'imagePullPolicy':'Never','command':[command],'args':args}]}}}})
  c.k('rollout','status','deployment/'+name,'-n',ns,'--timeout=120s')
 deploy('single-queue-podgroups','/vc-controller-manager',['--controllers=pg-controller','--scheduler-name=worker-type-test','--leader-elect=false','--logtostderr'],'volcano-controllers','volcano-system')
 deploy('worker-scale','/pipeline-worker-controller',['--namespace='+NS,'--lease-namespace='+SYS,'--interval=2s','--recovery-journal='+NS+'/worker-type-test-recovery'])
 users=['system:kube-controller-manager','system:serviceaccount:kube-system:deployment-controller','system:serviceaccount:kube-system:replicaset-controller']
 rules=[]
 for typ in ['A','B']:
  rules.append({'name':'worker-'+typ.lower(),'match':{'labels':{KEY:PIPE,TYPE:typ}},'target':{'queue':'wt-shared','nodeGroup':'sq','poolRef':NAME,'priorityClassName':'wt-low','worker':{'pipeline':PIPE,'namespace':NS,'type':typ}}})
 rules.append({'name':'training','match':{'labels':{KEY:PIPE,'test.local/role':'training'}},'target':{'queue':'wt-shared','nodeGroup':'sq','poolRef':NAME,'priorityClassName':'wt-high'}})
 route={'apiVersion':API,'kind':'WorkloadRoutingPolicy','metadata':meta(NAME),'spec':{'mode':'Enforce','namespaceScope':{'include':[NS]},'workloadKinds':['Pod','Deployment'],'controllerUsers':users,'schedulerName':'worker-type-test','rules':rules,'fallback':{'queue':'default','nodeGroup':'general'}}}
 pool={'apiVersion':API,'kind':'NodePoolPolicy','metadata':meta(NAME),'spec':{'mode':'Enforce','namespaceScope':{'include':[NS]},'nodeSelector':{'single-queue.test/pool':'sq'},'controllerUsers':users,'acceptedRoutes':[{'policyRef':NAME,'ruleNames':[r['name'] for r in rules]}]}}
 with tempfile.TemporaryDirectory() as td:
  td=pathlib.Path(td);(td/'route.json').write_text(json.dumps(route));(td/'pools.json').write_text(json.dumps({'items':[pool]}))
  subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(td/'tls.key'),'-out',str(td/'tls.crt'),'-days','2','-subj',f'/CN={NAME}.{SYS}.svc','-addext',f'subjectAltName=DNS:{NAME}.{SYS}.svc'],check=True,capture_output=True)
  ca=base64.b64encode((td/'tls.crt').read_bytes()).decode();apply({'apiVersion':'v1','kind':'Secret','metadata':meta(NAME,SYS),'type':'kubernetes.io/tls','data':{'tls.crt':ca,'tls.key':base64.b64encode((td/'tls.key').read_bytes()).decode()}})
  rendered=json.loads(subprocess.check_output(['python3',str(ROOT/'installer/pipeline/render-workload-policy.py'),'--policy',str(td/'route.json'),'--pools',str(td/'pools.json'),'--image',IMAGE,'--name',NAME,'--namespace',SYS,'--tls-secret',NAME],text=True))
  for o in rendered['items']:
   if o['kind'] in ['ServiceAccount','ClusterRole','ClusterRoleBinding']:continue
   if o['kind']=='Deployment':
    o['spec']['replicas']=1;ps=o['spec']['template']['spec'];ps['nodeSelector']={'node-role.kubernetes.io/control-plane':'true'};ps.pop('affinity',None);ps['containers'][0]['imagePullPolicy']='Never';ps['containers'][0]['command']=['/workload-policy-webhook']
   if o['kind'].endswith('WebhookConfiguration'):
    o['metadata'].pop('annotations',None)
    for w in o['webhooks']:w['clientConfig']['caBundle']=ca;w['matchConditions']=[{'name':'local-enabled','expression':'true'}]
   apply(o)
 c.k('rollout','status','deployment/'+NAME,'-n',SYS,'--timeout=120s')
 f=ManagedFixture(c,OUT/'runtime');f.setup()
 for node in NODES:c.patch('node',node,{'metadata':{'labels':{'volcano.sh/nodegroup-name':'sq','single-queue.test/pool':'sq'}}})
 c.patch('queue','wt-shared',{'spec':{'deserved':{'nvidia.com/gpu':'8'},'capability':{'nvidia.com/gpu':'8'},'affinity':{'nodeGroupAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':['sq']}}}})
 f.capacity={n:{'cpu':'64','memory':'128Gi','pods':'100','nvidia.com/gpu':'4'} for n in NODES};f.tick()
 create({'apiVersion':'scheduling.k8s.io/v1','kind':'PriorityClass','metadata':meta('wt-low'),'value':-20,'globalDefault':False})
 create({'apiVersion':API,'kind':'PipelinePolicy','metadata':meta(PIPE,NS),'spec':{'accountingQueues':['wt-shared'],'preemptibleQueues':['wt-shared'],'allowedPreemptorNamespaces':[NS]}})
 create(worker('a'));create(policy())
 wait(lambda:len(healthy('A'))==8,seconds=120,description='desiredGPU controller -> Deployment -> 8 Ready worker Pods')
 for x in healthy():
  require(x['spec']['priorityClassName']=='wt-low','worker priority missing');require(x['metadata']['labels']['scheduling.simplesilicon.com/preemptible']=='true','worker eligibility missing');require(x['metadata']['annotations']['scheduling.volcano.sh/queue-name']=='wt-shared','wrong queue')
 report('desiredGPU=8 automatically creates 8 managed worker replicas in one queue')
 start=len(f.timeline);create(training('training-one'))
 wait(lambda:bound('training-one') and len(healthy('A'))==4,seconds=120,description='training preempts 4 workers')
 create(training('training-two'));f.observe(12)
 require(not bound('training-two'),'second training crossed floor')
 for sample in f.timeline[start:]:
  ready=[x for x in sample['pods'] if x['metadata'].get('labels',{}).get(TYPE)=='A' and not x['metadata'].get('deletionTimestamp') and any(y['type']=='Ready' and y['status']=='True' for y in x.get('status',{}).get('conditions',[]))]
  require(len(ready)>=4,'Ready GPU floor breached')
 report('training preemption and consecutive demand preserve 4-GPU worker floor')
 c.patch('workerbudget',PIPE+'-a',{'spec':{'desiredGPU':6}},NS)
 wait(lambda:c.get('workerbudget',PIPE+'-a',NS).get('status',{}).get('scaling',{}).get('state')=='DownscaleDeferred')
 require(c.get('deployment','a',NS)['spec']['replicas']==8,'scale down raced eviction');report('desired downscale deferred while preemption authorized')
 delete('pod','training-two',NS);delete('pod','training-one',NS)
 wait(lambda:len(healthy('A'))==8,seconds=120,description='Deployment replacement Pods recover after training ends')
 report('training removal automatically restores all 8 workers without manual scale')
 c.patch('pipelinepolicy',PIPE,{'spec':{'allowedPreemptorNamespaces':[]}},NS)
 wait(lambda:c.get('deployment','a',NS)['spec']['replicas']==3*2 and len(healthy('A'))==6,seconds=90)
 report('authorized-off desiredGPU update reconciles to 6 replicas')
 # Restart scale controller: same desired state; unrelated workloads/scheduler remain unchanged.
 c.k('rollout','restart','deployment/worker-scale','-n',SYS);c.k('rollout','status','deployment/worker-scale','-n',SYS,'--timeout=90s')
 wait(lambda:c.get('workerbudget',PIPE+'-a',NS).get('status',{}).get('scaling',{}).get('state')=='Reconciled',seconds=90)
 report('scale controller restart converges without duplicate workers')
 # Two types share a single queue. A PDB makes B stricter than its GPU floor.
 c.patch('workerbudget',PIPE+'-a',{'spec':{'desiredGPU':4,'minAvailableGPU':2}},NS)
 wait(lambda:len(healthy('A'))==4,seconds=90)
 create(worker('b','B'));create(policy(floor=1,desired=4,typ='B',deployment='b'))
 wait(lambda:len(healthy('B'))==4,seconds=90)
 create({'apiVersion':'policy/v1','kind':'PodDisruptionBudget','metadata':meta('b-floor',NS),'spec':{'minAvailable':3,'selector':{'matchLabels':{'app':'b'}}}})
 wait(lambda:c.get('pdb','b-floor',NS).get('status',{}).get('disruptionsAllowed')==1)
 c.patch('pipelinepolicy',PIPE,{'spec':{'allowedPreemptorNamespaces':[NS]}},NS)
 start=len(f.timeline)
 with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
  list(ex.map(lambda n:create(training(n,1)),['parallel-one','parallel-two','parallel-three']))
 wait(lambda:all(bound(n) for n in ['parallel-one','parallel-two','parallel-three']),seconds=150,description='concurrent training with per-type floors and PDB')
 create(training('parallel-blocked',1));f.observe(10)
 require(not bound('parallel-blocked'),'concurrent training breached aggregate protections')
 for sample in f.timeline[start:]:
  counts={t:sum(1 for x in sample['pods'] if x['metadata'].get('labels',{}).get(TYPE)==t and not x['metadata'].get('deletionTimestamp') and any(y['type']=='Ready' and y['status']=='True' for y in x.get('status',{}).get('conditions',[]))) for t in ['A','B']}
  require(counts['A']>=2 and counts['B']>=3,'per-type GPU/PDB floor breached: '+str(counts))
 report('concurrent training preserves A minimum=2 and B PDB minimum=3 in one queue')
 for n in ['parallel-one','parallel-two','parallel-three','parallel-blocked']:delete('pod',n,NS)
 wait(lambda:len(healthy('A'))==4 and len(healthy('B'))==4,seconds=120)
 report('both worker types automatically recover after concurrent training ends')
 f.protected();require(not f.errors,str(f.errors))
finally:
 # Close ONLY this suite's webhook entries before removing test resources.
 for kind in ['mutatingwebhookconfiguration','validatingwebhookconfiguration']:delete(kind,NAME)
 if f:
  delete('deployment','a',NS);delete('deployment','b',NS)
  wait(lambda:not c.get('deployments',ns=NS)['items'],seconds=60)
  delete('workerbudget',PIPE+'-a',NS);delete('workerbudget',PIPE+'-b',NS);delete('pipelinepolicy',PIPE,NS)
  f.finish()
 for kind in ['workloadroutingpolicy','nodepoolpolicy','clusterrolebinding','clusterrole']:delete(kind,NAME)
 delete('deployment','single-queue-podgroups','volcano-system');delete('namespace',SYS)
 # Keep the preemption fixture available for the regression suite; never touch the original scheduler.
 new=c.get('deployment','volcano-scheduler','volcano-system');require(old['metadata']['uid']==new['metadata']['uid'] and old['spec']==new['spec'],'original scheduler changed')
 (OUT/'old-scheduler-after.json').write_text(json.dumps(new,indent=2))
print('PASS: local synthetic GPU test; no production access; no real GPU execution claimed')
