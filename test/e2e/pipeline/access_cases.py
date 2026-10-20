"""Real admission and cross-namespace authorization regression cases (local only)."""
import concurrent.futures
import json
from cases import case
from harness import NS, OWNER, require, wait
from pipeline_cases import policy, populate, cleanup_policy, API

NSS=['wt-access-data','wt-access-infra','wt-access-other']

def setup(f, floor=12):
 for ns in NSS:
  wait(lambda ns=ns:not f.c.k('get','namespace',ns,'--ignore-not-found').strip(),description='previous test namespace cleanup')
  f.c.create({'apiVersion':'v1','kind':'Namespace','metadata':{'name':ns,'labels':{'test.simplesilicon.com/queue-access':'true'}}})
  f.extra_namespaces.append(ns)
 policy(f,floor=floor)
 f.c.patch('pipelinepolicy','ego',{'spec':{'allowedPreemptorNamespaces':NSS[:2]}},NS)
 for q, allowed in [('wt-base',[NS]),('wt-shared',[NS,*NSS[:2]])]:
  f.c.create({'apiVersion':API,'kind':'QueueAccessPolicy','metadata':{'name':q},'spec':{'allowedNamespaces':allowed}})

def cleanup(f):
 # Cancel test demands BEFORE removing authorization (and preserve journal recovery).
 for ns in NSS:
  f.c.k('delete','pods','-n',ns,'-l',OWNER+'=preemption-e2e','--force','--grace-period=0','--wait=false')
 wait(lambda:not f.c.journal(),description='canceled remote demands drain journal')
 f.c.k('delete','queueaccesspolicies','-l',OWNER+'=preemption-e2e','--wait=true')
 cleanup_policy(f)

def denied(fn):
 try: fn()
 except RuntimeError as e:
  require('not authorized' in str(e) or 'authorized scheduler' in str(e),'Unexpected failure: '+str(e)); return str(e)
 raise AssertionError('Unauthorized request accepted')

def bound(f,ns,name='training'):
 return f.c.get('pod',name,ns)['spec'].get('nodeName')

def train(f,ns,name='training',**kw):
 f.train(name,gpu=4,pipeline='ego',namespace=ns,target_namespace=NS,**kw)

@case('P18','admission denies unauthorized queue, base and scheduler bypass; old Pod unchanged, reconstruction denied')
def admission(f,_):
 try:
  setup(f)
  evidence=[]
  evidence.append(denied(lambda:train(f,NSS[2])))
  evidence.append(denied(lambda:f.train('base-bypass',gpu=4,namespace=NSS[0],queue='wt-base')))
  # A bound old worker survives revocation, ordinary metadata updates remain possible.
  f.pod('old-worker',gpu=1,pipeline='ego')
  old=f.c.get('pod','old-worker',NS)
  f.c.patch('queueaccesspolicy','wt-shared',{'spec':{'allowedNamespaces':NSS[:2]}})
  f.c.patch('pod','old-worker',{'metadata':{'annotations':{'test.local/note':'unchanged-scheduling'}}},NS)
  require(f.c.get('pod','old-worker',NS)['metadata']['uid']==old['metadata']['uid'],'old worker replaced')
  replacement={'apiVersion':'v1','kind':'Pod','metadata':{'name':'recreated-worker','namespace':NS,'labels':old['metadata']['labels'],'annotations':old['metadata']['annotations']},'spec':old['spec']}
  replacement['spec'].pop('nodeName',None)
  evidence.append(denied(lambda:f.c.create(replacement)))
  # generateName Pods cannot bypass the scheduler check.
  replacement['metadata']={'generateName':'spoof-','namespace':NSS[0],'annotations':{'scheduling.volcano.sh/queue-name':'wt-shared'}}
  replacement['spec']['schedulerName']='default-scheduler'
  evidence.append(denied(lambda:f.c.create(replacement)))
  (f.evidence/'denials.json').write_text(json.dumps(evidence,indent=2))
  f.observe(4)
  require(not f.deleted(),'revocation evicted old worker')
 finally:cleanup(f)

@case('P19','data and infra concurrently preempt only shared with one shared GPU/PDB budget')
def concurrent_namespaces(f,_):
 try:
  setup(f);populate(f);f.pdb(minimum=3)
  wait(lambda:f.c.get('pdb','type-a',NS).get('status',{}).get('currentHealthy')==4)
  boundary=len(f.timeline)
  for ns in NSS[:2]:train(f,ns,gate=True)
  with concurrent.futures.ThreadPoolExecutor(2) as pool:
   list(pool.map(lambda ns:f.c.patch('pod','training',{'spec':{'schedulingGates':[]}},ns),NSS[:2]))
  wait(lambda:len(f.deleted())==1 and sum(bool(bound(f,ns)) for ns in NSS[:2])==1,description='one authorized cross-namespace binding')
  f.observe()
  require(len(f.deleted())==1 and all(x.startswith('shared-') for x in f.deleted()),'shared/base protection failed')
  for sample in f.timeline[boundary:]:
   healthy=[p for p in sample['pods'] if not p['metadata'].get('deletionTimestamp') and any(x['type']=='Ready' and x['status']=='True' for x in p.get('status',{}).get('conditions',[]))]
   require(len(healthy)>=3,'replica floor breached')
   gpu=sum(int(c['resources']['requests'].get('nvidia.com/gpu','0')) for p in healthy for c in p['spec']['containers'])
   require(gpu>=12,'GPU floor breached')
  f.protected()
 finally:cleanup(f)

@case('P20,P21','revoke pending queue access / cross-namespace eviction permission before releasing demand')
def revoke(f,ident):
 try:
  setup(f);populate(f)
  demand_ns=NSS[0] if ident=='P20' else NSS[1]
  train(f,demand_ns,gate=True)
  if ident=='P20':f.c.patch('queueaccesspolicy','wt-shared',{'spec':{'allowedNamespaces':[NS,NSS[1]]}})
  else:f.c.patch('pipelinepolicy','ego',{'spec':{'allowedPreemptorNamespaces':[NSS[0]]}},NS)
  f.c.patch('pod','training',{'spec':{'schedulingGates':[]}},demand_ns)
  f.observe()
  require(not f.deleted() and not bound(f,demand_ns),'revoked demand caused eviction')
  # Restore and prove the same pending demand can now progress.
  if ident=='P20':f.c.patch('queueaccesspolicy','wt-shared',{'spec':{'allowedNamespaces':[NS,*NSS[:2]]}})
  else:f.c.patch('pipelinepolicy','ego',{'spec':{'allowedPreemptorNamespaces':NSS[:2]}},NS)
  wait(lambda:bool(bound(f,demand_ns)) and len(f.deleted())==1,description='reauthorized demand resumes')
  f.protected()
 finally:cleanup(f)

@case('P22','ambiguous eviction recovery rechecks revoked preemptor authorization')
def recovery(f,_):
 try:
  setup(f);populate(f);f.c.fault('before',repeat=1000)
  train(f,NSS[0])
  wait(lambda:'FAULT_BEFORE' in f.c.logs('fault-proxy',f.since))
  f.c.patch('pipelinepolicy','ego',{'spec':{'allowedPreemptorNamespaces':[]}},NS)
  f.observe(8);f.c.fault();f.observe(12)
  require(not f.deleted() and not bound(f,NSS[0]),'revoked recovery evicted a worker')
  require(f.c.journal(),'ambiguous reservation prematurely released')
 finally:cleanup(f)

@case('P23','an existing ReplicaSet rebuilds while authorized; revocation leaves current replica intact and blocks next recreation')
def old_controller(f,_):
 try:
  setup(f)
  f.c.create({'apiVersion':'scheduling.volcano.sh/v1beta1','kind':'PodGroup','metadata':{'name':'legacy-controller','namespace':NS},'spec':{'queue':'wt-shared','minMember':1}})
  f.c.create({'apiVersion':'apps/v1','kind':'ReplicaSet','metadata':{'name':'legacy-controller','namespace':NS},'spec':{'replicas':1,'selector':{'matchLabels':{'app':'legacy-access'}},'template':{'metadata':{'labels':{'app':'legacy-access',OWNER:'preemption-e2e'},'annotations':{'scheduling.k8s.io/group-name':'legacy-controller'}},'spec':{'schedulerName':'worker-type-test','nodeSelector':{'worker-type-test':'true'},'tolerations':[{'key':'worker-type-test','operator':'Exists','effect':'NoSchedule'}],'containers':[{'name':'worker','image':'busybox:1.36','command':['sleep','3600'],'resources':{'requests':{'cpu':'100m','memory':'32Mi'}}}]}}}})
  def replicas():return [p for p in f.pods() if p['metadata'].get('labels',{}).get('app')=='legacy-access' and not p['metadata'].get('deletionTimestamp')]
  first=wait(lambda:next((p for p in replicas() if p['spec'].get('nodeName')),None),description='original replica scheduled')
  f.c.complete_deletion(first)
  second=wait(lambda:next((p for p in replicas() if p['metadata']['uid']!=first['metadata']['uid'] and p['spec'].get('nodeName')),None),description='old controller replacement authorized')
  f.c.patch('queueaccesspolicy','wt-shared',{'spec':{'allowedNamespaces':NSS[:2]}})
  f.observe(4)
  require(any(p['metadata']['uid']==second['metadata']['uid'] for p in replicas()),'revocation changed current replica')
  (f.evidence/'reconstruction-identities.json').write_text(json.dumps({'first':first,'replacement':second},indent=2))
  f.c.complete_deletion(second)
  wait(lambda:any(x.get('type')=='ReplicaFailure' and x.get('status')=='True' and 'not authorized' in x.get('message','') for x in f.c.get('rs','legacy-controller',NS).get('status',{}).get('conditions',[])),description='controller reports denied recreation')
  f.observe(4);require(not replicas(),'revoked controller created replacement')
  (f.evidence/'replicaset.json').write_text(json.dumps(f.c.get('rs','legacy-controller',NS),indent=2))
 finally:
  f.c.k('delete','rs','legacy-controller','-n',NS,'--ignore-not-found','--wait=true')
  cleanup(f)
