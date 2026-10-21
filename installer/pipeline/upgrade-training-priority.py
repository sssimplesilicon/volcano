#!/usr/bin/env python3
"""Operator-run webhook upgrade and single Pending PyTorchJob priority repair.
prepare/verify use GET only. No scheduler, worker, routing, authorization or CRD writes.
"""
import argparse,copy,json,pathlib,subprocess,time
p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','upgrade','repair','verify','rollback-image']);p.add_argument('--context',required=True);p.add_argument('--state',required=True);p.add_argument('--image');p.add_argument('--job',default='ego-prod-preempt-210446');a=p.parse_args();state=pathlib.Path(a.state)
NS='infra-group';SYS='volcano-system';WNS='infra-ego-pipeline';DEP='workload-policy';PRIORITY='ego-prod-training';QUEUE='infra-ego-pipeline-prod';DKEY='scheduling.simplesilicon.com/workload-decision'
def need(ok,msg):
 if not ok:raise RuntimeError(msg)
def k(*args):
 r=subprocess.run(['kubectl','--context',a.context,'--request-timeout=30s',*args],capture_output=True,text=True);need(r.returncode==0,r.stderr or r.stdout);return r.stdout
def get(kind,name=None,ns=None):return json.loads(k('get',kind,*([name] if name else []),*(['-n',ns] if ns else []),'-o','json'))
def save(name,o):(state/(name+'.json')).write_text(json.dumps(o,indent=2)+'\n')
def load(name):return json.loads((state/(name+'.json')).read_text())
def patch(kind,name,ns,ops):
 for dry in [True,False]:
  print(k('patch',kind,name,'-n',ns,'--type=json','-p',json.dumps(ops),*(['--dry-run=server'] if dry else []),'-o','name').strip())
def closed():
 for o in json.loads(k('get','pipelinepolicies.scheduling.simplesilicon.com','-A','-o','json'))['items']:
  need(o['spec'].get('allowedPreemptorNamespaces')==[],'Close preemption authorization first: '+o['metadata']['name'])
 j=get('configmap','ego-preemption-recovery',SYS);need(not json.loads(j.get('data',{}).get('intents.json','{}')),'Wait for recovery journal to drain; never clear it')
def pod(job):
 ps=get('pods',ns=NS)['items'];ps=[x for x in ps if any(o.get('uid')==job['metadata']['uid'] and o.get('kind')=='PyTorchJob' for o in x['metadata'].get('ownerReferences',[]))]
 need(len(ps)==1,'Expected exactly one test Pod');return ps[0]
def pending(o):need(not o['spec'].get('nodeName') and o['status']['phase']=='Pending' and not o['metadata'].get('deletionTimestamp'),'Test Pod must still be unbound Pending')
def current_image(d):return next(c['image'] for c in d['spec']['template']['spec']['containers'] if c['name']=='webhook')
def check_worker(b):
 now={o['metadata']['uid']:o for o in get('pods',ns=WNS)['items']};gone=[]
 for o in b['workerPods']['items']:
  x=now.get(o['metadata']['uid'])
  if not x:gone.append(o['metadata']['name']);continue
  need(x['spec']==o['spec'],'Existing worker Pod spec changed: '+o['metadata']['name'])
 if gone:print('NOTICE baseline Pods disappeared (cause not inferred): '+','.join(gone))
 for old in b['workerDeployments']['items']:
  cur=get('deployment',old['metadata']['name'],WNS);need(cur['metadata']['uid']==old['metadata']['uid'] and cur['spec']==old['spec'],'Worker Deployment changed: '+old['metadata']['name'])
 for kind,key in [('workerbudget','budget'),('deployment','scheduler')]:
  old=b[key];cur=get(kind,old['metadata']['name'],old['metadata']['namespace']);need(cur['metadata']['uid']==old['metadata']['uid'] and cur['spec']==old['spec'],'Protected '+key+' changed')
 for old in b['schedulerConfigs']:
  cur=get('configmap',old['metadata']['name'],SYS);need(cur['metadata']['uid']==old['metadata']['uid'] and cur.get('data')==old.get('data'),'Scheduler configuration changed')
 print('PASS worker specs, GPU budget and scheduler configuration unchanged')
if a.action=='prepare':
 need(a.image and '@sha256:' in a.image,'Use released image digest');need(not state.exists(),'Use a fresh state directory');closed()
 job=get('pytorchjobs.kubeflow.org',a.job,NS);po=pod(job);pending(po)
 decision=json.loads(job['metadata'].get('annotations',{}).get(DKEY,'{}'))
 need(decision.get('target',{}).get('priorityClassName')==PRIORITY and decision['target'].get('queue')==QUEUE,'Unexpected frozen route')
 need(po['spec'].get('priorityClassName')==PRIORITY and po['spec'].get('priority')==100000,'Unexpected test Pod priority')
 need(sum(int(c.get('resources',{}).get('requests',{}).get('nvidia.com/gpu',0)) for c in po['spec']['containers'])==1,'Expected one GPU')
 pg=get('podgroups.scheduling.volcano.sh',a.job,NS);need(any(x.get('uid')==job['metadata']['uid'] for x in pg['metadata'].get('ownerReferences',[])) and pg['spec']['queue']==QUEUE,'Unexpected PodGroup owner/queue')
 budget=get('workerbudget','ego-hand-detect-prod',WNS);need(budget['spec']['desiredGPU']==12 and budget['spec']['minAvailableGPU']==6,'Budget must remain 12/6')
 pc=get('priorityclass',PRIORITY);need(pc['value']==100000,'Training PriorityClass value changed')
 scheduler=get('deployment','volcano-infra-scheduler',SYS)
 configs=[get('configmap',v['configMap']['name'],SYS) for v in scheduler['spec']['template']['spec'].get('volumes',[]) if 'configMap' in v]
 state.mkdir(parents=True,mode=0o700)
 save('baseline',{'context':a.context,'image':a.image,'job':job,'pod':po,'pg':pg,'webhook':get('deployment',DEP,SYS),'scheduler':scheduler,'schedulerConfigs':configs,'budget':budget,'workerPods':get('pods',ns=WNS),'workerDeployments':get('deployments',ns=WNS)})
 print('PREPARED read-only snapshots; no production changes')
else:
 b=load('baseline');need(a.context==b['context'],'Context mismatch');closed();check_worker(b)
 dep=get('deployment',DEP,SYS);need(dep['metadata']['uid']==b['webhook']['metadata']['uid'],'Webhook Deployment replaced')
 if a.action in ['upgrade','rollback-image']:
  old=current_image(b['webhook']);target=b['image'] if a.action=='upgrade' else old
  need(current_image(dep) in [old,b['image']],'Unexpected current image')
  if current_image(dep)!=target:
   index=next(i for i,c in enumerate(dep['spec']['template']['spec']['containers']) if c['name']=='webhook')
   patch('deployment',DEP,SYS,[{'op':'test','path':'/metadata/uid','value':dep['metadata']['uid']},{'op':'test','path':'/spec/template','value':dep['spec']['template']},{'op':'replace','path':f'/spec/template/spec/containers/{index}/image','value':target}])
  print(k('-n',SYS,'rollout','status','deployment/'+DEP,'--timeout=180s'))
  if a.action=='rollback-image':print('Webhook image restored. Corrected task-group priority is retained; authorization stays closed.')
 else:
  need(current_image(dep)==b['image'] and dep['status'].get('observedGeneration',0)>=dep['metadata']['generation'] and dep['status'].get('updatedReplicas',0)==dep['spec']['replicas'] and dep['status'].get('availableReplicas',0)==dep['spec']['replicas'],'New webhook rollout not complete')
  job=get('pytorchjobs.kubeflow.org',b['job']['metadata']['name'],NS);need(job['metadata']['uid']==b['job']['metadata']['uid'],'Job replaced')
  po=pod(job);pending(po);need(po['metadata']['uid']==b['pod']['metadata']['uid'] and po['spec']==b['pod']['spec'],'Test Pod changed/recreated')
  expected=copy.deepcopy(b['job']['spec']);expected['runPolicy']['schedulingPolicy']['priorityClass']=PRIORITY
  need(job['spec'] in [b['job']['spec'],expected],'Unexpected Job spec change')
  if a.action=='repair' and job['spec']!=expected:
   patch('pytorchjobs.kubeflow.org',job['metadata']['name'],NS,[{'op':'test','path':'/metadata/uid','value':job['metadata']['uid']},{'op':'test','path':'/spec','value':job['spec']},{'op':'add','path':'/spec/runPolicy/schedulingPolicy/priorityClass','value':PRIORITY}])
  pg=get('podgroups.scheduling.volcano.sh',job['metadata']['name'],NS);need(pg['metadata']['uid']==b['pg']['metadata']['uid'],'PodGroup replaced')
  oldspec=copy.deepcopy(b['pg']['spec']);newspec=copy.deepcopy(oldspec);newspec['priorityClassName']=PRIORITY
  need(pg['spec'] in [oldspec,newspec],'Unexpected PodGroup spec change')
  if a.action=='repair' and pg['spec']!=newspec:
   patch('podgroups.scheduling.volcano.sh',pg['metadata']['name'],NS,[{'op':'test','path':'/metadata/uid','value':pg['metadata']['uid']},{'op':'test','path':'/spec','value':pg['spec']},{'op':'add','path':'/spec/priorityClassName','value':PRIORITY}])
  for _ in range(3):
   job=get('pytorchjobs.kubeflow.org',job['metadata']['name'],NS);pg=get('podgroups.scheduling.volcano.sh',pg['metadata']['name'],NS)
   need(job['spec']==expected and pg['spec']==newspec,'Priority repair not converged');time.sleep(2)
  closed();check_worker(b)
  print('PASS PyTorchJob and PodGroup priority='+PRIORITY+'; existing Pending Pod UID/spec unchanged; authorization closed')
