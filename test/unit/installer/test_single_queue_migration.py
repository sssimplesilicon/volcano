import copy,importlib.util,json,pathlib,tempfile,types,unittest
from unittest.mock import patch
FILE=pathlib.Path(__file__).resolve().parents[3]/'installer/pipeline/migrate-ego-single-queue.py'
spec=importlib.util.spec_from_file_location('migration',FILE);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def obj(kind,name,spec=None,ns=None):return {'apiVersion':'v1','kind':kind,'metadata':{'name':name,'uid':name+'-uid',**({'namespace':ns} if ns else {})},'spec':spec or {}}
def deployment(name,port):
 ps={'schedulerName':'volcano-infra','nodeSelector':{'egov2.simplesilicon.com/ssh-port':port,m.GROUP:'infra-ego-pipeline','storage':'yes'},'containers':[{'name':'main','image':'test','env':[{'name':'BUSINESS','value':'unchanged'}],'resources':{'requests':{'nvidia.com/gpu':'1'}}}]}
 ps['affinity']={'nodeAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':{'nodeSelectorTerms':[{'matchExpressions':[{'key':m.POOL,'operator':'In','values':['pipeline-shared']}]}]}}}
 template={'metadata':{'labels':{'app':name,m.WT:'hand-detect','scheduling.simplesilicon.com/route':'base'},'annotations':{'scheduling.simplesilicon.com/workload-decision':'frozen','scheduling.volcano.sh/queue-name':'old'}},'spec':ps}
 return obj('Deployment',name,{'replicas':4,'strategy':{'type':'RollingUpdate'},'selector':{'matchLabels':{'app':name}},'template':template},m.NS)
class Fake(m.Run):
 def __init__(self,a):super().__init__(a);self.db={};self.calls=[];self.maxgpu=0
 def key(self,k,n,ns):
  aliases={'deployments':'deployment','pods':'pod','nodes':'node','workloadroutingpolicies':'workloadroutingpolicy'}
  return aliases.get(k.lower(),k.lower()),n,ns
 def put(self,o):self.db[self.key(*self.resource(o))]=copy.deepcopy(o);return o
 def get(self,k,n=None,ns=None):
  if n is None:
   if k=='pods':return {'items':self.pods()}
   return {'items':[copy.deepcopy(v) for (kind,_,space),v in self.db.items() if kind==self.key(k,'',ns)[0] and (ns is None or ns==space)]}
  return copy.deepcopy(self.db[self.key(k,n,ns)])
 def optional(self,k,n,ns=None):return copy.deepcopy(self.db.get(self.key(k,n,ns)))
 def pods(self):
  out=[]
  for name in m.OLD+[m.DEP]:
   d=self.optional('deployment',name,m.NS)
   if not d:continue
   for i in range(d['spec']['replicas']):
    node=m.NODES[m.OLD.index(name)] if name in m.OLD else m.NODES[i//4]
    out.append({'metadata':{'name':name+'-'+str(i),'uid':name+'-'+str(i),'labels':{'app':name},'annotations':{'scheduling.volcano.sh/queue-name':m.QUEUE}},'spec':{'containers':d['spec']['template']['spec']['containers'],'schedulerName':'volcano-infra','nodeName':node},'status':{'phase':'Running','conditions':[{'type':'Ready','status':'True'}]}})
  return out
 def wait(self,fn,msg):m.need(fn(),msg)
 def account(self):
  total=sum(d['spec']['replicas'] for (k,n,ns),d in self.db.items() if k=='deployment' and n in m.OLD+[m.DEP]);self.maxgpu=max(self.maxgpu,total);m.need(total<=12,'oversubscribed migration')
 def k(self,*args,obj=None):
  self.calls.append(args)
  if args[0] in ['apply','cordon','uncordon'] or 'rollout' in args:return ''
  if args[:2]==('get','pods'):return json.dumps({'items':[] if '-l' in args else self.pods()})
  if args[:2]==('get','podgroups.scheduling.volcano.sh'):return '{"items":[]}'
  if args[0]=='create':
   o=copy.deepcopy(obj)
   if o['kind']=='Deployment' and o['metadata']['name']==m.DEP:
    t=o['spec']['template'];t['spec'].update(schedulerName='volcano-infra',priorityClassName='ego-prod-worker');t['metadata'].setdefault('annotations',{})['scheduling.volcano.sh/queue-name']=m.QUEUE
   if '--dry-run=server' in args:return json.dumps(o)
   o['metadata']['uid']=o['metadata']['name']+'-created';self.put(o);self.account();return json.dumps(o)
  if args[0]=='patch':
   kind,name=args[1:3]
   if kind.lower()=='deployment' and name in m.OLD+[m.DEP]:raise AssertionError('Migration script must not mutate GitOps Deployment')
   ns=args[args.index('-n')+1] if '-n' in args else None;o=self.get(kind,name,ns)
   for op in json.loads(args[args.index('-p')+1]):
    path=op['path'].strip('/').split('/');p=o
    for field in path[:-1]:p=p[field.replace('~1','/')]
    last=path[-1].replace('~1','/')
    if op['op']=='test':m.need(p[last]==op['value'],'test conflict')
    else:p[last]=op['value']
   if kind.lower()=='workerbudget' and 'desiredGPU' in o['spec']:
    d=self.get('deployment',m.DEP,m.NS);d['spec']['replicas']=o['spec']['desiredGPU'];d['status']={'readyReplicas':o['spec']['desiredGPU']};self.put(d)
   self.put(o);self.account();return json.dumps(o)
  raise AssertionError(args)
class MigrationTests(unittest.TestCase):
 def setup_run(self,td):
  a=types.SimpleNamespace(state=str(pathlib.Path(td)/'state'),context='fake-only',release=str(pathlib.Path(td)/'release.json'),batch=None)
  pathlib.Path(a.release).write_text(json.dumps({k:'registry/'+k+'@sha256:abc' for k in ['scheduler','webhook','worker']}));r=Fake(a)
  for i,n in enumerate(m.OLD):r.put(deployment(n,str(5022+i)))
  for n in m.NODES:
   o=obj('Node',n);o['metadata']['labels']={m.GROUP:'infra-ego-pipeline',m.POOL:'pipeline-shared'};o['status']={'allocatable':{'nvidia.com/gpu':'4'}};r.put(o)
  for name in ['business-routing','arena-pytorch']:r.put(obj('WorkloadRoutingPolicy',name,{'rules':[{'name':'ego-prod-explicit','target':{'queue':'old'}}] if name=='arena-pytorch' else []}))
  r.put(obj('NodePoolPolicy',m.PIPE,{'acceptedRoutes':[]}));r.put(obj('PipelinePolicy',m.PIPE,{'allowedPreemptorNamespaces':[]},m.NS))
  for n in m.MONITORS.values():r.put(obj('CronJob',n,{'suspend':False,'jobTemplate':{'spec':{'template':{'metadata':{'annotations':{'scheduling.simplesilicon.com/workload-decision':'frozen'}},'spec':{'nodeSelector':{m.GROUP:'infra-ego-pipeline'},'containers':[{'name':'check','image':'test'}]}}}}},m.NS))
  for n in ['workload-policy','volcano-infra-scheduler','ego-preemption-canary-budget']:r.put(obj('Deployment',n,{'replicas':1,'template':{'spec':{'containers':[{'image':'original'}]}}},m.SYS))
  r.prepare();return r
 def attach_gitops(self,r):
  r.put(obj('Application','infra-ego-pipeline',{'syncPolicy':{'syncOptions':['RespectIgnoreDifferences=true']},'ignoreDifferences':[{'kind':'Deployment','name':m.DEP,'jsonPointers':['/spec/replicas']}]},'argocd'))
  d=r.read('worker-reference-only');d['metadata']['annotations']={'argocd.argoproj.io/tracking-id':'infra-ego-pipeline:apps/Deployment:infra-ego-pipeline/'+m.DEP};r.k('create',obj=d);r.attach()
  for name in m.MONITORS.values():
   o=r.get('cronjob',name,m.NS);o['metadata']['name']=name+'-prod';o['metadata']['uid']=name+'-prod';o['spec']['suspend']=True;r.put(o)
 def gitops_batch(self,r,n):
  d=r.get('deployment',m.OLD[n-1],m.NS);d['spec']['replicas']=0;r.put(d);r.account()
  if n-1 in m.MONITORS:
   o=r.get('cronjob',m.MONITORS[n-1],m.NS);o['spec']['suspend']=True;r.put(o)
 def gitops_rollback(self,r):
  r.stop()
  for key in list(r.db):
   if key[1]==m.DEP or key[0]=='cronjob' and key[1].endswith('-prod'):del r.db[key]
  r.rollback()
  for name in m.OLD:
   d=r.get('deployment',name,m.NS);d['spec']['replicas']=4;r.put(d);r.account()
 def test_clone_preserves_business_and_removes_old_identity(self):
  old=deployment(m.OLD[0],'5022');new=m.worker_template(old)
  self.assertEqual(old['spec']['template']['spec']['containers'],new['spec']['template']['spec']['containers'])
  self.assertEqual(new['spec']['replicas'],0);self.assertNotIn('schedulerName',new['spec']['template']['spec']);self.assertEqual(new['metadata']['labels'][m.WT],'hand-detect')
  self.assertNotIn('scheduling.simplesilicon.com/route',new['spec']['template']['metadata']['labels']);self.assertEqual(new['spec']['template']['spec']['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchFields'][0]['values'],m.NODES)
 def test_three_batches_and_rollback_never_exceed_twelve(self):
  with tempfile.TemporaryDirectory() as td:
   r=self.setup_run(td);r.upgrade();r.configure();self.attach_gitops(r)
   for n in [1,2,3]:self.gitops_batch(r,n);r.a.batch=n;r.batch()
   self.assertEqual(r.get('workerbudget',m.WB,m.NS)['spec']['minAvailableGPU'],6)
   self.gitops_rollback(r);self.assertEqual(r.maxgpu,12)
   for n in m.OLD:self.assertEqual(r.get('deployment',n,m.NS)['spec']['replicas'],4)
   self.assertIsNone(r.optional('deployment',m.DEP,m.NS))
 def test_failure_after_drain_rolls_back_without_increase(self):
  with tempfile.TemporaryDirectory() as td:
   r=self.setup_run(td);r.upgrade();r.configure();self.attach_gitops(r);self.gitops_batch(r,1);r.a.batch=1
   original=r.node_label
   with patch.object(r,'node_label',side_effect=ValueError('injected node patch failure')):
    with self.assertRaisesRegex(ValueError,'injected'):r.batch()
   self.gitops_rollback(r);self.assertEqual(r.maxgpu,12);self.assertEqual(r.get('deployment',m.OLD[0],m.NS)['spec']['replicas'],4)
 def test_different_business_template_refused(self):
  a=deployment(m.OLD[0],'5022');b=deployment(m.OLD[1],'5023')
  self.assertEqual(m.normalized_template(a),m.normalized_template(b));b['spec']['template']['spec']['containers'][0]['image']='different';self.assertNotEqual(m.normalized_template(a),m.normalized_template(b))
 def test_foreign_uid_refused(self):
  with tempfile.TemporaryDirectory() as td:
   r=self.setup_run(td);r.upgrade();r.configure();self.attach_gitops(r);d=r.get('deployment',m.DEP,m.NS);d['metadata']['uid']='foreign';r.put(d)
   with self.assertRaisesRegex(ValueError,'replaced'):r.verify()
if __name__=='__main__':unittest.main()
