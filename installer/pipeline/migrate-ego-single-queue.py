#!/usr/bin/env python3
"""Guarded three-node GitOps migration. No action is implicit; prepare is read-only."""
import argparse,copy,json,os,pathlib,subprocess,time
ROOT=pathlib.Path(__file__).resolve().parent
NS='infra-ego-pipeline'; SYS='volcano-system'; PIPE='ego-pipeline-prod'; QUEUE='infra-ego-pipeline-prod'
DEP='egov2-hawor-vo-grpc-prod'; WB='ego-hand-detect-prod'; CTL='pipeline-worker-controller-'+NS
PL='pipeline.simplesilicon.com/name'; WT='scheduling.simplesilicon.com/worker-type'
POOL='scheduling.simplesilicon.com/pool'; GROUP='volcano.sh/nodegroup-name'; API='scheduling.simplesilicon.com/v1alpha1'
NODES=['ws-0buj1s9hweo2h3z8c','ws-0buj1sdt3w21c5hso','ws-0buj1sdt3w21c5hsn']
MONITORS={0:'cpfs-usage-alert',2:'cpfs-usage-alert-5024'}
OLD=['egov2-hawor-vo-grpc-'+n for n in ['5022','5023','5024']]
def need(ok,msg):
    if not ok: raise ValueError(msg)
def merge_crd_additions(current, desired, path='/spec'):
    """Preserve cluster-specific fields; reject changes to existing schema semantics."""
    if isinstance(current,dict) and isinstance(desired,dict):
        out=copy.deepcopy(current)
        for key,value in desired.items():
            out[key]=merge_crd_additions(current[key],value,path+'/'+key) if key in current else copy.deepcopy(value)
        return out
    if isinstance(current,list) and isinstance(desired,list):
        key={'versions':'name','additionalPrinterColumns':'name','x-kubernetes-validations':'rule'}.get(path.rsplit('/',1)[-1])
        if key:
            need(all(isinstance(x,dict) and key in x for x in current+desired),'Unexpected CRD list: '+path)
            need(len({x[key] for x in current})==len(current) and len({x[key] for x in desired})==len(desired),'Duplicate CRD entries: '+path)
            out=copy.deepcopy(current)
            for value in desired:
                idx=next((i for i,x in enumerate(out) if x[key]==value[key]),None)
                if idx is None:
                    need(not path.endswith('/versions'),'New CRD version requires separate migration')
                    out.append(copy.deepcopy(value))
                else:out[idx]=merge_crd_additions(out[idx],value,path+'/'+value[key])
            return out
    need(current==desired,'Existing CRD field differs; inspect instead of overwriting: '+path)
    return copy.deepcopy(current)

def clean(o):
    o=copy.deepcopy(o);o.pop('status',None);o['metadata']={k:v for k,v in o['metadata'].items() if k in ['name','namespace','labels','annotations']};o['metadata'].get('annotations',{}).pop('kubectl.kubernetes.io/last-applied-configuration',None);return o
def gpu(pod):
    return sum(int(c.get('resources',{}).get('requests',{}).get('nvidia.com/gpu',c.get('resources',{}).get('limits',{}).get('nvidia.com/gpu',0))) for c in pod['spec']['containers'])
def normalized_template(d):
    t=copy.deepcopy(d['spec']['template']);t['metadata']['labels'].pop('app',None);t['spec']['nodeSelector'].pop('egov2.simplesilicon.com/ssh-port',None);return t

def worker_template(old):
    d=clean(old);d['metadata']={'name':DEP,'namespace':NS,'labels':{PL:PIPE,WT:'hand-detect'}}
    d['spec']['replicas']=0;d['spec']['strategy']={'type':'Recreate'};d['spec']['selector']={'matchLabels':{'app':DEP}}
    t=d['spec']['template'];t['metadata']['labels']={k:v for k,v in t['metadata'].get('labels',{}).items() if not k.startswith(('scheduling.simplesilicon.com/','pipeline.simplesilicon.com/'))};t['metadata']['labels']['app']=DEP
    t['metadata']['annotations']={k:v for k,v in t['metadata'].get('annotations',{}).items() if not k.startswith(('scheduling.','pipeline.simplesilicon.com/'))}
    ps=t['spec']
    for k in ['schedulerName','priorityClassName','priority','preemptionPolicy','nodeName']:ps.pop(k,None)
    ps['nodeSelector'].pop('egov2.simplesilicon.com/ssh-port',None);ps['nodeSelector'].pop(GROUP,None)
    terms=ps.get('affinity',{}).get('nodeAffinity',{}).get('requiredDuringSchedulingIgnoredDuringExecution',{}).get('nodeSelectorTerms',[])
    if not terms:
        terms=[{}];ps.setdefault('affinity',{}).setdefault('nodeAffinity',{})['requiredDuringSchedulingIgnoredDuringExecution']={'nodeSelectorTerms':terms}
    for term in terms:
        term['matchExpressions']=[e for e in term.get('matchExpressions',[]) if e['key'] not in [POOL,GROUP]]
    # Route admission injects the target node group. Do not pin workers to migration inventory.
    if any(not t.get('matchExpressions') and not t.get('matchFields') for t in terms):
        # An unconstrained OR branch makes this required selector unnecessary.
        ps['affinity']['nodeAffinity'].pop('requiredDuringSchedulingIgnoredDuringExecution')
        if not ps['affinity']['nodeAffinity']:ps['affinity'].pop('nodeAffinity')
        if not ps['affinity']:ps.pop('affinity')
    return d

class Run:
    def __init__(self,a):self.a=a;self.p=pathlib.Path(a.state)
    def k(self,*args,obj=None):
        r=subprocess.run(['kubectl','--context',self.a.context,*args],input=json.dumps(obj) if obj is not None else None,text=True,capture_output=True)
        need(r.returncode==0,r.stderr or r.stdout);return r.stdout
    def get(self,k,n=None,ns=None):return json.loads(self.k('get',k,*([n] if n else []),*(['-n',ns] if ns else []),'-o','json'))
    def optional(self,k,n,ns=None):
        s=self.k('get',k,n,*(['-n',ns] if ns else []),'--ignore-not-found','-o','json');return json.loads(s) if s.strip() else None
    def save(self,n,o):
        f=self.p/(n+'.json');tmp=f.with_suffix('.tmp');tmp.write_text(json.dumps(o,indent=2)+'\n');tmp.replace(f)
    def read(self,n):return json.loads((self.p/(n+'.json')).read_text())
    def wait(self,fn,msg):
        end=time.time()+360
        while time.time()<end:
            if fn():return
            time.sleep(3)
        raise ValueError('STOP timeout: '+msg+'; retain state and run rollback')
    def pods(self):return self.get('pods',ns=NS)['items']
    def apppods(self,name):return [p for p in self.pods() if p['metadata'].get('labels',{}).get('app')==name and p.get('status',{}).get('phase') not in ['Succeeded','Failed']]
    def resource(self,obj):return obj['kind'],obj['metadata']['name'],obj['metadata'].get('namespace')
    def set_spec(self,old,spec):
        kind,name,ns=self.resource(old)
        patch=[{'op':'test','path':'/metadata/uid','value':old['metadata']['uid']},{'op':'test','path':'/spec','value':old['spec']},{'op':'replace','path':'/spec','value':spec}]
        self.k('patch',kind,name,*(['-n',ns] if ns else []),'--type=json','-p',json.dumps(patch))
        return self.get(kind,name,ns)
    def prepare(self):
        need(not self.p.exists(),'Use a new state directory');self.p.mkdir(parents=True,mode=0o700)
        release=json.loads(pathlib.Path(self.a.release).read_text());need(all('@sha256:' in release[k] for k in ['scheduler','worker','webhook']),'Immutable release refs required')
        old=[self.get('deployment',n,NS) for n in OLD];nodes=[self.get('node',n) for n in NODES]
        need(all(d['spec']['replicas']==4 and gpu(d['spec']['template'])==1 for d in old),'Expected 3 x 4 one-GPU replicas')
        need(all(normalized_template(d)==normalized_template(old[0]) for d in old),'Business templates differ; do not consolidate')
        need(not self.get('services',ns=NS)['items'],'Review Service selectors before consolidation')
        need(not any(x['spec']['scaleTargetRef']['name'] in OLD for x in self.get('hpa',ns=NS)['items']),'HPA owns old worker')
        for i,d in enumerate(old):
            need(not any('argocd' in k or 'helm' in k for k in d['metadata'].get('annotations',{})),'Review GitOps ownership')
            ps=self.apppods(OLD[i]);need(len(ps)==4 and all(p['spec'].get('nodeName')==NODES[i] and not p['metadata'].get('deletionTimestamp') for p in ps),'Old workers not stable on expected node')
        for n in nodes:
            need(n['metadata']['labels'].get(GROUP)=='infra-ego-pipeline' and n['metadata']['labels'].get(POOL)=='pipeline-shared','Node already moved or labels changed')
            need(n['status']['allocatable'].get('nvidia.com/gpu')=='4' and not n['spec'].get('unschedulable',False),'Expected schedulable 4-GPU nodes')
        need(not self.optional('deployment',DEP,NS) and not self.optional('workerbudget',WB,NS) and not self.optional('queue',QUEUE),'New resources already exist')
        need(not self.optional('deployment',CTL,SYS),'Worker controller already exists; review ownership')
        for pc in ['ego-prod-worker','ego-prod-training']:need(not self.optional('priorityclass',pc),'PriorityClass already exists')
        protected=[x for x in self.get('deployments',ns=NS)['items'] if x['metadata']['name'] not in OLD]
        before=[self.get('workloadroutingpolicy','business-routing'),self.get('workloadroutingpolicy','arena-pytorch'),self.get('nodepoolpolicy',PIPE),self.get('pipelinepolicy',PIPE,NS)]
        need(before[-1]['spec'].get('allowedPreemptorNamespaces')==[],'Close new pipeline authorization first')
        monitors=[self.get('cronjob',n,NS) for n in MONITORS.values()]
        need(all(not self.optional('cronjob',m['metadata']['name']+'-prod',NS) for m in monitors),'Migrated monitor already exists')
        pgs=json.loads(self.k('get','podgroups.scheduling.volcano.sh','-A','-o','json'))
        need(not [g for g in pgs['items'] if g['spec'].get('queue') in ['infra-ego-pipeline-base-prod','infra-ego-pipeline-share-prod',QUEUE]],'Old prod queues have PodGroups; inspect before changing routes')
        system=[self.get('deployment',n,SYS) for n in ['workload-policy','volcano-infra-scheduler','ego-preemption-canary-budget']]
        self.save('baseline',{'context':self.a.context,'old':old,'monitors':monitors,'nodes':nodes,'protected':protected,'policies':before,'system':system,'release':release,'podUIDs':[p['metadata']['uid'] for p in self.pods()]})
        self.save('worker-reference-only',worker_template(old[0]));self.save('progress',{'completed':0,'started':0,'created':[],'configured':False,'upgraded':False})
        self.save('plan',{'queue':QUEUE,'pipeline':PIPE,'deployment':DEP,'workerType':'hand-detect','batches':[{'node':NODES[i],'old':OLD[i],'desiredGPU':4*(i+1),'minAvailableGPU':2*(i+1)} for i in range(3)],'monitors':MONITORS,'pdb':'none','preemptionAuthorization':[],'note':'Old Deployments retained at zero for rollback. Business image/env/volumes unchanged. Manual/API deletion is not covered by GPU floor.'})
        print('PREPARED: read-only snapshots; inspect plan.json and worker-reference-only.json; no production change')
    def check(self):
        b=self.read('baseline');need(b['context']==self.a.context,'Context differs');return b
    def protected(self):
        for old in self.check()['protected']:
            now=self.get('deployment',old['metadata']['name'],NS)
            need(now['metadata']['uid']==old['metadata']['uid'] and now['spec']==old['spec'],'Unrelated worker changed: '+old['metadata']['name'])
        pp=self.get('pipelinepolicy',PIPE,NS);need(pp['spec'].get('allowedPreemptorNamespaces')==[],'New pipeline preemption must remain disabled')
    def upgrade_crds(self):
        plans=[]
        for filename in ['budget-crds.json','workload-routing-crd.json']:
            manifest=json.loads((ROOT/filename).read_text())
            for desired in manifest.get('items',[manifest]):
                name=desired['metadata']['name'];old=self.get('customresourcedefinition',name)
                backup='crd-before-'+name
                if (self.p/(backup+'.json')).exists():
                    need(self.read(backup)['metadata']['uid']==old['metadata']['uid'],'CRD replaced since first upgrade attempt: '+name)
                else:self.save(backup,old)
                spec=merge_crd_additions(old['spec'],desired['spec'])
                if spec==old['spec']:
                    print('CRD unchanged:',name);continue
                patch=[{'op':'test','path':'/metadata/uid','value':old['metadata']['uid']},
                       {'op':'test','path':'/spec','value':old['spec']},
                       {'op':'replace','path':'/spec','value':spec}]
                plans.append({'name':name,'patch':patch})
        self.save('crd-upgrade-plan',plans)
        # Validate every patch first; no Deployment is touched if any CRD preflight fails.
        for plan in plans:
            self.k('patch','customresourcedefinition',plan['name'],'--type=json','-p',json.dumps(plan['patch']),'--dry-run=server','-o','name')
        for plan in plans:
            self.k('patch','customresourcedefinition',plan['name'],'--type=json','-p',json.dumps(plan['patch']))
            print('CRD additions applied:',plan['name'])
    def upgrade(self):
        b=self.check();s=self.read('progress');need(not s['upgraded'],'Already upgraded')
        self.upgrade_crds()
        for old,role in zip(b['system'],['webhook','scheduler','scheduler']):
            now=self.get('deployment',old['metadata']['name'],SYS)
            spec=copy.deepcopy(old['spec']);spec['template']['spec']['containers'][0]['image']=b['release'][role]
            need(now['metadata']['uid']==old['metadata']['uid'] and now['spec'] in [old['spec'],spec],'System Deployment drift')
            if now['spec']!=spec:self.set_spec(now,spec)
            self.k('-n',SYS,'rollout','status','deployment/'+now['metadata']['name'],'--timeout=180s')
        s['upgraded']=True;self.save('progress',s);print('UPGRADED: images and additive CRDs only; scheduler actions unchanged')
    def create_tracked(self,obj):
        kind,n,ns=self.resource(obj);need(not self.optional(kind,n,ns),'Already exists: '+kind+'/'+n)
        # Save intent before the request so an interrupted response can be reconciled.
        s=self.read('progress');s['created'].append({'kind':kind,'name':n,'namespace':ns,'intent':obj});self.save('progress',s)
        self.k('create','-f','-',obj=obj);now=self.get(kind,n,ns);s['created'][-1]['uid']=now['metadata']['uid'];self.save('progress',s);return now
    def owned(self,obj):
        entry=next((e for e in self.read('progress')['created'] if (e['kind'],e['name'],e['namespace'])==self.resource(obj)),None)
        need(entry and entry.get('uid')==obj['metadata']['uid'],'Unconfirmed resource ownership; inspect creation journal: '+str(self.resource(obj)))
    def configure(self):
        b=self.check();s=self.read('progress');need(s['upgraded'] and not s['configured'] and not s['created'],'Upgrade first; configure is once-only; partial configure requires rollback')
        self.protected()
        for pc,value in [('ego-prod-worker',1000),('ego-prod-training',100000)]:
            self.create_tracked({'apiVersion':'scheduling.k8s.io/v1','kind':'PriorityClass','metadata':{'name':pc},'value':value,'globalDefault':False})
        self.create_tracked({'apiVersion':'scheduling.volcano.sh/v1beta1','kind':'Queue','metadata':{'name':QUEUE},'spec':{'parent':'root','reclaimable':False,'deserved':{'nvidia.com/gpu':'4'},'capability':{'nvidia.com/gpu':'4'},'affinity':{'nodeGroupAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':[QUEUE]}}}})
        for old in b['policies']:
            now=self.get(*self.resource(old));need(now['metadata']['uid']==old['metadata']['uid'] and now['spec']==old['spec'],'Policy drift')
            spec=copy.deepcopy(now['spec']);kind=old['kind']
            if kind=='WorkloadRoutingPolicy' and old['metadata']['name']=='business-routing':
                spec['rules']=[r for r in spec['rules'] if r['name'] not in ['ego-prod-base','ego-prod-shared']]
                spec['rules'].append({'name':'ego-prod-worker','match':{'labels':{PL:PIPE,WT:'hand-detect'},'legacyBatch':False},'target':{'queue':QUEUE,'nodeGroup':QUEUE,'poolRef':PIPE,'priorityClassName':'ego-prod-worker','worker':{'pipeline':PIPE,'namespace':NS,'type':'hand-detect'}}})
                spec['rules'].append({'name':'ego-prod-maintenance','match':{'labels':{PL:PIPE,'pipeline.simplesilicon.com/component':'storage-monitor'},'legacyBatch':False},'target':{'queue':QUEUE,'nodeGroup':QUEUE,'poolRef':PIPE}})
            elif kind=='WorkloadRoutingPolicy':
                for r in spec['rules']:
                    if r['name']=='ego-prod-explicit':r['target'].update({'queue':QUEUE,'priorityClassName':'ego-prod-training'})
            elif kind=='NodePoolPolicy':spec['acceptedRoutes']=[{'policyRef':'business-routing','ruleNames':['ego-prod-worker','ego-prod-maintenance']},{'policyRef':'arena-pytorch','ruleNames':['ego-prod-explicit']}]
            elif kind=='PipelinePolicy':spec.update({'accountingQueues':[QUEUE],'preemptibleQueues':[QUEUE],'allowedPreemptorNamespaces':[]})
            updated=self.set_spec(now,spec)
            self.save('active-'+old['kind']+'-'+old['metadata']['name'],updated)
        s=self.read('progress');s['configured']=True;self.save('progress',s)
        print('CONFIGURED: platform policies only. Sync GitOps stage 0, then run attach. No worker mutation.')
    def attach(self):
        b=self.check();s=self.read('progress');need(s['configured'],'Configure first')
        app=self.get('application','infra-ego-pipeline','argocd')
        need('RespectIgnoreDifferences=true' in app['spec'].get('syncPolicy',{}).get('syncOptions',[]) and any(x.get('kind')=='Deployment' and x.get('name')==DEP and '/spec/replicas' in x.get('jsonPointers',[]) for x in app['spec'].get('ignoreDifferences',[])),'Configure Argo replica ignore before attaching controller')
        d=self.get('deployment',DEP,NS);need(d['spec']['replicas']==0,'GitOps must create new Deployment at zero')
        t=d['spec']['template'];need(t['spec'].get('schedulerName')=='volcano-infra' and t['metadata']['annotations'].get('scheduling.volcano.sh/queue-name')==QUEUE and t['spec'].get('priorityClassName')=='ego-prod-worker','GitOps worker routing is incorrect')
        need(gpu(t)==1,'One GPU per new replica required')
        need(any('argocd' in k for k in d['metadata'].get('annotations',{})) or 'argocd.argoproj.io/instance' in d['metadata'].get('labels',{}),'New Deployment is not tracked by Argo CD')
        s['gitopsWorkerUID']=d['metadata']['uid'];self.save('progress',s)
        self.create_tracked({'apiVersion':API,'kind':'WorkerBudget','metadata':{'name':WB,'namespace':NS},'spec':{'pipelineRef':{'name':PIPE},'workerType':'hand-detect','resourceName':'nvidia.com/gpu','desiredGPU':0,'minAvailableGPU':0,'workloadRef':{'kind':'Deployment','name':DEP,'uid':d['metadata']['uid']}}})
        rendered=json.loads(subprocess.check_output(['python3',str(ROOT/'render-worker-controller.py'),'--image',b['release']['worker'],'--worker-namespace',NS,'--image-pull-secret','acr-credential-secret-aggregation','--recovery-journal',SYS+'/ego-preemption-recovery'],text=True))
        for o in rendered['items']:
            if o['kind']=='Deployment':o['spec']['template']['spec']['nodeSelector']={GROUP:'general'}
            self.create_tracked(o)
        self.k('-n',SYS,'rollout','status','deployment/'+CTL,'--timeout=180s')
        s=self.read('progress');s['attached']=True;self.save('progress',s)
        print('ATTACHED: only the new UID-bound GitOps Deployment is managed; desiredGPU=0')
    def node_label(self,node,new):
        now=self.get('node',node);baseline=self.check()['nodes'][NODES.index(node)]
        need(now['metadata']['uid']==baseline['metadata']['uid'],'Node replaced')
        need(now['metadata']['labels'].get(GROUP) in ['infra-ego-pipeline',QUEUE] and now['metadata']['labels'].get(POOL) in ['pipeline-shared',QUEUE],'Node pool drift')
        patch=[{'op':'test','path':'/metadata/uid','value':now['metadata']['uid']},{'op':'test','path':'/metadata/labels','value':now['metadata']['labels']}]
        for key,value in [(GROUP,QUEUE if new else 'infra-ego-pipeline'),(POOL,QUEUE if new else 'pipeline-shared')]:patch.append({'op':'replace','path':'/metadata/labels/'+key.replace('/','~1'),'value':value})
        self.k('patch','node',node,'--type=json','-p',json.dumps(patch))
    def verify(self):
        self.protected();s=self.read('progress');n=s['completed'];need(s['started']==n,'Incomplete batch; inspect or rollback')
        total=0
        for i,name in enumerate(OLD):
            d=self.get('deployment',name,NS);expected=0 if i<n else 4;need(d['spec']['replicas']==expected,'Old scale drift');total+=expected
            original=self.check()['old'][i];spec=copy.deepcopy(d['spec']);spec['replicas']=4;need(spec==original['spec'] and d['metadata']['uid']==original['metadata']['uid'],'Old template/identity changed')
        new=self.get('deployment',DEP,NS);need(new['metadata']['uid']==s.get('gitopsWorkerUID'),'GitOps worker replaced');need(new['spec']['replicas']==n*4,'New replica count mismatch');need(total+new['spec']['replicas']==12,'GPU replica total must remain 12')
        pods=self.apppods(DEP);need(len(pods)==n*4 and all(p['spec'].get('nodeName') in NODES[:n] and not p['metadata'].get('deletionTimestamp') and p['spec'].get('schedulerName')=='volcano-infra' and p['metadata']['annotations'].get('scheduling.volcano.sh/queue-name')==QUEUE and any(c['type']=='Ready' and c['status']=='True' for c in p.get('status',{}).get('conditions',[])) for p in pods),'New workers not Ready/routed on migrated nodes')
        for i,node in enumerate(NODES):
            labels=self.get('node',node)['metadata']['labels'];need(labels[GROUP]==(QUEUE if i<n else 'infra-ego-pipeline'),'Node group mismatch')
        budget=self.get('workerbudget',WB,NS);need(budget['spec']['desiredGPU']==4*n and budget['spec']['minAvailableGPU']==2*n,'GPU budget mismatch')
        print(f'PASS batch {n}: old={12-4*n}, new={4*n}, total=12 GPU; minGPU={2*n}; other templates unchanged; no PDB created')
    def batch(self):
        b=self.check();s=self.read('progress');need(s['configured'],'Configure first');i=self.a.batch-1
        need(i==s['completed'] and s['started']==s['completed'],'Execute batches 1,2,3 once; incomplete batch requires rollback')
        self.protected();need(s.get('attached'),'Attach new GitOps Deployment first');need(i<3,'Invalid batch')
        # Git has already reduced ONLY this batch; this script never scales a GitOps Deployment.
        for j,name in enumerate(OLD):
            d=self.get('deployment',name,NS);expected=0 if j<=i else 4
            need(d['metadata']['uid']==b['old'][j]['metadata']['uid'] and d['spec']['replicas']==expected,'Sync the matching GitOps batch first')
            check=copy.deepcopy(d['spec']);check['replicas']=4;need(check==b['old'][j]['spec'],'GitOps sync changed business template; stop')
        need(self.get('deployment',DEP,NS)['spec']['replicas']==i*4,'Unexpected new worker count')
        if i in MONITORS:need(self.get('cronjob',MONITORS[i],NS)['spec'].get('suspend') is True,'Suspend old node monitor through Git first')
        s['started']=i+1;self.save('progress',s)
        self.k('cordon',NODES[i])
        self.wait(lambda:not self.apppods(OLD[i]),'old Pods including Terminating must disappear')
        # Re-check every live GPU Pod on this node, independent of scheduler/owner.
        allpods=json.loads(self.k('get','pods','-A','-o','json'))['items']
        need(not [p for p in allpods if p['spec'].get('nodeName')==NODES[i] and p.get('status',{}).get('phase') not in ['Succeeded','Failed'] and gpu(p)>0],'Unexpected live GPU Pod on drained node')
        self.node_label(NODES[i],True);self.k('uncordon',NODES[i])
        q=self.get('queue',QUEUE);qs=copy.deepcopy(q['spec']);qs['capability']['nvidia.com/gpu']=str(4*(i+1));qs['deserved']['nvidia.com/gpu']=str(4*(i+1));self.set_spec(q,qs)
        w=self.get('workerbudget',WB,NS);sp=copy.deepcopy(w['spec']);sp.update(desiredGPU=4*(i+1),minAvailableGPU=2*(i+1));self.set_spec(w,sp)
        self.wait(lambda:self.get('deployment',DEP,NS).get('status',{}).get('readyReplicas',0)==4*(i+1),'new desired replicas Ready')
        s=self.read('progress');s['completed']=i+1;self.save('progress',s);self.verify()
    def cutover(self):
        b=self.check();self.protected();s=self.read('progress')
        need(s.get('attached') and s['started']==0 and s['completed']==0,'Attach at stage zero first; partial cutover requires rollback')
        for old in b['old']:
            now=self.get('deployment',old['metadata']['name'],NS);spec=copy.deepcopy(old['spec']);spec['replicas']=0
            need(now['metadata']['uid']==old['metadata']['uid'] and now['spec']==spec,'Stop all three old workers through GitOps first')
        for name in MONITORS.values():
            need(self.get('cronjob',name,NS)['spec'].get('suspend') is True,'Suspend old monitors first')
            need(self.get('cronjob',name+'-prod',NS)['spec'].get('suspend') is True,'Keep new monitors suspended during cutover')
        new=self.get('deployment',DEP,NS);need(new['metadata']['uid']==s['gitopsWorkerUID'] and new['spec']['replicas']==0 and not self.apppods(DEP),'New worker must still be at zero')
        w=self.get('workerbudget',WB,NS);self.owned(w);need(w['spec']['desiredGPU']==0,'Budget must still be zero')
        for node in NODES:
            now=self.get('node',node);old=b['nodes'][NODES.index(node)]
            need(now['metadata']['uid']==old['metadata']['uid'] and not now['spec'].get('unschedulable') and now['status']['allocatable'].get('nvidia.com/gpu')=='4','Node identity/capacity/readiness changed')
            need(now['metadata']['labels'].get(GROUP)=='infra-ego-pipeline' and now['metadata']['labels'].get(POOL)=='pipeline-shared','Node already moved')
        s['started']=3;self.save('progress',s)
        for node in NODES:self.k('cordon',node)
        self.wait(lambda:all(not self.apppods(name) for name in OLD),'all old GPU Pods including Terminating must disappear')
        allpods=json.loads(self.k('get','pods','-A','-o','json'))['items']
        need(not [p for p in allpods if p['spec'].get('nodeName') in NODES and p.get('status',{}).get('phase') not in ['Succeeded','Failed'] and gpu(p)>0],'Unexpected live GPU Pods remain; do not start new workers')
        self.protected()
        for node in NODES:self.node_label(node,True)
        q=self.get('queue',QUEUE);qs=copy.deepcopy(q['spec']);qs['capability']['nvidia.com/gpu']='12';qs['deserved']['nvidia.com/gpu']='12';self.set_spec(q,qs)
        for node in NODES:self.k('uncordon',node)
        w=self.get('workerbudget',WB,NS);sp=copy.deepcopy(w['spec']);sp.update(desiredGPU=12,minAvailableGPU=6);self.set_spec(w,sp)
        self.wait(lambda:self.get('deployment',DEP,NS).get('status',{}).get('readyReplicas',0)==12,'12 new worker replicas Ready')
        s=self.read('progress');s['completed']=3;self.save('progress',s);self.verify()
    def stop(self):
        self.check();self.protected()
        ctl=self.optional('deployment',CTL,SYS)
        if ctl:
            self.owned(ctl);sp=copy.deepcopy(ctl['spec']);sp['replicas']=0;self.set_spec(ctl,sp)
            self.wait(lambda:not json.loads(self.k('get','pods','-n',SYS,'-l','app='+CTL,'-o','json'))['items'],'worker controller stopped')
        w=self.optional('workerbudget',WB,NS)
        if w:
            self.owned(w);sp=copy.deepcopy(w['spec']);sp.pop('desiredGPU',None);sp.pop('workloadRef',None);self.set_spec(w,sp)
        print('STOPPED: scaler off, desired binding removed. Now sync GitOps rollback-stop and prune ONLY new worker/monitor objects.')
    def rollback(self):
        b=self.check();self.protected();s=self.read('progress')
        need(not self.optional('deployment',DEP,NS) and not self.apppods(DEP),'Run stop, then delete new worker through GitOps before restoring nodes')
        ctl=self.optional('deployment',CTL,SYS);need(not ctl or ctl['spec']['replicas']==0,'Run stop first')
        for name in MONITORS.values():need(not self.optional('cronjob',name+'-prod',NS),'Prune new GitOps monitors first')
        # Restore routing before bringing back old controllers. No old controller templates are rewritten.
        for old in b['policies']:
            now=self.get(*self.resource(old));need(now['metadata']['uid']==old['metadata']['uid'],'Policy replaced; manual review required')
            active=self.p/('active-'+old['kind']+'-'+old['metadata']['name']+'.json')
            need(now['spec']==old['spec'] or (active.exists() and now['spec']==json.loads(active.read_text())['spec']),'Policy changed since migration; inspect before restoring');self.set_spec(now,old['spec'])
        for i in reversed(range(s['started'])):
            self.node_label(NODES[i],False);self.k('uncordon',NODES[i])
        for old in reversed(b['system']):
            now=self.get('deployment',old['metadata']['name'],SYS)
            need(now['metadata']['uid']==old['metadata']['uid'],'System Deployment replaced')
            expected=copy.deepcopy(old['spec']);role='webhook' if old['metadata']['name']=='workload-policy' else 'scheduler';expected['template']['spec']['containers'][0]['image']=b['release'][role]
            need(now['spec'] in [old['spec'],expected],'System Deployment drift; inspect before rollback')
            spec=copy.deepcopy(now['spec']);spec['template']['spec']['containers'][0]['image']=old['spec']['template']['spec']['containers'][0]['image'];self.set_spec(now,spec)
            self.k('-n',SYS,'rollout','status','deployment/'+old['metadata']['name'],'--timeout=180s')
        s['rolledBack']=True;self.save('progress',s);print('PLATFORM ROLLED BACK: original labels/routes/images restored. Now sync GitOps rollback-restore to restore old replicas. Additive CRDs/empty queue/RBAC retained.')
    def main(self):
        if self.a.action=='prepare':return self.prepare()
        self.check();need(not self.read('progress').get('rolledBack'),'State already rolled back; prepare a new state for another attempt')
        return getattr(self,self.a.action)()
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['prepare','upgrade','configure','attach','batch','cutover','verify','stop','rollback']);p.add_argument('--context',required=True);p.add_argument('--state',required=True);p.add_argument('--release');p.add_argument('--batch',type=int,choices=[1,2,3]);a=p.parse_args()
    if a.action=='prepare':need(a.release,'--release release.json required')
    if a.action=='batch':need(a.batch,'--batch 1/2/3 required')
    Run(a).main()
if __name__=='__main__':main()
