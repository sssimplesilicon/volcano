"""v7 tests require infrastructure-v7 (pipelineBudgets enabled). Synthetic GPUs."""
import json
from cases import case
from harness import NS, NODES, TYPE, require, wait

PIPE = 'pipeline.simplesilicon.com/name'
API = 'scheduling.simplesilicon.com/v1alpha1'

def policy(f, name='ego', floor=12, budget=True, duplicate=False):
    f.c.create({'apiVersion': API, 'kind': 'PipelinePolicy', 'metadata': {'name': name, 'namespace': NS},
                'spec': {'accountingQueues': ['wt-base', 'wt-shared'], 'preemptibleQueues': ['wt-shared']}})
    if budget:
        for suffix in (['', '-duplicate'] if duplicate else ['']):
            f.c.create({'apiVersion': API, 'kind': 'WorkerBudget', 'metadata': {'name': name+'-a'+suffix, 'namespace': NS},
                        'spec': {'pipelineRef': {'name': name}, 'workerType': 'A', 'resourceName': 'nvidia.com/gpu', 'minAvailableGPU': floor}})

def populate(f, other=False, unready=False):
    for slot in ['a', 'b']:
        pipeline = 'other' if slot == 'b' and other else 'ego'
        f.pod('base-'+slot, queue='wt-base', node='wt-node-'+slot, gpu=4, pipeline=pipeline, ready=not unready)
        f.pod('shared-'+slot, node='wt-node-'+slot, gpu=4, pipeline=pipeline)

def check_floor(f, minimum):
    for sample in f.timeline:
        # Ignore construction samples; observations after training creation only.
        if not any(p['metadata']['name'].startswith('training') for p in sample['pods']):
            continue
        ready = sum(int(c['resources']['requests']['nvidia.com/gpu']) for p in sample['pods']
                    if p['metadata'].get('labels', {}).get(PIPE) == 'ego'
                    and p['metadata'].get('labels', {}).get(TYPE) == 'A'
                    and not p['metadata'].get('deletionTimestamp')
                    and any(x['type']=='Ready' and x['status']=='True' for x in p.get('status',{}).get('conditions',[]))
                    for c in p['spec']['containers'])
        require(ready >= minimum, 'GPU floor breached: '+str(ready))

def cleanup_policy(f):
    for kind in ['workerbudgets.scheduling.simplesilicon.com','pipelinepolicies.scheduling.simplesilicon.com']:
        f.c.k('delete',kind,'-n',NS,'-l','test.simplesilicon.com/suite=preemption-e2e','--wait=true')

@case('P01,P02,P03,P04,P05,P06,P07', 'pipeline identity, GPU floor, indivisible victims and fail-closed policy')
def budgets(f, ident):
    try:
        policy(f, floor=12 if ident in ('P01','P02') else 8, budget=ident!='P04', duplicate=ident=='P07')
        if ident=='P03': policy(f,'other',0)
        populate(f, other=ident=='P03', unready=ident=='P05')
        if ident=='P06':
            f.c.patch('workerbudget','ego-a',{'spec':{'minAvailableGPU':14}},NS)
        f.train(gpu=4,pipeline='ego')
        if ident in ('P01','P02'):
            f.expect(1)
            if ident=='P02':
                f.train('training-two',gpu=4,pipeline='ego')
                f.observe()
                require(not f.bound('training-two') and len(f.deleted())==1,'second demand broke floor')
            check_floor(f,12)
            wait(lambda: f.c.get('workerbudget','ego-a',NS).get('status',{}).get('readyGPU')==12,description='observed CR status')
            (f.evidence/'budget-status.json').write_text(json.dumps(f.c.get('workerbudget','ego-a',NS),indent=2))
        else:
            f.observe()
            require(not f.deleted() and not f.bound(),'protected worker was evicted')
    finally:
        cleanup_policy(f)

@case('P08', 'durable GPU budget reservation survives timeout and scheduler restart')
def restart(f, _):
    try:
        policy(f);populate(f)
        f.c.fault('after',target='journal-write')
        f.train(gpu=4,pipeline='ego')
        wait(lambda:'FAULT_AFTER' in f.c.logs('fault-proxy',f.since))
        intent=f.c.journal()
        require(intent and all(x.get('budget',{}).get('budgetUID') for x in intent.values()),'budget UID not persisted')
        (f.evidence/'intent-before-restart.json').write_text(json.dumps(intent,indent=2))
        f.c.k('rollout','restart','deployment/worker-type-test','-n','volcano-system')
        f.c.k('rollout','status','deployment/worker-type-test','-n','volcano-system','--timeout=60s')
        f.expect(1)
        wait(lambda:not f.c.journal())
        check_floor(f,12)
    finally: cleanup_policy(f)

@case('P09', 'pipeline budget and native PDB both constrain eviction')
def pdb(f,_):
    try:
        policy(f,floor=0);populate(f);f.pdb(minimum=4)
        wait(lambda:f.c.get('pdb','type-a',NS).get('status',{}).get('currentHealthy')==4)
        f.train(gpu=4,pipeline='ego');f.observe()
        require(not f.deleted() and not f.bound(),'PDB bypassed')
    finally:cleanup_policy(f)

@case('P10', 'unlabelled demand cannot reclaim pipeline capacity')
def missing_identity(f,_):
    try:
        policy(f,floor=0);populate(f);f.train(gpu=4);f.observe()
        require(not f.deleted() and not f.bound(),'unscoped demand evicted pipeline worker')
    finally:cleanup_policy(f)

@case('P11', 'simultaneous demands cannot spend the same GPU floor budget twice')
def concurrent(f,_):
    import concurrent.futures
    try:
        policy(f);populate(f)
        for name in ['training-one','training-two']:
            f.train(name,gpu=4,pipeline='ego',gate=True)
        with concurrent.futures.ThreadPoolExecutor(2) as ex:
            list(ex.map(lambda name:f.c.patch('pod',name,{'spec':{'schedulingGates':[]}},NS),['training-one','training-two']))
        wait(lambda:len(f.deleted())==1 and sum(bool(f.bound(n)) for n in ['training-one','training-two'])==1)
        f.observe()
        require(len(f.deleted())==1,'concurrent demand breached floor')
        check_floor(f,12)
    finally:cleanup_policy(f)

@case('P12', 'same worker type in two pipelines has independent budgets and victim scope')
def two_pipelines(f,_):
    try:
        policy(f,floor=8);policy(f,'other',4);populate(f,other=True)
        f.train('training-ego',gpu=4,pipeline='ego',selector={'worker-type-test':'true','test.local/slot':'a'})
        f.train('training-other',gpu=4,pipeline='other',selector={'worker-type-test':'true','test.local/slot':'b'})
        wait(lambda:f.bound('training-other'))
        f.observe()
        require(f.deleted()=={'shared-b'} and not f.bound('training-ego'),'pipeline accounting or victim scope leaked')
    finally:cleanup_policy(f)

@case('P13,P14', 'in-flight recovery rechecks increased floor and replacement budget UID')
def changed_budget(f, ident):
    try:
        policy(f);populate(f)
        f.c.fault('before',repeat=1000)
        f.train(gpu=4,pipeline='ego')
        wait(lambda:'FAULT_BEFORE' in f.c.logs('fault-proxy',f.since))
        original=f.c.get('workerbudget','ego-a',NS)
        if ident=='P13':
            f.c.patch('workerbudget','ego-a',{'spec':{'minAvailableGPU':16}},NS)
        else:
            f.c.k('delete','workerbudget','ego-a','-n',NS,'--wait=true')
            f.c.create({'apiVersion':API,'kind':'WorkerBudget','metadata':{'name':'ego-a','namespace':NS},'spec':original['spec']})
            require(f.c.get('workerbudget','ego-a',NS)['metadata']['uid']!=original['metadata']['uid'],'UID did not change')
        f.observe(8)
        f.c.fault()
        f.observe(12)
        require(not f.deleted() and not f.bound(),'changed budget bypassed during recovery')
        require(f.c.journal(),'ambiguous intent was prematurely discarded')
        f.c.k('delete','pod','training','-n',NS,'--wait=false')
        wait(lambda:not f.c.journal(),description='canceled demand drains blocked intent')
    finally:cleanup_policy(f)

@case('P15', 'pure CPU demand remains unable to evict workers in pipeline budget mode')
def cpu_only(f,_):
    try:
        policy(f,floor=0)
        for slot in ['a','b']:
            f.pod('shared-'+slot,node='wt-node-'+slot,gpu=8,cpu='64',pipeline='ego')
        f.train(gpu=0,cpu='1',pipeline='ego')
        f.observe()
        require(not f.deleted() and not f.bound(),'CPU-only demand evicted worker')
    finally:cleanup_policy(f)

@case('P16', 'pipeline-specific shared queue does not require scheduler restart')
def custom_queue(f,_):
    from harness import OWNER
    try:
        f.c.create({'apiVersion':'scheduling.volcano.sh/v1beta1','kind':'Queue','metadata':{'name':'wt-shared-other'},'spec':{'weight':1,'reclaimable':False}})
        policy(f,floor=12)
        f.c.patch('pipelinepolicy','ego',{'spec':{'accountingQueues':['wt-base','wt-shared-other'],'preemptibleQueues':['wt-shared-other']}},NS)
        for slot in ['a','b']:
            f.pod('base-'+slot,queue='wt-base',node='wt-node-'+slot,gpu=4,pipeline='ego')
            f.pod('shared-'+slot,queue='wt-shared-other',node='wt-node-'+slot,gpu=4,pipeline='ego')
        f.train(gpu=4,pipeline='ego',queue='wt-shared-other')
        f.expect(1);check_floor(f,12)
    finally:cleanup_policy(f)

@case('P17', 'two pipelines concurrently enforce GPU floor and PDB independently, then resume')
def combined_concurrency(f, _):
    import concurrent.futures
    try:
        # Ego is constrained by GPU floor; other is constrained by PDB.
        policy(f, 'ego', 6)
        policy(f, 'other', 4)
        names = []
        for pipeline, slot, minimum in [('ego', 'a', 1), ('other', 'b', 2)]:
            node = 'wt-node-' + slot
            f.pod('base-' + slot, queue='wt-base', node=node, gpu=4, pipeline=pipeline)
            for i in range(2):
                f.pod('shared-' + slot + '-' + str(i), node=node, gpu=2, pipeline=pipeline)
            f.c.create({'apiVersion':'policy/v1','kind':'PodDisruptionBudget',
                        'metadata':{'name':'floor-' + pipeline,'namespace':NS},
                        'spec':{'minAvailable':minimum,'selector':{'matchLabels':{PIPE:pipeline,TYPE:'A'}}}})
            wait(lambda pipeline=pipeline:f.c.get('pdb','floor-'+pipeline,NS).get('status',{}).get('currentHealthy') == 3)
            for i in range(2):
                name='training-'+pipeline+'-'+str(i)
                names.append(name)
                f.train(name,gpu=2,pipeline=pipeline,gate=True,
                        selector={'worker-type-test':'true','test.local/slot':slot})
        concurrency_start = len(f.timeline)  # Both pipelines and PDBs are fully initialized.
        with concurrent.futures.ThreadPoolExecutor(4) as ex:
            list(ex.map(lambda n:f.c.patch('pod',n,{'spec':{'schedulingGates':[]}},NS),names))
        wait(lambda:len(f.deleted())==2 and sum(bool(f.bound(n)) for n in names)==2,
             description='one victim and one training per pipeline')
        f.observe()
        for pipeline,slot in [('ego','a'),('other','b')]:
            require(sum(n.startswith('shared-'+slot+'-') for n in f.deleted())==1,
                    'Pipeline victim budget interfered: '+pipeline)
            require(sum(bool(f.bound(n)) for n in names if n.startswith('training-'+pipeline+'-'))==1,
                    'Expected one pending demand per pipeline')
        def assert_samples(samples, floors):
            count=0
            for sample in samples:
                if not any(p['metadata']['name'].startswith('training-') for p in sample['pods']):continue
                count+=1
                for pipeline,(gpu_min,replica_min) in floors.items():
                    ps=[p for p in sample['pods'] if p['metadata'].get('labels',{}).get(PIPE)==pipeline
                        and p['metadata'].get('labels',{}).get(TYPE)=='A'
                        and not p['metadata'].get('deletionTimestamp')
                        and any(c['type']=='Ready' and c['status']=='True' for c in p.get('status',{}).get('conditions',[]))]
                    gpu=sum(int(c['resources']['requests']['nvidia.com/gpu']) for p in ps for c in p['spec']['containers'])
                    require(gpu>=gpu_min and len(ps)>=replica_min, 'Observed combined floor breach: '+pipeline)
            require(count>0,'No concurrent observations')
        assert_samples(f.timeline[concurrency_start:],{'ego':(6,1),'other':(4,2)})
        boundary=len(f.timeline)
        f.c.patch('workerbudget','ego-a',{'spec':{'minAvailableGPU':4}},NS)
        f.c.patch('pdb','floor-other',{'spec':{'minAvailable':1}},NS)
        wait(lambda:len(f.deleted())==4 and all(f.bound(n) for n in names),
             description='only newly permitted victims reclaimed')
        f.observe()
        assert_samples(f.timeline[boundary:],{'ego':(4,1),'other':(4,1)})
        require(f.deleted()=={'shared-a-0','shared-a-1','shared-b-0','shared-b-1'},'Base or unrelated victim changed')
        wait(lambda:not f.c.journal())
    finally:cleanup_policy(f)
