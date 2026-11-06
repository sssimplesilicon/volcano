"""Local synthetic-node regression: stuck eviction must not freeze other pipelines."""
import json
from cases import case
from harness import NS, NODES, require, wait
from pipeline_cases import policy, cleanup_policy

@case('P42,P43', 'terminating residue isolation, terminal reconciliation and restart')
def residue(f, ident):
    f.finish_deletions = False
    try:
        policy(f, 'ego', 0)
        policy(f, 'other', 0)
        for i, pipe in enumerate(['ego', 'other']):
            f.pod('base-'+str(i),typ='BASE',queue='wt-base',node=NODES[i],gpu=4)
            f.pod('worker-'+str(i),node=NODES[i],gpu=4,pipeline=pipe)
        f.train('training',gpu=4,pipeline='ego',selector={'test.local/slot':'a'})
        wait(lambda: f.c.get('pod','worker-0',NS)['metadata'].get('deletionTimestamp'),description='first accepted eviction')
        old=f.c.get('pod','worker-0',NS)
        require(old['metadata']['uid'] in f.c.journal(),'missing original intent')
        if ident=='P43':
            f.c.k('rollout','restart','deployment/worker-type-test','-n','volcano-system')
            f.c.k('rollout','status','deployment/worker-type-test','-n','volcano-system','--timeout=120s')
        f.train('training-other',gpu=4,pipeline='other',selector={'test.local/slot':'b'})
        wait(lambda: f.c.get('pod','worker-1',NS)['metadata'].get('deletionTimestamp'),description='unrelated pipeline eviction while original remains')
        require(old['metadata']['uid'] in f.c.journal(),'original record silently dropped')
        f.c.complete_deletion(f.c.get('pod','worker-1',NS))
        wait(lambda: f.bound('training-other'))
        require(not f.bound(),'allocated over nonterminal residue')
        (f.evidence/'isolated-journal.json').write_text(json.dumps(f.c.journal(),indent=2))
        f.c.patch('pod','worker-0',{'status':{'phase':'Succeeded','conditions':[{'type':'Ready','status':'False'}], 'containerStatuses':[{'name':'worker','image':'busybox:1.36','imageID':'synthetic','ready':False,'restartCount':0,'state':{'terminated':{'exitCode':0,'reason':'Completed'}}}]}},NS,True)
        wait(lambda: old['metadata']['uid'] not in f.c.journal(),description='terminal residue reconciled without delete')
        require(f.c.get('pod','worker-0',NS)['metadata']['uid']==old['metadata']['uid'],'residue was deleted')
        wait(lambda: f.bound())
        require(all(f.c.get('pod','base-'+str(i),NS)['metadata']['uid']==f.initial['base-'+str(i)]['metadata']['uid'] for i in range(2)),'base changed')
    finally:
        f.finish_deletions=True
        cleanup_policy(f)
