#!/usr/bin/env python3
"""Compare stock/default-off behavior in the fixed local k3s; never installs CRDs."""
import argparse, json, pathlib, traceback
from harness import Cluster, Fixture, NODES, NS, require, wait
from cases import full


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute-local',action='store_true')
    p.add_argument('--allow-existing-budget-crds',action='store_true',help='Preserve installed CRDs; compare disabled behavior without deleting them')
    p.add_argument('--output',type=pathlib.Path,required=True)
    a=p.parse_args()
    if not a.execute_local:p.error('--execute-local required')
    c=Cluster()
    cfg=json.loads(c.k('config','view','--minify','-o','json'))
    require(cfg['clusters'][0]['cluster']['server']=='https://127.0.0.1:16473','Wrong endpoint')
    if not a.allow_existing_budget_crds:
        require(not c.k('get','crd','pipelinepolicies.scheduling.simplesilicon.com','workerbudgets.scheduling.simplesilicon.com','--ignore-not-found').strip(),'Run default-off tests before installing custom CRDs, or explicitly preserve them')
    require(not c.get('pods',ns=NS)['items'],'Fixture namespace is not empty')
    require(not {n['metadata']['name'] for n in c.get('nodes')['items']}&set(NODES),'Synthetic nodes already exist')
    a.output.mkdir(parents=True,exist_ok=False)
    old=c.get('deploy','volcano-scheduler','volcano-system')
    (a.output/'old-scheduler-before.json').write_text(json.dumps(old,indent=2))
    spec=c.get('deploy','worker-type-test','volcano-system')['spec']['template']['spec']
    require(len(spec['containers'])==2,'Default-off fixture must not run budget controller')
    scheduler=next(x for x in spec['containers'] if x['name']=='scheduler')
    require('--scheduler-name=worker-type-test' in scheduler['args'],'Wrong scheduler scope')
    require(not any(x.startswith(('--plugins-dir','--scheduler-conf','--enable-worker-type')) for x in scheduler['args']),'Not an upstream default configuration')
    args=list(scheduler['args']);rows=[]
    for variant,image,extra in [('official','docker.io/volcanosh/vc-scheduler:v1.15.3',[]),('static-default','volcano-ss:v1.15.3-ss.1-local',[]),('static-false','volcano-ss:v1.15.3-ss.1-local',['--enable-worker-type-preemption=false'])]:
        scheduler['image']=image;scheduler['args']=args+extra
        c.patch('deploy','worker-type-test',{'spec':{'template':{'spec':{'containers':spec['containers']}}}},'volcano-system')
        c.k('rollout','status','deployment/worker-type-test','-n','volcano-system','--timeout=60s')
        for ident in ['free-capacity','full-no-eviction','selector-mismatch']:
            f=Fixture(c,a.output/(variant+'-'+ident));row={'variant':variant,'id':ident,'status':'PASS'}
            try:
                wait(lambda:not c.get('pods',ns=NS)['items'])
                f.setup()
                if ident=='free-capacity':
                    f.pod('base-a',queue='wt-base',gpu=4);f.train();f.expect(0)
                elif ident=='full-no-eviction':
                    full(f);f.train();f.observe(10);require(not f.bound() and not f.deleted(),'Default mode unexpectedly preempted')
                else:
                    f.train(selector={'test.local/slot':'missing'});f.observe(8);require(not f.bound() and not f.deleted(),'Selector ignored')
                log=c.logs('scheduler',f.since)
                require('Static worker-type preemption extension enabled' not in log,'Extension activated by default')
                require('PIPELINE_BUDGET' not in log and 'PDB_GUARD' not in log,'Custom code ran while disabled')
                require('EVICTION_REQUEST' not in c.logs('fault-proxy',f.since),'Unexpected eviction')
                require(not c.journal(),'Custom journal changed while disabled')
                f.protected()
                row.update(bound=bool(f.bound()),victims=len(f.deleted()))
            except Exception as e:row.update(status='FAIL',detail=str(e),traceback=traceback.format_exc())
            finally:f.finish()
            rows.append(row);(a.output/'results.json').write_text(json.dumps(rows,indent=2));print(variant,ident,row['status'],flush=True)
            require(row['status']=='PASS',row.get('detail','failed'))
    for ident in ['free-capacity','full-no-eviction','selector-mismatch']:
        outcomes={(x['bound'],x['victims']) for x in rows if x['id']==ident};require(len(outcomes)==1,'Official/static behavior mismatch')
    new=c.get('deploy','volcano-scheduler','volcano-system');require(old['metadata']['uid']==new['metadata']['uid'] and old['spec']==new['spec'],'Original scheduler changed')
    (a.output/'old-scheduler-after.json').write_text(json.dumps(new,indent=2))
    print('9 default-off checks PASS; official and static outcomes match; budget CRDs preserved' if a.allow_existing_budget_crds else '9 default-off checks PASS; official and static outcomes match; no custom CRDs installed')

if __name__=='__main__':main()
