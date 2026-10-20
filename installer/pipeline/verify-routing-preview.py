#!/usr/bin/env python3
"""Production-safe verification: GET and server dry-run only, no persisted jobs."""
import argparse,json,pathlib,subprocess
p=argparse.ArgumentParser()
p.add_argument('--context',required=True)
p.add_argument('--namespace',default='infra-group')
p.add_argument('--output',required=True)
a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=True)
cases=[('legacy-label','allocentric',False,None,'infra-ego-pipeline-shared'),('legacy-selector','old-5880',True,None,'infra-ego-pipeline-shared'),('general','new-5880',False,None,'default'),('explicit-other','allocentric',False,'other-pipeline','default')]
for case,batch,selector,pipeline,expected in cases:
 labels={'app':'pytorchjob','safeguard':'arena-test'}
 if not selector:labels['batch']=batch
 if pipeline:labels['pipeline.simplesilicon.com/name']=pipeline
 pod={'containers':[{'name':'pytorch','image':'busybox:1.36','command':['sh','-c','echo routing-preview'],'resources':{'requests':{'cpu':'100m','memory':'64Mi'}}}]}
 if selector:pod['nodeSelector']={'batch':batch}
 obj={'apiVersion':'kubeflow.org/v1','kind':'PyTorchJob','metadata':{'name':'workload-routing-preview-'+case,'namespace':a.namespace,'labels':labels},'spec':{'runPolicy':{'suspend':True},'pytorchReplicaSpecs':{'Master':{'replicas':1,'restartPolicy':'Never','template':{'metadata':{'labels':{'safeguard':'arena-test'}},'spec':pod}}}}}
 result=subprocess.run(['kubectl','--context',a.context,'create','--dry-run=server','--validate=strict','-f','-','-o','json'],input=json.dumps(obj),text=True,capture_output=True,check=True)
 r=json.loads(result.stdout);(out/(case+'.json')).write_text(json.dumps(r,indent=2)+'\n')
 queue=r['spec'].get('runPolicy',{}).get('schedulingPolicy',{}).get('queue')
 provenance=r['metadata'].get('annotations',{}).get('scheduling.simplesilicon.com/workload-routing-policy')
 spec=r['spec']['pytorchReplicaSpecs']['Master']['template']['spec']
 assert queue==expected,(case,queue,expected)
 assert provenance and provenance.startswith('arena-pytorch/'),(case,'new router was not used')
 assert spec.get('schedulerName')=='volcano-infra',(case,spec)
 expected_group='infra-ego-pipeline' if expected!='default' else 'general'
 assert spec.get('nodeSelector',{}).get('volcano.sh/nodegroup-name')==expected_group
 if selector:assert spec['nodeSelector']['batch']==batch
 print(case,queue,expected_group,provenance,flush=True)
print('All preview checks passed; no task persisted.')
