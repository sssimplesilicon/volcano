#!/usr/bin/env python3
"""Offline, guarded handoff patches. Reads snapshots only; never calls kubectl.
Keep new validating admission enabled before allowing legacy rules to skip v2.
"""
import argparse,copy,json,pathlib
p=argparse.ArgumentParser()
p.add_argument('--policies',required=True,help='ClusterPolicy List snapshot')
p.add_argument('--workloads',required=True,help='List of root controllers AND their ancestors')
p.add_argument('--registry',required=True,help='pipeline-isolation-grandfather ConfigMap snapshot')
p.add_argument('--namespace',action='append',required=True)
p.add_argument('--protected-queue',action='append',required=True)
p.add_argument('--output',required=True);a=p.parse_args()
out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=True)
policies=json.load(open(a.policies))['items'];workloads=json.load(open(a.workloads))['items'];registry=json.load(open(a.registry))
nss=set(a.namespace);queues=set(a.protected_queue)
def write(name,obj):
 path=out/name
 if path.exists():raise SystemExit('Refusing to overwrite existing rollout state: '+str(path))
 path.write_text(json.dumps(obj,indent=2,ensure_ascii=False)+'\n')
def guarded(o,path,old,new):return [{'op':'test','path':'/metadata/uid','value':o['metadata']['uid']},{'op':'test','path':path,'value':old},{'op':'replace','path':path,'value':new}]
marker={'key':"{{ contains(`"+json.dumps(sorted(nss))+"`, request.namespace) && (request.object.metadata.annotations.\"scheduling.simplesilicon.com/workload-decision\" || '') != '' }}",'operator':'Equals','value':False}
changed=[]
for o in policies:
 name=o['metadata']['name']
 if name not in ['business-independent-routing','business-pods-use-volcano','pipeline-nodepool-isolation']:continue
 old=o['spec'];new=copy.deepcopy(old)
 for rule in new['rules']:
  pre=rule.setdefault('preconditions',{})
  if set(pre)-{'all'}:raise SystemExit('Unexpected precondition structure: '+name+'/'+rule['name'])
  if marker in pre.get('all',[]):raise SystemExit('Already handed off: '+name)
  pre.setdefault('all',[]).append(marker)
 write(name+'-forward.json',guarded(o,'/spec',old,new));write(name+'-rollback.json',guarded(o,'/spec',new,old));changed.append(name)
if set(changed)!={'business-independent-routing','business-pods-use-volcano','pipeline-nodepool-isolation'}:raise SystemExit('Required legacy policy snapshot missing')
byuid={x['metadata']['uid']:x for x in workloads}
def queue_values(v):
 found=set()
 if isinstance(v,dict):
  for k,x in v.items():
   if k in ['scheduling.volcano.sh/queue-name','queue'] and isinstance(x,str):found.add(x)
   found |= queue_values(x)
 elif isinstance(v,list):
  for x in v:found |= queue_values(x)
 return found
selected=[]
for o in workloads:
 m=o['metadata']
 if m.get('namespace') not in nss or not (queue_values(o.get('spec',{}))&queues):continue
 seen=set()
 while True:
  refs=[r for r in o['metadata'].get('ownerReferences',[]) if r.get('controller')]
  if not refs:break
  if len(refs)!=1 or refs[0]['uid'] in seen:raise SystemExit('Invalid controller chain')
  ref=refs[0];seen.add(ref['uid']);parent=byuid.get(ref['uid'])
  if not parent or parent['metadata'].get('namespace')!=m['namespace'] or parent['metadata']['name']!=ref['name']:raise SystemExit('Missing actual ancestor in snapshot: '+str(ref))
  o=parent
 if o['kind'] not in ['Deployment','StatefulSet','Job','CronJob','Workflow','PyTorchJob','ReplicaSet']:raise SystemExit('Unsupported legacy root '+o['kind'])
 if o['metadata'].get('annotations',{}).get('scheduling.simplesilicon.com/workload-decision'):continue
 selected.append({'namespace':m['namespace'],'kind':o['kind'],'name':o['metadata']['name'],'uid':o['metadata']['uid']})
old=registry.get('data',{});assert 'legacyOwners' in old,'legacyOwners required'
new=copy.deepcopy(old);new['legacyOwners']=json.dumps(sorted(set(json.loads(old['legacyOwners']))|{x['namespace']+'|'+x['uid'] for x in selected}))
write('registry-forward.json',guarded(registry,'/data',old,new));write('registry-rollback.json',guarded(registry,'/data',new,old));write('legacy-roots-review.json',selected)
write('handoff-notes.json',{'namespaces':a.namespace,'policies':changed,'legacyRootCount':len({x['uid'] for x in selected}),'warning':'Review root UIDs. Additive registry patch first. New validators must be active before skip patches. During coexistence raw Pods can be conservatively rejected when old isolation runs first; test dry-run before expanding. Rollback must retain admission for already-enrolled controllers.'})
print('Prepared guarded patches only. No cluster access. Review legacy-roots-review.json before applying.')
