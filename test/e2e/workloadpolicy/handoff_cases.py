# Executed by run.py after its local-only endpoint guard and existing 77 checks.
import importlib.util
hs=importlib.util.spec_from_file_location('handoff',ROOT/'installer/pipeline/arena-unified-handoff.py')
h=importlib.util.module_from_spec(hs);hs.loader.exec_module(h)
LEGACY=NAME+'-legacy';ARENA=NAME+'-arena';HROOT=NAME+'-arena-root';EXTRAS=NAME+'-arena-descendants'
# Ordinary hook gates are OFF; only the generated production handoff gates run.
gates(False)
policy['spec']['workloadKinds']=[x for x in policy['spec']['workloadKinds'] if x!='PyTorchJob'];apply(policy)
ap={'apiVersion':policy['apiVersion'],'kind':'WorkloadRoutingPolicy','metadata':meta(ARENA),'spec':{
 'namespaceScope':{'include':[BUS,SECOND]},'workloadKinds':['PyTorchJob'],'controllerUsers':users,'schedulerName':'default-scheduler',
 'fallback':{'queue':'default','nodeGroup':'general'},'rules':[{'name':'ego-legacy','match':{'batchValues':['old-5880','allocentric'],'legacyBatch':True},'queueAccessPolicyRef':'wp-ego-shared','target':{'queue':'wp-ego-shared','nodeGroup':'ego','poolRef':'wp-ego'}}]}}
apply(ap)
access={'apiVersion':policy['apiVersion'],'kind':'QueueAccessPolicy','metadata':meta('wp-ego-shared'),'spec':{'allowedNamespaces':[BUS]}}
assert not k('get','queueaccesspolicy','wp-ego-shared','--ignore-not-found','-o','name').stdout,'Refuse existing ACL'
apply(access)
pool=get('nodepoolpolicy','wp-ego');pool['spec']['acceptedRoutes'].append({'policyRef':ARENA,'ruleNames':['ego-legacy']});apply(pool)
# Dedicated service account: old backend needs ACL reads; unified backend does not.
apply({'apiVersion':'v1','kind':'ServiceAccount','metadata':meta(LEGACY,NS)})
role=get('clusterrole',NAME);role['metadata']=meta(LEGACY);role.pop('status',None)
role['rules'].append({'apiGroups':['scheduling.simplesilicon.com'],'resources':['queueaccesspolicies'],'verbs':['get','list','watch']});apply(role)
apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRoleBinding','metadata':meta(LEGACY),'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':LEGACY},'subjects':[{'kind':'ServiceAccount','name':LEGACY,'namespace':NS}]})
ld=get('deployment',NAME,NS);ld['metadata']=meta(LEGACY,NS);ld.pop('status',None)
ld['spec']['selector']={'matchLabels':{'app':LEGACY}};ld['spec']['template']['metadata']={'labels':{'app':LEGACY}}
ld['spec']['template']['spec']['serviceAccountName']=LEGACY
ld['spec']['template']['spec']['containers'][0]['image']='ss-container-lib-registry.cn-shanghai.cr.aliyuncs.com/infra/volcano:workload-policy-96e1a9aa'
apply(ld)
svc=get('service',NAME,NS);svc['metadata']=meta(LEGACY,NS);svc.pop('status',None)
svc['spec'].pop('clusterIP',None);svc['spec'].pop('clusterIPs',None);svc['spec']['selector']={'app':LEGACY};apply(svc)
created_extra=[]
def cleanup_handoff():
 for kind,name in [('mutatingwebhookconfiguration',HROOT),('mutatingwebhookconfiguration',EXTRAS),('validatingwebhookconfiguration',EXTRAS),('workloadroutingpolicy',ARENA),('queueaccesspolicy','wp-ego-shared'),('clusterrolebinding',LEGACY),('clusterrole',LEGACY)]:
  k('delete',kind,name,'--ignore-not-found','--wait=false',ok=False)
 for kind in ['deployment','service','serviceaccount']:k('delete',kind,LEGACY,'-n',NS,'--ignore-not-found','--wait=false',ok=False)
atexit.register(cleanup_handoff)
k('rollout','status','deployment/'+LEGACY,'-n',NS,'--timeout=90s')
root=get('mutatingwebhookconfiguration',NAME);root['metadata']=meta(HROOT);root['webhooks']=root['webhooks'][:1]
w=root['webhooks'][0];w['name']='arena-handoff-test.simplesilicon.com';w['rules']=[{'apiGroups':['kubeflow.org'],'apiVersions':['v1'],'resources':['pytorchjobs'],'operations':['CREATE'],'scope':'Namespaced'}]
w['matchConditions']=[{'name':'rollout-enabled','expression':'true'}];w['clientConfig']['service'].update(name=LEGACY,path='/mutate');w['reinvocationPolicy']='Never'
apply(root)
wait(lambda: preview(arena('legacy-ready',SECOND))[0] is not None)
o,err=preview(arena('old-acl-fallback',SECOND));check('handoff-old-backend-ACL-fallback',o is not None and o['spec']['runPolicy']['schedulingPolicy']['queue']=='default' and DKEY not in o['metadata'].get('annotations',{}),err)
k('create','-f','-',obj=arena('handoff-old'))
historical=get('pytorchjob','handoff-old',BUS)
hp=wait(lambda:next(iter(trainer_pods('handoff-old')),None))
# Generate EXACT narrow CEL/rule structure used by the production helper.
client=get('mutatingwebhookconfiguration',NAME)['webhooks'][0]['clientConfig']
extras=h.descendant_hooks(root,client,'unused-local-self-signed')
for obj in extras:
 obj['metadata']={'name':EXTRAS}
 for wh in obj['webhooks']:wh['name']='e2e-'+wh['name']
 apply(obj)
check('handoff-generated-hook-schema-accepted',True)
# Change only the original root service/path, preserving namespace filters.
w['clientConfig']['service'].update(name=NAME,path='/mutate-workload');apply(root)
for batch,queue in [('allocentric','wp-ego-shared'),('old-5880','wp-ego-shared'),('new-5880','default')]:
 o,err=preview(arena('handoff-'+batch,SECOND,batch));check('handoff-unified-no-ns-ACL-'+batch,o is not None and json.loads(o['metadata']['annotations'][DKEY])['target']['queue']==queue,err)
k('delete','pod',hp['metadata']['name'],'-n',BUS,'--wait=false')
hp2=wait(lambda:next((p for p in trainer_pods('handoff-old') if p['metadata']['uid']!=hp['metadata']['uid']),None))
check('handoff-old-pod-rebuild-unchanged',DKEY not in hp2['metadata'].get('annotations',{}) and hp2['spec']['schedulerName']==hp['spec']['schedulerName'] and hp2['spec']['nodeSelector']==hp['spec']['nodeSelector'])
k('create','-f','-',obj=arena('handoff-new',SECOND))
np=wait(lambda:next((p for p in get('pods',ns=SECOND)['items'] if any(r.get('name')=='handoff-new' and r.get('kind')=='PyTorchJob' for r in p['metadata'].get('ownerReferences',[]))),None))
check('handoff-new-pod-through-generated-hooks',json.loads(np['metadata']['annotations'][DKEY])['target']['queue']=='wp-ego-shared')
newjob=get('pytorchjob','handoff-new',SECOND);changed=copy.deepcopy(newjob);changed['metadata']['annotations'].pop(DKEY)
o,err=preview(changed,update=True);check('handoff-root-marker-removal-denied',o is None,err)
# Roll back only root endpoint. Keep new decisions, policies and descendant hooks.
w['clientConfig']['service'].update(name=LEGACY,path='/mutate');apply(root)
o,err=preview(arena('handoff-rollback-root',SECOND));check('handoff-rollback-old-backend-restored',o is not None and DKEY not in o['metadata'].get('annotations',{}) and o['spec']['runPolicy']['schedulingPolicy']['queue']=='default',err)
k('delete','pod',np['metadata']['name'],'-n',SECOND,'--wait=false')
np2=wait(lambda:next((p for p in get('pods',ns=SECOND)['items'] if p['metadata']['uid']!=np['metadata']['uid'] and any(r.get('name')=='handoff-new' and r.get('kind')=='PyTorchJob' for r in p['metadata'].get('ownerReferences',[]))),None))
check('handoff-rollback-v2-pod-rebuild',np2['metadata']['annotations'][DKEY]==np['metadata']['annotations'][DKEY])
check('handoff-rollback-old-root-unchanged',get('pytorchjob','handoff-old',BUS)['spec']==historical['spec'])
# The generated webhook must not make system namespace submits depend on it.
k('scale','deployment/'+NAME,'-n',NS,'--replicas=0');wait(lambda:not [p for p in get('pods',ns=NS)['items'] if p['metadata'].get('labels',{}).get('app')==NAME],45)
o,err=preview(workload('Pod','handoff-system-outage','kube-system'));check('handoff-generated-hooks-system-outage-unaffected',o is not None,err)
k('scale','deployment/'+NAME,'-n',NS,'--replicas=1');k('rollout','status','deployment/'+NAME,'-n',NS,'--timeout=90s')
cleanup_handoff();atexit.unregister(cleanup_handoff)
