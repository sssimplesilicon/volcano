#!/usr/bin/env python3
"""Render opt-in desiredGPU controller. Emits JSON only; does not access a cluster."""
import argparse,json
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--image',required=True)
p.add_argument('--recovery-journal',default='',help='scheduler journal namespace/name; omit to defer all downscales')
p.add_argument('--worker-namespace',required=True)
p.add_argument('--namespace',default='volcano-system')
p.add_argument('--name',default='pipeline-worker-controller')
p.add_argument('--image-pull-secret',action='append',default=[])
a=p.parse_args()
name=a.name+'-'+a.worker_namespace
if len(name)>63: p.error('controller name + worker namespace must fit 63 characters')
def meta(ns=None):return {'name':name,**({'namespace':ns} if ns else {})}
subject={'kind':'ServiceAccount','name':name,'namespace':a.namespace}
items=[{'apiVersion':'v1','kind':'ServiceAccount','metadata':meta(a.namespace)},
 {'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':meta(),'rules':[{'apiGroups':['scheduling.simplesilicon.com'],'resources':['pipelinepolicies','workerbudgets'],'verbs':['get','list','watch']},{'apiGroups':['scheduling.volcano.sh'],'resources':['podgroups'],'verbs':['get','list']},{'apiGroups':[''],'resources':['pods'],'verbs':['get','list']}]},
 {'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRoleBinding','metadata':meta(),'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':name},'subjects':[subject]}]
roles={}
roles.setdefault(a.worker_namespace,[]).extend([
 {'apiGroups':['apps'],'resources':['deployments'],'verbs':['get']},
 {'apiGroups':['apps'],'resources':['deployments/scale'],'verbs':['get','update']},
 {'apiGroups':['autoscaling'],'resources':['horizontalpodautoscalers'],'verbs':['list']},
 {'apiGroups':['scheduling.simplesilicon.com'],'resources':['workerbudgets/status'],'verbs':['update']}])
roles.setdefault(a.namespace,[]).append({'apiGroups':['coordination.k8s.io'],'resources':['leases'],'verbs':['get','list','watch','create','update']})
if a.recovery_journal:
 parts=a.recovery_journal.split('/')
 if len(parts)!=2 or not all(parts):p.error('recovery-journal must be namespace/name')
 roles.setdefault(parts[0],[]).append({'apiGroups':[''],'resources':['configmaps'],'resourceNames':[parts[1]],'verbs':['get']})
for ns,rules in roles.items():
 items.extend([{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'Role','metadata':meta(ns),'rules':rules},{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':meta(ns),'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':name},'subjects':[subject]}])
pod={'serviceAccountName':name,'schedulerName':'default-scheduler','securityContext':{'runAsNonRoot':True,'runAsUser':65532},'containers':[{'name':'controller','image':a.image,'command':['/pipeline-worker-controller'],'args':['--namespace='+a.worker_namespace,'--lease-namespace='+a.namespace,'--interval=3s'],'resources':{'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'memory':'256Mi'}},'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}}}]}
if a.recovery_journal:pod['containers'][0]['args'].append('--recovery-journal='+a.recovery_journal)
if a.image_pull_secret:pod['imagePullSecrets']=[{'name':n} for n in a.image_pull_secret]
items.append({'apiVersion':'apps/v1','kind':'Deployment','metadata':meta(a.namespace),'spec':{'replicas':1,'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{'app':name}},'spec':pod}}})
print(json.dumps({'apiVersion':'v1','kind':'List','items':items},indent=2))
