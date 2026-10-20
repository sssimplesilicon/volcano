#!/usr/bin/env python3
"""Render opt-in routing infrastructure; never talks to a cluster.

TLS secret must already exist. Admission remains disabled in generated output.
Use one WorkloadRoutingPolicy with multiple rules for multiple pipelines.
"""
import argparse,json
p=argparse.ArgumentParser()
p.add_argument('--policy',required=True)
p.add_argument('--image',required=True)
p.add_argument('--namespace',default='volcano-system')
p.add_argument('--name',default='workload-routing')
p.add_argument('--tls-secret',default='workload-routing-tls')
p.add_argument('--certificate',default='workload-routing-serving')
p.add_argument('--image-pull-secret',action='append',default=[])
p.add_argument('--nodegroup',default='general')
a=p.parse_args()
policy=json.load(open(a.policy));scope=policy['spec']['namespaceScope']
assert scope.get('include'),'explicit namespace scope required'
expr=[]
if '*' not in scope['include']:expr.append({'key':'kubernetes.io/metadata.name','operator':'In','values':scope['include']})
if scope.get('exclude'):expr.append({'key':'kubernetes.io/metadata.name','operator':'NotIn','values':scope['exclude']})
def md(name,ns=None):return {'name':name,**({'namespace':ns} if ns else {})}
items=[policy,
{'apiVersion':'v1','kind':'ServiceAccount','metadata':md(a.name,a.namespace)},
{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':md(a.name),'rules':[{'apiGroups':['scheduling.simplesilicon.com'],'resources':['workloadroutingpolicies','queueaccesspolicies'],'verbs':['get','list']}]},
{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRoleBinding','metadata':md(a.name),'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':a.name},'subjects':[{'kind':'ServiceAccount','name':a.name,'namespace':a.namespace}]},
{'apiVersion':'v1','kind':'Service','metadata':md(a.name,a.namespace),'spec':{'selector':{'app':a.name},'ports':[{'port':443,'targetPort':8443}]}},
{'apiVersion':'apps/v1','kind':'Deployment','metadata':md(a.name,a.namespace)|{'annotations':{'secret.reloader.stakater.com/reload':a.tls_secret}},'spec':{'replicas':2,'strategy':{'type':'RollingUpdate','rollingUpdate':{'maxSurge':0,'maxUnavailable':1}},'selector':{'matchLabels':{'app':a.name}},'template':{'metadata':{'labels':{'app':a.name}},'spec':{'serviceAccountName':a.name,'schedulerName':'default-scheduler','securityContext':{'runAsNonRoot':True,'runAsUser':65532,'fsGroup':65532},'affinity':{'podAntiAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':[{'topologyKey':'kubernetes.io/hostname','labelSelector':{'matchLabels':{'app':a.name}}}]}},'containers':[{'name':'webhook','image':a.image,'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},'resources':{'requests':{'cpu':'100m','memory':'64Mi'},'limits':{'memory':'256Mi'}},'readinessProbe':{'httpGet':{'scheme':'HTTPS','path':'/healthz','port':8443}},'volumeMounts':[{'name':'tls','mountPath':'/tls','readOnly':True}]}],'volumes':[{'name':'tls','secret':{'secretName':a.tls_secret}}]}}}},
{'apiVersion':'admissionregistration.k8s.io/v1','kind':'MutatingWebhookConfiguration','metadata':md(a.name)|{'annotations':{'cert-manager.io/inject-ca-from':a.namespace+'/'+a.certificate}},'webhooks':[{'name':'workload-routing.simplesilicon.com','admissionReviewVersions':['v1'],'sideEffects':'None','failurePolicy':'Fail','reinvocationPolicy':'Never','timeoutSeconds':15,'namespaceSelector':{'matchExpressions':expr},'matchConditions':[{'name':'rollout-enabled','expression':'false'}],'clientConfig':{'service':{'namespace':a.namespace,'name':a.name,'path':'/mutate','port':443}},'rules':[{'operations':['CREATE'],'apiGroups':['kubeflow.org'],'apiVersions':['v1'],'resources':['pytorchjobs'],'scope':'Namespaced'}]}]}]
for obj in items:
 if obj['kind']=='Deployment':
  pod=obj['spec']['template']['spec']
  pod['imagePullSecrets']=[{'name':s} for s in a.image_pull_secret]
  pod['nodeSelector']={'volcano.sh/nodegroup-name':a.nodegroup}
print(json.dumps({'apiVersion':'v1','kind':'List','items':items},indent=2))
