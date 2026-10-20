#!/usr/bin/env python3
"""Render opt-in workload routing/isolation. Both admission paths start disabled.
No network calls. Production rollout requires reviewing controller identities,
system namespace exclusions and the grandfather registry first.
"""
import argparse,json,pathlib,subprocess,sys
p=argparse.ArgumentParser()
p.add_argument('--policy',required=True);p.add_argument('--pools',required=True,help='JSON List of NodePoolPolicies')
p.add_argument('--image',required=True);p.add_argument('--namespace',default='volcano-system');p.add_argument('--name',default='workload-policy')
p.add_argument('--tls-secret',default='workload-policy-tls');p.add_argument('--certificate',default='workload-policy-serving')
p.add_argument('--image-pull-secret',action='append',default=[]);p.add_argument('--nodegroup',default='general')
a=p.parse_args();root=pathlib.Path(__file__).parent
args=[sys.executable,str(root/'render-routing.py'),'--policy',a.policy,'--image',a.image,'--namespace',a.namespace,'--name',a.name,'--tls-secret',a.tls_secret,'--certificate',a.certificate,'--nodegroup',a.nodegroup]
for s in a.image_pull_secret:args+=['--image-pull-secret',s]
result=json.loads(subprocess.check_output(args,text=True));items=result['items'];pools=json.load(open(a.pools))['items']
assert pools and all(x['kind']=='NodePoolPolicy' for x in pools)
# Separate scopes: isolation can cover Pods outside the routing enrollment scope.
# A common pool scope is required for the first renderer version.
scope=pools[0]['spec']['namespaceScope'];assert all(x['spec']['namespaceScope']==scope for x in pools),'pool scopes must agree for this renderer'
def selector(scope):
 expressions=[]
 if '*' not in scope['include']:expressions.append({'key':'kubernetes.io/metadata.name','operator':'In','values':scope['include']})
 if scope.get('exclude'):expressions.append({'key':'kubernetes.io/metadata.name','operator':'NotIn','values':scope['exclude']})
 return {'matchExpressions':expressions}
items.extend(pools)
for o in items:
 if o['kind']=='ClusterRole':
  o['rules'][0]['resources'].append('nodepoolpolicies')
  o['rules'] += [
   {'apiGroups':['apps'],'resources':['deployments','replicasets','statefulsets','daemonsets'],'verbs':['get']},
   {'apiGroups':['batch'],'resources':['jobs','cronjobs'],'verbs':['get']},
   {'apiGroups':['argoproj.io'],'resources':['workflows','workflowtemplates'],'verbs':['get']},
   {'apiGroups':['kubeflow.org'],'resources':['pytorchjobs'],'verbs':['get']}]
  # Do not grant arbitrary ConfigMap reads cluster-wide for legacy compatibility.
 if o['kind']=='MutatingWebhookConfiguration':
  original=o['webhooks'][0];o['webhooks']=[]
  validation={'apiVersion':o['apiVersion'],'kind':'ValidatingWebhookConfiguration','metadata':dict(o['metadata']),'webhooks':[]}
  for isolation in [False,True]:
   for validate in [False,True]:
    w=json.loads(json.dumps(original));w['name']=('validate' if validate else 'mutate')+('-isolation' if isolation else '-workload')+'.simplesilicon.com'
    w['clientConfig']['service']['path']=('/validate' if validate else '/mutate')+('-isolation' if isolation else '-workload')
    if validate:w.pop('reinvocationPolicy',None)
    else:w['reinvocationPolicy']='IfNeeded'
    if isolation:
     w['namespaceSelector']=selector(scope)
     w['rules']=[{'operations':['CREATE'],'apiGroups':[''],'apiVersions':['v1'],'resources':['pods'],'scope':'Namespaced'}]
    else:
     w['rules']=[]
     for group,version,resources in [('', 'v1',['pods']),('apps','v1',['deployments','replicasets','statefulsets']),('batch','v1',['jobs','cronjobs']),('argoproj.io','v1alpha1',['workflows']),('kubeflow.org','v1',['pytorchjobs'])]:
      w['rules'].append({'operations':['CREATE','UPDATE'],'apiGroups':[group],'apiVersions':[version],'resources':resources,'scope':'Namespaced'})
    (validation if validate else o)['webhooks'].append(w)
  break
items.append(validation)
registries={(x['spec']['legacyRegistry']['namespace'],x['spec']['legacyRegistry']['name']) for x in pools if x['spec'].get('legacyRegistry')}
for i,(ns,name) in enumerate(sorted(registries)):
 role=a.name+'-registry-'+str(i)
 items += [
  {'apiVersion':'rbac.authorization.k8s.io/v1','kind':'Role','metadata':{'name':role,'namespace':ns},'rules':[{'apiGroups':[''],'resources':['configmaps'],'resourceNames':[name],'verbs':['get']}]},
  {'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':{'name':role,'namespace':ns},'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':role},'subjects':[{'kind':'ServiceAccount','name':a.name,'namespace':a.namespace}]}]
print(json.dumps(result,indent=2))
