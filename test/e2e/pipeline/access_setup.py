#!/usr/bin/env python3
"""Install isolated queue-access test admission and scheduler on local k3s only."""
import base64
import json
import pathlib
import subprocess
import tempfile
from harness import Cluster, NS, require

c=Cluster()
cfg=json.loads(c.k('config','view','--minify','-o','json'))
require(cfg['clusters'][0]['cluster']['server']=='https://127.0.0.1:16473','Local k3s only')
root=pathlib.Path(__file__).resolve().parents[3]
for name in ['budget-crds.json','queue-access-crd.json']:
 c.k('apply','-f',str(root/'installer/pipeline'/name))
items=json.loads((pathlib.Path(__file__).parent/'infrastructure.json').read_text())['items']
for o in items:
 if o['kind']=='ConfigMap' and o['metadata']['name']=='worker-type-test-recovery':
  current=c.k('get','cm','worker-type-test-recovery','-n',NS,'--ignore-not-found','-o','json')
  if current:
   require(not json.loads(json.loads(current)['data']['intents.json']),'Outstanding recovery intents; refusing reset')
   continue
 if o['kind']=='ConfigMap' and o['metadata']['name']=='worker-type-test':
  conf=o['data']['volcano-scheduler.conf'].replace('- plugins:\n','- plugins:\n  - name: queue-access\n',1)
  o['data']['volcano-scheduler.conf']=conf+'    queueAccess: true\n'
 if o['kind']=='ClusterRole' and o['metadata']['name']=='worker-type-test-budgets':
  o['rules'][0]['resources'].append('queueaccesspolicies')
  o['rules'].append({'apiGroups':['scheduling.volcano.sh'],'resources':['podgroups'],'verbs':['get']})
 c.k('apply','-f','-',obj=o)
c.k('label','ns',NS,'test.simplesilicon.com/queue-access=true','--overwrite')
name='wt-queue-access'
existing=c.k('get','secret',name,'-n','volcano-system','--ignore-not-found','-o','json')
if existing:
 ca=json.loads(existing)['data']['tls.crt']
else:
 with tempfile.TemporaryDirectory() as td:
  cert,key=pathlib.Path(td)/'tls.crt',pathlib.Path(td)/'tls.key'
  subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','2','-subj','/CN='+name+'.volcano-system.svc','-addext','subjectAltName=DNS:'+name+'.volcano-system.svc'],check=True,capture_output=True)
  c.k('apply','-f','-',obj={'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':'volcano-system'},'type':'kubernetes.io/tls','data':{'tls.crt':base64.b64encode(cert.read_bytes()).decode(),'tls.key':base64.b64encode(key.read_bytes()).decode()}})
  ca=base64.b64encode(cert.read_bytes()).decode()
for obj in [
 {'apiVersion':'v1','kind':'Service','metadata':{'name':name,'namespace':'volcano-system'},'spec':{'selector':{'app':name},'ports':[{'port':443,'targetPort':8443}]}},
 {'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':name,'namespace':'volcano-system'},'spec':{'replicas':1,'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{'app':name}},'spec':{'serviceAccountName':'volcano-scheduler','nodeSelector':{'node-role.kubernetes.io/control-plane':'true'},'containers':[{'name':'webhook','image':'volcano-ss:v1.15.3-ss.1-local','imagePullPolicy':'Never','command':['/queue-access-webhook'],'args':['--scheduler-names=worker-type-test'],'volumeMounts':[{'name':'tls','mountPath':'/tls','readOnly':True}],'readinessProbe':{'httpGet':{'scheme':'HTTPS','path':'/healthz','port':8443}}}],'volumes':[{'name':'tls','secret':{'secretName':name}}]}}}},
]:c.k('apply','-f','-',obj=obj)
c.k('rollout','restart','deployment/'+name,'-n','volcano-system')
for deployment in ['worker-type-test',name]: c.k('rollout','status','deployment/'+deployment,'-n','volcano-system','--timeout=120s')
c.k('apply','-f','-',obj={'apiVersion':'admissionregistration.k8s.io/v1','kind':'ValidatingWebhookConfiguration','metadata':{'name':name},'webhooks':[{'name':'queue-access.test.simplesilicon.com','admissionReviewVersions':['v1'],'sideEffects':'None','failurePolicy':'Fail','timeoutSeconds':10,'namespaceSelector':{'matchLabels':{'test.simplesilicon.com/queue-access':'true'}},'clientConfig':{'service':{'namespace':'volcano-system','name':name,'path':'/validate'},'caBundle':ca},'rules':[{'operations':['CREATE','UPDATE'],'apiGroups':[''],'apiVersions':['v1'],'resources':['pods']},{'operations':['CREATE','UPDATE'],'apiGroups':['scheduling.volcano.sh'],'apiVersions':['v1beta1'],'resources':['podgroups']}]}]})
print('Isolated local queue-access infrastructure ready')
