#!/usr/bin/env python3
"""Remove the isolated local access test processes, retaining CRDs and journals."""
import json
from harness import Cluster, NS, OWNER, require
c=Cluster()
cfg=json.loads(c.k('config','view','--minify','-o','json'))
require(cfg['clusters'][0]['cluster']['server']=='https://127.0.0.1:16473','Local k3s only')
require(not c.journal(),'Refusing teardown with unresolved eviction intents')
require(not c.get('pods',ns=NS)['items'],'Finish fixture workloads before teardown')
c.k('delete','validatingwebhookconfiguration','wt-queue-access','--ignore-not-found')
for kind,names in [('deployment',['wt-queue-access','worker-type-test']),('service',['wt-queue-access']),('secret',['wt-queue-access'])]:
 for name in names:c.k('delete',kind,name,'-n','volcano-system','--ignore-not-found','--wait=true','--timeout=60s')
c.k('label','namespace',NS,'test.simplesilicon.com/queue-access-')
print('Local test admission and scheduler removed; official scheduler, CRDs and journal retained')
