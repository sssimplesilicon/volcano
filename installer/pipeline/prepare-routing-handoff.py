#!/usr/bin/env python3
"""Generate guarded Kyverno handoff patches from a user-supplied live snapshot.

Never reads or writes a cluster. Only the existing Arena routing policy is touched;
security policies and ownership conversion are deliberately out of scope.
"""
import argparse, copy, json, pathlib
KEY='scheduling.simplesilicon.com/workload-routing-policy'
p=argparse.ArgumentParser()
p.add_argument('--legacy',required=True)
p.add_argument('--output',required=True)
a=p.parse_args()
obj=json.load(open(a.legacy));assert obj['metadata']['name']=='arena-pytorch-independent-routing'
uid=obj['metadata']['uid'];before=obj['spec'];after=copy.deepcopy(before)
condition={'key':'{{ request.object.metadata.annotations."'+KEY+'" || \'\' }}','operator':'Equals','value':''}
expected={'route-new-pytorchjob','route-replica-templates','reject-inconsistent-replica-batch'}
assert {r['name'] for r in after['rules']}==expected,'Unexpected rules: review manually'
for rule in after['rules']:
 pre=rule.setdefault('preconditions',{'all':[]})
 assert isinstance(pre,dict) and set(pre)=={'all'},'Unexpected preconditions: review manually'
 assert condition not in pre['all'],'Handoff already installed; use original backup for rollback'
 pre['all'].append(condition)
out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=True)
for name,old,new in [('legacy-handoff-forward.json',before,after),('legacy-handoff-rollback.json',after,before)]:
 dest=out/name
 assert not dest.exists(),f'Refusing to overwrite {dest}'
 dest.write_text(json.dumps([{'op':'test','path':'/metadata/uid','value':uid},{'op':'test','path':'/spec','value':old},{'op':'replace','path':'/spec','value':new}],indent=2)+'\n')
print('Prepared guarded handoff and rollback. Only marked new roots skip legacy routing.')
