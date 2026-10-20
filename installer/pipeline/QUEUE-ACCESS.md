# Queue access and cross-namespace preemption

This is an opt-in development extension. Production activation requires a separate
review of existing routing, namespace exclusions, certificates, and RBAC. The
local E2E manifest is not a production installation manifest.

## One source of authorization per concern

- `QueueAccessPolicy` is cluster scoped and named exactly after its Volcano Queue.
  `allowedNamespaces` grants access using the object's real namespace, not a
  caller-supplied namespace label. No policy means unmanaged. An empty list denies
  all new access. **Deleting a policy removes protection**, it is not revocation.
- `PipelinePolicy` stays in the worker namespace. `allowedPreemptorNamespaces`
  specifies who can preempt its shared workers. An absent field preserves legacy
  same-namespace behavior. An explicitly empty list denies all preemptors.
- Existing WorkerBudget GPU floors and PDBs remain in the worker namespace and
  account for both base and shared. Authorization does not replace either floor.

See `queue-access-example.yaml`: base admits only `infra-ego-pipeline`; shared
admits that namespace plus `data-group` and `infra-group`. Only the latter two
may preempt. A training Pod must also carry:

```yaml
metadata:
  labels:
    pipeline.simplesilicon.com/name: ego-pipeline
    pipeline.simplesilicon.com/namespace: infra-ego-pipeline
```

Workers carry their own pipeline name and worker-type labels. Training must use
shared, request GPUs, have higher priority, and pass all existing eligibility
checks. Base is not in `preemptibleQueues` and is never a victim of this path.
Namespace membership is the authorization boundary in this version. This does
not distinguish an authorized user's hand-written GPU Pod from an Arena training
Pod. Do not grant untrusted users policy-write or namespace-management rights.

## Enforcement

1. Install `queue-access-crd.json` alongside `budget-crds.json`.
2. Enable `queue-access` in the custom scheduler tiers under the existing explicit
   scheduler opt-in flag. With the custom preemption action, require both
   `queueAccess: true` and `pipelineBudgets: true` in its action arguments. Keep
   `pipelineBudgets: true` on the worker-type plugin as well.
3. Enable the validating webhook either via `/queueaccess/validate` in
   `vc-webhook-manager` or via the separate `/queue-access-webhook` binary and
   `/validate` endpoint. It requires reviewed TLS and a deliberately scoped
   ValidatingWebhookConfiguration, with Fail failure policy and CREATE/UPDATE
   rules for Pods and PodGroups. It is **not enabled in the official defaults**.
4. The webhook needs read/list QueueAccessPolicy and get PodGroup permissions.
   Scheduler needs list QueueAccessPolicy plus existing budget permissions.
   Give these to dedicated service accounts in production. The standalone host
   accepts `--scheduler-names`, limiting protected queues to ACL-enabled schedulers.

Admission checks both Pod queue annotations and the effective PodGroup queue.
PodGroup creation/queue changes are checked too. A group may be created after its
Pod: scheduler checks the resolved queue again before scheduling. JobValid and
Predicate checks use a snapshot refreshed each scheduling session; preemption
additionally re-reads queue and pipeline authorization at the destructive boundary
and on recovery retries. These are not a transaction with Kubernetes policy
updates: revocation cannot retract a Bind or Eviction already accepted/in flight.

The admission scope must include **unauthorized business namespaces too**, not
only the allowlisted namespaces. Otherwise an excluded namespace using a different
scheduler could bypass admission. Namespace canaries only establish enforcement
inside their scope. Trusted system namespaces can be excluded, but cannot then be
claimed as an enforced boundary. Do not include a stock scheduler without the ACL
plugin in `--scheduler-names` for protected queues. Old workloads on unmanaged
queues keep their existing scheduler; new/recreated protected-queue Pods must use
an authorized scheduler.

The generic admission layer rejects Pod/PodGroup access, not every possible root
CRD. An unauthorized Deployment or PyTorchJob may exist but its child Pod or
PodGroup creation fails with a clear event. Existing Kyverno root-level validation
can provide earlier feedback without replacing this enforcement.

## Revocation and old workloads

Changing an allowlist never directly deletes or evicts an existing Pod. Ordinary
updates that do not change scheduling identity are allowed. Pending tasks are
rechecked on the next scheduling session. New/recreated Pods are CREATE requests
and must pass the current policy, including Pods recreated by old controllers.
Thus a running old workload can continue after revocation, but replacement is
intentionally denied; automatic grandfathering would defeat revocation.

Queue access and preemption rights are independent: removing only preemption
permission still permits an authorized namespace to use free shared capacity.
Removing queue access blocks scheduling and new preemption demand for that queue.

An ambiguous, previously submitted Eviction retains its journal reservation after
revocation. Recovery does not send another Eviction without current permission.
It may conservatively hold the custom action until the victim disappears, the
demand is canceled, or authorization is restored. Do not erase this record: a
late API acceptance cannot be ruled out. Ordinary allocation is separate.

Queue ACLs do not isolate physical nodes from tasks using other queues/schedulers.
Keep the existing node-pool admission isolation in place. System Pods outside the
webhook's namespace scope and unrelated unmanaged queues retain their behavior.

## Local tests

`access_setup.py` installs the isolated local fixture and a short-lived test TLS
certificate, scoped only to namespaces carrying the test label. Never reuse that
certificate/configuration for production. The setup refuses a non-local endpoint.
Use `PIPELINE_E2E_KUBECONFIG`, then run:

```sh
python3 test/e2e/pipeline/access_setup.py
python3 test/e2e/pipeline/run.py --execute-local --pipeline-budgets \
  --case P18 --case P19 --case P20 --case P21 --case P22 --case P23 --require-all-pass
```

P18 covers unauthorized entry, generated-name scheduler bypass, unchanged old
Pod and denied reconstruction after revocation. P19 covers concurrent remote
namespaces with one shared PDB/GPU budget and base protection. P20/P21 revoke
queue/preemption rights on pending demand, then restore them. P22 revokes during
ambiguous Eviction recovery. P23 uses a real ReplicaSet controller to verify
authorized recreation and denied recreation after revocation. GPU nodes are synthetic; this is API/scheduler
validation, not a physical GPU or business recovery test.

## Staged rollout / rollback contract

No production rollout is included in this change. For a future rollout, first
install CRDs, permissions, policy objects and an isolated validator; inspect all
current tenants of protected queues before expanding its namespace scope. Enable
scheduler ACL checks before enabling cross-namespace preemption. Keep preemption
explicitly disabled during admission-only canary tests.

To stop new preemption, set the pipeline's `allowedPreemptorNamespaces: []`.
Keep admission ACLs, budget controller and recovery processing alive until every
uncertain request is resolved. This is not a command to delete existing workers.
To withdraw queue use from a namespace, remove it from `allowedNamespaces`; expect
its pending/replacement tasks to be denied. Restore the list to roll back a mistaken
revocation. Do not delete a QueueAccessPolicy as a way to deny access.

The local `access_teardown.py` refuses unresolved journal records and removes
only the dedicated test admission and scheduler processes. It retains CRDs and
journal storage; it never deletes the original local Volcano scheduler.
