# Workload routing and node-pool isolation

This extension is opt-in. No production changes are performed by the renderer or
local tests. It does not enable preemption, change Volcano queues, broaden the
queue-access webhook, or modify the existing identity/CPFS/DNS Kyverno policies.

## Interfaces and compatibility

The original `/mutate` PyTorchJob interface remains available and its existing
policy defaults are unchanged. The new engine has four separate admission paths:

- `/mutate-workload` and `/validate-workload`: CREATE/UPDATE of Pod, Deployment,
  ReplicaSet descendants, StatefulSet, Job, CronJob, Workflow and PyTorchJob.
- `/mutate-isolation` and `/validate-isolation`: Pod CREATE only.

The new paths require the NodePoolPolicy CRD even when the list is empty.
`render-workload-policy.py` renders all four entries **disabled**. It separates
routing and isolation namespace selectors. Rendering is offline. The generated
service is independent of the live `workload-routing` service by default.

WorkloadRoutingPolicy adds `workloadKinds`, `controllerUsers`, `mode`, rule
`match.absentLabels`, and target `poolRef`/`cpuOnly`. Empty workloadKinds still
means PyTorchJob, preserving old policies. Preview reports the decision via
AdmissionResponse warnings without enrollment; Disabled stops new enrollment but
keeps inheritance for existing enrolled controllers. Enforce is the default when
mode is omitted. An explicit route with no matching rule is rejected (except the
`default` fallback). `absentLabels` matches missing or empty labels. Legacy batch
rules do not match an explicit pipeline or explicit route on the new interface.
Duplicate matches/policy scope overlaps fail; there is no rule-order precedence.

A rule with queueAccessPolicyRef retains the existing namespace authorization and
fallback behavior. An ordinary-workload rule without that reference deliberately
does not introduce new queue ACL enforcement. Do not interpret acceptedRoutes as
namespace authorization. This preserves the requested separation of concerns.

NodePoolPolicy defines one exact node label key/value, accepted policy/rule
references, namespace scope, explicit controller identities, optional system
component exceptions, and an optional UID registry. All active pools must use the
same label key and distinct values, making overlap statically detectable. Mode is
Disabled, Audit (warnings only), or Enforce (default). An admitted route must name
the pool via poolRef and appear in acceptedRoutes. All other active pools are
excluded. Audit is configuration preview, not a durable PolicyReport producer.

The initial schema intentionally does not support arbitrary selectors or
intersecting node pools. Base/shared are queues in the SAME node pool; they are
not two node partitions. Namespace isolation and routing scope can differ.

## Frozen decisions and real owners

The authoritative decision is stored in root/template annotation
`scheduling.simplesilicon.com/workload-decision`. It includes policy, rule,
generation, scheduler, target and explicit route. `routing-version=v2` is a
tracking marker, not an authorization credential. CREATE roots are recomputed.
UPDATE cannot add/remove/change the decision or change the route input labels.
Image and replica updates remain allowed. Status subresources are ignored.

Controller-created children resolve actual Kubernetes owners (matching UID,
namespace and non-terminating state, maximum depth 8). Their submitter must match
an exact configured controllerUsers entry. They inherit the original controller's
decision, not the current policy's changed target. A hand-written ownerReference
or copied Pod annotation does not grant admission. Unmanaged roots are never
automatically enrolled on UPDATE. Standalone ReplicaSets/DaemonSets are not root
enrollment kinds in this version.

The legacy registry retains `namespace|UID` entries in data.legacyOwners. Actual
ancestor objects and trusted controller identity are checked. No per-worker UID
registration is needed for new workloads. Existing v1 routed roots without v2
annotations must be included in the reviewed migration snapshot before isolation
is handed off. A same-name replacement with another UID is not grandfathered.

For system exceptions both the service account and real immediate controller
kind/name must match, in addition to trusted admission requester identity. Prefer
excluding trusted system namespaces at the API-server webhook selector. Exact
controllerUsers values depend on the cluster; do not copy test identities blindly.
Users who may change policies, impersonate controller identities, or write Nodes
are outside this admission protection boundary. Direct Binding permissions must
also remain restricted through Kubernetes RBAC.

## Node affinity and Workflow support

Isolation adds NotIn to EVERY required-affinity OR branch. It preserves other
expressions, matchFields, tolerations, resource requests, and optional affinity.
An originally empty match-none term is never turned into a schedulable term.
Explicit contradictory nodeSelector/all-impossible affinity is rejected.
Non-exempt direct nodeName is rejected. Running Pod specs are not rewritten and
no Pods are evicted by this extension.

Argo inline container/DAG/steps workflows and namespaced WorkflowTemplate/templateRef
references are supported; the actual generated Pod is checked again. The adapter
writes Workflow schedulerName and podMetadata, while child admission supplies the
remaining node/queue constraints. **ClusterWorkflowTemplate references, raw
podSpecPatch and resource templates are explicitly unsupported** in this initial
version and fail with a clear error. They must not be advertised as validated
workflow paths. The tests include a real, independent namespaced Argo controller.

## Configuration examples (not production manifests)

- `business-routing-example.json`: preview-only ordinary workloads, ego/video
  rules plus old ego route/batch compatibility. Existing PyTorchJob configuration
  is not implicitly replaced by this example.
- `nodepool-policy-example.json`: audit-only pools. Remove the illustrative video
  pool if unused. Fill actual controller identities before enabling inheritance.

Render after reviewing scopes and references:

```sh
python3 installer/pipeline/render-workload-policy.py \
  --policy reviewed-business-routing.json \
  --pools reviewed-nodepools.json \
  --image YOUR_REVIEWED_IMAGE_DIGEST \
  --image-pull-secret YOUR_PULL_SECRET > workload-policy-disabled.json
```

Install queue-access-crd.json, workload-routing-crd.json and nodepool-policy-crd.json
as prerequisites; review generated RBAC, TLS secret and cert-manager configuration.
This renderer does not create certificates or grant broad Secret access. It uses
get-only controller reads and resourceNames-scoped ConfigMap Roles for registries.
Reuse the existing scoped certificate renewal/reloader approach with a distinct
certificate/Secret for this service. Both validators must be ready before enabling
mutation or legacy skips. The renderer does NOT enable a production webhook.

## PyTorch bridge when taking over isolation

Do not turn off legacy pipeline isolation while new PyTorchJobs are still created
only through the old `/mutate` path: those roots have no v2 frozen decision.
Before that isolation handoff, move the reviewed PyTorch admission scope to the
new mutate/validate-workload paths, keep its existing rules/QAP references, and
add the correct target poolRef and NodePoolPolicy acceptedRoutes. Existing v1
PyTorch roots enter the UID compatibility snapshot; newly submitted roots obtain
v2 decisions and their Trainer-created Pods inherit them through the verified
Trainer service-account identity. Keep ordinary and PyTorch policy workloadKinds
disjoint. Re-run the existing batch/default/ACL previews before expansion.

## Kyverno handoff and rollback

`prepare-workload-handoff.py` reads local snapshots only. It emits UID+spec guarded
patches and a root UID review list. Inputs must include the ancestor controllers,
not just Pods/ReplicaSets. It preserves existing registry entries and excludes
already-v2 roots from new grandfather entries.

```sh
python3 installer/pipeline/prepare-workload-handoff.py \
  --policies policies-before.json --workloads workloads-before.json \
  --registry registry-before.json --namespace infra-group \
  --protected-queue infra-ego-pipeline-base \
  --protected-queue infra-ego-pipeline-shared --output reviewed-handoff
```

The skip precondition is restricted to the reviewed namespaces AND v2 marker.
Outside that scope a copied marker cannot skip Kyverno. Never install skips without
active new validators. Keep security/identity policies unchanged.

Order: snapshot -> add reviewed legacy registry entries -> deploy disabled service
-> dry-run-only gate and compare -> enable routing validators/mutators in a small
scope -> re-snapshot unmarked legacy roots created during preparation and merge
those UIDs -> enable isolation -> guarded legacy handoff -> positive/negative
dry-runs -> expand. The second snapshot closes the preparation window: once
routing is enforced, newly created supported roots acquire v2 decisions instead
of silently joining the legacy cohort.
Multiple admission configurations are not an atomic transaction. During overlap,
a raw Pod can be conservatively rejected if old isolation adds a conflicting
constraint before new routing. Do not silently remove user affinity to hide it;
complete the scoped handoff and verify before accepting that route in production.

Safe stop: set WorkloadRoutingPolicy.mode=Disabled while keeping the same scope,
controller identities, pool policies, validators and scoped legacy skips alive.
New roots return to the legacy/default path; existing enrolled roots still recreate
with their frozen decision. Do not delete the policies/service or remove namespace
scope while v2 controllers remain. Full uninstall/legacy reverse patches require
an inventory proving no dependent v2 controllers or active children remain, or an
explicit workload migration. Generated reverse patches are not an unconditional
permission to remove protection.

## Failure and observability

API reads are bounded; failures deny relevant requests rather than silently
allowing protected-pool access. Outside-scope requests are not sent to these
webhooks. Config changes and API reads are not a cross-resource transaction;
concurrent policy edits may cause a conservative retry/rejection. Existing Pods
are not touched. Do not revoke policy-write permissions by deleting policy objects.

`/metrics` exposes bounded per-endpoint allow/deny counters. Denials log namespace,
kind/name and reason (no workload payload or Secrets). Preview/Audit return warnings.
This first version has no background status/Ready reconciler or durable audit DB;
configuration health is verified by schema checks and admission probes.

See `test/e2e/workloadpolicy/VALIDATION.md` for local evidence and boundaries.
