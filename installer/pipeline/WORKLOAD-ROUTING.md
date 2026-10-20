# PyTorchJob workload routing (opt-in)

This independent mutating webhook replaces **only** Arena/PyTorchJob business
routing. It does not replace Kyverno security policies, migrate running jobs,
change queue quotas, or enable preemption. No production rollout is included.

## Policy model

`WorkloadRoutingPolicy` is cluster scoped. A single object can contain multiple
pipeline rules. It has:

- `namespaceScope.include`: explicit namespaces, or `"*"`; `exclude` always wins.
  This is rollout scope, not queue permission. Outside scope is unchanged.
- `schedulerName`: scheduler applied to every replica template.
- `rules[].match.labels`: exact label map, all entries must match. Use
  `pipeline.simplesilicon.com/name` to identify a pipeline.
- `rules[].match.batchValues`: optional allowed effective batch values, ANDed
  with label conditions. Root `metadata.labels.batch` takes precedence; absent
  or empty falls back to replica `nodeSelector.batch`. All replicas must have
  identical selector batch values (missing counts as empty), even with a root label.
- `rules[].match.legacyBatch`: compatibility for ego's old batch-only input.
  Matches ONLY when the explicit pipeline label is absent/empty. Thus an explicit
  video pipeline with `old-5880` never accidentally selects ego.
- `queueAccessPolicyRef`: must equal `target.queue`; reads the existing
  QueueAccessPolicy allowedNamespaces. No duplicate namespace ACL.
- `target`: `queue` and `nodeGroup`; root schedulingPolicy and all replica queue
  annotations, schedulerName and nodegroup selector are patched together.
- `fallback`: default/general for no matching rule or unauthorized namespace.
  Fallback is also checked against QueueAccessPolicy if it is managed.

Missing or terminating referenced QueueAccessPolicy is a configuration error,
NOT authorization and NOT silent fallback. Intentionally empty allowedNamespaces
means deny all and therefore select fallback. Two matching rules are rejected
before checking permission; list order does not choose a winner. Overlapping
namespace scopes in two policy objects are rejected, including during deletion.
Use one policy with multiple pipeline rules for a shared rollout scope.

The example `workload-routing-example.json` contains explicit ego and legacy ego
rules. Add a rule with a distinct pipeline label and its own queue/access policy
for another pipeline. Reuse a single webhook deployment for all pipelines.

## Preservation and limitations

Only `CREATE kubeflow.org/v1/PyTorchJob` root objects are routed. UPDATE,
subresources, controller-owned roots and roots marked
`routing.example.org/controller=old` are untouched. A recreated root with a NEW
Kubernetes UID is a new submission and uses current rules. The stamped template
of an existing root is not recalculated when policies change; child Pods retain
that template, including after deletion/recreation. User annotations, resources,
tolerations, CPU/memory requests and other node constraints are preserved.

Provenance is stored on root and templates as annotation
`scheduling.simplesilicon.com/workload-routing-policy: policy/rule` (or
`policy/fallback`). A supplied provenance annotation does not bypass admission.
The existing `training-routing-version=v1` marker is also set for compatibility.

**Preserving old templates does not grandfather queue authorization.** If an ACL
is revoked, an existing job's recreated Pods can still be rejected by the queue
validator or held pending by queue-access scheduling checks. Routing never restores
revoked access. Existing Pods are not evicted by this component.

Falling back to general preserves user batch/affinity. If those constraints only
match pipeline nodes, the job may be Pending. This webhook does not silently
broaden user placement constraints. It does not prove that a queue/nodegroup has
capacity or even exists; operators must provision and validate target resources.

It is not an authentication replacement: root labels/old-controller markers rely
on the existing submission/ownership controls. Queue access validation and the
scheduler's ACL checks remain the protection against bypass attempts.

## Build and local verification

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o _output/pipeline/workload-routing-webhook ./cmd/workload-routing-webhook
docker build -t workload-routing:local \
  -f installer/pipeline/Dockerfile.routing _output/pipeline
```

Import the image into the local k3s server (test deployment is pinned to that
control-plane node). Run:

```sh
go test ./pkg/webhooks/admission/workloadrouting ./cmd/workload-routing-webhook
ROUTING_E2E_KUBECONFIG=/absolute/path/to/local/kubeconfig \
ROUTING_E2E_OUTPUT=/absolute/path/to/evidence \
python3 test/e2e/workloadrouting/run.py
```

The runner refuses any endpoint other than `https://127.0.0.1:16473`; it never
uses the current context. Requires existing PyTorchJob CRD and local k3s setup.
It creates only wr-e2e namespaces/resources, a suspended root, and a ReplicaSet
whose Pods remain Pending on synthetic nodegroup selectors. No GPU allocation.
The unrelated local migration adapter is temporarily excluded from those test
namespaces and restored on exit. No production operations.

## Deployment preparation (not applied automatically)

1. Apply the routing CRD and create/verify queue access policies and target queues.
2. Prepare a dedicated TLS Secret and Certificate with Service DNS names. Current
   server reads TLS at startup; arrange certificate renewal and scoped Reloader
   as with queue-access. Never reuse a certificate lacking the new Service SAN.
3. Render from the policy as the single source of namespace scope:

   ```sh
   python3 installer/pipeline/render-routing.py \
     --policy installer/pipeline/workload-routing-example.json \
     --image REGISTRY/IMAGE@sha256:DIGEST > routing-disabled.json
   ```

   Generated admission is **disabled**. RBAC is read-only list/get on the two
   CRDs, no eviction, workload mutation or Kubernetes Secret read permission.
   TLS is mounted as a Secret. Add site-specific imagePullSecrets and node
   placement before deployment. Two distinct eligible nodes are required by
   webhook anti-affinity. Keep infrastructure namespaces excluded from business
   routing/isolation mutation as appropriate; do not disable safety policies.
4. Use `prepare-routing-handoff.py` to add the guarded ownership-marker
   precondition to the OLD Kyverno **PyTorchJob routing** rules. New-router-stamped
   roots skip legacy routing. If legacy runs first, the new router overwrites its
   route; if new runs first, legacy skips it. Unstamped requests retain old behavior.
   Do not modify unrelated security rules or the controller ownership adapter.
   See `WORKLOAD-ROUTING-ROLLOUT.md` for the tested handoff sequence.
5. Verify CA injection and ready endpoints, then enable only dry-run requests,
   exercise allowed/unauthorized/conflicting rules, and enable real requests last.

Changes to CRD namespace scope must also regenerate/review the webhook
namespaceSelector from the same file; it is deliberately not auto-expanded by a
controller. Narrow webhook scope is essential for fault isolation. Merely checking
scope inside Go would still make excluded submissions depend on a failing service.
Rendered configuration supports one multi-pipeline policy; do not overwrite the
same webhook with different renderings for multiple policy objects.

## Rollback

Disable the dedicated MutatingWebhookConfiguration's `rollout-enabled` condition
first. Preserve the CRD and existing stamped templates. Restore the old Kyverno preconditions using the guarded original backup.
Unstamped new submissions already use legacy routing once the new webhook is disabled. Do not delete workloads, queue ACLs or scheduling
configuration. FailurePolicy Fail means only enrolled CREATE PyTorchJob requests
are blocked during outage, until the condition is disabled. Existing root updates
and child recreation do not call this webhook.
