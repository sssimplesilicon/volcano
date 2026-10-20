> 此文保留历史分服务发布流程。当前统一服务请先阅读
> [UNIFIED-WORKLOAD-POLICY.md](UNIFIED-WORKLOAD-POLICY.md)，不要直接套用旧版退役/回滚顺序。

# Workload routing: infra-group production canary

This deploys a **new independent webhook**, not a replacement scheduler image.
No native/custom preemption actions, queue quotas, queue ACLs, or existing task
specs are changed. Use the release's **routing** image digest, not the old
scheduler image. Commands are for the operator to execute; no production changes
were made during development.

## 1. Set paths and capture the old routing policy

Use the release commit checkout as SRC. Keep this state directory for rollback.

```sh
: "${CTX:?set production context explicitly}"
: "${ROUTING_IMAGE:?set the published routing image digest}"
export SRC="$HOME/volcano-workload-routing-release"
export ROUTING_STATE="$HOME/volcano/workload-routing-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$ROUTING_STATE"

kubectl --context "$CTX" get clusterpolicy arena-pytorch-independent-routing \
  -o json > "$ROUTING_STATE/legacy-before.json"
kubectl --context "$CTX" get queueaccesspolicies.scheduling.simplesilicon.com \
  -o json > "$ROUTING_STATE/queue-access-before.json"

python3 "$SRC/installer/pipeline/prepare-routing-handoff.py" \
  --legacy "$ROUTING_STATE/legacy-before.json" --output "$ROUTING_STATE"

kubectl --context "$CTX" -n volcano-system get issuer ego-queue-access-ca
kubectl --context "$CTX" -n volcano-system get secret acr-credential-secret-aggregation -o name
kubectl --context "$CTX" get nodes -l volcano.sh/nodegroup-name=general
```

Require CA issuer Ready and at least two eligible general nodes (the webhook has
hard pod anti-affinity). The shared QueueAccessPolicy must still include
infra-group, data-group, intern-group and infra-ego-pipeline. Do not remove intern.
Inspect any unexpected existing WorkloadRoutingPolicy before proceeding; do not
create policies with overlapping namespace scopes.

## 2. Install CRD, dedicated serving certificate and disabled infrastructure

```sh
kubectl --context "$CTX" apply \
  -f "$SRC/installer/pipeline/workload-routing-crd.json"
kubectl --context "$CTX" wait --for=condition=Established \
  crd/workloadroutingpolicies.scheduling.simplesilicon.com --timeout=60s
kubectl --context "$CTX" get workloadroutingpolicies

kubectl --context "$CTX" create \
  -f "$SRC/installer/pipeline/workload-routing-certificate.yaml"
kubectl --context "$CTX" -n volcano-system wait --for=condition=Ready \
  certificate/workload-routing-serving --timeout=180s

python3 "$SRC/installer/pipeline/render-routing.py" \
  --policy "$SRC/installer/pipeline/workload-routing-example.json" \
  --image "$ROUTING_IMAGE" \
  --image-pull-secret acr-credential-secret-aggregation \
  > "$ROUTING_STATE/routing-disabled.json"

kubectl --context "$CTX" create --dry-run=server --validate=strict \
  -f "$ROUTING_STATE/routing-disabled.json" -o name
kubectl --context "$CTX" create -f "$ROUTING_STATE/routing-disabled.json"
kubectl --context "$CTX" -n volcano-system rollout status \
  deployment/workload-routing --timeout=180s
```

`create` intentionally stops if a resource already exists. Inspect an interrupted
installation rather than blindly replacing live policy. The example enrolls only
infra-group. New certificate Secret is workload-routing-tls; existing
queue-access CA/serving certificates are not replaced. Its label reuses the
installed Reloader filter; the new Deployment explicitly references only this
new Secret. Existing ego-queue-access reload annotation stays unchanged.

```sh
kubectl --context "$CTX" -n volcano-system get pods -l app=workload-routing -o wide
kubectl --context "$CTX" -n volcano-system get endpointslices \
  -l kubernetes.io/service-name=workload-routing
kubectl --context "$CTX" get mutatingwebhookconfiguration workload-routing -o json \
  > "$ROUTING_STATE/webhook-disabled.json"
kubectl --context "$CTX" -n volcano-system get secret workload-routing-tls \
  -o jsonpath='{.data.ca\.crt}' > "$ROUTING_STATE/serving-ca.b64"
kubectl --context "$CTX" get mutatingwebhookconfiguration workload-routing \
  -o jsonpath='{.webhooks[0].clientConfig.caBundle}' > "$ROUTING_STATE/webhook-ca.b64"
test -s "$ROUTING_STATE/serving-ca.b64" && \
  cmp "$ROUTING_STATE/serving-ca.b64" "$ROUTING_STATE/webhook-ca.b64"
```

Require both replicas Ready on different nodes, endpoints ready, matching nonempty
CA, namespaceSelector limited to infra-group and rollout-enabled=false. A ready
pod alone does not prove API-server TLS connectivity; next step does.

## 3. Install marker handoff and enable dry-run only

The guarded patch only adds a precondition to the three **Arena routing** rules:
roots already stamped by the new router skip legacy business routing. Unstamped
roots retain legacy behavior. No safety policies or controller-ownership adapter
are edited. If legacy runs first, the new router overwrites its route. If the new
router runs first, legacy skips the stamped object. The final result is therefore
independent of webhook ordering. This handoff was tested with real local Kyverno.
Do not add a global namespace exclusion to legacy routing for this procedure.

```sh
kubectl --context "$CTX" patch clusterpolicy arena-pytorch-independent-routing \
  --type=json --patch-file "$ROUTING_STATE/legacy-handoff-forward.json" \
  --dry-run=server -o name
kubectl --context "$CTX" patch clusterpolicy arena-pytorch-independent-routing \
  --type=json --patch-file "$ROUTING_STATE/legacy-handoff-forward.json"

kubectl --context "$CTX" patch mutatingwebhookconfiguration workload-routing \
  --type=json -p='[
    {"op":"test","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-enabled","expression":"false"}]},
    {"op":"replace","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-enabled","expression":"has(request.dryRun) && request.dryRun == true"}]}
  ]'

python3 "$SRC/installer/pipeline/verify-routing-preview.py" \
  --context "$CTX" --namespace infra-group \
  --output "$ROUTING_STATE/previews"
```

All four previews must pass. They create no task and request no GPU. Expected:
legacy allocentric/old-5880 -> shared/pipeline; new-5880 -> default/general;
explicit other-pipeline with allocentric -> default/general (no legacy collision).
Each result must include arena-pytorch/rule provenance. If tests fail, disable
new admission using rollback below and investigate; do not activate real requests.
The current infra-group ownership adapter still runs: a production preview is
mandatory even after local tests passed.

## 4. Activate infra-group only

```sh
kubectl --context "$CTX" patch mutatingwebhookconfiguration workload-routing \
  --type=json -p='[
    {"op":"test","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-enabled","expression":"has(request.dryRun) && request.dryRun == true"}]},
    {"op":"replace","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-enabled","expression":"true"}]}
  ]'
python3 "$SRC/installer/pipeline/verify-routing-preview.py" \
  --context "$CTX" --namespace infra-group --output "$ROUTING_STATE/active-previews"
```

No existing workloads are restarted. New infra-group PyTorchJob roots get the new
route; other namespaces still use old routing. In particular, this canary does
NOT fix unauthorized batch routing in algo-group yet. Expand scope only after
canary validation, by updating both the CRD and generated webhook scope from the
same source. FailurePolicy Fail blocks enrolled CREATE PyTorchJobs if this service
is unavailable. UPDATE and child Pod creation do not call this webhook.

Preserved user nodeSelector/affinity can conflict with default/general after
fallback; this is not silently rewritten. Existing queue validator/scheduler ACLs
still apply. No preemption is enabled by this rollout.

## 5. Rollback (also usable during dry-run)

Disable new admission first. New unstamped submissions immediately fall back to
legacy routing. Then restore the legacy spec using the guarded original backup.

```sh
kubectl --context "$CTX" patch mutatingwebhookconfiguration workload-routing \
  --type=json -p='[
    {"op":"test","path":"/webhooks/0/matchConditions/0/name","value":"rollout-enabled"},
    {"op":"replace","path":"/webhooks/0/matchConditions/0/expression","value":"false"}
  ]'
kubectl --context "$CTX" patch clusterpolicy arena-pytorch-independent-routing \
  --type=json --patch-file "$ROUTING_STATE/legacy-handoff-rollback.json"
```

If the reverse patch test fails, someone changed the legacy policy after the
snapshot. Keep new admission disabled and review the diff; never force-replace
that policy. Preserve the new CRD and existing workloads/templates. Do not delete
queue ACLs, queues, certificates, or change the scheduler to perform this rollback.
No live pod migration is part of rollback. Removing namespace authorization may
still prevent an existing task's replacement Pods from scheduling: preservation
of route templates is not a grandfathered permission grant.
