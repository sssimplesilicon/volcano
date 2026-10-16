# Optional pipeline preemption (v1.15.3)

This downstream branch starts at Volcano v1.15.3. It statically links the v7
worker-type preemption extension and its budget controller. No Go `.so` loader,
initContainer, or CGO runtime is required. Upstream scheduling algorithms and
the upstream default configuration are retained.

## Activation

The scheduler flag `--enable-worker-type-preemption` defaults to **false**.
Without it, the extension is not registered and custom CRDs are not required.
Ordinary upstream scheduler configurations still work, including upstream native
preemption configurations. Merely installing the image does not enable custom
preemption.

To enable the extension, all of the following are required:

1. Install `budget-crds.json` and run `/budget-controller` with reviewed RBAC.
2. Configure PipelinePolicy and WorkerBudget resources; see
   `pipeline-example.yaml`. Use namespace + pipeline + worker-type identity.
3. Start a single active scheduler with `--enable-worker-type-preemption=true`.
4. Explicitly configure the `worker-type-preemption` plugin and
   `worker-type-preempt` action, with `pipelineBudgets: true` in both blocks.
5. Provide a persistent recovery ConfigMap and Eviction/PDB/budget RBAC.

`test/e2e/pipeline/infrastructure.json` is the complete **local test fixture**.
It is not a production manifest: it includes a fault proxy, test queues and
short recovery timeouts. Do not install it into production.

Custom configuration without opt-in is rejected at startup. Hot reloads that
violate the opt-in or mix custom preemption with native `preempt`/`reclaim`
actions are rejected while retaining the previous configuration. An enabled
plugin with the custom action removed is supported for controlled shutdown.

## Semantics

Only GPU-demanding training triggers this eviction path. Candidate shared
workers are ranked by GPU allocation aggregated by worker type; allocation is
re-evaluated after selections. Base workers are protected. Namespaced pipeline
budgets account for base and shared, and enforce minimum available GPU counts.
Native PDBs provide an additional replica floor through the Eviction API.

Reservations and recovery records cover concurrent requests, partial success,
timeouts and scheduler restart. Cancelled training stops outstanding recovery
from initiating further evictions. Already accepted Evictions cannot be undone.
Budgets and PDBs do not prevent node failures, direct Pod deletion or arbitrary
external controllers from removing workers.

This version supports one active custom scheduler. Do not combine it with an
independent eviction path acting on the same protected workers. Multi-active
scheduler safety is not claimed.

## Build and local verification

`build-local.sh` builds scheduler and controller with CGO disabled. Set
`TARGETARCH=amd64` for x86 nodes or `TARGETARCH=arm64` for ARM nodes. Build a
runtime image from `_output/pipeline` using the adjacent Dockerfile.

```sh
./installer/pipeline/build-local.sh
docker build -f installer/pipeline/Dockerfile \
  -t volcano-ss:v1.15.3-ss.1-local _output/pipeline
python3 -m unittest discover -s test/e2e/pipeline -p test_offline.py
```

The E2E entry points require an explicit `PIPELINE_E2E_KUBECONFIG` and
`--execute-local`; they reject endpoints other than the dedicated local k3s
`https://127.0.0.1:16473`. Run default-off comparisons before installing custom
CRDs. Run budget-mode and legacy-mode suites separately. Evidence is written to
ignored `test/e2e/pipeline/evidence/` directories. Tests use simulated GPU nodes;
they do not establish physical GPU, wuying, business database or HA correctness.

## Upgrade / rollback

Track upstream releases as an upstream remote, carry this extension in a small
set of downstream commits, and rebuild against each target release. Re-run Go,
default-off, budget and legacy E2E before adopting an upstream version.

For an enabled deployment, first stop routing new preemption demands to this
scheduler. Keep the custom action, plugin and controller running until the
recovery journal is empty and no Evictions remain unresolved. Then remove the
custom action and plugin, use a normal upstream scheduler configuration, and
remove the opt-in flag (or restore the official image). Removing the action
pauses its recovery processing; it is not a journal-draining operation.
Keep budget CRDs and recovery data until all recovery obligations are settled.
Disabling preemption does not recreate evicted Pods; their workload controllers
own replacement. Do not delete CRDs or unresolved recovery records to roll back.

The test-only fault proxy source is included at
`test/e2e/pipeline/faultproxy`. The fixture currently reuses the previously
validated local image tag `worker-type-prototype:v7-e2e-final`; only its
`/faultproxy` binary and `curl` are used, not its scheduler or `.so` files.
To reproduce on a fresh local cluster, build that directory's Go binary with
CGO disabled for the cluster architecture and use its Dockerfile with that tag.
