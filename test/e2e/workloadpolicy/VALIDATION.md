# Local validation — 2026-10-20

This implementation was tested only against local k3s at
`https://127.0.0.1:16473`. Both E2E runners refuse another API endpoint.
No production operation, image publication, or preemption enablement was performed.

## Results

| Suite | Result |
| --- | --- |
| New ordinary-workload routing and node-pool isolation E2E | 47 checks passed |
| Existing PyTorch routing E2E against the same new binary | 38 checks passed |
| `go test -race` for workloadpolicy, workloadrouting and queueaccess | Passed |

The 85 E2E checks include:

- Pod, Deployment, StatefulSet, Job, CronJob, Workflow and PyTorchJob admission.
- Explicit base/shared/cpu/default routes, legacy batch fallback, namespace
  exclusion and multiple pipelines; explicit pipeline selection overrides legacy
  compatibility.
- Actual Deployment → ReplicaSet → Pod inheritance and replacement, and real
  StatefulSet, Job, CronJob and independently namespaced Argo controllers.
- Policy/rule/pool conflicts, missing referenced pools, Preview mode, and
  concurrent admission requests.
- CPU-only GPU rejection, direct node binding rejection, conflicting selectors,
  and exclusion in every required-affinity OR branch.
- Frozen routing decisions across policy changes and scale/image updates;
  rejection of routing changes on already-enrolled workloads.
- Historical controller recreation, actual owner UID checks, and rejection of
  forged ownership. Existing scheduler Deployment specs remain unchanged.
- Real Kyverno handoff fixtures, scoped guarded patches, and unrelated policy
  preservation.
- Webhook outage inside/outside scope, stopping new enrollment, and preserving
  recreation of previously enrolled workloads after that stop.
- Original PyTorch namespace/QueueAccessPolicy authorization, revocation,
  fallback, replica-selector consistency, legacy routing and guarded rollback.

Additional unit tests cover match-none affinity preservation, exact system
component exceptions, unsupported owner kinds, old-controller label spoofing,
Workflow reference/metadata checks, idempotence and API-read failures.

## Evidence

Local artifacts under the workspace `outputs` directory:

- `workload-policy-e2e/results.json`: 47 successful named checks.
- `workload-policy-pytorch-regression/results.json`: 38 successful named checks.
- `workload-policy-e2e/routing-before.json`: existing routing snapshot used by
  the runner.

After the final run, read-only checks confirmed all four new test webhook entries
and the old-interface test entry were disabled, temporary migration-webhook
exclusions were removed, temporary Kyverno fixtures were removed, and no workload
Pods remained in `wp-e2e-business`.

## Reproduction

Build `cmd/workload-routing-webhook` for the local node architecture, build
`installer/pipeline/Dockerfile.routing`, and import the resulting image into local
k3s as `workload-policy:local`. The PyTorch runner uses `workload-routing:local`;
tag/import the same image under that name for regression.

```sh
go test -race \
  ./pkg/webhooks/admission/workloadpolicy \
  ./pkg/webhooks/admission/workloadrouting \
  ./pkg/scheduler/plugins/queueaccess

WORKLOAD_POLICY_E2E_KUBECONFIG=/path/to/local-k3s-kubeconfig \
  python3 test/e2e/workloadpolicy/run.py

ROUTING_E2E_KUBECONFIG=/path/to/local-k3s-kubeconfig \
  python3 test/e2e/workloadrouting/run.py
```

The runners create test namespaces, RBAC, CRDs and webhook infrastructure.
The new runner requires local Kyverno, the existing local Argo demo controller
configuration, and a local image import. See the runner for exact fixture names.
Use the dedicated local test cluster; these are not production validation commands.

## Boundaries

Worker Pods have a deliberately unmatched test node selector and do not request
GPU resources. This validates admission and real controller creation/recreation,
not physical GPU scheduling, packing, quota exhaustion or preemption.

The first implementation supports pools expressed through one common label key
with distinct values. It does not support ClusterWorkflowTemplate references,
raw `podSpecPatch` or Workflow resource templates. Controller identities and
production system-component exclusions require review before rollout.

There is no automatic policy Ready/status reconciler or durable audit database.
The concurrency check is functional, not a production-scale load test.
Production migration must follow `installer/pipeline/WORKLOAD-POLICY.md`,
including the PyTorch bridge, reviewed legacy snapshot, disabled initial
webhooks and preserving enrolled workloads during rollback.
