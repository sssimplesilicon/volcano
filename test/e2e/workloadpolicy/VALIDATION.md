# Unified workload-policy local validation — 2026-10-20

Only the local k3s API `https://127.0.0.1:16473` was used. The tests made no production reads or
writes, production deployment, or preemption enablement. Registry publication is
a separate release step.

## Results

| Suite | Result |
| --- | --- |
| Unified admission + real controller + Arena CLI E2E | 77 checks passed |
| Go race: workloadpolicy, workloadrouting, queueaccess plugin/webhook | Passed |
| Both command packages | Compiled |
| Offline bundle and guarded image upgrade contracts | 5 checks passed |
| Post-run cleanup | Gates disabled, test Pods/queue removed, temporary adapter exclusion and Kyverno fixtures removed |

The E2E uses a single webhook backend for both ordinary workloads and PyTorch.
Its ServiceAccount has no QueueAccessPolicy read permission. Unit tests exercise
the real API loader with that API unavailable. Legacy queueAccessPolicyRef fields
warn but do not block the handoff or affect queue selection.

## Covered behaviors

- Pod, Deployment, StatefulSet, Job, CronJob, Workflow and PyTorchJob admission.
- default/base/shared/cpu, multiple pipelines, explicit selection, ambiguous
  policy/rule/pool rejection, CPU-route GPU rejection and node-pool isolation.
- `/mutate` compatibility URL pointed to the same unified engine.
- allocentric/old-5880 -> shared, new-5880 -> default, in two business namespaces.
- Label precedence, selector.batch fallback, inconsistent replica selector denial.
- **Actual Arena CLI** (explicit local kubeconfig), three batch submissions and
  real Trainer child Pods with the expected queue/frozen decision.
- Historical v1 PyTorch job and historical Deployment: delete a test Pod and
  verify the replacement is not enrolled and retains scheduling fields.
- Newly enrolled PyTorch and Deployment: delete a test Pod and verify the new UID
  inherits the original frozen route. Real StatefulSet/Job/CronJob/Argo descendants.
- Policy edits, scale changes, image updates, immutable routing identity and
  forged-owner rejection. Concurrent admission requests.
- Real Kyverno compatibility fixtures, stale-patch rejection and outside-scope
  behavior. Existing unrelated policies/scheduler specs retained.
- System-namespace Pod/Deployment/PyTorch probes stay outside unified enrollment.
  Webhook outage rejects in-scope requests while system/outside scopes still work.
- Stop enrollment with Disabled mode: new roots are not enrolled; already-enrolled
  Deployment and PyTorch descendants still rebuild with their frozen decisions.
- Active scheduler ConfigMaps unchanged, preemption actions absent; all real test
  Pods request zero GPUs. Cleanup verified after callbacks finish.

## Release rerun

The complete 77-check suite was rerun before publication using the canonical
`Dockerfile.workload-policy`, including both executable paths in the image. Go
race checks and the five offline deployment contract tests also passed. Release
evidence: `outputs/unified-workload-policy-release-e2e/results.json`. A stale
call-site signature caught by the initial release build was resolved before the
successful build and full rerun.

## Evidence

Workspace artifacts (outside the repository):

- `outputs/unified-workload-policy-e2e/results.json`: all 77 named checks.
- `outputs/unified-workload-policy-e2e/cleanup.json`: local endpoint and final cleanup assertions.
- `outputs/unified-workload-policy-e2e/routing-before.json`: unrelated policy baseline.

Earlier attempts exposed fixture issues: the second namespace was still subject
to the existing local Trainer migration gate; real Trainer needed a test Queue;
the system PyTorch probe included suspend unsupported by the old Operator. The
fixtures were corrected, their temporary changes restored, and the complete final
suite passed. These were not production changes.

## Reproduction

Requires the dedicated local k3s environment, local Kyverno, the existing local
Argo demo controller configuration, the Trainer test controller and Arena CLI.
The runner refuses every endpoint except `https://127.0.0.1:16473`.

```sh
GOCACHE=/tmp/volcano-unified-go-cache go test -race \
  ./pkg/webhooks/admission/workloadpolicy \
  ./pkg/webhooks/admission/workloadrouting \
  ./pkg/scheduler/plugins/queueaccess \
  ./pkg/webhooks/admission/queueaccess

python3 -m unittest discover -s test/e2e/workloadpolicy -p 'test_*.py' -v

mkdir -p _output/unified-release/arm64
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o _output/unified-release/arm64/workload-policy-webhook ./cmd/workload-policy-webhook
docker build --platform linux/arm64 -t workload-policy:local \
  -f installer/pipeline/Dockerfile.workload-policy _output/unified-release
# Import this image into every node of the dedicated LOCAL k3s before running.
WORKLOAD_POLICY_E2E_KUBECONFIG=/path/to/local-k3s-kubeconfig \
WORKLOAD_POLICY_E2E_OUTPUT=/path/to/local-evidence \
  python3 test/e2e/workloadpolicy/run.py
```

`test/e2e/workloadrouting/run.py` delegates to this suite. Its former ACL-based
expectations describe retired behavior, not current acceptance criteria.

## Limits

Worker Pods use a deliberately unmatched node selector and zero GPU requests.
The suite verifies admission, real controller creation/recreation and rollback;
it does not verify physical GPU placement, utilization, packing or preemption.

The local migration is not a production rollout rehearsal against current live
snapshots. Production cutover still needs reviewed controller identities, legacy
UID registry, selectors/CA bundles, actual mounted scheduler ConfigMap and guarded
patches. Both old namespace-ACL execution points must be retired separately.
Shared CA/Issuer/Reloader resources must remain. See
`installer/pipeline/UNIFIED-WORKLOAD-POLICY.md` for the staged migration and the
boundary between stopping new enrollment and fully reverting an installed image.
