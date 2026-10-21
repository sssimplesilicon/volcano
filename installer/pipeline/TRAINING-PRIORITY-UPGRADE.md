# PyTorchJob task-group priority repair

The route already assigned `spec.pytorchReplicaSpecs.*.template.spec.priorityClassName`, but did not assign `spec.runPolicy.schedulingPolicy.priorityClass`. The Kubeflow Trainer derives the Volcano PodGroup's `spec.priorityClassName` from the latter field. A high-priority Pod could therefore belong to a priority-zero job and fail the scheduler's cross-job priority check.

The unified workload-policy webhook now injects both fields from the frozen route's `target.priorityClassName`. Explicit conflicting values are rejected. Routes without a managed priority preserve the user's value. Already-enrolled roots without the new field may still be updated; the upgrade does not rewrite or recreate their Pods. Once explicitly repaired, deleting or changing the managed priority is rejected.

Only the workload-policy webhook image changes. No scheduler, worker controller, queue, routing CRD, namespace permission, GPU budget or worker Deployment change is needed. QueueAccessPolicy is unrelated to this fix; current unified routing ignores it and the deployed scheduler's queueAccess switch is off. PipelinePolicy.allowedPreemptorNamespaces remains the separate preemption authorization.

## Operator commands

Use the released multi-architecture webhook image **digest** supplied with this release. Do not use a scheduler-only image. Commands below are for the existing one-GPU test task `infra-group/ego-prod-preempt-210446`; they create no GPU workload.

First close the existing test window. This stops new preemption attempts; it cannot reverse evictions already accepted by the API server.

```sh
: "${CTX:?Set production context}"
: "${TEST_STATE:?Keep the existing ego-prod test state directory}"
export TEST_TOOL='/Users/zoisite/Documents/Codex/2026-10-10/volcano/outputs/ego-prod-preemption-test/test-window.py'
python3 "$TEST_TOOL" stop --context "$CTX" --state "$TEST_STATE"

export VOLCANO_REPO='/Users/zoisite/Documents/Codex/2026-10-10/volcano/work/volcano-ss'
git -C "$VOLCANO_REPO" pull --ff-only origin ss/v1.15.3-pipeline-preemption
export PG_TOOL="$VOLCANO_REPO/installer/pipeline/upgrade-training-priority.py"
export PG_STATE="$HOME/volcano/training-priority-$(date +%Y%m%d-%H%M%S)"
# Set PG_IMAGE to the published immutable digest from the release instructions.
: "${PG_IMAGE:?Set the released workload-policy image digest}"

python3 "$PG_TOOL" prepare --context "$CTX" --state "$PG_STATE" --image "$PG_IMAGE"
python3 "$PG_TOOL" upgrade --context "$CTX" --state "$PG_STATE"
python3 "$PG_TOOL" repair --context "$CTX" --state "$PG_STATE"
python3 "$PG_TOOL" verify --context "$CTX" --state "$PG_STATE"
```

Run sequentially and stop on an error. `prepare` is read-only and stores local snapshots. All actions require empty preemption authorizations and an empty recovery journal. The script never clears recovery records. Upgrade changes only the webhook container image using UID/template guards and server dry-run. Repair changes only the named PyTorchJob's task-group priority and its existing PodGroup's priority. It requires the original single-GPU Pod to remain Pending/unbound with the same UID and spec, and retains the 12-GPU desired / 6-GPU minimum budget. A different/completed/recreated test task must be inspected before preparing a new state directory.

Expected repair result:

```sh
kubectl --context "$CTX" -n infra-group get pytorchjob ego-prod-preempt-210446 \
  -o jsonpath='{.spec.runPolicy.schedulingPolicy.priorityClass}{"\n"}'
kubectl --context "$CTX" -n infra-group get podgroup ego-prod-preempt-210446 \
  -o jsonpath='{.spec.priorityClassName}{"\n"}'
```

Both should be `ego-prod-training` (PriorityClass value 100000). The worker class stays unchanged. Keep authorization closed until all checks pass. To resume the already-approved one-GPU test:

```sh
python3 "$TEST_TOOL" authorize --context "$CTX" --state "$TEST_STATE"
kubectl --context "$CTX" -n infra-group wait --for=condition=Ready \
  pod/ego-prod-preempt-210446-master-0 --timeout=180s
python3 "$TEST_TOOL" status --context "$CTX" --state "$TEST_STATE"
```

For one training GPU, the normal settled state is 11 ready one-GPU workers and one bound training Pod; the minimum remains 6. A timeout needs inspection, not repeated authorization. After training terminates, requests are released and replacement workers should restore 12 ready GPU. Use the existing test-window cleanup procedure to remove only this test job, revoke authorization, wait for the recovery journal to drain, and restore the original test route.

## Rollback

```sh
python3 "$TEST_TOOL" stop --context "$CTX" --state "$TEST_STATE"
python3 "$PG_TOOL" rollback-image --context "$CTX" --state "$PG_STATE"
```

This restores only the webhook image saved at prepare time. The corrected task/PodGroup priority is deliberately retained: it is valid with the old image, and removing it would restore the scheduling bug. No worker is deleted or scaled. If training already ran, stop authorization and let accepted evictions finish before rollback; journal checks will refuse an unsafe in-progress rollback. Rollback does not stop a running training Pod or restore its occupied GPU by itself.

## Validation scope

Go tests cover priority injection, conflicting inputs, unmanaged priorities, legacy enrolled root updates, explicit repair, and rejecting priority removal after repair. The local k3s single-queue suite uses the real webhook, Kubeflow Trainer, PodGroup controller, scheduler, Eviction API and worker controller. It uses synthetic GPU capacity/readiness/completion, not physical GPU execution. Positive worker priority reproduces the original cross-job comparison; authorization-off, single-GPU preemption, GPU floor, completion recovery, consecutive and concurrent training, optional PDB interaction and controller restart are checked. Production is not modified by these tests.
