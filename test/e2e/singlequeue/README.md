# Single-queue worker E2E

Local endpoint must be exactly `https://127.0.0.1:16473`. Set `PIPELINE_E2E_KUBECONFIG` explicitly; the runner never uses current context. It shares the pipeline suite lock and dedicated `worker-type-prototype` fixture. The fixture scheduler/recovery ConfigMap must already exist and its journal must be empty. No unrelated Pods may exist in the fixture namespace.

Build an ARM64 local image `volcano-single-queue:local` containing:

- `/vc-scheduler`: existing statically integrated scheduler
- `/budget-controller`: current pipeline-budget-controller
- `/pipeline-worker-controller`: current new controller
- `/workload-policy-webhook`: current unified webhook
- `/vc-controller-manager`: Volcano PodGroup controller

Import it into the local k3s control-plane container. The dedicated fault proxy image `worker-type-prototype:v7-e2e-final` is also required. The runner pins new controllers to the control-plane node and uses `imagePullPolicy: Never`; it does not push images.

```sh
PIPELINE_E2E_KUBECONFIG=/absolute/path/to/local-kubeconfig \
python3 test/e2e/singlequeue/run.py --execute-local --output /absolute/path/to/new-evidence-directory
```

The actual admission webhooks, Deployment controller, PodGroup controller, PDB controller, scale API, custom scheduler and Eviction API run in k3s. GPU capacity, kubelet readiness and completion of already-approved deletion are simulated on `wt-node-a/b`. This is not CUDA, GPU-driver, wuying, or application shutdown validation.

The dedicated scheduler uses capacity/nodegroup/binpack and an 8-GPU queue cap, matching the production plugin composition.

Assertions:

1. desiredGPU=8 creates eight 1-GPU worker Pods from a zero-replica Deployment, without business-side scheduler/queue/priority/preemptible fields.
2. No PDB: training occupies four GPUs, the worker GPU floor stays at four; subsequent demand remains Pending.
3. Lowering desiredGPU during authorized preemption defers ordinary Deployment downscale.
4. Removing training restores eight worker replicas automatically.
5. With preemption authorization explicitly empty and recovery journal drained, desiredGPU=6 converges.
6. Restarting the scale controller retains the same desired replica count.
7. Two worker types and three concurrent training requests: A's GPU floor=2 and B's PDB floor=3 (GPU floor=1) both hold. A fourth request cannot cross either protection.
8. Removing concurrent training restores both types to four replicas each.

Every run captures timeline, victims, journal and scheduler logs, and compares the original official scheduler's UID/spec before/after. Webhook/test workloads and synthetic nodes are cleaned up; the dedicated test scheduler is retained for legacy regression. Never erase a nonempty recovery journal to start a test.

For regression against the assembled local image, set `PIPELINE_E2E_IMAGE=volcano-single-queue:local` for the existing `test/e2e/pipeline/run.py` runner. The original image check remains the default when that variable is absent.

Task-group priority regression: a real local Trainer creates a one-GPU PyTorchJob
PodGroup from the injected `runPolicy.schedulingPolicy.priorityClass`. Worker
priority is positive (20), so the previous missing-PodGroup-priority bug (default
0) cannot accidentally pass. Authorization-off keeps all eight workers running;
authorization-on allows one eviction, and synthetic training completion restores
eight workers. The local migration adapter is excluded only for the fixture
namespace and its exact webhook configuration is restored on exit.
