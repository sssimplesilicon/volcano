// Package workerscale maps an opt-in WorkerBudget desired GPU count to Deployment replicas.
// It never evicts, changes a Pod template, or changes a scheduler policy.
package workerscale

import (
	"context"
	"encoding/json"
	"fmt"
	autoscaling "k8s.io/api/autoscaling/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"math"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
	wp "volcano.sh/volcano/pkg/webhooks/admission/workloadpolicy"
)

type Observation struct {
	State           string `json:"state"`
	DesiredReplicas int32  `json:"desiredReplicas"`
	GPUPerReplica   int64  `json:"gpuPerReplica"`
	Message         string `json:"message,omitempty"`
}

func gpuPerReplica(spec core.PodSpec) (int64, error) {
	var total int64
	for _, c := range spec.InitContainers {
		rq, lq := c.Resources.Requests[core.ResourceName("nvidia.com/gpu")], c.Resources.Limits[core.ResourceName("nvidia.com/gpu")]
		if rq.Sign() != 0 || lq.Sign() != 0 {
			return 0, fmt.Errorf("GPU init containers are not supported for desiredGPU scaling")
		}
	}
	if q := spec.Overhead[core.ResourceName("nvidia.com/gpu")]; q.Sign() != 0 {
		return 0, fmt.Errorf("GPU overhead is not supported")
	}
	for _, c := range spec.Containers {
		q, ok := c.Resources.Requests[core.ResourceName("nvidia.com/gpu")]
		limit, hasLimit := c.Resources.Limits[core.ResourceName("nvidia.com/gpu")]
		if !ok {
			q = limit
		}
		v, exact := q.AsInt64()
		if !exact || v < 0 || !hasLimit && v != 0 || hasLimit && q.Cmp(limit) != 0 {
			return 0, fmt.Errorf("GPU requests/limits must be equal whole cards")
		}
		if v > math.MaxInt64-total {
			return 0, fmt.Errorf("GPU quantity overflow")
		}
		total += v
	}
	if total <= 0 {
		return 0, fmt.Errorf("desiredGPU requires a GPU worker; use replica management for CPU workers")
	}
	return total, nil
}
func Reconcile(ctx context.Context, c kubernetes.Interface, s *pb.Snapshot, b *pb.WorkerBudget, downscaleGuards ...func(context.Context) error) (Observation, error) {
	o := Observation{State: "Unmanaged"}
	if b.Spec.DesiredGPU == nil {
		return o, nil
	}
	o.State = "Blocked"
	ref := b.Spec.WorkloadRef
	if b.DeletionTimestamp != nil || ref == nil || ref.Kind != "Deployment" || ref.Name == "" || ref.UID == "" {
		return o, fmt.Errorf("desiredGPU requires a live budget and UID-bound Deployment workloadRef")
	}
	want := *b.Spec.DesiredGPU
	if want < 0 || want < b.Spec.MinAvailableGPU {
		return o, fmt.Errorf("desiredGPU must be >= minAvailableGPU >= 0")
	}
	for _, other := range s.Budgets {
		r := other.Spec.WorkloadRef
		if other.Namespace == b.Namespace && other.Name != b.Name && other.Spec.DesiredGPU != nil && r != nil && r.Kind == ref.Kind && r.Name == ref.Name {
			return o, fmt.Errorf("multiple budgets own Deployment %s", ref.Name)
		}
	}
	d, err := c.AppsV1().Deployments(b.Namespace).Get(ctx, ref.Name, meta.GetOptions{})
	if err != nil {
		return o, err
	}
	if d.UID != ref.UID || d.DeletionTimestamp != nil || d.Spec.Paused {
		return o, fmt.Errorf("Deployment replaced, terminating or paused")
	}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: b.Namespace, Labels: d.Spec.Template.Labels}, Spec: d.Spec.Template.Spec}
	if p.Labels[pb.PipelineLabel] != b.Spec.PipelineRef.Name || p.Labels[pb.TypeLabel] != b.Spec.WorkerType || p.Labels[pb.PipelineNamespaceLabel] != b.Namespace {
		return o, fmt.Errorf("Deployment worker identity differs from budget")
	}
	policy, _, err := s.Resolve(p)
	if err != nil {
		return o, err
	}
	if len(policy.Spec.AccountingQueues) != 1 || len(policy.Spec.PreemptibleQueues) != 1 || policy.Spec.AccountingQueues[0] != policy.Spec.PreemptibleQueues[0] {
		return o, fmt.Errorf("desiredGPU mode requires one accounting/preemptible queue")
	}
	var decision wp.Decision
	if err = json.Unmarshal([]byte(d.Spec.Template.Annotations[wp.DecisionKey]), &decision); err != nil {
		return o, fmt.Errorf("managed routing decision required: %w", err)
	}
	w := decision.Target.Worker
	if w == nil || w.Pipeline != b.Spec.PipelineRef.Name || w.Namespace != b.Namespace || w.Type != b.Spec.WorkerType || decision.Target.Queue != policy.Spec.AccountingQueues[0] || decision.Target.PriorityClassName == "" || decision.Target.PriorityClassName != p.Spec.PriorityClassName || decision.Scheduler != p.Spec.SchedulerName || p.Labels["scheduling.simplesilicon.com/preemptible"] != "true" || d.Spec.Template.Annotations["scheduling.volcano.sh/queue-name"] != decision.Target.Queue {
		return o, fmt.Errorf("Deployment is not enrolled in the managed worker route")
	}
	per, err := gpuPerReplica(p.Spec)
	if err != nil {
		return o, err
	}
	o.GPUPerReplica = per
	if want%per != 0 || want/per > math.MaxInt32 {
		return o, fmt.Errorf("desiredGPU must be an exact, supported multiple of per-replica GPUs (%d)", per)
	}
	o.DesiredReplicas = int32(want / per)
	hpas, err := c.AutoscalingV2().HorizontalPodAutoscalers(b.Namespace).List(ctx, meta.ListOptions{})
	if err != nil {
		return o, err
	}
	for _, h := range hpas.Items {
		r := h.Spec.ScaleTargetRef
		if r.Kind == "Deployment" && r.Name == ref.Name {
			return o, fmt.Errorf("HPA already manages this Deployment")
		}
	}
	current := int32(1)
	if d.Spec.Replicas != nil {
		current = *d.Spec.Replicas
	}
	if current == o.DesiredReplicas {
		o.State = "Reconciled"
		return o, nil
	}
	if o.DesiredReplicas < current {
		// Ordinary scale-down bypasses PDB. Do not race it with the eviction path.
		if policy.Spec.AllowedPreemptorNamespaces == nil || len(*policy.Spec.AllowedPreemptorNamespaces) != 0 {
			o.Message = "downscale deferred until preemption authorization is explicitly empty"
			o.State = "DownscaleDeferred"
			return o, nil
		}
		if d.Status.ObservedGeneration < d.Generation || d.Status.Replicas != current || d.Status.ReadyReplicas != current || d.Status.UpdatedReplicas != current {
			o.Message = "downscale deferred until current replicas are updated and Ready"
			o.State = "DownscaleDeferred"
			return o, nil
		}
		if len(downscaleGuards) != 1 {
			o.State, o.Message = "DownscaleDeferred", "recovery journal guard not configured"
			return o, nil
		}
		if err := downscaleGuards[0](ctx); err != nil {
			o.State, o.Message = "DownscaleDeferred", err.Error()
			return o, nil
		}
	}
	// A Deployment resourceVersion is also its /scale resourceVersion: fail on concurrent edits.
	_, err = c.AppsV1().Deployments(b.Namespace).UpdateScale(ctx, d.Name, &autoscaling.Scale{ObjectMeta: meta.ObjectMeta{Name: d.Name, Namespace: d.Namespace, ResourceVersion: d.ResourceVersion}, Spec: autoscaling.ScaleSpec{Replicas: o.DesiredReplicas}}, meta.UpdateOptions{})
	if err != nil {
		return o, err
	}
	o.State = "Scaled"
	return o, nil
}
