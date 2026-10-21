// Package pipelinebudget defines the v1alpha1 policy contract and conservative
// accounting shared by the plugin and the read-only status controller.
package pipelinebudget

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"volcano.sh/volcano/pkg/scheduler/api"
)

const PipelineLabel = "pipeline.simplesilicon.com/name"
const PipelineNamespaceLabel = "pipeline.simplesilicon.com/namespace"
const TypeLabel = "scheduling.simplesilicon.com/worker-type"
const GroupPath = "/apis/scheduling.simplesilicon.com/v1alpha1"

type PipelinePolicy struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata"`
	Spec            struct {
		AccountingQueues           []string  `json:"accountingQueues"`
		PreemptibleQueues          []string  `json:"preemptibleQueues"`
		AllowedPreemptorNamespaces *[]string `json:"allowedPreemptorNamespaces,omitempty"`
	} `json:"spec"`
}
type WorkerBudget struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata"`
	Status          map[string]interface{} `json:"status,omitempty"`
	Spec            struct {
		PipelineRef struct {
			Name string `json:"name"`
		} `json:"pipelineRef"`
		WorkerType      string `json:"workerType"`
		ResourceName    string `json:"resourceName"`
		MinAvailableGPU int64  `json:"minAvailableGPU"`
		// Optional: an explicit UID-bound Deployment is managed only by the worker scale controller.
		DesiredGPU  *int64       `json:"desiredGPU,omitempty"`
		WorkloadRef *WorkloadRef `json:"workloadRef,omitempty"`
	} `json:"spec"`
}
type WorkloadRef struct {
	Kind string    `json:"kind"`
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}
type Pin struct {
	PipelineUID types.UID `json:"pipelineUID"`
	BudgetUID   types.UID `json:"budgetUID"`
	Identity    string    `json:"identity"`
}
type Snapshot struct {
	Policies []PipelinePolicy
	Budgets  []WorkerBudget
	Pods     []v1.Pod
	Queues   map[string]string
}

func Key(p *v1.Pod) string {
	if p == nil || p.Labels[PipelineLabel] == "" || p.Labels[TypeLabel] == "" {
		return ""
	}
	return p.Namespace + "/" + p.Labels[PipelineLabel] + "/" + p.Labels[TypeLabel]
}
func SamePipeline(a, b *v1.Pod) bool {
	return a != nil && b != nil && a.Namespace == b.Namespace && a.Labels[PipelineLabel] != "" && a.Labels[PipelineLabel] == b.Labels[PipelineLabel]
}
func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}
func Read(client kubernetes.Interface, path string, target interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := client.CoreV1().RESTClient().Get().AbsPath(path).SetHeader("Accept", "application/json").DoRaw(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, target)
}
func Load(client kubernetes.Interface) (*Snapshot, error) {
	s := &Snapshot{Queues: map[string]string{}}
	var ps struct {
		Items []PipelinePolicy `json:"items"`
	}
	var bs struct {
		Items []WorkerBudget `json:"items"`
	}
	if err := Read(client, GroupPath+"/pipelinepolicies", &ps); err != nil {
		return nil, err
	}
	if err := Read(client, GroupPath+"/workerbudgets", &bs); err != nil {
		return nil, err
	}
	s.Policies, s.Budgets = ps.Items, bs.Items
	var pg struct {
		Items []struct {
			meta.ObjectMeta `json:"metadata"`
			Spec            struct {
				Queue string `json:"queue"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := Read(client, "/apis/scheduling.volcano.sh/v1beta1/podgroups", &pg); err != nil {
		return nil, err
	}
	for _, g := range pg.Items {
		q := g.Spec.Queue
		if q == "" {
			q = "default"
		}
		s.Queues[g.Namespace+"/"+g.Name] = q
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pods, err := client.CoreV1().Pods("").List(ctx, meta.ListOptions{})
	if err != nil {
		return nil, err
	}
	s.Pods = pods.Items
	return s, nil
}
func (s *Snapshot) Queue(p *v1.Pod) string {
	return s.Queues[p.Namespace+"/"+p.Annotations["scheduling.k8s.io/group-name"]]
}
func (s *Snapshot) Resolve(p *v1.Pod) (*PipelinePolicy, *WorkerBudget, error) {
	if Key(p) == "" {
		return nil, nil, fmt.Errorf("missing pipeline/worker identity")
	}
	var policy *PipelinePolicy
	for i := range s.Policies {
		x := &s.Policies[i]
		if x.Namespace == p.Namespace && x.Name == p.Labels[PipelineLabel] && x.DeletionTimestamp == nil {
			policy = x
		}
	}
	if policy == nil {
		return nil, nil, fmt.Errorf("pipeline policy missing")
	}
	if len(policy.Spec.AccountingQueues) == 0 || len(policy.Spec.PreemptibleQueues) == 0 {
		return nil, nil, fmt.Errorf("empty queue scope")
	}
	for _, q := range policy.Spec.PreemptibleQueues {
		if !contains(policy.Spec.AccountingQueues, q) {
			return nil, nil, fmt.Errorf("preemptible queue outside accounting scope")
		}
	}
	var budget *WorkerBudget
	for i := range s.Budgets {
		x := &s.Budgets[i]
		if x.Namespace == p.Namespace && x.Spec.PipelineRef.Name == policy.Name && x.Spec.WorkerType == p.Labels[TypeLabel] && x.DeletionTimestamp == nil {
			if budget != nil {
				return nil, nil, fmt.Errorf("overlapping worker budgets")
			}
			budget = x
		}
	}
	if budget == nil || budget.Spec.ResourceName != "nvidia.com/gpu" || budget.Spec.MinAvailableGPU < 0 {
		return nil, nil, fmt.Errorf("missing/invalid worker budget")
	}
	return policy, budget, nil
}
func GPU(p *v1.Pod) float64 { return api.NewTaskInfo(p).Resreq.Get(v1.ResourceName("nvidia.com/gpu")) }
func Ready(p *v1.Pod) bool {
	if p.Spec.NodeName == "" || p.DeletionTimestamp != nil || p.Status.Phase != v1.PodRunning {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == v1.PodReady && c.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

// Remaining counts only healthy capacity. Pending reservations are subtracted
// only while the corresponding UID still contributes to ready capacity.
func (s *Snapshot) Capacity(p *v1.Pod, reserved map[types.UID]bool) (allocated, ready, held float64, err error) {
	policy, _, err := s.Resolve(p)
	if err != nil {
		return 0, 0, 0, err
	}
	for i := range s.Pods {
		x := &s.Pods[i]
		if Key(x) != Key(p) || !contains(policy.Spec.AccountingQueues, s.Queue(x)) {
			continue
		}
		if x.Spec.NodeName != "" && x.Status.Phase != v1.PodSucceeded && x.Status.Phase != v1.PodFailed {
			allocated += GPU(x)
		}
		if Ready(x) {
			ready += GPU(x)
			if reserved[x.UID] {
				held += GPU(x)
			}
		}
	}
	return
}
func (s *Snapshot) Check(p *v1.Pod, reserved map[types.UID]bool, expected *Pin) (*Pin, error) {
	policy, budget, err := s.Resolve(p)
	if err != nil {
		return nil, err
	}
	pin := &Pin{policy.UID, budget.UID, Key(p)}
	if expected != nil && *expected != *pin {
		return nil, fmt.Errorf("policy/budget identity changed")
	}
	if !contains(policy.Spec.PreemptibleQueues, s.Queue(p)) || p.Labels["scheduling.simplesilicon.com/preemptible"] != "true" {
		return nil, fmt.Errorf("worker outside preemptible scope")
	}
	_, ready, held, err := s.Capacity(p, reserved)
	if err != nil {
		return nil, err
	}
	cost := float64(0)
	if Ready(p) && !reserved[p.UID] {
		cost = GPU(p)
	}
	if ready-held-cost < float64(budget.Spec.MinAvailableGPU)*1000 {
		return nil, fmt.Errorf("GPU floor blocks %s: ready=%g reserved=%g candidate=%g floor=%d", Key(p), ready, held, cost, budget.Spec.MinAvailableGPU)
	}
	return pin, nil
}

// DemandPolicy resolves an explicit remote target; absent target means same namespace.
func (s *Snapshot) DemandPolicy(p *v1.Pod) *PipelinePolicy {
	if p == nil || p.Labels[PipelineLabel] == "" {
		return nil
	}
	ns := p.Labels[PipelineNamespaceLabel]
	if ns == "" {
		ns = p.Namespace
	}
	for i := range s.Policies {
		x := &s.Policies[i]
		if x.Namespace == ns && x.Name == p.Labels[PipelineLabel] && x.DeletionTimestamp == nil {
			return x
		}
	}
	return nil
}
func (s *Snapshot) DemandAuthorized(p *v1.Pod) bool {
	policy := s.DemandPolicy(p)
	if policy == nil {
		return false
	}
	if policy.Spec.AllowedPreemptorNamespaces == nil {
		return policy.Namespace == p.Namespace
	}
	return contains(*policy.Spec.AllowedPreemptorNamespaces, p.Namespace)
}
func (s *Snapshot) CanPreempt(demand, victim *v1.Pod) bool {
	if !s.DemandAuthorized(demand) || !s.PreemptibleScope(victim) {
		return false
	}
	policy := s.DemandPolicy(demand)
	return policy.Namespace == victim.Namespace && policy.Name == victim.Labels[PipelineLabel] &&
		s.Queue(demand) == s.Queue(victim) && contains(policy.Spec.PreemptibleQueues, s.Queue(demand))
}

// Worker scope is always its real namespace. Demand uses an explicitly named target.
func (s *Snapshot) PreemptibleScope(p *v1.Pod) bool {
	if p == nil || p.Labels[PipelineLabel] == "" {
		return false
	}
	ns := p.Namespace
	if p.Labels[TypeLabel] == "" {
		ns = p.Labels[PipelineNamespaceLabel]
		if ns == "" {
			ns = p.Namespace
		}
	}
	for _, policy := range s.Policies {
		if policy.Namespace == ns && policy.Name == p.Labels[PipelineLabel] && policy.DeletionTimestamp == nil {
			return contains(policy.Spec.PreemptibleQueues, s.Queue(p)) && contains(policy.Spec.AccountingQueues, s.Queue(p))
		}
	}
	return false
}
