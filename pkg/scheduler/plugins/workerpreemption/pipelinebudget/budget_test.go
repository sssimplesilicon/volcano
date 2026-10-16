package pipelinebudget

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
)

func worker(name, pipeline, queue string, gpu string, ready bool) v1.Pod {
	status := v1.ConditionFalse
	if ready {
		status = v1.ConditionTrue
	}
	return v1.Pod{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name), Labels: map[string]string{PipelineLabel: pipeline, TypeLabel: "detect", "scheduling.simplesilicon.com/preemptible": "true"}, Annotations: map[string]string{"scheduling.k8s.io/group-name": queue}}, Spec: v1.PodSpec{NodeName: "node", Containers: []v1.Container{{Name: "worker", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"nvidia.com/gpu": resource.MustParse(gpu)}}}}}, Status: v1.PodStatus{Phase: v1.PodRunning, Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: status}}}}
}
func fixture() *Snapshot {
	p := PipelinePolicy{ObjectMeta: meta.ObjectMeta{Name: "ego", Namespace: "ns", UID: "policy"}}
	p.Spec.AccountingQueues = []string{"base", "shared"}
	p.Spec.PreemptibleQueues = []string{"shared"}
	b := WorkerBudget{ObjectMeta: meta.ObjectMeta{Name: "detect", Namespace: "ns", UID: "budget"}}
	b.Spec.PipelineRef.Name = "ego"
	b.Spec.WorkerType = "detect"
	b.Spec.ResourceName = "nvidia.com/gpu"
	b.Spec.MinAvailableGPU = 8
	return &Snapshot{Policies: []PipelinePolicy{p}, Budgets: []WorkerBudget{b}, Queues: map[string]string{"ns/base": "base", "ns/shared": "shared"}, Pods: []v1.Pod{worker("base", "ego", "base", "4", true), worker("a", "ego", "shared", "4", true), worker("b", "ego", "shared", "4", true), worker("other", "other", "shared", "16", true)}}
}
func TestBudget(t *testing.T) {
	for _, name := range []string{"allows", "reservation", "unready", "base", "missing", "overlap", "replacement", "terminating", "different-type", "indivisible"} {
		t.Run(name, func(t *testing.T) {
			s := fixture()
			p := &s.Pods[1]
			held := map[types.UID]bool{}
			var pin *Pin
			allow := false
			switch name {
			case "allows":
				allow = true
			case "reservation":
				held["b"] = true
			case "unready":
				s.Pods[0].Status.Conditions[0].Status = v1.ConditionFalse
			case "base":
				p = &s.Pods[0]
			case "missing":
				s.Budgets = nil
			case "overlap":
				s.Budgets = append(s.Budgets, s.Budgets[0])
			case "replacement":
				pin = &Pin{"policy", "old", Key(p)}
			case "terminating":
				now := meta.Now()
				s.Pods[2].DeletionTimestamp = &now
				held["b"] = true
			case "different-type":
				s.Pods[0].Labels[TypeLabel] = "other"
			case "indivisible":
				s.Budgets[0].Spec.MinAvailableGPU = 10
			}
			_, err := s.Check(p, held, pin)
			if (err == nil) != allow {
				t.Fatalf("allow=%t err=%v", allow, err)
			}
		})
	}
}
func TestNoDoubleDebitAndIsolation(t *testing.T) {
	s := fixture()
	now := meta.Now()
	s.Pods[2].DeletionTimestamp = &now
	_, ready, held, err := s.Capacity(&s.Pods[1], map[types.UID]bool{"b": true})
	if err != nil || ready != 8000 || held != 0 {
		t.Fatalf("ready=%g held=%g err=%v", ready, held, err)
	}
	if SamePipeline(&s.Pods[1], &s.Pods[3]) || Key(&s.Pods[1]) == Key(&s.Pods[3]) {
		t.Fatal("pipeline identities merged")
	}
}

func TestQueueScope(t *testing.T) {
	s := fixture()
	demand := worker("training", "ego", "shared", "1", false)
	delete(demand.Labels, TypeLabel)
	if !s.PreemptibleScope(&demand) {
		t.Fatal("same pipeline demand should not need worker type")
	}
	demand.Annotations["scheduling.k8s.io/group-name"] = "base"
	if s.PreemptibleScope(&demand) {
		t.Fatal("base must not be preemptible")
	}
	demand.Labels[PipelineLabel] = "other"
	if s.PreemptibleScope(&demand) {
		t.Fatal("missing policy should fail closed")
	}
}
