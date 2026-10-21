package workloadpolicy

import (
	"context"
	"encoding/json"
	admission "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"testing"
	wr "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

func TestManagedWorkerAttributes(t *testing.T) {
	e, s, _ := setup()
	var target wr.Target
	if err := json.Unmarshal([]byte(`{"queue":"shared","nodeGroup":"ego","priorityClassName":"pipeline-worker","worker":{"pipeline":"ego","namespace":"business","type":"A"}}`), &target); err != nil {
		t.Fatal(err)
	}
	s.Routes[0].Spec.Rules = []wr.Rule{{Name: "worker-a", Match: wr.Match{Labels: map[string]string{wr.PipelineLabel: "ego", "scheduling.simplesilicon.com/worker-type": "A"}}, Target: target}}
	o := workload("Deployment")
	o.SetLabels(map[string]string{wr.PipelineLabel: "ego", "scheduling.simplesilicon.com/worker-type": "A"})
	out := mutate(t, o, e.Routing(context.Background(), req(o), false))
	pod, err := podTemplate(out, []string{"spec", "template"})
	if err != nil {
		t.Fatal(err)
	}
	if pod.GetLabels()["scheduling.simplesilicon.com/preemptible"] != "true" || pod.GetLabels()[wr.PipelineLabel] != "ego" || pod.GetLabels()["scheduling.simplesilicon.com/worker-type"] != "A" {
		t.Fatalf("worker identity not injected: %v", pod.GetLabels())
	}
	priority, _, _ := unstructured.NestedString(pod.Object, "spec", "priorityClassName")
	if priority != "pipeline-worker" {
		t.Fatal("worker priority not injected")
	}
	if r := e.Routing(context.Background(), req(out), true); !r.Allowed {
		t.Fatal(r.Result)
	}
}

func TestManagedWorkerRejectsConflictsAndTampering(t *testing.T) {
	for _, scenario := range []string{"priority", "template-type", "namespace", "update-type"} {
		t.Run(scenario, func(t *testing.T) {
			e, s, _ := setup()
			target := wr.Target{Queue: "shared", NodeGroup: "ego", PriorityClassName: "worker", Worker: &wr.WorkerTarget{Pipeline: "ego", Namespace: "business", Type: "A"}}
			s.Routes[0].Spec.Rules = []wr.Rule{{Name: "worker", Match: wr.Match{Labels: map[string]string{wr.PipelineLabel: "ego"}}, Target: target}}
			o := workload("Deployment")
			o.SetLabels(map[string]string{wr.PipelineLabel: "ego", "scheduling.simplesilicon.com/worker-type": "A"})
			switch scenario {
			case "priority":
				unstructured.SetNestedField(o.Object, "admin", "spec", "template", "spec", "priorityClassName")
			case "template-type":
				unstructured.SetNestedStringMap(o.Object, map[string]string{"scheduling.simplesilicon.com/worker-type": "B"}, "spec", "template", "metadata", "labels")
			case "namespace":
				s.Routes[0].Spec.Rules[0].Target.Worker.Namespace = "different"
			case "update-type":
				old := mutate(t, o, e.Routing(context.Background(), req(o), false))
				updated := old.DeepCopy()
				labels := updated.GetLabels()
				labels["scheduling.simplesilicon.com/worker-type"] = "B"
				updated.SetLabels(labels)
				r := req(updated)
				r.Operation = admission.Update
				r.OldObject.Raw, _ = json.Marshal(old.Object)
				if e.Routing(context.Background(), r, true).Allowed {
					t.Fatal("changed worker type allowed")
				}
				return
			}
			if e.Routing(context.Background(), req(o), false).Allowed {
				t.Fatal("conflicting managed attributes allowed")
			}
		})
	}
}

func TestManagedPodPriorityReinvocation(t *testing.T) {
	e, s, _ := setup()
	s.Routes[0].Spec.Rules = []wr.Rule{{Name: "training", Match: wr.Match{Labels: map[string]string{"training": "true"}}, Target: wr.Target{Queue: "shared", NodeGroup: "ego", PriorityClassName: "training"}}}
	o := pod()
	o.SetLabels(map[string]string{"training": "true"})
	unstructured.SetNestedField(o.Object, int64(0), "spec", "priority")
	unstructured.SetNestedField(o.Object, "PreemptLowerPriority", "spec", "preemptionPolicy")
	out := mutate(t, o, e.Routing(context.Background(), req(o), false))
	if _, found, _ := unstructured.NestedFieldNoCopy(out.Object, "spec", "priority"); found {
		t.Fatal("stale default numeric priority retained")
	}
	// Simulate built-in Priority admission reinvocation. A subsequent webhook pass must retain it.
	unstructured.SetNestedField(out.Object, int64(100), "spec", "priority")
	out = mutate(t, out, e.Routing(context.Background(), req(out), false))
	if n, _, _ := unstructured.NestedFieldNoCopy(out.Object, "spec", "priority"); n != float64(100) {
		t.Fatal("resolved priority removed")
	}
}
