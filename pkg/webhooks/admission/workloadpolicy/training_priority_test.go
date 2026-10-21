package workloadpolicy

import (
	"context"
	"encoding/json"
	admission "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"testing"
)

var trainingPriorityPath = []string{"spec", "runPolicy", "schedulingPolicy", "priorityClass"}

func prioritySetup() Engine {
	e, s, _ := trainingSetup()
	s.Routes[0].Spec.Rules[0].Target.PriorityClassName = "training-high"
	return e
}
func TestTrainingPodGroupPriorityInjection(t *testing.T) {
	e := prioritySetup()
	o := trainingObject("allocentric", false)
	out := mutate(t, o, e.Routing(context.Background(), req(o), false))
	got, _, _ := unstructured.NestedString(out.Object, trainingPriorityPath...)
	if got != "training-high" {
		t.Fatalf("task-group priority=%q, want training-high", got)
	}
	if !e.Routing(context.Background(), req(out), true).Allowed {
		t.Fatal("validation failed")
	}
	for _, v := range []string{"", "wrong"} {
		bad := out.DeepCopy()
		unstructured.SetNestedField(bad.Object, v, trainingPriorityPath...)
		if e.Routing(context.Background(), req(bad), true).Allowed {
			t.Fatalf("invalid CREATE priority accepted: %q", v)
		}
	}
	conflict := o.DeepCopy()
	unstructured.SetNestedField(conflict.Object, "wrong", trainingPriorityPath...)
	if e.Routing(context.Background(), req(conflict), false).Allowed {
		t.Fatal("explicit conflicting task-group priority accepted")
	}
}
func TestTrainingPriorityLegacyUpdateAndRepair(t *testing.T) {
	e := prioritySetup()
	o := mutate(t, trainingObject("allocentric", false), e.Routing(context.Background(), req(trainingObject("allocentric", false)), false))
	unstructured.RemoveNestedField(o.Object, trainingPriorityPath...)
	check := func(old, now *unstructured.Unstructured, want bool) {
		t.Helper()
		r := req(now)
		r.Operation = admission.Update
		b, _ := json.Marshal(old.Object)
		r.OldObject = runtime.RawExtension{Raw: b}
		res := e.Routing(context.Background(), r, true)
		if res.Allowed != want {
			t.Fatalf("allowed=%v want=%v: %+v", res.Allowed, want, res.Result)
		}
	}
	check(o, o.DeepCopy(), true) // Previously enrolled roots are not frozen by this upgrade.
	repaired := o.DeepCopy()
	unstructured.SetNestedField(repaired.Object, "training-high", trainingPriorityPath...)
	check(o, repaired, true)
	check(repaired, o, false) // Once repaired, removing the priority is forbidden.
	wrong := o.DeepCopy()
	unstructured.SetNestedField(wrong.Object, "wrong", trainingPriorityPath...)
	check(o, wrong, false)
}
func TestTrainingWithoutManagedPriorityUnchanged(t *testing.T) {
	e, _, _ := trainingSetup()
	o := trainingObject("allocentric", false)
	unstructured.SetNestedField(o.Object, "user-priority", trainingPriorityPath...)
	out := mutate(t, o, e.Routing(context.Background(), req(o), false))
	v, _, _ := unstructured.NestedString(out.Object, trainingPriorityPath...)
	if v != "user-priority" {
		t.Fatal("unmanaged priority changed")
	}
}
