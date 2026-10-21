package workloadpolicy

import (
	"context"
	"encoding/json"
	admission "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"maps"
	"reflect"
	"testing"
	wr "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

func trainingSetup() (Engine, *State, memory) {
	e, s, m := setup()
	s.Routes[0].Spec.WorkloadKinds = []string{"PyTorchJob"}
	s.Routes[0].Spec.Rules = []wr.Rule{{Name: "ego-legacy", Match: wr.Match{BatchValues: []string{"allocentric", "old-5880"}, LegacyBatch: true}, Target: wr.Target{Queue: "shared", NodeGroup: "ego", PreemptionTarget: &wr.PipelineTarget{Name: "ego-pipeline", Namespace: "infra-ego-pipeline"}}}}
	return e, s, m
}
func trainingObject(batch string, selector bool) *unstructured.Unstructured {
	o := workload("PyTorchJob")
	if selector {
		unstructured.SetNestedField(o.Object, batch, "spec", "pytorchReplicaSpecs", "Master", "template", "spec", "nodeSelector", "batch")
	} else {
		o.SetLabels(map[string]string{"batch": batch})
	}
	replica, _, _ := unstructured.NestedMap(o.Object, "spec", "pytorchReplicaSpecs", "Master")
	unstructured.SetNestedMap(o.Object, replica, "spec", "pytorchReplicaSpecs", "Worker")
	return o
}
func checkTarget(t *testing.T, o *unstructured.Unstructured, name, ns string) {
	t.Helper()
	if o.GetLabels()[wr.PipelineLabel] != name || o.GetLabels()[wr.PipelineNamespaceLabel] != ns {
		t.Fatalf("unexpected identity: %v", o.GetLabels())
	}
}
func TestTrainingTargetInjection(t *testing.T) {
	for _, batch := range []string{"allocentric", "old-5880", "new-5880"} {
		for _, selector := range []bool{false, true} {
			t.Run(batch+map[bool]string{false: "-label", true: "-selector"}[selector], func(t *testing.T) {
				e, _, _ := trainingSetup()
				o := trainingObject(batch, selector)
				out := mutate(t, o, e.Routing(context.Background(), req(o), false))
				if !maps.Equal(o.GetLabels(), func() map[string]string { l := out.GetLabels(); delete(l, VersionLabel); return l }()) {
					t.Fatal("root routing input changed")
				}
				// Root input is not rewritten: validation and reinvocation still match legacyBatch.
				if r := e.Routing(context.Background(), req(out), true); !r.Allowed {
					t.Fatal(r.Result)
				}
				again := mutate(t, out, e.Routing(context.Background(), req(out), false))
				if !reflect.DeepEqual(out.Object, again.Object) {
					t.Fatal("mutation is not idempotent")
				}
				paths, _ := templatePaths(out)
				for _, path := range paths {
					p, _ := podTemplate(out, path)
					before, _ := podTemplate(o, path)
					if batch == "new-5880" {
						checkTarget(t, p, "", "")
					} else {
						checkTarget(t, p, "ego-pipeline", "infra-ego-pipeline")
					}
					if p.GetLabels()["scheduling.simplesilicon.com/worker-type"] != "" {
						t.Fatal("training must not acquire worker identity")
					}
					for _, field := range []string{"containers", "tolerations", "priorityClassName"} {
						a, _, _ := unstructured.NestedFieldNoCopy(before.Object, "spec", field)
						b, _, _ := unstructured.NestedFieldNoCopy(p.Object, "spec", field)
						if !reflect.DeepEqual(a, b) {
							t.Fatalf("changed %s", field)
						}
					}
				}
			})
		}
	}
}
func TestTrainingTargetFrozenAcrossPolicyChange(t *testing.T) {
	for _, old := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "legacy-decision"}[old], func(t *testing.T) {
			e, s, m := trainingSetup()
			if old {
				s.Routes[0].Spec.Rules[0].Target.PreemptionTarget = nil
			}
			o := trainingObject("allocentric", true)
			parent := mutate(t, o, e.Routing(context.Background(), req(o), false))
			parent.SetUID("job-uid")
			m["business/PyTorchJob/probe"] = parent
			s.Routes[0].Spec.Rules[0].Target.PreemptionTarget = &wr.PipelineTarget{Name: "other-pipeline", Namespace: "other-ns"}
			child := pod()
			owned(child, parent)
			r := req(child)
			r.UserInfo.Username = "controller"
			out := mutate(t, child, e.Routing(context.Background(), r, false))
			if old {
				checkTarget(t, out, "", "")
			} else {
				checkTarget(t, out, "ego-pipeline", "infra-ego-pipeline")
			}
			// Parent updates retain the old frozen target too.
			update := req(parent)
			update.Operation = admission.Update
			update.OldObject = update.Object
			if x := e.Routing(context.Background(), update, true); !x.Allowed {
				t.Fatal(x.Result)
			}
			if !old {
				out.SetLabels(map[string]string{wr.PipelineLabel: "ego-pipeline", wr.PipelineNamespaceLabel: "forged"})
				update = req(out)
				update.Operation = admission.Update
				original := mutate(t, child, e.Routing(context.Background(), r, false))
				update.OldObject.Raw, _ = json.Marshal(original.Object)
				if e.Routing(context.Background(), update, true).Allowed {
					t.Fatal("Pod target namespace tampering allowed")
				}
			}
		})
	}
}
func TestTrainingTargetConflictAndLegacyBypass(t *testing.T) {
	e, s, m := trainingSetup()
	o := trainingObject("allocentric", false)
	unstructured.SetNestedField(o.Object, "wrong", "spec", "pytorchReplicaSpecs", "Worker", "template", "metadata", "labels", wr.PipelineNamespaceLabel)
	if e.Routing(context.Background(), req(o), false).Allowed {
		t.Fatal("conflicting replica identity accepted")
	}
	// Excluded system namespaces, old trainer roots, and unenrolled old descendants stay unchanged.
	s.Routes[0].Spec.NamespaceScope.Exclude = []string{"system"}
	for _, kind := range []string{"system", "old-root", "old-child", "ordinary"} {
		o = trainingObject("allocentric", false)
		switch kind {
		case "system":
			o.SetNamespace("system")
		case "old-root":
			o.SetLabels(map[string]string{"routing.example.org/controller": "old"})
		case "old-child":
			o.SetUID("old-uid")
			m["business/PyTorchJob/probe"] = o
			o = pod()
			owned(o, m["business/PyTorchJob/probe"])
		case "ordinary":
			o = workload("Deployment")
		}
		r := req(o)
		r.UserInfo.Username = "controller"
		out := mutate(t, o, e.Routing(context.Background(), r, false))
		if !reflect.DeepEqual(o.Object, out.Object) {
			t.Fatalf("%s changed", kind)
		}
	}
}

func TestExplicitTrainingTargetAndIndependentPipelines(t *testing.T) {
	e, s, m := trainingSetup()
	s.Routes[0].Spec.Rules = append(s.Routes[0].Spec.Rules, wr.Rule{Name: "video-explicit", Match: wr.Match{Labels: map[string]string{wr.PipelineLabel: "video"}, BatchValues: []string{"allocentric"}}, Target: wr.Target{Queue: "video-shared", NodeGroup: "video", PreemptionTarget: &wr.PipelineTarget{Name: "video", Namespace: "video-workers"}}})
	o := trainingObject("allocentric", false)
	o.SetLabels(map[string]string{"batch": "allocentric", wr.PipelineLabel: "video"})
	parent := mutate(t, o, e.Routing(context.Background(), req(o), false))
	if r := e.Routing(context.Background(), req(parent), true); !r.Allowed {
		t.Fatal(r.Result)
	}
	paths, _ := templatePaths(parent)
	for _, path := range paths {
		p, _ := podTemplate(parent, path)
		checkTarget(t, p, "video", "video-workers")
	}
	parent.SetUID("video-uid")
	m["business/PyTorchJob/probe"] = parent
	child := pod()
	owned(child, parent)
	r := req(child)
	if e.Routing(context.Background(), r, false).Allowed {
		t.Fatal("untrusted owner claim accepted")
	}
	r.UserInfo.Username = "controller"
	out := mutate(t, child, e.Routing(context.Background(), r, false))
	checkTarget(t, out, "video", "video-workers")
	labels := out.GetLabels()
	delete(labels, wr.PipelineNamespaceLabel)
	out.SetLabels(labels)
	vr := req(out)
	vr.UserInfo.Username = "controller"
	if e.Routing(context.Background(), vr, true).Allowed {
		t.Fatal("missing final Pod identity accepted")
	}
	// Rolling back policy injection stops new targets, but keeps an enrolled job's descendants stable.
	s.Routes[0].Spec.Rules[1].Target.PreemptionTarget = nil
	out = mutate(t, child, e.Routing(context.Background(), r, false))
	checkTarget(t, out, "video", "video-workers")
	fresh := mutate(t, o, e.Routing(context.Background(), req(o), false))
	for _, path := range paths {
		p, _ := podTemplate(fresh, path)
		checkTarget(t, p, "", "")
	}
}
