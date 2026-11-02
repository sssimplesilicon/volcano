package workloadpolicy

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"reflect"
	"strings"
	"testing"
	wr "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

func TestL20XFallbackTemplateMatching(t *testing.T) {
	for _, tc := range []struct {
		name, accelerator, batch             string
		gpu, mixed, zero, conflict, rootOnly bool
		want                                 string
	}{
		{name: "unchanged-selector-submission", accelerator: "nvidia-l20x", gpu: true, want: "l20x"},
		{name: "cpu-only", accelerator: "nvidia-l20x", want: "fallback"},
		{name: "other-accelerator", accelerator: "nvidia-h20", gpu: true, want: "fallback"},
		{name: "no-selector", gpu: true, want: "fallback"},
		{name: "root-label-is-not-placement", accelerator: "nvidia-l20x", gpu: true, rootOnly: true, want: "fallback"},
		{name: "old-batch-wins", accelerator: "nvidia-l20x", batch: "old-5880", gpu: true, want: "legacy"},
		{name: "mixed-replicas-preserved", accelerator: "nvidia-l20x", gpu: true, mixed: true, want: "fallback"},
		{name: "inactive-replica-ignored", accelerator: "nvidia-l20x", gpu: true, mixed: true, zero: true, want: "l20x"},
		{name: "fallback-conflict-rejected", accelerator: "nvidia-l20x", gpu: true, conflict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, s, _ := setup()
			s.Pools = nil
			s.Routes[0].Spec.WorkloadKinds = []string{"PyTorchJob"}
			r := wr.Rule{Name: "l20x", FallbackOnly: true, Match: wr.Match{NodeSelector: map[string]string{"accelerator": "nvidia-l20x"}, GPUOnly: true}, Target: wr.Target{Queue: "default", NodeGroup: "general", PriorityClassName: "llm-training", PreemptionTarget: &wr.PipelineTarget{Name: "l20x-llm", Namespace: "infra-llm"}}}
			// Fallback precedes the old rule in the list deliberately: tier is independent of order.
			s.Routes[0].Spec.Rules = []wr.Rule{r, {Name: "legacy", Match: wr.Match{BatchValues: []string{"old-5880"}}, Target: wr.Target{Queue: "shared", NodeGroup: "ego"}}}
			if tc.conflict {
				r.Name = "other-l20x"
				s.Routes[0].Spec.Rules = append(s.Routes[0].Spec.Rules, r)
			}
			o := workload("PyTorchJob")
			o.SetLabels(map[string]string{"batch": tc.batch})
			path := []string{"spec", "pytorchReplicaSpecs", "Master", "template"}
			if tc.accelerator != "" {
				if tc.rootOnly {
					o.SetLabels(map[string]string{"accelerator": tc.accelerator})
				} else {
					unstructured.SetNestedStringMap(o.Object, map[string]string{"accelerator": tc.accelerator, "custom": "retain"}, append(path, "spec", "nodeSelector")...)
				}
			}
			if tc.gpu {
				unstructured.SetNestedSlice(o.Object, []interface{}{map[string]interface{}{"name": "pytorch", "image": "busybox", "resources": map[string]interface{}{"limits": map[string]interface{}{"nvidia.com/gpu": "1"}}}}, append(path, "spec", "containers")...)
			}
			if tc.mixed {
				role, _, _ := unstructured.NestedMap(o.Object, "spec", "pytorchReplicaSpecs", "Master")
				unstructured.SetNestedStringMap(role, map[string]string{"accelerator": "nvidia-h20"}, "template", "spec", "nodeSelector")
				if tc.zero {
					role["replicas"] = int64(0)
				}
				unstructured.SetNestedMap(o.Object, role, "spec", "pytorchReplicaSpecs", "Worker")
			}
			before, _, _ := unstructured.NestedStringMap(o.Object, append(path, "spec", "nodeSelector")...)
			res := e.Routing(context.Background(), req(o), false)
			if tc.conflict {
				if res.Allowed || !strings.Contains(res.Result.Message, "conflicting") {
					t.Fatal(res)
				}
				return
			}
			out := mutate(t, o, res)
			d, err := decision(out)
			if err != nil || d == nil || d.Rule != tc.want {
				t.Fatalf("decision=%+v err=%v", d, err)
			}
			after, _, _ := unstructured.NestedStringMap(out.Object, append(path, "spec", "nodeSelector")...)
			delete(after, GroupLabel)
			if !reflect.DeepEqual(before, after) && !(len(before) == 0 && len(after) == 0) {
				t.Fatalf("selectors changed %v -> %v", before, after)
			}
			if tc.want == "l20x" {
				v, _, _ := unstructured.NestedString(out.Object, "spec", "runPolicy", "schedulingPolicy", "priorityClass")
				if v != "llm-training" {
					t.Fatal("missing group priority")
				}
			}
			if !e.Routing(context.Background(), req(out), true).Allowed {
				t.Fatal("validation failed")
			}
		})
	}
}

func TestL20XPreservesExplicitPriorities(t *testing.T) {
	for _, field := range []string{"priorityClassName", "preemptionPolicy", "group"} {
		t.Run(field, func(t *testing.T) {
			e, s, _ := setup()
			s.Pools = nil
			s.Routes[0].Spec.WorkloadKinds = []string{"PyTorchJob"}
			s.Routes[0].Spec.Rules = []wr.Rule{{Name: "l20x", FallbackOnly: true, Match: wr.Match{NodeSelector: map[string]string{"accelerator": "nvidia-l20x"}}, Target: wr.Target{Queue: "default", NodeGroup: "general", PriorityClassName: "llm-training"}}}
			o := workload("PyTorchJob")
			path := []string{"spec", "pytorchReplicaSpecs", "Master", "template", "spec"}
			unstructured.SetNestedStringMap(o.Object, map[string]string{"accelerator": "nvidia-l20x"}, append(path, "nodeSelector")...)
			value := "custom-priority"
			if field == "group" {
				unstructured.SetNestedField(o.Object, value, "spec", "runPolicy", "schedulingPolicy", "priorityClass")
			} else {
				if field == "preemptionPolicy" {
					value = "Never"
				}
				unstructured.SetNestedField(o.Object, value, append(path, field)...)
			}
			out := mutate(t, o, e.Routing(context.Background(), req(o), false))
			d, _ := decision(out)
			if d.Rule != "fallback" {
				t.Fatal("explicit priority was overridden")
			}
		})
	}
}
