package workloadrouting

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	admission "k8s.io/api/admission/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
)

func fixture() (Snapshot, *qa.Snapshot, *unstructured.Unstructured) {
	p := Policy{ObjectMeta: meta.ObjectMeta{Name: "route"}, Spec: Spec{NamespaceScope: Scope{Include: []string{"allowed", "denied"}, Exclude: []string{"system"}}, SchedulerName: "volcano-test", Fallback: Target{"default", "general"}, Rules: []Rule{
		{"ego", Match{Labels: map[string]string{PipelineLabel: "ego"}, BatchValues: []string{"old"}}, "ego-shared", Target{"ego-shared", "ego"}},
		{"legacy", Match{BatchValues: []string{"old"}, LegacyBatch: true}, "ego-shared", Target{"ego-shared", "ego"}},
		{"video", Match{Labels: map[string]string{PipelineLabel: "video"}}, "video-shared", Target{"video-shared", "video"}},
	}}}
	access := &qa.Snapshot{}
	for _, name := range []string{"ego-shared", "video-shared"} {
		a := qa.Policy{}
		a.Name = name
		a.Spec.AllowedNamespaces = []string{"allowed"}
		access.Items = append(access.Items, a)
	}
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	json.Unmarshal([]byte(`{"metadata":{"labels":{"batch":"old"}},"spec":{"pytorchReplicaSpecs":{"Master":{"replicas":1,"template":{"spec":{"containers":[{"name":"pytorch","image":"busybox"}],"nodeSelector":{"batch":"old","user":"kept"},"tolerations":[{"operator":"Exists"}]}}}}}}`), &obj.Object)
	return Snapshot{Items: []Policy{p}}, access, obj
}
func TestRouting(t *testing.T) {
	for _, tc := range []struct{ name, ns, pipeline, batch, queue, rule string }{
		{"legacy", "allowed", "", "old", "ego-shared", "legacy"},
		{"explicit", "allowed", "ego", "old", "ego-shared", "ego"},
		{"video-no-legacy-collision", "allowed", "video", "old", "video-shared", "video"},
		{"unauthorized", "denied", "", "old", "default", "fallback"},
		{"unknown-pipeline", "allowed", "other", "old", "default", "fallback"},
		{"batch-default", "allowed", "", "new", "default", "fallback"},
		{"outside-scope", "outside", "", "old", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a, o := fixture()
			labels := o.GetLabels()
			labels["batch"] = tc.batch
			if tc.pipeline != "" {
				labels[PipelineLabel] = tc.pipeline
			}
			o.SetLabels(labels)
			spec, rule, err := Route(o, tc.ns, s, a)
			if err != nil {
				t.Fatal(err)
			}
			if tc.queue == "" {
				if spec != nil {
					t.Fatal("outside scope mutated")
				}
				return
			}
			q, _, _ := unstructured.NestedString(spec, "runPolicy", "schedulingPolicy", "queue")
			if q != tc.queue || rule != "route/"+tc.rule {
				t.Fatalf("%s %s", q, rule)
			}
			preserved, _, _ := unstructured.NestedString(spec, "pytorchReplicaSpecs", "Master", "template", "spec", "nodeSelector", "user")
			if preserved != "kept" {
				t.Fatal("lost user selector")
			}
		})
	}
}
func TestInvalidRouting(t *testing.T) {
	for _, kind := range []string{"conflict", "policy-overlap", "missing-access", "terminating-access", "replica-conflict", "invalid-policy", "unavailable", "fallback-denied"} {
		t.Run(kind, func(t *testing.T) {
			s, a, o := fixture()
			switch kind {
			case "conflict":
				r := s.Items[0].Spec.Rules[1]
				r.Name = "duplicate"
				s.Items[0].Spec.Rules = append(s.Items[0].Spec.Rules, r)
			case "policy-overlap":
				s.Items = append(s.Items, s.Items[0])
			case "missing-access":
				a.Items = nil
			case "terminating-access":
				now := meta.Now()
				a.Items[0].DeletionTimestamp = &now
			case "replica-conflict":
				r, _, _ := unstructured.NestedMap(o.Object, "spec", "pytorchReplicaSpecs", "Master")
				unstructured.SetNestedField(r, "other", "template", "spec", "nodeSelector", "batch")
				unstructured.SetNestedMap(o.Object, r, "spec", "pytorchReplicaSpecs", "Worker")
			case "invalid-policy":
				s.Items[0].Spec.Rules[0].QueueAccessPolicyRef = "wrong"
			case "unavailable":
				a = nil
			case "fallback-denied":
				p := qa.Policy{}
				p.Name = "default"
				a.Items = append(a.Items, p)
				o.SetLabels(map[string]string{"batch": "new"})
			}
			if _, _, err := Route(o, "allowed", s, a); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}
func TestAdmission(t *testing.T) {
	s, a, o := fixture()
	raw, _ := json.Marshal(o.Object)
	req := admission.AdmissionReview{Request: &admission.AdmissionRequest{UID: "test", Operation: admission.Create, Namespace: "allowed", Resource: meta.GroupVersionResource{Group: "kubeflow.org", Version: "v1", Resource: "pytorchjobs"}, Object: runtime.RawExtension{Raw: raw}}}
	load := func() (Snapshot, *qa.Snapshot, error) { return s, a, nil }
	response := Mutate(req, load)
	if !response.Allowed || len(response.Patch) == 0 || response.UID != "test" {
		t.Fatalf("%+v", response)
	}
	if !strings.Contains(string(response.Patch), "ego-shared") {
		t.Fatal("missing route")
	}
	for _, kind := range []string{"update", "pod", "old-owner", "child"} {
		t.Run(kind, func(t *testing.T) {
			r := *req.Request
			switch kind {
			case "update":
				r.Operation = admission.Update
			case "pod":
				r.Resource = meta.GroupVersionResource{Version: "v1", Resource: "pods"}
			case "old-owner":
				c := o.DeepCopy()
				l := c.GetLabels()
				l["routing.example.org/controller"] = "old"
				c.SetLabels(l)
				r.Object.Raw, _ = json.Marshal(c.Object)
			case "child":
				c := o.DeepCopy()
				c.SetOwnerReferences([]meta.OwnerReference{{Kind: "Test", Name: "owner", UID: "uid", APIVersion: "v1"}})
				r.Object.Raw, _ = json.Marshal(c.Object)
			}
			got := Mutate(admission.AdmissionReview{Request: &r}, func() (Snapshot, *qa.Snapshot, error) {
				t.Fatal("must not reload policy for existing workloads")
				return s, a, nil
			})
			if !got.Allowed || len(got.Patch) > 0 {
				t.Fatal(got)
			}
		})
	}
	got := Mutate(req, func() (Snapshot, *qa.Snapshot, error) { return s, nil, fmt.Errorf("API unavailable") })
	if got.Allowed {
		t.Fatal("must fail closed")
	}
}
func TestSelectorFallbackAndExclusion(t *testing.T) {
	s, a, o := fixture()
	o.SetLabels(nil)
	spec, rule, err := Route(o, "allowed", s, a)
	if err != nil || spec == nil || rule != "route/legacy" {
		t.Fatalf("%v %s", err, rule)
	}
	s.Items[0].Spec.NamespaceScope.Exclude = []string{"allowed"}
	spec, _, err = Route(o, "allowed", s, a)
	if err != nil || spec != nil {
		t.Fatal("excluded namespace modified")
	}
}
