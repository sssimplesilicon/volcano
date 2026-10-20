package workloadpolicy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	admission "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
	wr "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

// Exercise admission with the real API loader and no QueueAccessPolicy API/RBAC.
// A namespace must select a policy scope, not obtain permission for its queue.
func TestAdmissionWithoutQueueAccessAPI(t *testing.T) {
	_, state, _ := setup()
	policy, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&state.Routes[0])
	if err != nil {
		t.Fatal(err)
	}
	obj := &unstructured.Unstructured{Object: policy}
	obj.SetAPIVersion("scheduling.simplesilicon.com/v1alpha1")
	obj.SetKind("WorkloadRoutingPolicy")
	gv := schema.GroupVersion{Group: "scheduling.simplesilicon.com", Version: "v1alpha1"}
	client := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		gv.WithResource("workloadroutingpolicies"): "WorkloadRoutingPolicyList",
		gv.WithResource("nodepoolpolicies"):        "NodePoolPolicyList",
		gv.WithResource("queueaccesspolicies"):     "QueueAccessPolicyList",
	}, obj)
	client.PrependReactor("*", "queueaccesspolicies", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("QueueAccessPolicy API is not installed")
	})
	engine := New(client)
	for _, kind := range []string{"PyTorchJob", "Deployment", "Pod"} {
		t.Run(kind, func(t *testing.T) {
			o := workload(kind)
			o.SetLabels(map[string]string{RouteLabel: "base"})
			out := mutate(t, o, engine.Routing(context.Background(), req(o), false))
			if !engine.Routing(context.Background(), req(out), true).Allowed {
				t.Fatal("routed workload failed validation")
			}
			if d, _ := decision(out); d == nil || d.Target.Queue != "ego-base" {
				t.Fatalf("wrong decision: %+v", d)
			}
		})
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "queueaccesspolicies" {
			t.Fatal("admission must not read QueueAccessPolicy")
		}
	}
}

func TestUnifiedArenaRules(t *testing.T) {
	for _, tc := range []struct {
		name, ns, label, selector, worker, want string
		reject                                  bool
	}{
		{"allocentric", "business", "allocentric", "", "", "shared", false},
		{"old-batch", "business", "old-5880", "", "", "shared", false},
		{"new-batch", "business", "new-5880", "", "", "default", false},
		{"selector-fallback", "business", "", "allocentric", "", "shared", false},
		{"label-precedence", "business", "new-5880", "allocentric", "", "default", false},
		{"other-namespace", "another-business", "allocentric", "", "", "shared", false},
		{"inconsistent-replicas", "business", "allocentric", "allocentric", "old-5880", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, s, _ := setup()
			s.Routes[0].Spec.NamespaceScope.Include = []string{"*"}
			s.Routes[0].Spec.Rules = append(s.Routes[0].Spec.Rules, wr.Rule{
				Name: "legacy", Match: wr.Match{BatchValues: []string{"allocentric", "old-5880"}, LegacyBatch: true},
				Target: wr.Target{Queue: "shared", NodeGroup: "ego"},
			})
			o := workload("PyTorchJob")
			o.SetNamespace(tc.ns)
			if tc.label != "" {
				o.SetLabels(map[string]string{"batch": tc.label})
			}
			path := []string{"spec", "pytorchReplicaSpecs", "Master", "template", "spec", "nodeSelector", "batch"}
			if tc.selector != "" {
				unstructured.SetNestedField(o.Object, tc.selector, path...)
			}
			if tc.worker != "" {
				replica, _, _ := unstructured.NestedMap(o.Object, "spec", "pytorchReplicaSpecs", "Master")
				unstructured.SetNestedField(replica, tc.worker, "template", "spec", "nodeSelector", "batch")
				unstructured.SetNestedMap(o.Object, replica, "spec", "pytorchReplicaSpecs", "Worker")
			}
			r := engine.Routing(context.Background(), req(o), false)
			if tc.reject {
				if r.Allowed || !strings.Contains(r.Result.Message, "identical") {
					t.Fatalf("inconsistent replica selectors accepted: %+v", r)
				}
				return
			}
			out := mutate(t, o, r)
			queue, _, _ := unstructured.NestedString(out.Object, "spec", "runPolicy", "schedulingPolicy", "queue")
			if queue != tc.want {
				t.Fatalf("queue = %q, want %q", queue, tc.want)
			}
			if !engine.Routing(context.Background(), req(out), true).Allowed {
				t.Fatal("final validation failed")
			}
		})
	}
}

func TestCompatibilityURLUsesUnifiedEngine(t *testing.T) {
	engine, _, _ := setup()
	o := workload("PyTorchJob")
	o.SetLabels(map[string]string{RouteLabel: "base"})
	body, _ := json.Marshal(admission.AdmissionReview{Request: req(o)})
	handler := NewHandler(engine)
	var responses []*admission.AdmissionResponse
	for _, path := range []string{"/mutate", "/mutate-workload"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("POST", path, bytes.NewReader(body)))
		var review admission.AdmissionReview
		if err := json.Unmarshal(rec.Body.Bytes(), &review); err != nil {
			t.Fatal(err)
		}
		if !review.Response.Allowed || !strings.Contains(string(review.Response.Patch), DecisionKey) {
			t.Fatalf("%s did not enroll with a v2 decision: %s", path, rec.Body.String())
		}
		responses = append(responses, review.Response)
	}
	if !reflect.DeepEqual(responses[0], responses[1]) {
		t.Fatal("compatibility URL differs from unified engine")
	}
}

func TestRetiredQueueReferenceWarnsWithoutBlockingHandoff(t *testing.T) {
	engine, s, _ := setup()
	s.Routes[0].Spec.Rules[0].QueueAccessPolicyRef = "ego-base"
	o := workload("PyTorchJob")
	o.SetLabels(map[string]string{RouteLabel: "base"})
	r := engine.Routing(context.Background(), req(o), false)
	if !r.Allowed || len(r.Warnings) == 0 || !strings.Contains(r.Warnings[0], "ignored") {
		t.Fatal("old ACL reference must warn without blocking the handoff")
	}
	out := mutate(t, o, r)
	if d, _ := decision(out); d == nil || d.Target.Queue != "ego-base" {
		t.Fatal("old ACL reference changed routing")
	}
}
