package workloadpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	admission "k8s.io/api/admission/v1"
	auth "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"strings"
	"testing"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
	wr "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

type memory map[string]*unstructured.Unstructured

func (m memory) Get(_ context.Context, av, k, ns, n string) (*unstructured.Unstructured, error) {
	o := m[ns+"/"+k+"/"+n]
	if o == nil {
		return nil, fmt.Errorf("not found")
	}
	return o.DeepCopy(), nil
}
func setup() (Engine, *State, memory) {
	p := wr.Policy{ObjectMeta: metav1.ObjectMeta{Name: "business", Generation: 1}, Spec: wr.Spec{NamespaceScope: wr.Scope{Include: []string{"business"}}, WorkloadKinds: []string{"Pod", "Deployment", "StatefulSet", "Job", "CronJob", "Workflow", "PyTorchJob"}, ControllerUsers: []string{"controller"}, SchedulerName: "test-scheduler", Fallback: wr.Target{Queue: "default", NodeGroup: "general"}, Rules: []wr.Rule{
		{Name: "base", Match: wr.Match{Labels: map[string]string{RouteLabel: "base"}}, Target: wr.Target{Queue: "ego-base", NodeGroup: "ego", PoolRef: "ego"}},
		{Name: "cpu", Match: wr.Match{Labels: map[string]string{RouteLabel: "cpu"}}, Target: wr.Target{Queue: "cpu", NodeGroup: "general", CPUOnly: true}},
	}}}
	s := &State{Routes: []wr.Policy{p}, Access: &qa.Snapshot{}}
	for _, name := range []string{"ego", "video"} {
		s.Pools = append(s.Pools, Pool{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: PoolSpec{NamespaceScope: wr.Scope{Include: []string{"business"}}, NodeSelector: map[string]string{"test/pool": name}, ControllerUsers: []string{"controller"}, AcceptedRoutes: []RouteRef{{Policy: "business", Rules: []string{"base"}}}}})
	}
	m := memory{}
	return Engine{Reader: m, Load: func(context.Context) (State, error) { return *s, nil }}, s, m
}
func pod() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]interface{}{"name": "probe", "namespace": "business"}, "spec": map[string]interface{}{"containers": []interface{}{map[string]interface{}{"name": "worker", "image": "busybox"}}}}}
}
func workload(kind string) *unstructured.Unstructured {
	p := pod()
	if kind == "Pod" {
		return p
	}
	o := pod()
	o.SetKind(kind)
	template := map[string]interface{}{"metadata": map[string]interface{}{}, "spec": p.Object["spec"]}
	spec := map[string]interface{}{"template": template}
	switch kind {
	case "Deployment", "StatefulSet", "ReplicaSet":
		o.SetAPIVersion("apps/v1")
	case "Job":
		o.SetAPIVersion("batch/v1")
	case "CronJob":
		o.SetAPIVersion("batch/v1")
		spec = map[string]interface{}{"jobTemplate": map[string]interface{}{"spec": spec}}
	case "Workflow":
		o.SetAPIVersion("argoproj.io/v1alpha1")
		spec = map[string]interface{}{"entrypoint": "worker", "templates": []interface{}{map[string]interface{}{"name": "worker", "container": map[string]interface{}{"image": "busybox"}}}}
	case "PyTorchJob":
		o.SetAPIVersion("kubeflow.org/v1")
		spec = map[string]interface{}{"pytorchReplicaSpecs": map[string]interface{}{"Master": map[string]interface{}{"replicas": int64(1), "template": template}}}
	}
	o.Object["spec"] = spec
	return o
}
func req(o *unstructured.Unstructured) *admission.AdmissionRequest {
	r, _ := ownerResource(o.GetAPIVersion(), o.GetKind())
	b, _ := json.Marshal(o.Object)
	return &admission.AdmissionRequest{UID: "request", Namespace: o.GetNamespace(), Operation: admission.Create, Kind: metav1.GroupVersionKind{Group: r.Group, Version: r.Version, Kind: o.GetKind()}, Resource: metav1.GroupVersionResource{Group: r.Group, Version: r.Version, Resource: r.Resource}, UserInfo: auth.UserInfo{Username: "user"}, Object: runtime.RawExtension{Raw: b}}
}
func mutate(t *testing.T, o *unstructured.Unstructured, r *admission.AdmissionResponse) *unstructured.Unstructured {
	t.Helper()
	if !r.Allowed {
		t.Fatal(r.Result)
	}
	if len(r.Patch) == 0 {
		return o.DeepCopy()
	}
	out := o.DeepCopy()
	var ops []struct {
		Path  string
		Value interface{}
	}
	if e := json.Unmarshal(r.Patch, &ops); e != nil {
		t.Fatal(e)
	}
	for _, p := range ops {
		unstructured.SetNestedField(out.Object, p.Value, strings.Split(strings.TrimPrefix(p.Path, "/"), "/")...)
	}
	return out
}
func owned(o, p *unstructured.Unstructured) {
	yes := true
	o.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: p.GetAPIVersion(), Kind: p.GetKind(), Name: p.GetName(), UID: p.GetUID(), Controller: &yes}})
}
func TestWorkloadKindsAndUpdate(t *testing.T) {
	for _, kind := range []string{"Pod", "Deployment", "StatefulSet", "Job", "CronJob", "Workflow", "PyTorchJob"} {
		t.Run(kind, func(t *testing.T) {
			e, _, _ := setup()
			o := workload(kind)
			o.SetLabels(map[string]string{RouteLabel: "base"})
			out := mutate(t, o, e.Routing(context.Background(), req(o), false))
			if !e.Routing(context.Background(), req(out), true).Allowed {
				t.Fatal("final validation failed")
			}
			d, _ := decision(out)
			if d == nil || d.Target.Queue != "ego-base" {
				t.Fatal(d)
			}
			update := req(out)
			update.Operation = admission.Update
			update.OldObject = update.Object
			out.SetAnnotations(map[string]string{DecisionKey: "forged"})
			update.Object.Raw, _ = json.Marshal(out.Object)
			if e.Routing(context.Background(), update, true).Allowed {
				t.Fatal("decision modification allowed")
			}
		})
	}
}
func TestOwnerVerificationAndPolicyChange(t *testing.T) {
	e, s, m := setup()
	o := workload("Deployment")
	o.SetLabels(map[string]string{RouteLabel: "base"})
	parent := mutate(t, o, e.Routing(context.Background(), req(o), false))
	parent.SetUID(types.UID("original"))
	m["business/Deployment/probe"] = parent
	child := pod()
	owned(child, parent)
	r := req(child)
	r.UserInfo.Username = "controller"
	out := mutate(t, child, e.Routing(context.Background(), r, false))
	d, _ := decision(out)
	if d.Target.Queue != "ego-base" {
		t.Fatal(d)
	}
	s.Routes[0].Spec.Rules[0].Target.Queue = "changed"
	again := mutate(t, child, e.Routing(context.Background(), r, false))
	d, _ = decision(again)
	if d.Target.Queue != "ego-base" {
		t.Fatal("existing controller rerouted")
	}
	r.UserInfo.Username = "untrusted"
	if e.Routing(context.Background(), r, false).Allowed {
		t.Fatal("forged owner accepted")
	}
	r.UserInfo.Username = "controller"
	parent.SetUID("replacement")
	if e.Routing(context.Background(), r, false).Allowed {
		t.Fatal("same-name owner replacement accepted")
	}
}
func TestIsolationORBranchesAndNoBroadening(t *testing.T) {
	e, _, _ := setup()
	o := pod()
	terms := []interface{}{map[string]interface{}{"matchExpressions": []interface{}{map[string]interface{}{"key": "zone", "operator": "In", "values": []interface{}{"a"}}}}, map[string]interface{}{"matchFields": []interface{}{map[string]interface{}{"key": "metadata.name", "operator": "In", "values": []interface{}{"n"}}}}}
	path := []string{"spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms"}
	unstructured.SetNestedSlice(o.Object, terms, path...)
	out := mutate(t, o, e.Isolation(context.Background(), req(o), false))
	got, _, _ := unstructured.NestedSlice(out.Object, path...)
	for _, x := range got {
		m := x.(map[string]interface{})
		if len(m["matchExpressions"].([]interface{})) == 0 {
			t.Fatal("branch not protected")
		}
	}
	if !e.Isolation(context.Background(), req(out), true).Allowed {
		t.Fatal("not idempotent")
	}
	unstructured.SetNestedSlice(o.Object, []interface{}{map[string]interface{}{}}, path...)
	if e.Isolation(context.Background(), req(o), false).Allowed {
		t.Fatal("empty match-none branch broadened")
	}
}
func TestIsolationAllowedPoolAndUntrustedMarker(t *testing.T) {
	e, _, _ := setup()
	o := pod()
	o.SetLabels(map[string]string{RouteLabel: "base"})
	out := mutate(t, o, e.Routing(context.Background(), req(o), false))
	out = mutate(t, out, e.Isolation(context.Background(), req(out), false))
	sel, _, _ := unstructured.NestedStringMap(out.Object, "spec", "nodeSelector")
	if sel["test/pool"] != "ego" {
		t.Fatal(sel)
	}
	if !e.Isolation(context.Background(), req(out), true).Allowed {
		t.Fatal("final isolation failed")
	}
	fake := pod()
	fake.SetAnnotations(out.GetAnnotations())
	fake.Object["spec"] = out.Object["spec"]
	if e.Isolation(context.Background(), req(fake), false).Allowed {
		t.Fatal("forged decision bypass")
	}
}
func TestCPUAndConflicts(t *testing.T) {
	e, s, _ := setup()
	o := pod()
	o.SetLabels(map[string]string{RouteLabel: "cpu"})
	unstructured.SetNestedSlice(o.Object, []interface{}{map[string]interface{}{"name": "init", "resources": map[string]interface{}{"limits": map[string]interface{}{"nvidia.com/gpu": "1"}}}}, "spec", "initContainers")
	if e.Routing(context.Background(), req(o), false).Allowed {
		t.Fatal("GPU init container allowed")
	}
	s.Routes[0].Spec.Rules = append(s.Routes[0].Spec.Rules, s.Routes[0].Spec.Rules[0])
	o = pod()
	if e.Routing(context.Background(), req(o), false).Allowed {
		t.Fatal("duplicate rules accepted")
	}
}
func TestGrandfatherTrustedUIDOnly(t *testing.T) {
	e, s, m := setup()
	parent := workload("Deployment")
	parent.SetUID("legacy")
	m["business/Deployment/probe"] = parent
	s.Pools[0].Spec.LegacyRegistry = &RegistryRef{Namespace: "system", Name: "registry"}
	m["system/ConfigMap/registry"] = &unstructured.Unstructured{Object: map[string]interface{}{"data": map[string]interface{}{"legacyOwners": "[\"business|legacy\"]"}}}
	o := pod()
	owned(o, parent)
	unstructured.SetNestedStringMap(o.Object, map[string]string{"test/pool": "ego"}, "spec", "nodeSelector")
	r := req(o)
	r.UserInfo.Username = "controller"
	if !e.Isolation(context.Background(), r, false).Allowed {
		t.Fatal("trusted legacy recreation denied")
	}
	r.UserInfo.Username = "user"
	if e.Isolation(context.Background(), r, false).Allowed {
		t.Fatal("legacy forged owner allowed")
	}
}
func TestPreviewDisabledAndLoadFailure(t *testing.T) {
	e, s, _ := setup()
	s.Routes[0].Spec.Mode = "Preview"
	r := e.Routing(context.Background(), req(pod()), false)
	if !r.Allowed || len(r.Patch) > 0 || len(r.Warnings) == 0 {
		t.Fatal(r)
	}
	s.Routes[0].Spec.Mode = "Disabled"
	r = e.Routing(context.Background(), req(pod()), false)
	if !r.Allowed || len(r.Patch) > 0 {
		t.Fatal(r)
	}
	e.Load = func(context.Context) (State, error) { return State{}, fmt.Errorf("unavailable") }
	if e.Routing(context.Background(), req(pod()), false).Allowed {
		t.Fatal("read failure allowed")
	}
}
func TestUnmanagedUpdatePreserved(t *testing.T) {
	e, _, _ := setup()
	o := workload("Deployment")
	r := req(o)
	r.Operation = admission.Update
	r.OldObject = r.Object
	unstructured.SetNestedField(o.Object, int64(3), "spec", "replicas")
	r.Object.Raw, _ = json.Marshal(o.Object)
	if !e.Routing(context.Background(), r, true).Allowed {
		t.Fatal("old scale update blocked")
	}
}

func TestForgedOldAndUnsupportedOwner(t *testing.T) {
	e, _, m := setup()
	p := workload("Deployment")
	p.SetLabels(map[string]string{RouteLabel: "base"})
	p = mutate(t, p, e.Routing(context.Background(), req(p), false))
	p.SetLabels(map[string]string{"routing.example.org/controller": "old"})
	// An ordinary root cannot bypass validation by setting the training old marker.
	if e.Routing(context.Background(), req(p), true).Allowed {
		t.Fatal("old marker bypass")
	}
	p.SetKind("DaemonSet")
	p.SetUID("daemon")
	m["business/DaemonSet/probe"] = p
	child := pod()
	owned(child, p)
	r := req(child)
	r.UserInfo.Username = "controller"
	if e.Routing(context.Background(), r, false).Allowed {
		t.Fatal("unmanaged DaemonSet decision trusted")
	}
}
func TestWorkflowTemplateAndMetadata(t *testing.T) {
	e, _, m := setup()
	o := workload("Workflow")
	o = mutate(t, o, e.Routing(context.Background(), req(o), false))
	r := req(o)
	r.Operation = admission.Update
	r.OldObject = r.Object
	unstructured.SetNestedField(o.Object, "bad", "spec", "podMetadata", "annotations", DecisionKey)
	r.Object.Raw, _ = json.Marshal(o.Object)
	if e.Routing(context.Background(), r, true).Allowed {
		t.Fatal("Workflow Pod metadata changed")
	}
	o = workload("Workflow")
	unstructured.SetNestedMap(o.Object, map[string]interface{}{"name": "ref"}, "spec", "workflowTemplateRef")
	m["business/WorkflowTemplate/ref"] = &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{"podSpecPatch": "{}"}}}
	if e.Routing(context.Background(), req(o), false).Allowed {
		t.Fatal("unsafe referenced template")
	}
}

func TestExplicitPipelineExcludesLegacyRoute(t *testing.T) {
	e, s, _ := setup()
	s.Routes[0].Spec.Rules[0].Match.AbsentLabels = []string{wr.PipelineLabel}
	s.Routes[0].Spec.Rules = append(s.Routes[0].Spec.Rules, wr.Rule{Name: "video", Match: wr.Match{Labels: map[string]string{wr.PipelineLabel: "video"}}, Target: wr.Target{Queue: "video", NodeGroup: "video", PoolRef: "video"}})
	o := pod()
	o.SetLabels(map[string]string{RouteLabel: "base", wr.PipelineLabel: "video"})
	out := mutate(t, o, e.Routing(context.Background(), req(o), false))
	d, _ := decision(out)
	if d.Target.Queue != "video" {
		t.Fatal(d)
	}
}
func TestSystemComponentExactIdentity(t *testing.T) {
	e, s, m := setup()
	ds := workload("Deployment")
	ds.SetKind("DaemonSet")
	ds.SetUID("ds")
	m["business/DaemonSet/probe"] = ds
	for i := range s.Pools {
		s.Pools[i].Spec.SystemComponents = []Component{{Namespace: "business", ServiceAccount: "node-agent", OwnerKind: "DaemonSet", OwnerName: "probe"}}
	}
	o := pod()
	owned(o, ds)
	unstructured.SetNestedField(o.Object, "node-agent", "spec", "serviceAccountName")
	unstructured.SetNestedField(o.Object, "direct-system-node", "spec", "nodeName")
	r := req(o)
	r.UserInfo.Username = "controller"
	if !e.Isolation(context.Background(), r, false).Allowed {
		t.Fatal("known system DaemonSet blocked")
	}
	r.UserInfo.Username = "user"
	if e.Isolation(context.Background(), r, false).Allowed {
		t.Fatal("system component spoof accepted")
	}
}

func TestMissingReferencedPoolFailsClosed(t *testing.T) {
	e, s, _ := setup()
	o := pod()
	o.SetLabels(map[string]string{RouteLabel: "base"})
	out := mutate(t, o, e.Routing(context.Background(), req(o), false))
	s.Pools = nil
	if e.Isolation(context.Background(), req(out), false).Allowed {
		t.Fatal("missing referenced pool admitted")
	}
}
