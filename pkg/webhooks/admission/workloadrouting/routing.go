// Package workloadrouting routes new PyTorchJobs before controllers create Pods.
// Queue authorization is always read from QueueAccessPolicy, never duplicated.
package workloadrouting

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	admission "k8s.io/api/admission/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
)

const Path = "/apis/scheduling.simplesilicon.com/v1alpha1/workloadroutingpolicies"
const PipelineLabel = "pipeline.simplesilicon.com/name"
const RouteAnnotation = "scheduling.simplesilicon.com/workload-routing-policy"

type Scope struct {
	Include []string `json:"include"`
	Exclude []string `json:"exclude,omitempty"`
}
type Match struct {
	Labels       map[string]string `json:"labels,omitempty"`
	AbsentLabels []string          `json:"absentLabels,omitempty"`
	BatchValues  []string          `json:"batchValues,omitempty"`
	LegacyBatch  bool              `json:"legacyBatch,omitempty"`
}
type Target struct {
	Queue     string `json:"queue"`
	NodeGroup string `json:"nodeGroup"`
	PoolRef   string `json:"poolRef,omitempty"`
	CPUOnly   bool   `json:"cpuOnly,omitempty"`
}
type Rule struct {
	Name                 string `json:"name"`
	Match                Match  `json:"match"`
	QueueAccessPolicyRef string `json:"queueAccessPolicyRef"`
	Target               Target `json:"target"`
}
type Spec struct {
	NamespaceScope Scope  `json:"namespaceScope"`
	SchedulerName  string `json:"schedulerName"`
	Rules          []Rule `json:"rules"`
	Fallback       Target `json:"fallback"`
	// Empty kinds retain the original PyTorchJob-only interface.
	WorkloadKinds   []string `json:"workloadKinds,omitempty"`
	ControllerUsers []string `json:"controllerUsers,omitempty"`
	Mode            string   `json:"mode,omitempty"`
}
type Policy struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata"`
	Spec            Spec `json:"spec"`
}
type Snapshot struct {
	Items []Policy `json:"items"`
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
func (s Scope) Includes(ns string) bool {
	return !contains(s.Exclude, ns) && (contains(s.Include, "*") || contains(s.Include, ns))
}
func (p Policy) Validate() error {
	if len(p.Spec.NamespaceScope.Include) == 0 || p.Spec.SchedulerName == "" || p.Spec.Fallback.Queue == "" || p.Spec.Fallback.NodeGroup == "" {
		return fmt.Errorf("policy %s: namespace scope, scheduler and fallback are required", p.Name)
	}
	names := map[string]bool{}
	for _, r := range p.Spec.Rules {
		if r.Name == "" || names[r.Name] {
			return fmt.Errorf("policy %s: duplicate or empty rule name", p.Name)
		}
		names[r.Name] = true
		if r.QueueAccessPolicyRef == "" || r.QueueAccessPolicyRef != r.Target.Queue || r.Target.NodeGroup == "" {
			return fmt.Errorf("rule %s: queueAccessPolicyRef must equal target queue and nodeGroup must be set", r.Name)
		}
		if len(r.Match.Labels) == 0 && len(r.Match.BatchValues) == 0 {
			return fmt.Errorf("rule %s: empty match is not allowed", r.Name)
		}
		if r.Match.LegacyBatch && (len(r.Match.BatchValues) == 0 || r.Match.Labels[PipelineLabel] != "") {
			return fmt.Errorf("rule %s: legacyBatch requires batch values and no explicit pipeline label", r.Name)
		}
	}
	return nil
}

// Route returns a new spec and provenance. Only CREATE roots are passed here.
func Route(obj *unstructured.Unstructured, ns string, policies Snapshot, access *qa.Snapshot) (map[string]interface{}, string, error) {
	var selected *Policy
	for i := range policies.Items {
		p := &policies.Items[i]
		if len(p.Spec.WorkloadKinds) > 0 && !contains(p.Spec.WorkloadKinds, "PyTorchJob") {
			continue
		}
		if !p.Spec.NamespaceScope.Includes(ns) {
			continue
		}
		if selected != nil {
			return nil, "", fmt.Errorf("multiple routing policies match namespace %s: %s, %s", ns, selected.Name, p.Name)
		}
		selected = p
	}
	if selected == nil {
		return nil, "", nil
	}
	p := selected
	if p.DeletionTimestamp != nil {
		return nil, "", fmt.Errorf("routing policy %s is terminating", p.Name)
	}
	if err := p.Validate(); err != nil {
		return nil, "", err
	}
	reps, ok, err := unstructured.NestedMap(obj.Object, "spec", "pytorchReplicaSpecs")
	if err != nil || !ok || len(reps) == 0 {
		return nil, "", fmt.Errorf("pytorchReplicaSpecs must be a nonempty map")
	}
	first := true
	batch := ""
	for _, v := range reps {
		r, ok := v.(map[string]interface{})
		if !ok {
			return nil, "", fmt.Errorf("invalid replica spec")
		}
		b, _, e := unstructured.NestedString(r, "template", "spec", "nodeSelector", "batch")
		if e != nil {
			return nil, "", e
		}
		if !first && batch != b {
			return nil, "", fmt.Errorf("replica nodeSelector.batch values must be identical")
		}
		first = false
		batch = b
	}
	labels := obj.GetLabels()
	if labels["batch"] != "" {
		batch = labels["batch"]
	}
	target := p.Spec.Fallback
	matched := "fallback"
	var rule *Rule
	for i := range p.Spec.Rules {
		r := &p.Spec.Rules[i]
		if r.Match.LegacyBatch && labels[PipelineLabel] != "" {
			continue
		}
		matches := true
		for k, v := range r.Match.Labels {
			if actual, exists := labels[k]; !exists || actual != v {
				matches = false
			}
		}
		for _, key := range r.Match.AbsentLabels {
			if labels[key] != "" {
				matches = false
			}
		}
		if len(r.Match.BatchValues) > 0 && !contains(r.Match.BatchValues, batch) {
			matches = false
		}
		if !matches {
			continue
		}
		if rule != nil {
			return nil, "", fmt.Errorf("ambiguous routing rules: %s and %s", rule.Name, r.Name)
		}
		rule = r
	}
	if access == nil {
		return nil, "", fmt.Errorf("queue authorization unavailable")
	}
	if rule != nil {
		// A missing/terminating policy is a configuration error, not permission to use the queue.
		if !access.Managed(rule.QueueAccessPolicyRef) {
			return nil, "", fmt.Errorf("missing QueueAccessPolicy %s", rule.QueueAccessPolicyRef)
		}
		for _, a := range access.Items {
			if a.Name == rule.QueueAccessPolicyRef && a.DeletionTimestamp != nil {
				return nil, "", fmt.Errorf("QueueAccessPolicy %s is terminating", a.Name)
			}
		}
		if access.Check(ns, rule.Target.Queue) == nil {
			target = rule.Target
			matched = rule.Name
		}
	}
	if err := access.Check(ns, target.Queue); err != nil {
		return nil, "", fmt.Errorf("selected queue is not authorized: %w", err)
	}
	result := obj.DeepCopy()
	if err := unstructured.SetNestedField(result.Object, target.Queue, "spec", "runPolicy", "schedulingPolicy", "queue"); err != nil {
		return nil, "", err
	}
	for key, v := range reps {
		r := v.(map[string]interface{})
		for _, field := range []struct {
			value string
			path  []string
		}{
			{p.Spec.SchedulerName, []string{"template", "spec", "schedulerName"}},
			{target.NodeGroup, []string{"template", "spec", "nodeSelector", "volcano.sh/nodegroup-name"}},
			{target.Queue, []string{"template", "metadata", "annotations", qa.QueueAnnotation}},
			{p.Name + "/" + matched, []string{"template", "metadata", "annotations", RouteAnnotation}},
			{"v1", []string{"template", "metadata", "labels", "scheduling.simplesilicon.com/training-routing-version"}},
		} {
			if err := unstructured.SetNestedField(r, field.value, field.path...); err != nil {
				return nil, "", err
			}
		}
		reps[key] = r
	}
	if err := unstructured.SetNestedMap(result.Object, reps, "spec", "pytorchReplicaSpecs"); err != nil {
		return nil, "", err
	}
	spec, _, err := unstructured.NestedMap(result.Object, "spec")
	return spec, p.Name + "/" + matched, err
}

type Loader func() (Snapshot, *qa.Snapshot, error)

func Load(client kubernetes.Interface) Loader {
	return func() (Snapshot, *qa.Snapshot, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		raw, err := client.CoreV1().RESTClient().Get().AbsPath(Path).DoRaw(ctx)
		if err != nil {
			return Snapshot{}, nil, err
		}
		var s Snapshot
		if err = json.Unmarshal(raw, &s); err != nil {
			return s, nil, err
		}
		a, err := qa.Load(client)
		return s, a, err
	}
}
func Mutate(review admission.AdmissionReview, load Loader) *admission.AdmissionResponse {
	response := &admission.AdmissionResponse{Allowed: true}
	r := review.Request
	if r == nil {
		return &admission.AdmissionResponse{Allowed: false, Result: &meta.Status{Message: "missing admission request", Code: 400}}
	}
	response.UID = r.UID
	if r.Operation != admission.Create || r.SubResource != "" || r.Resource.Group != "kubeflow.org" || r.Resource.Resource != "pytorchjobs" {
		return response
	}
	fail := func(err error) *admission.AdmissionResponse {
		response.Allowed = false
		response.Result = &meta.Status{Code: 403, Message: err.Error()}
		return response
	}
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(r.Object.Raw, &obj.Object); err != nil {
		return fail(err)
	}
	// Existing roots keep their original template on UPDATE. Old-controller jobs
	// and controller-owned child resources must not be migrated by admission.
	if len(obj.GetOwnerReferences()) > 0 || obj.GetLabels()["routing.example.org/controller"] == "old" {
		return response
	}
	policies, access, err := load()
	if err != nil {
		return fail(err)
	}
	spec, provenance, err := Route(obj, r.Namespace, policies, access)
	if err != nil {
		return fail(err)
	}
	if spec == nil {
		return response
	}
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[RouteAnnotation] = provenance
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels["scheduling.simplesilicon.com/training-routing-version"] = "v1"
	patch := []map[string]interface{}{{"op": "add", "path": "/spec", "value": spec}, {"op": "add", "path": "/metadata/annotations", "value": annotations}, {"op": "add", "path": "/metadata/labels", "value": labels}}
	response.Patch, err = json.Marshal(patch)
	if err != nil {
		return fail(err)
	}
	pt := admission.PatchTypeJSONPatch
	response.PatchType = &pt
	return response
}
