package workloadpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	admission "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
	wr "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

func encode(v interface{}) string          { b, _ := json.Marshal(v); return string(b) }
func decode(s string, v interface{}) error { return json.Unmarshal([]byte(s), v) }
func supported(r *admission.AdmissionRequest) bool {
	pairs := map[string]string{"/pods": "Pod", "apps/deployments": "Deployment", "apps/replicasets": "ReplicaSet", "apps/statefulsets": "StatefulSet", "batch/jobs": "Job", "batch/cronjobs": "CronJob", "argoproj.io/workflows": "Workflow", "kubeflow.org/pytorchjobs": "PyTorchJob"}
	return r.SubResource == "" && pairs[r.Resource.Group+"/"+r.Resource.Resource] == r.Kind.Kind
}
func owner(o *unstructured.Unstructured) (*metav1.OwnerReference, error) {
	var out *metav1.OwnerReference
	for _, v := range o.GetOwnerReferences() {
		if v.Controller != nil && *v.Controller {
			if out != nil {
				return nil, fmt.Errorf("multiple controller owners")
			}
			c := v
			out = &c
		}
	}
	return out, nil
}
func (e Engine) ancestors(ctx context.Context, o *unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	result := []*unstructured.Unstructured{}
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		ref, err := owner(o)
		if err != nil {
			return nil, err
		}
		if ref == nil {
			return result, nil
		}
		if seen[string(ref.UID)] {
			return nil, fmt.Errorf("owner cycle")
		}
		seen[string(ref.UID)] = true
		if e.Reader == nil {
			return nil, fmt.Errorf("owner resolver unavailable")
		}
		p, err := e.Reader.Get(ctx, ref.APIVersion, ref.Kind, o.GetNamespace(), ref.Name)
		if err != nil {
			return nil, fmt.Errorf("resolve controller: %w", err)
		}
		if p.GetUID() != ref.UID || p.GetDeletionTimestamp() != nil {
			return nil, fmt.Errorf("owner UID mismatch or terminating controller")
		}
		result = append(result, p)
		o = p
	}
	return nil, fmt.Errorf("controller chain too deep")
}
func selectPolicy(s State, ns, kind string) (*wr.Policy, error) {
	var out *wr.Policy
	for i := range s.Routes {
		p := &s.Routes[i]
		kinds := p.Spec.WorkloadKinds
		if len(kinds) == 0 {
			kinds = []string{"PyTorchJob"}
		}
		if mode(p.Spec.Mode) == "Disabled" || !p.Spec.NamespaceScope.Includes(ns) || !has(kinds, kind) {
			continue
		}
		if out != nil {
			return nil, fmt.Errorf("overlapping workload policies %s and %s", out.Name, p.Name)
		}
		out = p
	}
	if out != nil && out.DeletionTimestamp != nil {
		return nil, fmt.Errorf("routing policy is terminating")
	}
	return out, nil
}
func effectiveBatch(o *unstructured.Unstructured) (string, error) {
	batch := ""
	if o.GetKind() == "PyTorchJob" {
		paths, err := templatePaths(o)
		if err != nil {
			return "", err
		}
		for i, path := range paths {
			p, err := podTemplate(o, path)
			if err != nil {
				return "", err
			}
			b, _, err := unstructured.NestedString(p.Object, "spec", "nodeSelector", "batch")
			if err != nil {
				return "", err
			}
			if i > 0 && b != batch {
				return "", fmt.Errorf("replica nodeSelector.batch values must be identical")
			}
			batch = b
		}
	}
	if v := o.GetLabels()["batch"]; v != "" {
		batch = v
	}
	return batch, nil
}
func choose(_ State, p *wr.Policy, o *unstructured.Unstructured) (Decision, error) {
	d := Decision{Policy: p.Name, Generation: p.Generation, Rule: "fallback", Scheduler: p.Spec.SchedulerName, Target: p.Spec.Fallback, Route: o.GetLabels()[RouteLabel]}
	if err := p.Validate(); err != nil {
		return d, err
	}
	batch, err := effectiveBatch(o)
	if err != nil {
		return d, err
	}
	var matched *wr.Rule
	for i := range p.Spec.Rules {
		r := &p.Spec.Rules[i]
		if r.Match.LegacyBatch && (o.GetLabels()[wr.PipelineLabel] != "" || o.GetLabels()[RouteLabel] != "") {
			continue
		}
		ok := true
		for k, v := range r.Match.Labels {
			if actual, exists := o.GetLabels()[k]; !exists || actual != v {
				ok = false
			}
		}
		for _, key := range r.Match.AbsentLabels {
			if o.GetLabels()[key] != "" {
				ok = false
			}
		}
		if len(r.Match.BatchValues) > 0 && !has(r.Match.BatchValues, batch) {
			ok = false
		}
		if !ok {
			continue
		}
		if matched != nil {
			return d, fmt.Errorf("conflicting routing rules %s and %s", matched.Name, r.Name)
		}
		matched = r
	}
	if matched != nil {
		if matched.Target.Queue == "" {
			return d, fmt.Errorf("target queue required")
		}
		d.Rule = matched.Name
		d.Target = matched.Target
	}
	if d.Route != "" && d.Route != "default" && matched == nil {
		return d, fmt.Errorf("explicit route has no matching rule")
	}
	return d, nil
}

// inherited resolves the immediate controller's frozen decision. It never trusts
// a decision copied onto the child by the submitting client.
func (e Engine) inherited(ctx context.Context, s State, o *unstructured.Unstructured, user string) (*Decision, error) {
	ref, err := owner(o)
	if err != nil || ref == nil {
		return nil, err
	}
	ancestors, err := e.ancestors(ctx, o)
	if err != nil {
		return nil, err
	}
	for _, p := range ancestors {
		d, err := decision(p)
		if err != nil {
			return nil, err
		}
		if d == nil {
			continue
		}
		var policy *wr.Policy
		for i := range s.Routes {
			if s.Routes[i].Name == d.Policy {
				policy = &s.Routes[i]
			}
		}
		if policy == nil || policy.DeletionTimestamp != nil || !policy.Spec.NamespaceScope.Includes(o.GetNamespace()) {
			return nil, fmt.Errorf("inherited routing policy unavailable")
		}
		if p.GetKind() == "DaemonSet" || p.GetKind() == "ConfigMap" || p.GetKind() == "WorkflowTemplate" {
			return nil, fmt.Errorf("unsupported routing owner kind")
		}
		if !trusted(user, policy.Spec.ControllerUsers) {
			return nil, fmt.Errorf("requester is not a configured workload controller")
		}
		return d, nil
	}
	return nil, nil
}
func response(r *admission.AdmissionRequest, err error) *admission.AdmissionResponse {
	x := &admission.AdmissionResponse{Allowed: err == nil}
	if r != nil {
		x.UID = r.UID
	}
	if err != nil {
		x.Result = &metav1.Status{Code: 403, Message: err.Error()}
	}
	return x
}
func patchResponse(r *admission.AdmissionRequest, o *unstructured.Unstructured) *admission.AdmissionResponse {
	x := response(r, nil)
	pt := admission.PatchTypeJSONPatch
	x.PatchType = &pt
	x.Patch, _ = json.Marshal([]map[string]interface{}{{"op": "add", "path": "/spec", "value": o.Object["spec"]}, {"op": "add", "path": "/metadata/annotations", "value": o.GetAnnotations()}, {"op": "add", "path": "/metadata/labels", "value": o.GetLabels()}})
	return x
}
func object(r *admission.AdmissionRequest) (*unstructured.Unstructured, error) {
	o := &unstructured.Unstructured{}
	err := json.Unmarshal(r.Object.Raw, &o.Object)
	if err == nil {
		o.SetNamespace(r.Namespace)
	}
	return o, err
}
func (e Engine) Routing(ctx context.Context, r *admission.AdmissionRequest, validate bool) *admission.AdmissionResponse {
	if r == nil {
		return response(r, fmt.Errorf("missing request"))
	}
	if !supported(r) || (r.Operation != admission.Create && r.Operation != admission.Update) {
		return response(r, nil)
	}
	o, err := object(r)
	if err != nil {
		return response(r, err)
	}
	if r.Operation == admission.Update {
		return e.update(ctx, r, o, validate)
	}
	// Do not change old-controller training jobs; their Pods are handled separately.
	if o.GetKind() == "PyTorchJob" && o.GetLabels()["routing.example.org/controller"] == "old" {
		if hasDecision(o.Object) {
			return response(r, fmt.Errorf("old training job cannot claim a new workload decision"))
		}
		return response(r, nil)
	}
	s, err := e.Load(ctx)
	if err != nil {
		return response(r, err)
	}
	ref, err := owner(o)
	if err != nil {
		return response(r, err)
	}
	var d *Decision
	var p *wr.Policy
	if ref != nil {
		d, err = e.inherited(ctx, s, o, r.UserInfo.Username)
	} else {
		p, err = selectPolicy(s, r.Namespace, o.GetKind())
		if err == nil && p != nil {
			v, x := choose(s, p, o)
			d = &v
			err = x
		}
	}
	preview := p != nil && mode(p.Spec.Mode) == "Preview"
	// Old policies can remain during the service handoff. Emit an explicit
	// warning instead of loading the retired ACL or blocking a rolling upgrade.
	warnings := []string{}
	if p != nil {
		for _, rule := range p.Spec.Rules {
			if rule.QueueAccessPolicyRef != "" {
				warnings = []string{"queueAccessPolicyRef is deprecated and ignored by unified routing; queue selection no longer uses namespace authorization"}
				break
			}
		}
	}
	if err != nil {
		if preview {
			x := response(r, nil)
			x.Warnings = []string{"routing preview: " + err.Error()}
			return x
		}
		return response(r, err)
	}
	if d == nil {
		if o.GetAnnotations()[DecisionKey] != "" {
			return response(r, fmt.Errorf("unowned workload decision cannot be supplied by client"))
		}
		return response(r, nil)
	}
	if ref == nil && o.GetKind() == "Pod" && o.GetAnnotations()[qa.GroupAnnotation] != "" {
		return response(r, fmt.Errorf("ordinary root Pod cannot select a PodGroup"))
	}
	if err = e.workflowReferences(ctx, o); err != nil {
		return response(r, err)
	}
	if preview {
		x := response(r, nil)
		x.Warnings = []string{"routing preview: " + encode(d)}
		return x
	}
	if validate {
		actual, err := decision(o)
		if err != nil {
			return response(r, err)
		}
		if actual == nil || !reflect.DeepEqual(*actual, *d) {
			return response(r, fmt.Errorf("routing decision differs from authoritative policy/controller"))
		}
		x := response(r, routeWorkload(o, *d, true))
		x.Warnings = warnings
		return x
	}
	setDecision(o, *d)
	if err = routeWorkload(o, *d, false); err != nil {
		return response(r, err)
	}
	x := patchResponse(r, o)
	x.Warnings = warnings
	return x
}
func (e Engine) update(ctx context.Context, r *admission.AdmissionRequest, o *unstructured.Unstructured, validate bool) *admission.AdmissionResponse {
	if !validate {
		return response(r, nil)
	}
	old := &unstructured.Unstructured{}
	if err := json.Unmarshal(r.OldObject.Raw, &old.Object); err != nil {
		return response(r, err)
	}
	if old.GetAnnotations()[DecisionKey] != o.GetAnnotations()[DecisionKey] {
		return response(r, fmt.Errorf("routing enrollment/decision is immutable; recreate to change route"))
	}
	d, err := decision(old)
	if err != nil {
		return response(r, err)
	}
	if d == nil {
		// A copied template decision must not enroll an old controller via UPDATE.
		if hasDecision(o.Object) && !hasDecision(old.Object) {
			return response(r, fmt.Errorf("cannot enroll old workload via template update"))
		}
		return response(r, nil)
	}
	for _, k := range []string{RouteLabel, VersionLabel, wr.PipelineLabel, "batch"} {
		if o.GetLabels()[k] != old.GetLabels()[k] {
			return response(r, fmt.Errorf("routing input %s is immutable", k))
		}
	}
	if d.Target.Worker != nil && o.GetLabels()["scheduling.simplesilicon.com/worker-type"] != old.GetLabels()["scheduling.simplesilicon.com/worker-type"] {
		return response(r, fmt.Errorf("managed worker type is immutable; recreate to change type"))
	}
	// Bound Pods may have nodeName set by the scheduler. Do not intercept status or
	// ordinary metadata updates; protect only their scheduling identity here.
	if o.GetKind() == "Pod" {
		if err := managedAttributes(o, *d, true); err != nil {
			return response(r, err)
		}
		if err := trainingIdentity(o, *d, true); err != nil {
			return response(r, err)
		}
		for _, path := range [][]string{{"spec", "schedulerName"}, {"spec", "nodeSelector"}, {"spec", "affinity"}} {
			a, _, _ := unstructured.NestedFieldNoCopy(old.Object, path...)
			b, _, _ := unstructured.NestedFieldNoCopy(o.Object, path...)
			if !reflect.DeepEqual(a, b) {
				return response(r, fmt.Errorf("Pod scheduling identity is immutable"))
			}
		}
		if o.GetAnnotations()[qa.QueueAnnotation] != old.GetAnnotations()[qa.QueueAnnotation] {
			return response(r, fmt.Errorf("Pod queue is immutable"))
		}
		return response(r, nil)
	}
	if err = e.workflowReferences(ctx, o); err != nil {
		return response(r, err)
	}
	validationObject := o
	if o.GetKind() == "PyTorchJob" && d.Target.PriorityClassName != "" {
		path := []string{"spec", "runPolicy", "schedulingPolicy", "priorityClass"}
		before, _, beforeErr := unstructured.NestedString(old.Object, path...)
		after, _, afterErr := unstructured.NestedString(o.Object, path...)
		if beforeErr == nil && afterErr == nil && before == "" && after == "" {
			// Preserve updates to roots enrolled before task-group priority injection.
			// Do not mutate them on upgrade. An explicit repair must match the frozen
			// decision; once present, the normal validation forbids removing it.
			validationObject = o.DeepCopy()
			if err := unstructured.SetNestedField(validationObject.Object, d.Target.PriorityClassName, path...); err != nil {
				return response(r, err)
			}
		}
	}
	if err = routeWorkload(validationObject, *d, true); err != nil {
		return response(r, err)
	}
	// The decision must remain in each template, not just on the root.
	if o.GetKind() != "Workflow" {
		paths, _ := templatePaths(o)
		for _, path := range paths {
			p, err := podTemplate(o, path)
			if err != nil {
				return response(r, err)
			}
			pd, err := decision(p)
			if err != nil || pd == nil || !reflect.DeepEqual(pd, d) {
				return response(r, fmt.Errorf("template decision must be preserved"))
			}
		}
	}
	return response(r, nil)
}
func hasDecision(v interface{}) bool {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, v := range x {
			if k == DecisionKey {
				return true
			}
			if hasDecision(v) {
				return true
			}
		}
	case []interface{}:
		for _, v := range x {
			if hasDecision(v) {
				return true
			}
		}
	}
	return false
}
