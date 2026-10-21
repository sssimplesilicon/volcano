package workloadpolicy

import (
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
	wr "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

// templatePaths is the only kind-specific part of routing. Workflow PodSpecPatch
// is deliberately rejected: final child admission handles referenced templates.
func templatePaths(o *unstructured.Unstructured) ([][]string, error) {
	switch o.GetAPIVersion() + "/" + o.GetKind() {
	case "v1/Pod":
		return [][]string{{}}, nil
	case "apps/v1/Deployment", "apps/v1/ReplicaSet", "apps/v1/StatefulSet", "apps/v1/DaemonSet", "batch/v1/Job":
		return [][]string{{"spec", "template"}}, nil
	case "batch/v1/CronJob":
		return [][]string{{"spec", "jobTemplate", "spec", "template"}}, nil
	case "kubeflow.org/v1/PyTorchJob":
		m, ok, e := unstructured.NestedMap(o.Object, "spec", "pytorchReplicaSpecs")
		if e != nil || !ok || len(m) == 0 {
			return nil, fmt.Errorf("nonempty pytorchReplicaSpecs required")
		}
		keys := []string{}
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		paths := [][]string{}
		for _, k := range keys {
			paths = append(paths, []string{"spec", "pytorchReplicaSpecs", k, "template"})
		}
		return paths, nil
	case "argoproj.io/v1alpha1/Workflow":
		return nil, nil
	}
	return nil, fmt.Errorf("unsupported workload %s/%s", o.GetAPIVersion(), o.GetKind())
}
func podTemplate(o *unstructured.Unstructured, path []string) (*unstructured.Unstructured, error) {
	if len(path) == 0 {
		return o.DeepCopy(), nil
	}
	m, ok, e := unstructured.NestedMap(o.Object, path...)
	if e != nil || !ok {
		return nil, fmt.Errorf("missing Pod template %v", path)
	}
	return &unstructured.Unstructured{Object: m}, nil
}
func putTemplate(o, p *unstructured.Unstructured, path []string) error {
	if len(path) == 0 {
		o.Object = p.Object
		return nil
	}
	return unstructured.SetNestedMap(o.Object, p.Object, path...)
}
func decision(o *unstructured.Unstructured) (*Decision, error) {
	raw := o.GetAnnotations()[DecisionKey]
	if raw == "" {
		return nil, nil
	}
	var d Decision
	if e := decode(raw, &d); e != nil {
		return nil, fmt.Errorf("invalid workload decision: %w", e)
	}
	if d.Policy == "" || d.Rule == "" || d.Scheduler == "" || d.Target.Queue == "" {
		return nil, fmt.Errorf("incomplete workload decision")
	}
	return &d, nil
}
func setDecision(o *unstructured.Unstructured, d Decision) {
	a := o.GetAnnotations()
	if a == nil {
		a = map[string]string{}
	}
	a[DecisionKey] = encode(d)
	a[wr.RouteAnnotation] = d.Policy + "/" + d.Rule
	o.SetAnnotations(a)
	l := o.GetLabels()
	if l == nil {
		l = map[string]string{}
	}
	l[VersionLabel] = "v2"
	if d.Route != "" {
		l[RouteLabel] = d.Route
	}
	o.SetLabels(l)
}
func routePod(p *unstructured.Unstructured, d Decision, validate bool) error {
	if err := managedAttributes(p, d, validate); err != nil {
		return err
	}
	if err := trainingIdentity(p, d, validate); err != nil {
		return err
	}
	spec, ok, e := unstructured.NestedMap(p.Object, "spec")
	if e != nil || !ok {
		return fmt.Errorf("missing Pod spec")
	}
	if n, _ := spec["nodeName"].(string); n != "" {
		return fmt.Errorf("routed Pod must not set nodeName")
	}
	// PyTorch and Workflow controllers own their PodGroups. A raw Pod cannot choose
	// a group; this is checked at the admission root rather than on trusted children.
	if d.Target.CPUOnly {
		for _, field := range []string{"containers", "initContainers"} {
			xs, _ := spec[field].([]interface{})
			for _, x := range xs {
				c, ok := x.(map[string]interface{})
				if !ok {
					return fmt.Errorf("invalid container")
				}
				for _, r := range []string{"requests", "limits"} {
					v, found, e := unstructured.NestedString(c, "resources", r, "nvidia.com/gpu")
					if e != nil {
						return e
					}
					if found {
						q, e := resource.ParseQuantity(v)
						if e != nil || q.Sign() != 0 {
							return fmt.Errorf("CPU route cannot request NVIDIA GPUs")
						}
					}
				}
			}
		}
	}
	if validate {
		if spec["schedulerName"] != d.Scheduler || p.GetAnnotations()[qa.QueueAnnotation] != d.Target.Queue {
			return fmt.Errorf("scheduler/queue differs from inherited routing decision")
		}
		if d.Target.NodeGroup != "" {
			g, _, _ := unstructured.NestedString(spec, "nodeSelector", GroupLabel)
			if g != d.Target.NodeGroup {
				return fmt.Errorf("nodegroup differs from routing decision")
			}
		}
		return nil
	}
	spec["schedulerName"] = d.Scheduler
	if d.Target.NodeGroup != "" {
		sel, _, e := unstructured.NestedStringMap(spec, "nodeSelector")
		if e != nil {
			return e
		}
		if sel == nil {
			sel = map[string]string{}
		}
		if old := sel[GroupLabel]; old != "" && old != d.Target.NodeGroup {
			return fmt.Errorf("nodegroup selector conflicts with selected route")
		}
		sel[GroupLabel] = d.Target.NodeGroup
		unstructured.SetNestedStringMap(spec, sel, "nodeSelector")
	}
	unstructured.SetNestedMap(p.Object, spec, "spec")
	a := p.GetAnnotations()
	if a == nil {
		a = map[string]string{}
	}
	a[qa.QueueAnnotation] = d.Target.Queue
	p.SetAnnotations(a)
	setDecision(p, d)
	return nil
}

// managedAttributes owns only explicitly configured fields; old decisions remain unchanged.
func managedAttributes(p *unstructured.Unstructured, d Decision, validate bool) error {
	if priority := d.Target.PriorityClassName; priority != "" {
		old, _, err := unstructured.NestedString(p.Object, "spec", "priorityClassName")
		if err != nil || (validate && old != priority) || (!validate && old != "" && old != priority) {
			return fmt.Errorf("priorityClassName conflicts with managed routing decision")
		}
		if !validate {
			// Priority admission ran before this webhook. Let its reinvocation resolve
			// numeric priority and preemption policy for the newly injected class.
			if old == "" {
				unstructured.RemoveNestedField(p.Object, "spec", "priority")
				unstructured.RemoveNestedField(p.Object, "spec", "preemptionPolicy")
			}
			unstructured.SetNestedField(p.Object, priority, "spec", "priorityClassName")
		}
	}
	w := d.Target.Worker
	if w == nil {
		return nil
	}
	if p.GetNamespace() != "" && p.GetNamespace() != w.Namespace {
		return fmt.Errorf("worker must run in its configured namespace")
	}
	labels := p.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	for k, want := range map[string]string{wr.PipelineLabel: w.Pipeline, wr.PipelineNamespaceLabel: w.Namespace, "scheduling.simplesilicon.com/worker-type": w.Type, "scheduling.simplesilicon.com/preemptible": "true"} {
		if (validate && labels[k] != want) || (!validate && labels[k] != "" && labels[k] != want) {
			return fmt.Errorf("worker label %s conflicts with managed routing decision", k)
		}
		if !validate {
			labels[k] = want
		}
	}
	if !validate {
		p.SetLabels(labels)
	}
	return nil
}

// Only decisions made with an explicit training target enroll descendants.
// Old decisions remain unchanged across policy updates and controller restarts.
func trainingIdentity(p *unstructured.Unstructured, d Decision, validate bool) error {
	target := d.Target.PreemptionTarget
	if target == nil {
		return nil
	}
	labels := p.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	for key, want := range map[string]string{wr.PipelineLabel: target.Name, wr.PipelineNamespaceLabel: target.Namespace} {
		if (validate && labels[key] != want) || (!validate && labels[key] != "" && labels[key] != want) {
			return fmt.Errorf("training label %s conflicts with frozen preemption target", key)
		}
		if !validate {
			labels[key] = want
		}
	}
	if !validate {
		p.SetLabels(labels)
	}
	return nil
}
func routeWorkload(o *unstructured.Unstructured, d Decision, validate bool) error {
	if w := d.Target.Worker; w != nil {
		if o.GetNamespace() != w.Namespace {
			return fmt.Errorf("worker must run in its configured namespace")
		}
		if o.GetKind() == "PyTorchJob" || o.GetKind() == "Workflow" {
			return fmt.Errorf("managed worker target is not supported on training/Workflow roots")
		}
	}
	if o.GetKind() == "Workflow" {
		if d.Target.PriorityClassName != "" {
			return fmt.Errorf("managed priority on Workflow is not supported")
		}
		// These override the entire Pod and can bypass workflow-level routing.
		if e := workflowSafe(o.Object["spec"]); e != nil {
			return e
		}
		if validate {
			v, _, _ := unstructured.NestedString(o.Object, "spec", "schedulerName")
			if v != d.Scheduler {
				return fmt.Errorf("Workflow scheduler differs from decision")
			}
			q, _, _ := unstructured.NestedString(o.Object, "spec", "podMetadata", "annotations", qa.QueueAnnotation)
			saved, _, _ := unstructured.NestedString(o.Object, "spec", "podMetadata", "annotations", DecisionKey)
			if q != d.Target.Queue || saved != encode(d) {
				return fmt.Errorf("Workflow Pod routing metadata must be preserved")
			}

			return nil
		}
		unstructured.SetNestedField(o.Object, d.Scheduler, "spec", "schedulerName")
		unstructured.SetNestedField(o.Object, d.Target.Queue, "spec", "podMetadata", "annotations", qa.QueueAnnotation)
		unstructured.SetNestedField(o.Object, encode(d), "spec", "podMetadata", "annotations", DecisionKey)
		return nil
	}
	paths, e := templatePaths(o)
	if e != nil {
		return e
	}
	for _, path := range paths {
		p, e := podTemplate(o, path)
		if e != nil {
			return e
		}
		if e = routePod(p, d, validate); e != nil {
			return e
		}
		if !validate {
			if e = putTemplate(o, p, path); e != nil {
				return e
			}
		}
	}
	if o.GetKind() == "PyTorchJob" {
		// The trainer creates PodGroup priority from schedulingPolicy.priorityClass,
		// independently of the replica Pod's priorityClassName.
		if priority := d.Target.PriorityClassName; priority != "" {
			path := []string{"spec", "runPolicy", "schedulingPolicy", "priorityClass"}
			old, _, err := unstructured.NestedString(o.Object, path...)
			if err != nil || (old != "" && old != priority) || (validate && old != priority) {
				return fmt.Errorf("PyTorchJob task-group priority differs from decision")
			}
			if !validate {
				if err := unstructured.SetNestedField(o.Object, priority, path...); err != nil {
					return err
				}
			}
		}
		if validate {
			q, _, _ := unstructured.NestedString(o.Object, "spec", "runPolicy", "schedulingPolicy", "queue")
			if q != d.Target.Queue {
				return fmt.Errorf("PyTorchJob queue differs from decision")
			}
		} else {
			unstructured.SetNestedField(o.Object, d.Target.Queue, "spec", "runPolicy", "schedulingPolicy", "queue")
		}
	}
	return nil
}
func workflowSafe(v interface{}) error {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, v := range x {
			if (k == "podSpecPatch" || k == "nodeName") && v != "" && v != nil {
				return fmt.Errorf("routed Workflow %s is not supported; use structured Pod fields", k)
			}
			if k == "resource" {
				return fmt.Errorf("routed Workflow resource templates require a separate child-workload policy")
			}
			if e := workflowSafe(v); e != nil {
				return e
			}
		}
	case []interface{}:
		for _, v := range x {
			if e := workflowSafe(v); e != nil {
				return e
			}
		}
	}
	return nil
}
