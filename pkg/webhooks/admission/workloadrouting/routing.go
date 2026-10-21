// Package workloadrouting defines the shared routing CRD. All admission paths
// are implemented by workloadpolicy; routing has no queue namespace ACL dependency.
package workloadrouting

import (
	"fmt"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const PipelineLabel = "pipeline.simplesilicon.com/name"
const PipelineNamespaceLabel = "pipeline.simplesilicon.com/namespace"
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
	// PreemptionTarget is frozen into new training decisions, not inferred from a queue name.
	PreemptionTarget *PipelineTarget `json:"preemptionTarget,omitempty"`
	// Managed attributes are frozen with the queue decision. Omission preserves legacy behavior.
	PriorityClassName string        `json:"priorityClassName,omitempty"`
	Worker            *WorkerTarget `json:"worker,omitempty"`
}
type WorkerTarget struct {
	Pipeline  string `json:"pipeline"`
	Namespace string `json:"namespace"`
	Type      string `json:"type"`
}
type PipelineTarget struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}
type Rule struct {
	Name  string `json:"name"`
	Match Match  `json:"match"`
	// Deprecated: accepted for rolling upgrades, never consulted by unified routing.
	QueueAccessPolicyRef string `json:"queueAccessPolicyRef,omitempty"`
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
	for _, target := range append([]Target{p.Spec.Fallback}, ruleTargets(p.Spec.Rules)...) {
		if target.PriorityClassName != "" && len(validation.IsDNS1123Subdomain(target.PriorityClassName)) != 0 {
			return fmt.Errorf("invalid managed priorityClassName")
		}
		if w := target.Worker; w != nil {
			if target.PreemptionTarget != nil || target.CPUOnly || target.PriorityClassName == "" {
				return fmt.Errorf("worker target requires priorityClassName and cannot be a training or CPU-only target")
			}
			if w.Pipeline == "" || len(w.Pipeline) > 63 || len(validation.IsDNS1123Subdomain(w.Pipeline)) != 0 || w.Namespace == "" || len(validation.IsDNS1123Label(w.Namespace)) != 0 || w.Type == "" || len(validation.IsValidLabelValue(w.Type)) != 0 {
				return fmt.Errorf("invalid worker pipeline, namespace or type")
			}
		}
		if t := target.PreemptionTarget; t != nil {
			if t.Name == "" || t.Namespace == "" || len(t.Name) > 63 || len(validation.IsDNS1123Subdomain(t.Name)) != 0 || len(validation.IsDNS1123Label(t.Namespace)) != 0 {
				return fmt.Errorf("preemptionTarget requires a valid pipeline name (at most 63 characters) and namespace")
			}
			if target.CPUOnly || (len(p.Spec.WorkloadKinds) != 0 && (len(p.Spec.WorkloadKinds) != 1 || p.Spec.WorkloadKinds[0] != "PyTorchJob")) {
				return fmt.Errorf("preemptionTarget is only supported on PyTorchJob routes without cpuOnly")
			}
		}
	}
	names := map[string]bool{}
	for _, r := range p.Spec.Rules {
		if r.Name == "" || names[r.Name] {
			return fmt.Errorf("policy %s: duplicate or empty rule name", p.Name)
		}
		names[r.Name] = true
		if r.Target.Queue == "" || r.Target.NodeGroup == "" {
			return fmt.Errorf("rule %s: target queue and nodeGroup must be set", r.Name)
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
func ruleTargets(rules []Rule) []Target {
	out := make([]Target, 0, len(rules))
	for _, r := range rules {
		out = append(out, r.Target)
	}
	return out
}
