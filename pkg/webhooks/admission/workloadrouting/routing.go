// Package workloadrouting defines the shared routing CRD. All admission paths
// are implemented by workloadpolicy; routing has no queue namespace ACL dependency.
package workloadrouting

import (
	"fmt"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
