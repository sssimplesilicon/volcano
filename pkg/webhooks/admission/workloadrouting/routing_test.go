package workloadrouting

import "testing"

func TestScopeAndPolicy(t *testing.T) {
	p := Policy{Spec: Spec{NamespaceScope: Scope{Include: []string{"*"}, Exclude: []string{"system"}}, SchedulerName: "volcano-infra", Fallback: Target{Queue: "default", NodeGroup: "general"}, Rules: []Rule{{Name: "ego", Match: Match{BatchValues: []string{"allocentric", "old-5880"}}, Target: Target{Queue: "shared", NodeGroup: "ego"}}}}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if !p.Spec.NamespaceScope.Includes("any-business") || p.Spec.NamespaceScope.Includes("system") {
		t.Fatal("namespace scope broken")
	}
	p.Spec.Rules[0].QueueAccessPolicyRef = "shared"
	if err := p.Validate(); err != nil {
		t.Fatal("legacy reference must remain readable during handoff:", err)
	}
	p.Spec.Rules[0].QueueAccessPolicyRef = ""
	p.Spec.Rules = append(p.Spec.Rules, p.Spec.Rules[0])
	if p.Validate() == nil {
		t.Fatal("duplicate rule allowed")
	}
}
