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

func TestPreemptionTargetValidation(t *testing.T) {
	for _, tc := range []struct {
		name, ns string
		kinds    []string
		cpu, bad bool
	}{
		{"ego-pipeline", "infra-ego-pipeline", nil, false, false},
		{"video", "video-system", []string{"PyTorchJob"}, false, false},
		{"ego", "", nil, false, true},
		{"ego", "Bad_Namespace", nil, false, true},
		{"ego", "workers", []string{"Deployment"}, false, true},
		{"ego", "workers", []string{"PyTorchJob", "Pod"}, false, true},
		{"ego", "workers", nil, true, true},
	} {
		p := Policy{Spec: Spec{NamespaceScope: Scope{Include: []string{"*"}}, SchedulerName: "s", Fallback: Target{Queue: "default", NodeGroup: "general"}, WorkloadKinds: tc.kinds, Rules: []Rule{{Name: "training", Match: Match{BatchValues: []string{"allocentric"}}, Target: Target{Queue: "shared", NodeGroup: "pipeline", CPUOnly: tc.cpu, PreemptionTarget: &PipelineTarget{Name: tc.name, Namespace: tc.ns}}}}}}
		if err := p.Validate(); (err != nil) != tc.bad {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
}
