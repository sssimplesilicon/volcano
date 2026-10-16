package workerpreemption

import (
	"testing"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

func TestExplicitRegistration(t *testing.T) {
	if _, ok := framework.GetAction("worker-type-preempt"); ok {
		t.Fatal("action registered without opt-in")
	}
	if _, ok := framework.GetPluginBuilder(name); ok {
		t.Fatal("plugin registered without opt-in")
	}
	Register()
	if _, ok := framework.GetAction("worker-type-preempt"); !ok {
		t.Fatal("action missing after opt-in")
	}
	if _, ok := framework.GetPluginBuilder(name); !ok {
		t.Fatal("plugin missing after opt-in")
	}
}

func TestConfigIsolation(t *testing.T) {
	for _, tc := range []struct {
		name, config  string
		enabled, fail bool
	}{
		{"upstream default", `actions: "enqueue, allocate, backfill"`, false, false},
		{"upstream native preempt", `actions: "enqueue, allocate, preempt, reclaim"`, false, false},
		{"enabled but unused", `actions: "enqueue, allocate, backfill"`, true, false},
		{"action without opt-in", `actions: "worker-type-preempt"`, false, true},
		{"action without plugin", `actions: "worker-type-preempt"`, true, true},
		{"plugin without opt-in", "tiers:\n- plugins:\n  - name: worker-type-preemption", false, true},
		{"valid", "actions: enqueue, allocate, worker-type-preempt\ntiers:\n- plugins:\n  - name: worker-type-preemption", true, false},
		{"conflicting preempt", "actions: preempt, worker-type-preempt\ntiers:\n- plugins:\n  - name: worker-type-preemption", true, true},
		{"conflicting reclaim", "actions: reclaim\ntiers:\n- plugins:\n  - name: worker-type-preemption", true, true},
		{"action disabled for recovery", "actions: allocate\ntiers:\n- plugins:\n  - name: worker-type-preemption", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfig(tc.config, tc.enabled)
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v want failure=%v", err, tc.fail)
			}
		})
	}
}
