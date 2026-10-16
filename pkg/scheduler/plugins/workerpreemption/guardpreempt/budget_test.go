package guardpreempt

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kt "k8s.io/client-go/testing"
	"testing"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

func task(n string) *api.TaskInfo {
	return api.NewTaskInfo(&v1.Pod{ObjectMeta: meta.ObjectMeta{Name: n, Namespace: "n", UID: types.UID(n), Labels: map[string]string{"type": "A"}}})
}
func budget() policy.PodDisruptionBudget {
	return policy.PodDisruptionBudget{ObjectMeta: meta.ObjectMeta{Name: "a", Namespace: "n", UID: "pdb", Generation: 1}, Spec: policy.PodDisruptionBudgetSpec{Selector: &meta.LabelSelector{MatchLabels: map[string]string{"type": "A"}}}, Status: policy.PodDisruptionBudgetStatus{ObservedGeneration: 1, DisruptionsAllowed: 1}}
}
func TestBudgetReservationRollback(t *testing.T) {
	g := testGuard()
	g.reset([]policy.PodDisruptionBudget{budget()})
	a, b := task("a"), task("b")
	if !g.reserve(a) || g.reserve(b) {
		t.Fatal("overreserved")
	}
	g.release(a)
	g.release(a)
	if !g.reserve(b) || g.reserve(a) {
		t.Fatal("rollback not exact")
	}
}
func TestStaleAndOverlappingFailClosed(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		g := testGuard()
		p := budget()
		ps := []policy.PodDisruptionBudget{p}
		if overlap {
			p.UID = "other"
			ps = append(ps, p)
		} else {
			ps[0].Status.ObservedGeneration = 0
		}
		g.reset(ps)
		if g.reserve(task("a")) {
			t.Fatal("unsafe reservation")
		}
	}
}
func TestEvictionRejectionNoDelete(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		g := testGuard()
		c := newTestClient()
		calls := 0
		c.PrependReactor("create", "pods", func(a kt.Action) (bool, runtime.Object, error) {
			calls++
			e := a.(kt.CreateAction).GetObject().(*policy.Eviction)
			if e.DeleteOptions.Preconditions.UID == nil || *e.DeleteOptions.Preconditions.UID != "a" {
				t.Fatal("UID guard missing")
			}
			if ambiguous {
				return true, nil, fmt.Errorf("connection lost")
			}
			return true, nil, errors.NewTooManyRequests("budget exhausted", 0)
		})
		g.evict(c, []*v1.Pod{task("a").Pod, task("b").Pod})
		if calls != 1 {
			t.Fatal("continued after failure")
		}
		if (len(g.pending) == 1) != ambiguous {
			t.Fatal("ambiguous handling")
		}
		for _, a := range c.Actions() {
			if a.GetVerb() == "delete" {
				t.Fatal("direct Delete bypass")
			}
		}
	}
}
func TestPartialSuccessKeepsAccepted(t *testing.T) {
	g := testGuard()
	c := newTestClient()
	calls := 0
	c.PrependReactor("create", "pods", func(a kt.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return true, nil, nil
		}
		return true, nil, errors.NewTooManyRequests("denied", 0)
	})
	g.evict(c, []*v1.Pod{task("a").Pod, task("b").Pod, task("c").Pod})
	if calls != 2 || len(g.pending) != 1 || g.pending["a"] == nil {
		t.Fatal("partial success lost")
	}
}

func TestDiscardNominationRestored(t *testing.T) {
	a := task("a")
	original := &api.TransactionContext{Status: api.Pending}
	a.LastTransaction = original
	s := &framework.Session{Jobs: map[api.JobID]*api.JobInfo{"j": {Tasks: map[api.TaskID]*api.TaskInfo{a.UID: a}}}}
	restore := restoreTransactions(s)
	a.LastTransaction = &api.TransactionContext{Status: api.Pipelined, NodeName: "fake", EvictionOccurred: true}
	a.EvictionOccurred = true
	restore()
	if a.LastTransaction != original || a.EvictionOccurred {
		t.Fatal("speculative nomination leaked")
	}
}
