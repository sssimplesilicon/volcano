package guardpreempt

import (
	"k8s.io/apimachinery/pkg/types"
	"testing"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func TestPopFreshReordersAfterEveryRelease(t *testing.T) {
	totals := map[string]int{"A": 5, "B": 4}
	tasks := []*api.TaskInfo{}
	for _, g := range []string{"A", "B"} {
		for i := 0; i < 4; i++ {
			tasks = append(tasks, &api.TaskInfo{UID: api.TaskID(types.UID(g + string(rune('0'+i)))), Name: g})
		}
	}
	build := func(ts []*api.TaskInfo) *util.PriorityQueue {
		q := util.NewPriorityQueue(func(l, r interface{}) bool {
			a, b := l.(*api.TaskInfo), r.(*api.TaskInfo)
			if totals[a.Name] != totals[b.Name] {
				return totals[a.Name] > totals[b.Name]
			}
			return a.UID < b.UID
		})
		for _, x := range ts {
			q.Push(x)
		}
		return q
	}
	for i, want := range []string{"A", "A", "B", "A"} {
		chosen, rest := popFresh(tasks, build)
		if chosen.Name != want {
			t.Fatalf("pick %d: got %s want %s totals=%v", i, chosen.Name, want, totals)
		}
		totals[chosen.Name]--
		tasks = rest
	}
	// Transaction discard restores totals; a new plan must start from restored state.
	totals["A"], totals["B"] = 5, 4
	chosen, _ := popFresh(tasks, build)
	if chosen.Name != "A" {
		t.Fatal("restored accounting not used")
	}
}

// Reproduces the defect the new loop fixes: heap keys depend on mutable totals.
func TestOldHeapHasStaleRootAfterAccountingChange(t *testing.T) {
	totals := map[string]int{"A": 5, "B": 4}
	q := util.NewPriorityQueue(func(l, r interface{}) bool {
		a, b := l.(*api.TaskInfo), r.(*api.TaskInfo)
		if totals[a.Name] != totals[b.Name] {
			return totals[a.Name] > totals[b.Name]
		}
		return a.UID < b.UID
	})
	for _, g := range []string{"A", "B"} {
		for i := 0; i < 4; i++ {
			q.Push(&api.TaskInfo{UID: api.TaskID(types.UID(g + string(rune('0'+i)))), Name: g})
		}
	}
	for i := 0; i < 2; i++ {
		v := q.Pop().(*api.TaskInfo)
		totals[v.Name]--
	}
	stale := q.Pop().(*api.TaskInfo)
	if totals[stale.Name] >= totals["B"] {
		t.Fatalf("counterexample no longer reproduces; review upstream heap behavior: %v", totals)
	}
}
