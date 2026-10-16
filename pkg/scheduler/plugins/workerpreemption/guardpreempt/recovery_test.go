package guardpreempt

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
	"testing"
	"time"
)

func newTestClient(objects ...runtime.Object) *fake.Clientset {
	cm := &v1.ConfigMap{ObjectMeta: meta.ObjectMeta{Name: "worker-type-preemption-recovery", Namespace: "volcano-system", Labels: map[string]string{"scheduling.simplesilicon.com/recovery-journal": "v1"}}, Data: map[string]string{journalKey: "{}"}}
	hasTraining := false
	for _, o := range objects {
		if p, ok := o.(*v1.Pod); ok && p.Name == "training" {
			hasTraining = true
		}
	}
	if !hasTraining {
		objects = append(objects, trainingPod())
	}
	return fake.NewSimpleClientset(append([]runtime.Object{cm}, objects...)...)
}
func allowRecovery(evictionIntent, *v1.Pod) bool { return true }
func TestRecoveryTimeoutBeforeAndAfterRestart(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			p := task("a").Pod
			c := newTestClient(p)
			g := testGuard()
			calls := 0
			c.PrependReactor("create", "pods", func(a kt.Action) (bool, runtime.Object, error) {
				calls++
				if a.GetSubresource() != "eviction" {
					t.Fatal("not Eviction")
				}
				o, err := c.Tracker().Get(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "volcano-system", "worker-type-preemption-recovery")
				entries := map[string]evictionIntent{}
				if err == nil {
					err = json.Unmarshal([]byte(o.(*v1.ConfigMap).Data[journalKey]), &entries)
				}
				if err != nil || len(entries) != 1 {
					t.Fatal("missing durable intent before POST", err)
				}
				if accepted || calls > 1 {
					c.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, p.Namespace, p.Name)
				}
				if calls == 1 {
					return true, nil, context.DeadlineExceeded
				}
				return true, nil, nil
			})
			g.evict(c, []*v1.Pod{p})
			restarted := newBudgetGuard()
			restarted.journal.now = func() time.Time { return time.Now().Add(time.Hour) }
			_ = restarted.reconcile(c, allowRecovery)
			if err := restarted.reconcile(c, allowRecovery); err != nil {
				t.Fatal(err)
			}
			want := 2
			if accepted {
				want = 1
			}
			if calls != want || len(restarted.journal.entries) != 0 {
				t.Fatal("incorrect recovery", calls)
			}
		})
	}
}
func TestRecoveryBackoffAndReplacement(t *testing.T) {
	p := task("a").Pod
	c := newTestClient(p)
	g := testGuard()
	calls := 0
	c.PrependReactor("create", "pods", func(a kt.Action) (bool, runtime.Object, error) { calls++; return true, nil, context.DeadlineExceeded })
	g.evict(c, []*v1.Pod{p})
	_ = g.reconcile(c, allowRecovery)
	if calls != 1 {
		t.Fatal("ignored backoff")
	}
	p.UID = "replacement"
	c.CoreV1().Pods(p.Namespace).Update(context.Background(), p, meta.UpdateOptions{})
	if err := g.reconcile(c, allowRecovery); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("evicted replacement")
	}
}
func TestJournalWriteFailurePreventsEviction(t *testing.T) {
	c := newTestClient()
	g := testGuard()
	c.PrependReactor("update", "configmaps", func(kt.Action) (bool, runtime.Object, error) { return true, nil, fmt.Errorf("write unavailable") })
	g.evict(c, []*v1.Pod{task("a").Pod})
	for _, a := range c.Actions() {
		if a.GetSubresource() == "eviction" {
			t.Fatal("POST without durable write")
		}
	}
}
func TestPDBInFlightReservationWaits(t *testing.T) {
	p := task("a").Pod
	b := budget()
	b.Status.DisruptedPods = map[string]meta.Time{p.Name: meta.Now()}
	c := newTestClient(p, &b)
	g := testGuard()
	calls := 0
	c.PrependReactor("create", "pods", func(kt.Action) (bool, runtime.Object, error) { calls++; return true, nil, context.DeadlineExceeded })
	g.evict(c, []*v1.Pod{p})
	g.journal.now = func() time.Time { return time.Now().Add(time.Hour) }
	_ = g.reconcile(c, allowRecovery)
	if calls != 1 {
		t.Fatal("retried in-flight PDB reservation")
	}
}

func TestCanceledTrainingStopsRecovery(t *testing.T) {
	p := task("worker").Pod
	training := trainingPod()
	c := newTestClient(p, training)
	g := testGuard()
	calls := 0
	c.PrependReactor("create", "pods", func(kt.Action) (bool, runtime.Object, error) { calls++; return true, nil, context.DeadlineExceeded })
	g.evict(c, []*v1.Pod{p})
	c.CoreV1().Pods(training.Namespace).Delete(context.Background(), training.Name, meta.DeleteOptions{})
	restarted := newBudgetGuard()
	restarted.journal.now = func() time.Time { return time.Now().Add(time.Hour) }
	_ = restarted.reconcile(c, allowRecovery)
	if calls != 1 {
		t.Fatalf("canceled training caused %d Evictions; want 1", calls)
	}
}

func testGuard() *budgetGuard {
	g := newBudgetGuard()
	g.demand = []podIdentity{{Namespace: "n", Name: "training", UID: "training"}}
	return g
}

func TestRecoveryDemandStates(t *testing.T) {
	for _, mode := range []string{"deleted", "replaced", "bound", "succeeded", "failed", "terminating", "read-error", "legacy", "cpu-only"} {
		t.Run(mode, func(t *testing.T) {
			p := task("worker").Pod
			c := newTestClient(p)
			g := testGuard()
			calls := 0
			c.PrependReactor("create", "pods", func(kt.Action) (bool, runtime.Object, error) { calls++; return true, nil, context.DeadlineExceeded })
			g.evict(c, []*v1.Pod{p})
			if len(g.journal.entries[p.UID].Demand) != 1 {
				t.Fatal("missing durable training identity")
			}
			training, _ := c.CoreV1().Pods("n").Get(context.Background(), "training", meta.GetOptions{})
			switch mode {
			case "deleted":
				c.CoreV1().Pods("n").Delete(context.Background(), "training", meta.DeleteOptions{})
			case "replaced":
				training.UID = "new-training"
			case "cpu-only":
				training.Spec.Containers[0].Resources.Requests = nil
			case "bound":
				training.Spec.NodeName = "node"
			case "succeeded":
				training.Status.Phase = v1.PodSucceeded
			case "failed":
				training.Status.Phase = v1.PodFailed
			case "terminating":
				now := meta.Now()
				training.DeletionTimestamp = &now
			case "read-error":
				c.PrependReactor("get", "pods", func(a kt.Action) (bool, runtime.Object, error) {
					if a.(kt.GetAction).GetName() == "training" {
						return true, nil, fmt.Errorf("unavailable")
					}
					return false, nil, nil
				})
			case "legacy":
				e := g.journal.entries[p.UID]
				e.Demand = nil
				g.journal.entries[p.UID] = e
				g.journal.save(c)
			}
			if mode != "deleted" {
				c.CoreV1().Pods("n").Update(context.Background(), training, meta.UpdateOptions{})
			}
			restarted := newBudgetGuard()
			restarted.journal.now = func() time.Time { return time.Now().Add(time.Hour) }
			err := restarted.reconcile(c, allowRecovery)
			if calls != 1 {
				t.Fatalf("unexpected retry: %d", calls)
			}
			if mode == "read-error" {
				if err == nil || len(restarted.journal.entries) != 1 {
					t.Fatal("read failure must retain intent")
				}
			} else if err != nil || len(restarted.journal.entries) != 0 {
				t.Fatalf("intent not retired: %v", err)
			}
		})
	}
}

func TestCancellationDuringJournalWrite(t *testing.T) {
	c := newTestClient(task("worker").Pod)
	c.PrependReactor("update", "configmaps", func(kt.Action) (bool, runtime.Object, error) {
		c.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, "n", "training")
		return false, nil, nil
	})
	g := testGuard()
	g.evict(c, []*v1.Pod{task("worker").Pod})
	for _, a := range c.Actions() {
		if a.GetSubresource() == "eviction" {
			t.Fatal("Eviction after canceled during journal write")
		}
	}
	if len(g.journal.entries) != 0 {
		t.Fatal("canceled record retained")
	}
}

func TestCancellationAfterFirstAcceptedEviction(t *testing.T) {
	c := newTestClient(task("a").Pod, task("b").Pod)
	calls := 0
	c.PrependReactor("create", "pods", func(kt.Action) (bool, runtime.Object, error) {
		calls++
		c.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, "n", "training")
		return true, nil, nil
	})
	g := testGuard()
	g.evict(c, []*v1.Pod{task("a").Pod, task("b").Pod})
	if calls != 1 || len(g.journal.entries) != 1 {
		t.Fatal("continued batch after cancellation", calls)
	}
}

func trainingPod() *v1.Pod {
	p := task("training").Pod
	p.Spec.Containers = []v1.Container{{Name: "training", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}}}
	return p
}
