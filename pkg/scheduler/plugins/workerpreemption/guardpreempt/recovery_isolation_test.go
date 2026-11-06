package guardpreempt

import (
	"context"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"strings"
	"testing"
	"time"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
)

// A terminal but still-present victim must not stop unrelated recovery planning.
func TestTerminalResidueDoesNotGloballyBlock(t *testing.T) {
	p := task("residue").Pod
	now := meta.NewTime(time.Now().Add(-time.Hour))
	p.DeletionTimestamp = &now
	p.Finalizers = []string{"test.local/hold"}
	p.Status.Phase = v1.PodSucceeded
	c := newTestClient(p)
	g := testGuard()
	if err := g.journal.load(c); err != nil {
		t.Fatal(err)
	}
	if err := g.journal.before(c, p, "q", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.reconcile(c, allowRecovery); err != nil {
		t.Fatalf("unrelated planning blocked: %v", err)
	}
	if len(g.journal.entries) != 1 {
		t.Fatal("must retain evidence until cache/PDB reconciliation")
	}
}

func TestTerminalReconciliationRequiresCacheAndPDB(t *testing.T) {
	for _, phase := range []v1.PodPhase{v1.PodSucceeded, v1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			p := task("ended").Pod
			p.Status.Phase = phase
			p.Spec.Containers = []v1.Container{{Name: "worker"}}
			p.Status.ContainerStatuses = []v1.ContainerStatus{{Name: "worker", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{}}}}
			b := budget()
			b.Status.DisruptedPods = map[string]meta.Time{p.Name: meta.Now()}
			c := newTestClient(p, &b)
			g := testGuard()
			g.terminalObserved = func(*v1.Pod) bool { return false }
			if err := g.journal.load(c); err != nil {
				t.Fatal(err)
			}
			if err := g.journal.before(c, p, "q", nil); err != nil {
				t.Fatal(err)
			}
			if err := g.reconcile(c, allowRecovery); err != nil {
				t.Fatal(err)
			}
			if len(g.journal.entries) != 1 {
				t.Fatal("stale cache released")
			}
			g.terminalObserved = func(*v1.Pod) bool { return true }
			if err := g.reconcile(c, allowRecovery); err != nil {
				t.Fatal(err)
			}
			if len(g.journal.entries) != 1 {
				t.Fatal("PDB reservation released")
			}
			b.Status.DisruptedPods = nil
			if _, err := c.PolicyV1().PodDisruptionBudgets(b.Namespace).UpdateStatus(context.Background(), &b, meta.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := g.reconcile(c, allowRecovery); err != nil {
				t.Fatal(err)
			}
			if len(g.journal.entries) != 0 {
				t.Fatal("settled terminal record retained")
			}
			if _, err := c.CoreV1().Pods(p.Namespace).Get(context.Background(), p.Name, meta.GetOptions{}); err != nil {
				t.Fatal("must not delete residue", err)
			}
		})
	}
}

func TestRecoveryConflictDomains(t *testing.T) {
	old := task("old").Pod
	old.Spec.NodeName = "old-node"
	old.Labels[pb.PipelineLabel] = "ego"
	old.Labels[pb.TypeLabel] = "A"
	g := testGuard()
	g.reset([]policy.PodDisruptionBudget{budget()})
	g.pending = map[types.UID]*v1.Pod{old.UID: old}
	g.journal.entries = map[types.UID]evictionIntent{old.UID: {Budget: &pb.Pin{Identity: pb.Key(old)}, Demand: []podIdentity{{Namespace: "train", UID: "demand"}}}}
	candidate := old.DeepCopy()
	candidate.UID = "other"
	candidate.Name = "other"
	if g.recoveryConflict(candidate) != "RecoveryNodePending" {
		t.Fatal("same node not fenced")
	}
	candidate.Spec.NodeName = "other-node"
	if g.recoveryConflict(candidate) != "RecoveryBudgetPending" {
		t.Fatal("same budget not fenced")
	}
	candidate.Labels[pb.TypeLabel] = "B"
	if g.recoveryConflict(candidate) != "RecoveryPDBPending" {
		t.Fatal("shared PDB not fenced")
	}
	candidate.Labels["type"] = "B"
	if reason := g.recoveryConflict(candidate); reason != "" {
		t.Fatal("unrelated scope blocked", reason)
	}
	if !g.demandRecovering(&v1.Pod{ObjectMeta: meta.ObjectMeta{Namespace: "train", UID: "demand"}}) {
		t.Fatal("duplicate demand not blocked")
	}
	if g.demandRecovering(&v1.Pod{ObjectMeta: meta.ObjectMeta{Namespace: "train", UID: "replacement"}}) {
		t.Fatal("new UID confused with original")
	}
}

func TestTerminalStatusNeedsAllContainers(t *testing.T) {
	p := task("terminal").Pod
	p.Status.Phase = v1.PodSucceeded
	p.Spec.Containers = []v1.Container{{Name: "worker"}}
	if terminalContainersStopped(p) {
		t.Fatal("missing status accepted")
	}
	p.Status.ContainerStatuses = []v1.ContainerStatus{{Name: "worker", State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}}
	if terminalContainersStopped(p) {
		t.Fatal("running container accepted")
	}
}

func TestCorruptJournalStillStopsRecovery(t *testing.T) {
	c := newTestClient()
	g := testGuard()
	cm, err := c.CoreV1().ConfigMaps("volcano-system").Get(context.Background(), "worker-type-preemption-recovery", meta.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cm.Data[journalKey] = "null"
	if _, err = c.CoreV1().ConfigMaps(cm.Namespace).Update(context.Background(), cm, meta.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = g.reconcile(c, allowRecovery); err == nil || !strings.Contains(err.Error(), "null recovery") {
		t.Fatal("corrupt journal allowed", err)
	}
}

func TestRecoveryWarningIsThrottledAndAuditable(t *testing.T) {
	p := task("stalled").Pod
	p.Spec.NodeName = "node-a"
	c := newTestClient(p)
	g := testGuard()
	now := time.Now()
	g.journal.now = func() time.Time { return now }
	e := evictionIntent{UID: p.UID, Namespace: p.Namespace, Name: p.Name, FirstAttempt: now.Add(-time.Hour), Attempts: 1}
	g.journal.entries = map[types.UID]evictionIntent{p.UID: e}
	g.recoveryNotice(c, p, e, "WaitingForCompletion", true)
	g.recoveryNotice(c, p, e, "WaitingForCompletion", true)
	for until := time.Now().Add(3 * time.Second); ; {
		list, err := c.CoreV1().Events(p.Namespace).List(context.Background(), meta.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list.Items) > 0 {
			if len(list.Items) != 1 || list.Items[0].Reason != "PreemptionRecoveryStalled" || list.Items[0].InvolvedObject.UID != p.UID || !strings.Contains(list.Items[0].Message, "node-a") {
				t.Fatal("invalid or duplicate recovery audit", list.Items)
			}
			break
		}
		if time.Now().After(until) {
			t.Fatal("warning event not published")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(g.journal.entries) != 1 {
		t.Fatal("warning released recovery protection")
	}
}
