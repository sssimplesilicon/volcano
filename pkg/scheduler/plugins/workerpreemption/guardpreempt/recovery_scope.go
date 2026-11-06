package guardpreempt

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"time"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
)

// Best-effort Events/logs are independent of scheduling and never authorize a
// budget release. Old journals without firstAttempt keep that time unknown.
func (g *budgetGuard) recoveryNotice(client kubernetes.Interface, p *v1.Pod, e evictionIntent, state string, warning bool) {
	now := g.journal.now()
	if g.recoveryNotices == nil {
		g.recoveryNotices = map[types.UID]time.Time{}
	}
	for uid := range g.recoveryNotices {
		if _, ok := g.journal.entries[uid]; !ok {
			delete(g.recoveryNotices, uid)
		}
	}
	if warning && now.Sub(g.recoveryNotices[p.UID]) < 5*time.Minute {
		return
	}
	g.recoveryNotices[p.UID] = now
	payload, _ := json.Marshal(map[string]interface{}{"version": 1, "state": state, "podUID": p.UID, "namespace": p.Namespace, "pod": p.Name, "node": p.Spec.NodeName, "phase": p.Status.Phase, "deletionTimestamp": p.DeletionTimestamp, "firstAttempt": e.FirstAttempt, "attempts": e.Attempts, "queue": e.Queue, "budget": e.Budget, "demand": e.Demand, "at": now.UTC().Format(time.RFC3339Nano)})
	klog.Infof("PDB_RECOVERY_STATE %s", payload)
	typ := v1.EventTypeNormal
	reason := "PreemptionRecoveryResolved"
	if warning {
		typ = v1.EventTypeWarning
		reason = "PreemptionRecoveryStalled"
	}
	ts := meta.NewTime(now)
	enqueueAudit(client, &v1.Event{ObjectMeta: meta.ObjectMeta{Name: fmt.Sprintf("preemption-recovery-%s-%d", p.UID, now.UnixNano()), Namespace: p.Namespace}, InvolvedObject: v1.ObjectReference{Kind: "Pod", APIVersion: "v1", Namespace: p.Namespace, Name: p.Name, UID: p.UID}, Reason: reason, Type: typ, Source: v1.EventSource{Component: "volcano-pipeline-preemption"}, Message: string(payload), FirstTimestamp: ts, LastTimestamp: ts, Count: 1})
}

func terminalContainersStopped(p *v1.Pod) bool {
	if p.Status.Phase != v1.PodSucceeded && p.Status.Phase != v1.PodFailed {
		return false
	}
	stopped := func(spec []v1.Container, statuses []v1.ContainerStatus) bool {
		for _, c := range spec {
			found := false
			for _, s := range statuses {
				if s.Name == c.Name && s.State.Terminated != nil {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	if !stopped(p.Spec.Containers, p.Status.ContainerStatuses) || !stopped(p.Spec.InitContainers, p.Status.InitContainerStatuses) {
		return false
	}
	for _, c := range p.Spec.EphemeralContainers {
		found := false
		for _, s := range p.Status.EphemeralContainerStatuses {
			if s.Name == c.Name && s.State.Terminated != nil {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func terminalPDBsSettled(client kubernetes.Interface, p *v1.Pod) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	list, err := client.PolicyV1().PodDisruptionBudgets(p.Namespace).List(ctx, meta.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, b := range list.Items {
		// Also honor an old disruptedPods entry after a selector/label change.
		if _, ok := b.Status.DisruptedPods[p.Name]; ok {
			return false, nil
		}
		sel, err := meta.LabelSelectorAsSelector(b.Spec.Selector)
		if err != nil {
			return false, err
		}
		if sel.Matches(labels.Set(p.Labels)) && b.Status.ObservedGeneration < b.Generation {
			return false, nil
		}
	}
	return true, nil
}

func (g *budgetGuard) demandRecovering(p *v1.Pod) bool {
	for _, e := range g.journal.entries {
		for _, d := range e.Demand {
			if d.UID == p.UID && d.Namespace == p.Namespace {
				return true
			}
		}
	}
	return false
}

// Conservative conflict domains: same victim/node/budget/PDB. Legacy entries
// without a budget pin fence their namespace. No timeout releases these fences.
func (g *budgetGuard) recoveryConflict(p *v1.Pod) string {
	for uid, old := range g.pending {
		if uid == p.UID {
			return "RecoveryVictimPending"
		}
		if old.Spec.NodeName == "" {
			return "RecoveryNodeUnknown"
		}
		if old.Spec.NodeName == p.Spec.NodeName {
			return "RecoveryNodePending"
		}
		e, ok := g.journal.entries[uid]
		if !ok {
			return "RecoveryIdentityUnknown"
		}
		if e.Budget != nil {
			if e.Budget.Identity == pb.Key(p) {
				return "RecoveryBudgetPending"
			}
		} else if old.Namespace == p.Namespace {
			return "RecoveryLegacyNamespacePending"
		}
		for _, b := range g.pdbs {
			if b.Namespace != p.Namespace || old.Namespace != p.Namespace {
				continue
			}
			sel, err := meta.LabelSelectorAsSelector(b.Spec.Selector)
			if err != nil {
				return "InvalidPDBSelector"
			}
			_, disrupted := b.Status.DisruptedPods[old.Name]
			if sel.Matches(labels.Set(p.Labels)) && (disrupted || sel.Matches(labels.Set(old.Labels))) {
				return "RecoveryPDBPending"
			}
		}
	}
	return ""
}
