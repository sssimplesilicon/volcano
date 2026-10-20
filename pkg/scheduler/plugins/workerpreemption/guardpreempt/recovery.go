package guardpreempt

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler/api"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
)

const journalKey = "intents.json"

type podIdentity struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
}

type evictionIntent struct {
	Budget      *pb.Pin       `json:"budget,omitempty"`
	Demand      []podIdentity `json:"demand,omitempty"`
	Namespace   string        `json:"namespace"`
	Name        string        `json:"name"`
	UID         types.UID     `json:"uid"`
	Queue       string        `json:"queue,omitempty"`
	Attempts    int           `json:"attempts"`
	NextAttempt time.Time     `json:"nextAttempt"`
}
type recoveryJournal struct {
	namespace, name string
	cm              *v1.ConfigMap
	entries         map[types.UID]evictionIntent
	now             func() time.Time
}

func newJournal(ns, name string) *recoveryJournal {
	return &recoveryJournal{namespace: ns, name: name, now: time.Now}
}
func (j *recoveryJournal) load(client kubernetes.Interface) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cm, err := client.CoreV1().ConfigMaps(j.namespace).Get(ctx, j.name, meta.GetOptions{})
	if err != nil {
		return err
	}
	if cm.Labels["scheduling.simplesilicon.com/recovery-journal"] != "v1" {
		return fmt.Errorf("unrecognized recovery ConfigMap %s/%s", j.namespace, j.name)
	}
	entries := map[types.UID]evictionIntent{}
	if err = json.Unmarshal([]byte(cm.Data[journalKey]), &entries); err != nil {
		return fmt.Errorf("invalid recovery journal: %w", err)
	}
	if entries == nil {
		return fmt.Errorf("null recovery journal")
	}
	for uid, e := range entries {
		if uid == "" || uid != e.UID || e.Namespace == "" || e.Name == "" || e.Attempts < 1 {
			return fmt.Errorf("invalid recovery entry %s", uid)
		}
	}
	j.cm = cm
	j.entries = entries
	return nil
}
func (j *recoveryJournal) save(client kubernetes.Interface) error {
	b, err := json.Marshal(j.entries)
	if err != nil {
		return err
	}
	cm := j.cm.DeepCopy()
	cm.Data[journalKey] = string(b)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	updated, err := client.CoreV1().ConfigMaps(j.namespace).Update(ctx, cm, meta.UpdateOptions{})
	if err != nil {
		return err
	}
	j.cm = updated
	return nil // resourceVersion conflict stops submission; never overwrite forcibly.
}
func (j *recoveryJournal) remove(client kubernetes.Interface, uid types.UID) error {
	delete(j.entries, uid)
	return j.save(client)
}
func (j *recoveryJournal) before(client kubernetes.Interface, p *v1.Pod, queue string, demand []podIdentity) error {
	e := j.entries[p.UID]
	if e.UID == "" {
		e.Demand = append([]podIdentity(nil), demand...)
	}
	e.Namespace = p.Namespace
	e.Name = p.Name
	e.UID = p.UID
	if queue != "" {
		e.Queue = queue
	}
	e.Attempts++
	shift := e.Attempts - 1
	if shift > 4 {
		shift = 4
	}
	e.NextAttempt = j.now().Add(time.Duration(2<<shift) * time.Second) // 2,4,8,16,32 seconds; persisted across restart.
	j.entries[p.UID] = e
	return j.save(client)
}
func definiteRejection(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsTooManyRequests(err) || apierrors.IsNotFound(err) || apierrors.IsConflict(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsUnauthorized(err)
}
func (g *budgetGuard) request(client kubernetes.Interface, p *v1.Pod) error {
	if g.journal.cm == nil {
		if err := g.journal.load(client); err != nil {
			return err
		}
	}
	demand := g.demand
	if existing, ok := g.journal.entries[p.UID]; ok {
		demand = existing.Demand
	}
	active, err := demandActive(client, demand)
	if err != nil {
		return err
	}
	if !active {
		return fmt.Errorf("training demand no longer pending")
	}
	if g.pipelineBudgets {
		var expected *pb.Pin
		if entry, exists := g.journal.entries[p.UID]; exists {
			if entry.Budget == nil {
				return fmt.Errorf("unscoped legacy intent cannot retry in pipeline mode")
			}
			expected = entry.Budget
		}
		pin, err := g.checkPipeline(client, p, demand, expected)
		if err != nil {
			return err
		}
		e := g.journal.entries[p.UID]
		e.Budget = pin
		g.journal.entries[p.UID] = e
	} else if g.journal.entries[p.UID].Budget != nil {
		return fmt.Errorf("pipeline intent requires pipeline budget mode")
	}
	if err := g.journal.before(client, p, g.queueByUID[p.UID], g.demand); err != nil {
		return fmt.Errorf("journal write failed; no Eviction submitted: %w", err)
	}
	// Recheck after the durable write: it may have taken long enough for the
	// training to be canceled. Never interpret a read failure as cancellation.
	active, err = demandActive(client, demand)
	if err != nil {
		return err
	}
	if !active {
		if err := g.journal.remove(client, p.UID); err != nil {
			return err
		}
		delete(g.pending, p.UID)
		return fmt.Errorf("training demand canceled before Eviction")
	}
	if g.pipelineBudgets {
		if _, err := g.checkPipeline(client, p, demand, g.journal.entries[p.UID].Budget); err != nil {
			return err
		}
	}
	g.pending[p.UID] = p
	ctx, cancel := context.WithTimeout(context.Background(), g.evictionTimeout)
	defer cancel()
	uid := p.UID
	err = client.PolicyV1().Evictions(p.Namespace).Evict(ctx, &policy.Eviction{ObjectMeta: meta.ObjectMeta{Name: p.Name, Namespace: p.Namespace}, DeleteOptions: &meta.DeleteOptions{Preconditions: &meta.Preconditions{UID: &uid}}})
	if err != nil && definiteRejection(err) {
		if removeErr := g.journal.remove(client, uid); removeErr != nil {
			return fmt.Errorf("Eviction rejected (%v), journal reconciliation required: %w", err, removeErr)
		}
		delete(g.pending, uid)
	}
	return err
}

// Reconcile uses live API reads, never treats a timeout as free resources.
func (g *budgetGuard) reconcile(client kubernetes.Interface, eligible func(evictionIntent, *v1.Pod) bool) error {
	if err := g.journal.load(client); err != nil {
		return err
	}
	g.pending = map[types.UID]*v1.Pod{}
	ids := []string{}
	for uid := range g.journal.entries {
		ids = append(ids, string(uid))
	}
	sort.Strings(ids)
	for _, id := range ids {
		uid := types.UID(id)
		e := g.journal.entries[uid]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		p, err := client.CoreV1().Pods(e.Namespace).Get(ctx, e.Name, meta.GetOptions{})
		cancel()
		if apierrors.IsNotFound(err) || (err == nil && p.UID != uid) {
			if err := g.journal.remove(client, uid); err != nil {
				return err
			}
			klog.Infof("PDB_RECOVERY_RESOLVED pod=%s/%s uid=%s", e.Namespace, e.Name, uid)
			continue
		}
		if err != nil {
			return err
		}
		g.pending[uid] = p
		if p.DeletionTimestamp != nil {
			continue
		}
		if g.pipelineBudgets && e.Budget == nil {
			if err := g.journal.remove(client, uid); err != nil {
				return err
			}
			delete(g.pending, uid)
			continue
		}
		active, err := demandActive(client, e.Demand)
		if err != nil {
			return err
		}
		if !active {
			if err := g.journal.remove(client, uid); err != nil {
				return err
			}
			delete(g.pending, uid)
			klog.Infof("PDB_RECOVERY_CANCEL_DEMAND pod=%s/%s uid=%s", e.Namespace, e.Name, uid)
			continue
		}
		// No retries after a worker leaves its original shared queue or becomes protected.
		if !eligible(e, p) {
			if err := g.journal.remove(client, uid); err != nil {
				return err
			}
			delete(g.pending, uid)
			klog.Warningf("PDB_RECOVERY_CANCEL protected or missing owner pod=%s/%s", e.Namespace, e.Name)
			continue
		}
		if g.journal.now().Before(e.NextAttempt) {
			continue
		}
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		pdbs, err := client.PolicyV1().PodDisruptionBudgets(e.Namespace).List(ctx, meta.ListOptions{})
		cancel()
		if err != nil {
			return err
		}
		inFlight := false
		for _, b := range pdbs.Items {
			sel, err := meta.LabelSelectorAsSelector(b.Spec.Selector)
			if err != nil {
				return err
			}
			if sel.Matches(labels.Set(p.Labels)) {
				if _, ok := b.Status.DisruptedPods[p.Name]; ok {
					inFlight = true
				}
				if b.Status.ObservedGeneration < b.Generation {
					inFlight = true
				}
			}
		}
		if inFlight {
			continue
		} // PDB controller clears abandoned disruption reservations; no arbitrary expiry here.
		klog.Infof("PDB_RECOVERY_RETRY pod=%s/%s uid=%s attempt=%d", e.Namespace, e.Name, uid, e.Attempts+1)
		if err := g.request(client, p); err != nil {
			return err
		}
	}
	if len(g.journal.entries) > 0 {
		return fmt.Errorf("waiting for %d durable eviction intents", len(g.journal.entries))
	}
	return nil
}

// Fresh API reads tie recovery to the original pending Pods, not their names or
// an informer snapshot. Legacy records without demand are retired, never blindly
// retried; a later scheduling cycle may make a fresh, fully guarded plan.
func demandActive(client kubernetes.Interface, demand []podIdentity) (bool, error) {
	for _, ref := range demand {
		if ref.Namespace == "" || ref.Name == "" || ref.UID == "" {
			return false, fmt.Errorf("invalid training identity")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		p, err := client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, meta.GetOptions{})
		cancel()
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return false, err
		}
		if RequestsGPU(api.NewTaskInfo(p)) && p.UID == ref.UID && p.DeletionTimestamp == nil && p.Spec.NodeName == "" && p.Status.Phase != v1.PodSucceeded && p.Status.Phase != v1.PodFailed {
			return true, nil
		}
	}
	return false, nil
}

// Re-read both policy and ready Pods at the destructive boundary. Journal entries
// account for ambiguous submissions; a terminating Pod is already absent from ready capacity.
func (g *budgetGuard) checkPipeline(client kubernetes.Interface, p *v1.Pod, demand []podIdentity, expected *pb.Pin) (*pb.Pin, error) {
	s, err := pb.Load(client)
	if err != nil {
		return nil, err
	}
	var live *v1.Pod
	for i := range s.Pods {
		if s.Pods[i].UID == p.UID {
			live = &s.Pods[i]
			break
		}
	}
	if live == nil || live.DeletionTimestamp != nil {
		return nil, fmt.Errorf("victim missing/terminating")
	}
	var access *qa.Snapshot
	if g.queueAccess {
		access, err = qa.Load(client)
		if err != nil {
			return nil, err
		}
	}
	matched := false
	for _, ref := range demand {
		for i := range s.Pods {
			d := &s.Pods[i]
			if d.UID == ref.UID && d.Namespace == ref.Namespace && d.Name == ref.Name && s.CanPreempt(d, live) && RequestsGPU(api.NewTaskInfo(d)) && d.Spec.NodeName == "" && d.DeletionTimestamp == nil && d.Status.Phase != v1.PodSucceeded && d.Status.Phase != v1.PodFailed {
				if g.queueAccess && (access.Check(d.Namespace, s.Queue(d)) != nil || access.Check(live.Namespace, s.Queue(live)) != nil) {
					continue
				}
				if s.Queue(d) == s.Queue(live) && d.Spec.Priority != nil && (live.Spec.Priority == nil || *d.Spec.Priority > *live.Spec.Priority) {
					matched = true
				}
			}
		}
	}
	if !matched {
		return nil, fmt.Errorf("no active same-pipeline higher-priority demand")
	}
	held := map[types.UID]bool{}
	for uid := range g.journal.entries {
		if uid != p.UID {
			held[uid] = true
		}
	}
	return s.Check(live, held, expected)
}
