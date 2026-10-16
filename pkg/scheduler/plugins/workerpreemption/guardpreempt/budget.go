package guardpreempt

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
)

// The ledger is local to this action. API-server Eviction admission is the final
// concurrent/restart-safe authority. No direct Delete fallback is permitted.
type budgetGuard struct {
	pipelineBudgets bool
	snapshot        *pb.Snapshot
	demand          []podIdentity
	pdbs            []policy.PodDisruptionBudget
	available       map[types.UID]int32
	reserved        map[types.UID]*api.TaskInfo
	matches         map[types.UID][]types.UID
	pending         map[types.UID]*v1.Pod
	order           []types.UID
	journal         *recoveryJournal
	evictionTimeout time.Duration
	queueByUID      map[types.UID]string
}

func newBudgetGuard() *budgetGuard {
	return &budgetGuard{pending: map[types.UID]*v1.Pod{}, journal: newJournal("volcano-system", "worker-type-preemption-recovery"), evictionTimeout: 10 * time.Second, queueByUID: map[types.UID]string{}}
}
func (g *budgetGuard) open(s *framework.Session) error {
	if g.pipelineBudgets {
		var err error
		g.snapshot, err = pb.Load(s.KubeClient())
		if err != nil {
			return err
		}
	}
	if err := g.reconcile(s.KubeClient(), func(e evictionIntent, p *v1.Pod) bool {
		if p.Labels["scheduling.simplesilicon.com/preemptible"] != "true" || p.Labels["scheduling.simplesilicon.com/worker-type"] == "" {
			return false
		}
		for _, j := range s.Jobs {
			if string(j.Queue) != e.Queue {
				continue
			}
			for _, t := range j.Tasks {
				if t.Pod != nil && t.Pod.UID == e.UID {
					return true
				}
			}
		}
		return false
	}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	list, err := s.KubeClient().PolicyV1().PodDisruptionBudgets("").List(ctx, meta.ListOptions{})
	if err != nil {
		return err
	}
	g.reset(list.Items)
	s.AddEventHandler(&framework.EventHandler{AllocateFunc: func(e *framework.Event) { g.release(e.Task) }})
	return nil
}
func (g *budgetGuard) reset(pdbs []policy.PodDisruptionBudget) {
	g.pdbs = pdbs
	g.available = map[types.UID]int32{}
	g.reserved = map[types.UID]*api.TaskInfo{}
	g.matches = map[types.UID][]types.UID{}
	g.order = nil
	for _, p := range pdbs {
		if p.Status.ObservedGeneration >= p.Generation {
			g.available[p.UID] = p.Status.DisruptionsAllowed
		}
	}
}
func (g *budgetGuard) reserve(t *api.TaskInfo) bool {
	uid := t.Pod.UID
	if _, ok := g.reserved[uid]; ok {
		return false
	}
	if g.pipelineBudgets {
		held := map[types.UID]bool{}
		for id := range g.reserved {
			held[id] = true
		}
		for id := range g.pending {
			held[id] = true
		}
		if g.snapshot == nil {
			return false
		}
		if _, err := g.snapshot.Check(t.Pod, held, nil); err != nil {
			klog.Infof("GPU_BUDGET_BLOCK %v", err)
			return false
		}
	}
	ids := []types.UID{}
	for _, p := range g.pdbs {
		if p.Namespace != t.Namespace {
			continue
		}
		sel, err := meta.LabelSelectorAsSelector(p.Spec.Selector)
		if err != nil {
			return false
		}
		if !sel.Matches(labels.Set(t.Pod.Labels)) {
			continue
		}
		// Conservatively protect unhealthy Pods too. Overlapping PDBs are rejected
		// by Eviction admission; reject them at planning time rather than delete.
		if g.available[p.UID] <= 0 {
			return false
		}
		if _, ok := p.Status.DisruptedPods[t.Name]; ok {
			return false
		}
		ids = append(ids, p.UID)
	}
	if len(ids) > 1 {
		return false
	}
	for _, id := range ids {
		g.available[id]--
	}
	g.reserved[uid] = t
	g.order = append(g.order, uid)
	g.matches[uid] = ids
	klog.Infof("PDB_RESERVE pod=%s/%s budgets=%v", t.Namespace, t.Name, ids)
	return true
}
func (g *budgetGuard) release(t *api.TaskInfo) {
	uid := t.Pod.UID
	if _, ok := g.reserved[uid]; !ok {
		return
	}
	for _, id := range g.matches[uid] {
		g.available[id]++
	}
	delete(g.reserved, uid)
	delete(g.matches, uid)
	for i, id := range g.order {
		if id == uid {
			g.order = append(g.order[:i], g.order[i+1:]...)
			break
		}
	}
	klog.Infof("PDB_RELEASE pod=%s/%s", t.Namespace, t.Name)
}
func (g *budgetGuard) submit(s *framework.Session, stmt *framework.Statement, preemptor *api.JobInfo) {
	g.demand = nil
	for _, t := range preemptor.Tasks {
		if p := t.Pod; RequestsGPU(t) && p != nil && p.Spec.NodeName == "" && p.DeletionTimestamp == nil && p.Status.Phase != v1.PodSucceeded && p.Status.Phase != v1.PodFailed {
			g.demand = append(g.demand, podIdentity{Namespace: p.Namespace, Name: p.Name, UID: p.UID})
		}
	}
	victims := []*v1.Pod{}
	for _, uid := range g.order {
		t := g.reserved[uid]
		victims = append(victims, t.Pod.DeepCopy())
		if j := s.Jobs[t.Job]; j != nil {
			g.queueByUID[t.Pod.UID] = string(j.Queue)
		}
	}
	// Undo ALL speculative session resource/type/PDB changes, including pipeline
	// assignments. Actual deletion must be observed before allocation proceeds.
	stmt.Discard()
	g.evict(s.KubeClient(), victims)
}
func (g *budgetGuard) evict(client kubernetes.Interface, victims []*v1.Pod) {
	for _, p := range victims {
		uid := p.UID
		if err := g.request(client, p); err != nil {
			klog.Errorf("PDB_EVICTION_STOP pod=%s/%s err=%v pending=%t", p.Namespace, p.Name, err, g.pending[uid] != nil)
			return
		}
		g.pending[uid] = p
		klog.Infof("PDB_EVICTION_ACCEPTED pod=%s/%s uid=%s", p.Namespace, p.Name, uid)
	}
}

// Preserve state from the preceding allocate action. Statement.Discard records
// LastTransaction for normal scheduler nomination, which is inappropriate when
// our external eviction may be denied and no resources have been released.
func restoreTransactions(s *framework.Session) func() {
	type saved struct {
		task     *api.TaskInfo
		last     *api.TransactionContext
		eviction bool
	}
	entries := []saved{}
	for _, j := range s.Jobs {
		for _, t := range j.Tasks {
			entries = append(entries, saved{t, t.LastTransaction, t.EvictionOccurred})
		}
	}
	return func() {
		for _, e := range entries {
			e.task.LastTransaction = e.last
			e.task.EvictionOccurred = e.eviction
		}
	}
}
