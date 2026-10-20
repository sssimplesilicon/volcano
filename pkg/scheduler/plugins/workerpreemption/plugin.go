// Package workerpreemption provides opt-in pipeline-aware guarded preemption.
package workerpreemption

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
	preempt "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/guardpreempt"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
)

const name = "worker-type-preemption"
const typeKey = "scheduling.simplesilicon.com/worker-type"
const allowKey = "scheduling.simplesilicon.com/preemptible"

var active bool

type action struct{ delegate framework.Action }

func (a *action) Name() string  { return "worker-type-preempt" }
func (a *action) Initialize()   { a.delegate.Initialize() }
func (a *action) UnInitialize() { a.delegate.UnInitialize() }
func (a *action) Execute(s *framework.Session) {
	active = true
	defer func() { active = false }()
	a.delegate.Execute(s)
}

// Register enables the statically linked extension. It is never called by package init.
func Register() {
	framework.RegisterAction(&action{delegate: preempt.New()})
	framework.RegisterPluginBuilder(name, New)
	framework.RegisterPluginBuilder(qa.Name, qa.New)
}

type plugin struct {
	queue           string
	pipelineBudgets bool
}

func New(args framework.Arguments) framework.Plugin {
	q := "wt-shared"
	if x, ok := args["victimQueue"].(string); ok {
		q = x
	}
	enabled := false
	args.GetBool(&enabled, "pipelineBudgets")
	return &plugin{queue: q, pipelineBudgets: enabled}
}
func (p *plugin) Name() string                        { return name }
func (p *plugin) OnSessionClose(s *framework.Session) {}
func key(t *api.TaskInfo) string {
	if t.Pod == nil || t.Pod.Labels[typeKey] == "" {
		return ""
	}
	if t.Pod.Labels[pb.PipelineLabel] != "" {
		return pb.Key(t.Pod)
	}
	return t.Namespace + "/" + t.Pod.Labels[typeKey]
}
func gpu(t *api.TaskInfo) float64 { return t.Resreq.Get(v1.ResourceName("nvidia.com/gpu")) }
func (p *plugin) OnSessionOpen(s *framework.Session) {
	totals := map[string]float64{}
	var snapshot *pb.Snapshot
	if p.pipelineBudgets {
		var err error
		snapshot, err = pb.Load(s.KubeClient())
		if err != nil {
			klog.Errorf("PIPELINE_BUDGET unavailable: %v", err)
		}
	}
	eligible := func(t *api.TaskInfo) bool {
		j := s.Jobs[t.Job]
		scope := j != nil && string(j.Queue) == p.queue
		if p.pipelineBudgets {
			scope = snapshot != nil && snapshot.PreemptibleScope(t.Pod)
		}
		return scope && key(t) != "" && t.Pod.Labels[allowKey] == "true" && t.Pod.DeletionTimestamp == nil
	}
	for _, j := range s.Jobs {
		if !p.pipelineBudgets && string(j.Queue) != p.queue {
			continue
		}
		for _, t := range j.Tasks {
			if (!p.pipelineBudgets || (snapshot != nil && snapshot.PreemptibleScope(t.Pod))) && key(t) != "" && api.AllocatedStatus(t.Status) && t.Status != api.Releasing {
				totals[key(t)] += gpu(t)
			}
		}
	}
	s.AddEventHandler(&framework.EventHandler{AllocateFunc: func(e *framework.Event) {
		if j := s.Jobs[e.Task.Job]; j != nil && ((!p.pipelineBudgets && string(j.Queue) == p.queue) || (p.pipelineBudgets && snapshot != nil && snapshot.PreemptibleScope(e.Task.Pod))) && key(e.Task) != "" {
			totals[key(e.Task)] += gpu(e.Task)
		}
	}, DeallocateFunc: func(e *framework.Event) {
		if j := s.Jobs[e.Task.Job]; j != nil && ((!p.pipelineBudgets && string(j.Queue) == p.queue) || (p.pipelineBudgets && snapshot != nil && snapshot.PreemptibleScope(e.Task.Pod))) && key(e.Task) != "" {
			totals[key(e.Task)] -= gpu(e.Task)
		}
	}})
	s.AddPreemptableFn(name, func(pre *api.TaskInfo, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		out := []*api.TaskInfo{}
		j := s.Jobs[pre.Job]
		if !active || j == nil || (!p.pipelineBudgets && string(j.Queue) != p.queue) || (p.pipelineBudgets && (snapshot == nil || !snapshot.PreemptibleScope(pre.Pod))) || !preempt.RequestsGPU(pre) {
			return out, 1
		}
		for _, t := range candidates {
			if eligible(t) && pre.Priority > t.Priority {
				if p.pipelineBudgets {
					if snapshot == nil || !snapshot.CanPreempt(pre.Pod, t.Pod) {
						continue
					}
					if _, err := snapshot.Check(t.Pod, nil, nil); err != nil {
						continue
					}
				}
				out = append(out, t)
			}
		}
		return out, 1
	})
	jobKey := func(j *api.JobInfo) string {
		k := ""
		for _, t := range j.Tasks {
			x := key(t)
			if x == "" || (k != "" && k != x) {
				return ""
			}
			k = x
		}
		return k
	}
	s.AddJobOrderFn(name, func(l, r interface{}) int {
		if !active {
			return 0
		}
		a, b := l.(*api.JobInfo), r.(*api.JobInfo)
		if !p.pipelineBudgets && (string(a.Queue) != p.queue || string(b.Queue) != p.queue) {
			return 0
		}
		ka, kb := jobKey(a), jobKey(b)
		if ka == "" || kb == "" {
			return 0
		}
		if totals[ka] > totals[kb] {
			return 1
		}
		if totals[ka] < totals[kb] {
			return -1
		}
		if ka < kb {
			return 1
		}
		if ka > kb {
			return -1
		}
		return 0
	})
	s.AddNodeOrderFn(name, func(pre *api.TaskInfo, n *api.NodeInfo) (float64, error) {
		if !active {
			return 0, nil
		}
		score, local := float64(0), float64(0)
		for _, t := range n.Tasks {
			if eligible(t) && pre.Priority > t.Priority {
				v := totals[key(t)]
				if v > score {
					score = v
					local = gpu(t)
				} else if v == score {
					local += gpu(t)
				}
			}
		}
		return score*1000000 + local, nil
	})
	if len(totals) > 0 {
		klog.V(3).Infof("worker-type-preemption totals=%v queue=%s", totals, p.queue)
	}
}
