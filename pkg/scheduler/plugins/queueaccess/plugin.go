package queueaccess

import (
	"fmt"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

type plugin struct{}

func New(framework.Arguments) framework.Plugin    { return &plugin{} }
func (*plugin) Name() string                      { return Name }
func (*plugin) OnSessionClose(*framework.Session) {}
func (*plugin) OnSessionOpen(s *framework.Session) {
	policies, loadErr := Load(s.KubeClient())
	check := func(job *api.JobInfo) error {
		if loadErr != nil {
			return fmt.Errorf("queue access policy read failed: %w", loadErr)
		}
		return policies.Check(job.Namespace, string(job.Queue))
	}
	s.AddJobValidFn(Name, func(obj interface{}) *api.ValidateResult {
		job, ok := obj.(*api.JobInfo)
		if !ok {
			return &api.ValidateResult{Pass: false, Reason: "QueueAccessDenied", Message: "invalid job"}
		}
		if err := check(job); err != nil {
			return &api.ValidateResult{Pass: false, Reason: "QueueAccessDenied", Message: err.Error()}
		}
		return nil
	})
	s.AddPredicateFn(Name, func(task *api.TaskInfo, node *api.NodeInfo) error {
		job := s.Jobs[task.Job]
		if job == nil {
			return fmt.Errorf("missing job for queue access")
		}
		if err := check(job); err != nil {
			return err
		}
		return policies.CheckPod(task.Pod, string(job.Queue))
	})
}
