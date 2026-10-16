package guardpreempt

import (
	v1 "k8s.io/api/core/v1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// RequestsGPU gates disruptive action, not ordinary allocation or global queue
// ordering. InitResreq is Volcano's effective Pod request including init containers.
// Values come from the admitted Pod, after API defaulting and LimitRange.
func RequestsGPU(t *api.TaskInfo) bool {
	return t != nil && t.InitResreq != nil && t.InitResreq.Get(v1.ResourceName("nvidia.com/gpu")) > 0
}
