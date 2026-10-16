package guardpreempt

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestGPURequestGate(t *testing.T) {
	if RequestsGPU(nil) {
		t.Fatal("nil task")
	}
	for _, tc := range []struct {
		name     string
		requests v1.ResourceList
		init     bool
		want     bool
	}{
		{"missing", nil, false, false},
		{"cpu-memory", v1.ResourceList{"cpu": resource.MustParse("64"), "memory": resource.MustParse("128Gi")}, false, false},
		{"zero-gpu", v1.ResourceList{"nvidia.com/gpu": resource.MustParse("0")}, false, false},
		{"gpu", v1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}, false, true},
		{"init-gpu", v1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "worker"}}}}
			if tc.init {
				p.Spec.InitContainers = []v1.Container{{Name: "init", Resources: v1.ResourceRequirements{Requests: tc.requests}}}
			} else {
				p.Spec.Containers[0].Resources.Requests = tc.requests
			}
			if got := RequestsGPU(api.NewTaskInfo(p)); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
