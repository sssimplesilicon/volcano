package workerpreemption

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestTypeIdentityAndGPU(t *testing.T) {
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "one", Labels: map[string]string{typeKey: "a"}}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "x", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("4")}}}}}}
	task := api.NewTaskInfo(pod)
	if key(task) != "one/a" {
		t.Fatal(key(task))
	}
	if gpu(task) <= 0 {
		t.Fatal("GPU count not extracted")
	}
	delete(pod.Labels, typeKey)
	if key(task) != "" {
		t.Fatal("unlabelled worker must not join a type")
	}
}
