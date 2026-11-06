package guardpreempt

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"testing"
	"time"
)

func TestAuditDeduplicates(t *testing.T) {
	p := &v1.Pod{ObjectMeta: meta.ObjectMeta{Name: "worker", Namespace: "test", UID: "one"}, Spec: v1.PodSpec{Containers: []v1.Container{{Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}}}}}}
	c := fake.NewSimpleClientset()
	e := auditEvent(p, "q", nil)
	for i := 0; i < 2; i++ {
		if err := publishAudit(auditWork{c, e}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := c.CoreV1().Events("test").List(context.Background(), meta.ListOptions{})
	if err != nil || len(events.Items) != 1 {
		t.Fatal(events, err)
	}
}

// Publishing telemetry is optional; API failure is never an Eviction failure.
func TestAuditPublicationFailure(t *testing.T) {
	c := fake.NewSimpleClientset()
	c.PrependReactor("create", "events", func(a clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("collector path unavailable")
	})
	e := auditEvent(&v1.Pod{ObjectMeta: meta.ObjectMeta{Name: "x", Namespace: "test", UID: "uid"}}, "q", nil)
	if publishAudit(auditWork{c, e}) == nil {
		t.Fatal("failure must be observable")
	}
	start := time.Now()
	emitEvictionAudit(c, &v1.Pod{ObjectMeta: meta.ObjectMeta{Name: "x", Namespace: "test", UID: "uid"}}, "q", nil)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("telemetry blocked scheduler")
	}
}
