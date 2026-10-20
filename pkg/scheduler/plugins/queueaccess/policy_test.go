package queueaccess

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestQueueAccess(t *testing.T) {
	p := Policy{ObjectMeta: meta.ObjectMeta{Name: "shared"}}
	p.Spec.AllowedNamespaces = []string{"data", "workers"}
	s := &Snapshot{Items: []Policy{p}}
	for _, x := range []struct {
		ns, q string
		ok    bool
	}{{"data", "shared", true}, {"other", "shared", false}, {"other", "default", true}, {"workers", "shared", true}} {
		if (s.Check(x.ns, x.q) == nil) != x.ok {
			t.Fatalf("unexpected authorization: %+v", x)
		}
	}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: "data"}}
	// An absent annotation must not invent a conflicting default policy.
	d := Policy{ObjectMeta: meta.ObjectMeta{Name: "default"}}
	s.Items = append(s.Items, d)
	if err := s.CheckPod(pod, "shared"); err != nil {
		t.Fatal(err)
	}
	pod.Annotations = map[string]string{QueueAnnotation: "default"}
	if s.CheckPod(pod, "shared") == nil {
		t.Fatal("advertised queue bypass")
	}
	s.Items[0].Spec.AllowedNamespaces = nil
	if s.Check("data", "shared") == nil {
		t.Fatal("empty whitelist must deny")
	}
	if (*Snapshot)(nil).Check("data", "shared") == nil {
		t.Fatal("unavailable policy must deny")
	}
}
