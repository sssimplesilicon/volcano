// Package queueaccess implements opt-in namespace authorization for queues.
package queueaccess

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const Name = "queue-access"
const Path = "/apis/scheduling.simplesilicon.com/v1alpha1/queueaccesspolicies"
const QueueAnnotation = "scheduling.volcano.sh/queue-name"
const GroupAnnotation = "scheduling.k8s.io/group-name"

type Policy struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata"`
	Spec            struct {
		AllowedNamespaces []string `json:"allowedNamespaces"`
	} `json:"spec"`
}
type Snapshot struct {
	Items []Policy `json:"items"`
}

func Load(client kubernetes.Interface) (*Snapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := client.CoreV1().RESTClient().Get().AbsPath(Path).DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	var s Snapshot
	err = json.Unmarshal(raw, &s)
	return &s, err
}

// A policy is named exactly after its Queue, preventing overlapping rules.
// No policy means unmanaged. Keep an empty whitelist to deny everyone.
func (s *Snapshot) Check(namespace, queue string) error {
	if s == nil {
		return fmt.Errorf("queue access policies unavailable")
	}
	if queue == "" {
		queue = "default"
	}
	for _, p := range s.Items {
		if p.Name == queue {
			if p.DeletionTimestamp != nil {
				return fmt.Errorf("queue policy is terminating")
			}
			for _, ns := range p.Spec.AllowedNamespaces {
				if ns == namespace {
					return nil
				}
			}
			return fmt.Errorf("namespace %s is not authorized for queue %s", namespace, queue)
		}
	}
	return nil
}
func (s *Snapshot) Managed(queue string) bool {
	for _, p := range s.Items {
		if p.Name == queue {
			return true
		}
	}
	return false
}

// CheckPod checks both advertised and effective queue. A pending controller Pod
// may reference a not-yet-created group; the scheduler must check that group later.
func (s *Snapshot) CheckPod(p *core.Pod, effective string) error {
	if advertised := p.Annotations[QueueAnnotation]; advertised != "" || effective == "" {
		if err := s.Check(p.Namespace, advertised); err != nil {
			return err
		}
	}
	if effective != "" {
		return s.Check(p.Namespace, effective)
	}
	return nil
}
