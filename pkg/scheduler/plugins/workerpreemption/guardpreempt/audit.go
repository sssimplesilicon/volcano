package guardpreempt

// Best-effort telemetry only: no DB/network work runs on the scheduling path.
// Kubernetes Events survive collector restarts until their API-server TTL expires.
import (
	"context"
	"encoding/json"
	"fmt"
	v1 "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sync"
	"time"
)

type evictionAudit struct {
	Version    int           `json:"version"`
	ID         string        `json:"id"`
	At         string        `json:"at"`
	PodUID     string        `json:"podUID"`
	Namespace  string        `json:"namespace"`
	Pod        string        `json:"pod"`
	Node       string        `json:"node"`
	Pipeline   string        `json:"pipeline"`
	WorkerType string        `json:"workerType"`
	Queue      string        `json:"queue"`
	GPU        int64         `json:"gpu"`
	Demand     []podIdentity `json:"demand"`
}
type auditWork struct {
	client kubernetes.Interface
	event  *v1.Event
}

var auditOnce sync.Once
var auditQueue = make(chan auditWork, 256)

func auditEvent(p *v1.Pod, queue string, demand []podIdentity) *v1.Event {
	// NVIDIA extended resources are integral. Include restartable init containers.
	var app, side, peak int64
	gpu := func(c v1.Container) int64 {
		r, ok := c.Resources.Requests[v1.ResourceName("nvidia.com/gpu")]
		if !ok {
			r = c.Resources.Limits[v1.ResourceName("nvidia.com/gpu")]
		}
		return r.Value()
	}
	for _, c := range p.Spec.Containers {
		app += gpu(c)
	}
	for _, c := range p.Spec.InitContainers {
		n := gpu(c)
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			side += n
			n = side
		} else {
			n += side
		}
		if n > peak {
			peak = n
		}
	}
	app += side
	if peak > app {
		app = peak
	}
	now := meta.Now()
	id := "pipeline-eviction-" + string(p.UID)
	a := evictionAudit{1, id, now.UTC().Format(time.RFC3339Nano), string(p.UID), p.Namespace, p.Name, p.Spec.NodeName, p.Labels["pipeline.simplesilicon.com/name"], p.Labels["scheduling.simplesilicon.com/worker-type"], queue, app, demand}
	b, _ := json.Marshal(a)
	return &v1.Event{ObjectMeta: meta.ObjectMeta{Name: id, Namespace: p.Namespace}, InvolvedObject: v1.ObjectReference{Kind: "Pod", Namespace: p.Namespace, Name: p.Name, UID: p.UID, APIVersion: "v1"}, Reason: "PipelineEvictionAccepted", Type: v1.EventTypeNormal, Source: v1.EventSource{Component: "volcano-pipeline-preemption"}, Message: string(b), FirstTimestamp: now, LastTimestamp: now, Count: 1}
}
func emitEvictionAudit(client kubernetes.Interface, p *v1.Pod, queue string, demand []podIdentity) {
	enqueueAudit(client, auditEvent(p, queue, demand))
}
func enqueueAudit(client kubernetes.Interface, event *v1.Event) {
	auditOnce.Do(func() {
		go func() {
			for w := range auditQueue {
				if err := publishAudit(w); err != nil {
					klog.Errorf("PREEMPTION_AUDIT_GAP event=%s err=%v", w.event.Name, err)
				}
			}
		}()
	})
	select {
	case auditQueue <- auditWork{client, event}:
	default:
		klog.Errorf("PREEMPTION_AUDIT_GAP uid=%s queue_full=true", event.InvolvedObject.UID)
	}
}
func publishAudit(w auditWork) error {
	var err error
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = w.client.CoreV1().Events(w.event.Namespace).Create(ctx, w.event, meta.CreateOptions{})
		cancel()
		if err == nil || errors.IsAlreadyExists(err) {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
	return fmt.Errorf("event publication failed: %w", err)
}
