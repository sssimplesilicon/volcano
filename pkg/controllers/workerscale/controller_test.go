package workerscale

import (
	"context"
	"encoding/json"
	"fmt"
	apps "k8s.io/api/apps/v1"
	autoscaling "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
	"testing"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
)

func fixture(t *testing.T) (*fake.Clientset, *pb.Snapshot, *pb.WorkerBudget, *int32) {
	t.Helper()
	var b pb.WorkerBudget
	json.Unmarshal([]byte(`{"metadata":{"name":"a","namespace":"workers","uid":"budget"},"spec":{"pipelineRef":{"name":"ego"},"workerType":"A","resourceName":"nvidia.com/gpu","minAvailableGPU":2,"desiredGPU":6,"workloadRef":{"kind":"Deployment","name":"a","uid":"deployment"}}}`), &b)
	var p pb.PipelinePolicy
	json.Unmarshal([]byte(`{"metadata":{"name":"ego","namespace":"workers"},"spec":{"accountingQueues":["shared"],"preemptibleQueues":["shared"],"allowedPreemptorNamespaces":[]}}`), &p)
	n := int32(0)
	d := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "a", Namespace: "workers", UID: "deployment", ResourceVersion: "1"}, Spec: apps.DeploymentSpec{Replicas: &n, Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{pb.PipelineLabel: "ego", pb.TypeLabel: "A", pb.PipelineNamespaceLabel: "workers", "scheduling.simplesilicon.com/preemptible": "true"}, Annotations: map[string]string{"scheduling.volcano.sh/queue-name": "shared", "scheduling.simplesilicon.com/workload-decision": `{"policy":"workers","rule":"a","scheduler":"volcano-infra","target":{"queue":"shared","priorityClassName":"worker","worker":{"pipeline":"ego","namespace":"workers","type":"A"}}}`}}, Spec: core.PodSpec{SchedulerName: "volcano-infra", PriorityClassName: "worker", Containers: []core.Container{{Name: "worker", Resources: core.ResourceRequirements{Requests: core.ResourceList{"nvidia.com/gpu": resource.MustParse("2")}, Limits: core.ResourceList{"nvidia.com/gpu": resource.MustParse("2")}}}}}}}}
	c := fake.NewSimpleClientset(d)
	writes := int32(-1)
	c.PrependReactor("update", "deployments", func(a kt.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "scale" {
			t.Fatal("must write scale only")
		}
		s := a.(kt.UpdateAction).GetObject().(*autoscaling.Scale)
		writes = s.Spec.Replicas
		if s.ResourceVersion != "1" {
			t.Fatal("missing concurrency guard")
		}
		return true, s, nil
	})
	return c, &pb.Snapshot{Policies: []pb.PipelinePolicy{p}, Budgets: []pb.WorkerBudget{b}}, &b, &writes
}
func TestDesiredGPUScalesDeployment(t *testing.T) {
	c, s, b, w := fixture(t)
	o, e := Reconcile(context.Background(), c, s, b)
	if e != nil {
		t.Fatal(e)
	}
	if *w != 3 || o.DesiredReplicas != 3 {
		t.Fatalf("6 GPUs with 2 per replica must request 3 replicas: %+v %d", o, *w)
	}
}

func TestScalingRefusesUnsafeInputs(t *testing.T) {
	for _, name := range []string{"replaced-deployment", "not-divisible", "below-floor", "duplicate-owner", "init-gpu", "hpa-owner"} {
		t.Run(name, func(t *testing.T) {
			c, s, b, w := fixture(t)
			switch name {
			case "replaced-deployment":
				b.Spec.WorkloadRef.UID = "another-uid"
			case "not-divisible":
				v := int64(5)
				b.Spec.DesiredGPU = &v
			case "below-floor":
				v := int64(0)
				b.Spec.DesiredGPU = &v
			case "duplicate-owner":
				other := *b
				other.Name = "duplicate"
				s.Budgets = append(s.Budgets, other)
			case "hpa-owner":
				c.Tracker().Add(&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: meta.ObjectMeta{Name: "a", Namespace: "workers"}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: "a"}}})
			case "init-gpu":
				d, _ := c.AppsV1().Deployments("workers").Get(context.Background(), "a", meta.GetOptions{})
				d.Spec.Template.Spec.InitContainers = []core.Container{{Name: "init", Resources: core.ResourceRequirements{Limits: core.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}}}
				c.Tracker().Update(apps.SchemeGroupVersion.WithResource("deployments"), d, "workers")
			}
			if _, e := Reconcile(context.Background(), c, s, b); e == nil {
				t.Fatal("expected rejection")
			}
			if *w != -1 {
				t.Fatal("unsafe input changed scale")
			}
		})
	}
}
func TestDownscaleDeferredWhilePreemptionEnabled(t *testing.T) {
	c, s, b, w := fixture(t)
	d, _ := c.AppsV1().Deployments("workers").Get(context.Background(), "a", meta.GetOptions{})
	n := int32(4)
	d.Spec.Replicas = &n
	d.Status = apps.DeploymentStatus{Replicas: 4, ReadyReplicas: 4, UpdatedReplicas: 4}
	c.Tracker().Update(apps.SchemeGroupVersion.WithResource("deployments"), d, "workers")
	ns := []string{"training"}
	s.Policies[0].Spec.AllowedPreemptorNamespaces = &ns
	o, e := Reconcile(context.Background(), c, s, b)
	if e != nil || o.State != "DownscaleDeferred" || *w != -1 {
		t.Fatalf("%+v %v %d", o, e, *w)
	}
	ns = []string{}
	o, e = Reconcile(context.Background(), c, s, b, func(context.Context) error { return fmt.Errorf("outstanding intent") })
	if e != nil || o.State != "DownscaleDeferred" || *w != -1 {
		t.Fatal("journal must block shrink", o, e)
	}
	o, e = Reconcile(context.Background(), c, s, b, func(context.Context) error { return nil })
	if e != nil || o.State != "Scaled" || *w != 3 {
		t.Fatalf("%+v %v %d", o, e, *w)
	}
}
func TestLegacyBudgetDoesNotScale(t *testing.T) {
	c, s, b, w := fixture(t)
	b.Spec.DesiredGPU = nil
	o, e := Reconcile(context.Background(), c, s, b)
	if e != nil || o.State != "Unmanaged" || *w != -1 {
		t.Fatal(o, e)
	}
}

func TestDownscaleJournalGuard(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		allow     bool
	}{{"empty", "{}", true}, {"outstanding", `{"pod-uid":{}}`, false}, {"null", "null", false}, {"broken", "oops", false}} {
		t.Run(tc.name, func(t *testing.T) {
			cm := &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Name: "journal", Namespace: "system", Labels: map[string]string{"scheduling.simplesilicon.com/recovery-journal": "v1"}}, Data: map[string]string{"intents.json": tc.raw}}
			c := fake.NewSimpleClientset(cm)
			if err := JournalEmpty(context.Background(), c, "system/journal"); (err == nil) != tc.allow {
				t.Fatal(err)
			}
			if err := JournalEmpty(context.Background(), c, ""); err == nil {
				t.Fatal("missing journal allowed")
			}
		})
	}
}

func TestZeroDesiredGPUCanStopOptInType(t *testing.T) {
	c, s, b, w := fixture(t)
	z := int64(0)
	b.Spec.DesiredGPU = &z
	b.Spec.MinAvailableGPU = 0
	d, _ := c.AppsV1().Deployments("workers").Get(context.Background(), "a", meta.GetOptions{})
	n := int32(2)
	d.Spec.Replicas = &n
	d.Status = apps.DeploymentStatus{Replicas: 2, ReadyReplicas: 2, UpdatedReplicas: 2}
	c.Tracker().Update(apps.SchemeGroupVersion.WithResource("deployments"), d, "workers")
	o, e := Reconcile(context.Background(), c, s, b)
	if e != nil || o.State != "DownscaleDeferred" || *w != -1 {
		t.Fatal("no journal guard must block", o, e)
	}
	o, e = Reconcile(context.Background(), c, s, b, func(context.Context) error { return nil })
	if e != nil || o.State != "Scaled" || *w != 0 {
		t.Fatal("explicit zero target not reconciled", o, e)
	}
}
