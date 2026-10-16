// budget-controller publishes observations only. It does not scale or evict.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"time"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "optional local kubeconfig")
	flag.Parse()
	var cfg *rest.Config
	var err error
	if *kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		panic(err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}
	for {
		if err := observe(client); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		time.Sleep(3 * time.Second)
	}
}
func observe(client kubernetes.Interface) error {
	s, err := pb.Load(client)
	if err != nil {
		return err
	}
	for _, b := range s.Budgets {
		pod := &v1.Pod{ObjectMeta: meta.ObjectMeta{Namespace: b.Namespace, Labels: map[string]string{pb.PipelineLabel: b.Spec.PipelineRef.Name, pb.TypeLabel: b.Spec.WorkerType}}}
		allocated, ready, _, err := s.Capacity(pod, nil)
		state, reason, message := "True", "Observed", "capacity observed; status is not an eviction authorization"
		if err != nil {
			state, reason, message = "False", "InvalidBudget", err.Error()
		}
		if err == nil && ready < float64(b.Spec.MinAvailableGPU)*1000 {
			state, reason, message = "False", "BelowFloor", "ready GPU capacity is below configured floor"
		}
		body := map[string]interface{}{"apiVersion": "scheduling.simplesilicon.com/v1alpha1", "kind": "WorkerBudget", "metadata": map[string]interface{}{"name": b.Name, "namespace": b.Namespace, "resourceVersion": b.ResourceVersion}, "status": map[string]interface{}{
			"observedGeneration": b.Generation, "allocatedGPU": allocated / 1000, "readyGPU": ready / 1000, "observedAt": time.Now().UTC().Format(time.RFC3339),
			"conditions": []map[string]interface{}{{"type": "Ready", "status": state, "reason": reason, "message": message, "observedGeneration": b.Generation, "lastTransitionTime": time.Now().UTC().Format(time.RFC3339)}},
		}}
		data, _ := json.Marshal(body)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = client.CoreV1().RESTClient().Put().AbsPath(pb.GroupPath+"/namespaces/"+b.Namespace+"/workerbudgets/"+b.Name+"/status").SetHeader("Content-Type", "application/json").Body(data).DoRaw(ctx)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}
