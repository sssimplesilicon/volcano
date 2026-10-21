// pipeline-worker-controller manages only explicit desiredGPU + UID-bound Deployment budgets.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	scale "volcano.sh/volcano/pkg/controllers/workerscale"
	pb "volcano.sh/volcano/pkg/scheduler/plugins/workerpreemption/pipelinebudget"
)

func cycle(ctx context.Context, c kubernetes.Interface, ns, journal string) error {
	s, err := pb.Load(c)
	if err != nil {
		return err
	}
	for i := range s.Budgets {
		b := &s.Budgets[i]
		if b.Namespace != ns || b.Spec.DesiredGPU == nil {
			continue
		}
		observation, e := scale.Reconcile(ctx, c, s, b, func(ctx context.Context) error { return scale.JournalEmpty(ctx, c, journal) })
		if e != nil {
			observation.State = "Blocked"
			observation.Message = e.Error()
			log.Printf("%s/%s: %v", ns, b.Name, e)
		}
		status := b.Status
		if status == nil {
			status = map[string]interface{}{}
		}
		status["scaling"] = map[string]interface{}{"state": observation.State, "desiredReplicas": observation.DesiredReplicas, "gpuPerReplica": observation.GPUPerReplica, "message": observation.Message, "observedGeneration": b.Generation}
		body, _ := json.Marshal(map[string]interface{}{"apiVersion": "scheduling.simplesilicon.com/v1alpha1", "kind": "WorkerBudget", "metadata": map[string]interface{}{"name": b.Name, "namespace": ns, "resourceVersion": b.ResourceVersion}, "status": status})
		writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = c.CoreV1().RESTClient().Put().AbsPath(pb.GroupPath+"/namespaces/"+ns+"/workerbudgets/"+b.Name+"/status").SetHeader("Content-Type", "application/json").Body(body).DoRaw(writeCtx)
		cancel()
		if err != nil {
			log.Printf("%s/%s status: %v", ns, b.Name, err)
		}
	}
	return nil
}
func main() {
	kcfg := flag.String("kubeconfig", "", "optional LOCAL kubeconfig")
	ns := flag.String("namespace", "", "required namespace containing managed budgets/Deployments")
	leaseNS := flag.String("lease-namespace", "", "leader lease namespace (defaults to managed namespace)")
	journal := flag.String("recovery-journal", "", "namespace/name of scheduler recovery journal; required for downscale")
	interval := flag.Duration("interval", 3*time.Second, "reconcile interval")
	flag.Parse()
	if *journal != "" && len(strings.Split(*journal, "/")) != 2 {
		log.Fatal("recovery-journal must be namespace/name")
	}
	if *ns == "" || *interval < time.Second {
		log.Fatal("--namespace and interval >= 1s required")
	}
	if *leaseNS == "" {
		*leaseNS = *ns
	}
	var cfg *rest.Config
	var err error
	if *kcfg != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", *kcfg)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		log.Fatal(err)
	}
	cfg.Timeout = 15 * time.Second
	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	host, _ := os.Hostname()
	identity := fmt.Sprintf("%s-%s", host, uuid.NewUUID())
	lock := &resourcelock.LeaseLock{LeaseMeta: meta.ObjectMeta{Name: "pipeline-worker-" + *ns, Namespace: *leaseNS}, Client: c.CoordinationV1(), LockConfig: resourcelock.ResourceLockConfig{Identity: identity}}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{Lock: lock, LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second, ReleaseOnCancel: false, Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: func(ctx context.Context) {
		log.Printf("leader acquired; namespace=%s", *ns)
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for {
			if err := cycle(ctx, c, *ns, *journal); err != nil {
				log.Print(err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}, OnStoppedLeading: func() { log.Print("leader stopped") }}})
}
