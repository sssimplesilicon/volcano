// Package workloadpolicy implements opt-in workload routing and node-pool isolation.
// Decisions are persisted on real controllers; Pod annotations alone confer no access.
package workloadpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
	wr "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

const DecisionKey = "scheduling.simplesilicon.com/workload-decision"
const IsolationKey = "scheduling.simplesilicon.com/nodepool-policy"
const RouteLabel = "scheduling.simplesilicon.com/route"
const VersionLabel = "scheduling.simplesilicon.com/routing-version"
const GroupLabel = "volcano.sh/nodegroup-name"

// Decision contains scheduling identity, never container commands or credentials.
type Decision struct {
	Policy     string    `json:"policy"`
	Rule       string    `json:"rule"`
	Generation int64     `json:"generation"`
	Scheduler  string    `json:"scheduler"`
	Target     wr.Target `json:"target"`
	Route      string    `json:"route,omitempty"`
}
type RouteRef struct {
	Policy string   `json:"policyRef"`
	Rules  []string `json:"ruleNames"`
}
type RegistryRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}
type Component struct {
	Namespace      string `json:"namespace"`
	ServiceAccount string `json:"serviceAccount"`
	OwnerKind      string `json:"ownerKind"`
	OwnerName      string `json:"ownerName"`
}
type PoolSpec struct {
	NamespaceScope wr.Scope `json:"namespaceScope"`
	// One common node label key across pools makes overlap provably detectable.
	NodeSelector     map[string]string `json:"nodeSelector"`
	AcceptedRoutes   []RouteRef        `json:"acceptedRoutes"`
	ControllerUsers  []string          `json:"controllerUsers"`
	SystemComponents []Component       `json:"systemComponents,omitempty"`
	LegacyRegistry   *RegistryRef      `json:"legacyRegistry,omitempty"`
	Mode             string            `json:"mode,omitempty"`
}
type Pool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              PoolSpec `json:"spec"`
}
type State struct {
	Routes []wr.Policy
	Pools  []Pool
	Access *qa.Snapshot
}

// Reader resolves actual owners and the legacy registry. All calls are read-only.
type Reader interface {
	Get(context.Context, string, string, string, string) (*unstructured.Unstructured, error)
}
type Engine struct {
	Load   func(context.Context) (State, error)
	Reader Reader
}
type APIReader struct{ Client dynamic.Interface }

func ownerResource(apiVersion, kind string) (schema.GroupVersionResource, error) {
	gv, e := schema.ParseGroupVersion(apiVersion)
	if e != nil {
		return schema.GroupVersionResource{}, e
	}
	resources := map[string]string{"v1/Pod": "pods", "v1/ConfigMap": "configmaps", "apps/v1/Deployment": "deployments", "apps/v1/ReplicaSet": "replicasets", "apps/v1/StatefulSet": "statefulsets", "apps/v1/DaemonSet": "daemonsets", "batch/v1/Job": "jobs", "batch/v1/CronJob": "cronjobs", "argoproj.io/v1alpha1/Workflow": "workflows", "argoproj.io/v1alpha1/WorkflowTemplate": "workflowtemplates", "kubeflow.org/v1/PyTorchJob": "pytorchjobs"}
	r, ok := resources[apiVersion+"/"+kind]
	if !ok {
		return schema.GroupVersionResource{}, fmt.Errorf("unsupported owner %s/%s", apiVersion, kind)
	}
	return gv.WithResource(r), nil
}
func (a APIReader) Get(ctx context.Context, av, kind, ns, name string) (*unstructured.Unstructured, error) {
	r, e := ownerResource(av, kind)
	if e != nil {
		return nil, e
	}
	return a.Client.Resource(r).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
}
func New(client dynamic.Interface) Engine {
	return Engine{Reader: APIReader{client}, Load: func(ctx context.Context) (State, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		s := State{Access: &qa.Snapshot{}}
		for _, r := range []string{"workloadroutingpolicies", "nodepoolpolicies", "queueaccesspolicies"} {
			list, e := client.Resource(schema.GroupVersionResource{Group: "scheduling.simplesilicon.com", Version: "v1alpha1", Resource: r}).List(ctx, metav1.ListOptions{})
			if e != nil {
				return s, e
			}
			for _, o := range list.Items {
				b, e := json.Marshal(o.Object)
				if e != nil {
					return s, e
				}
				switch r {
				case "workloadroutingpolicies":
					var p wr.Policy
					if e = json.Unmarshal(b, &p); e != nil {
						return s, e
					}
					s.Routes = append(s.Routes, p)
				case "nodepoolpolicies":
					var p Pool
					if e = json.Unmarshal(b, &p); e != nil {
						return s, e
					}
					s.Pools = append(s.Pools, p)
				case "queueaccesspolicies":
					var p qa.Policy
					if e = json.Unmarshal(b, &p); e != nil {
						return s, e
					}
					s.Access.Items = append(s.Access.Items, p)
				}
			}
		}
		return s, nil
	}}
}
func has(xs []string, x string) bool {
	for _, v := range xs {
		if x == v {
			return true
		}
	}
	return false
}
func mode(s string) string {
	if s == "" {
		return "Enforce"
	}
	return s
}
func trusted(user string, users []string) bool { return user != "" && has(users, user) }
