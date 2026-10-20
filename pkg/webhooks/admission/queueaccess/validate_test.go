package queueaccess

import (
	"encoding/json"
	admission "k8s.io/api/admission/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, namespace, queue, scheduler string
		generated, update, group          bool
		allow                             bool
	}{
		{name: "allowed", namespace: "workers", queue: "shared", scheduler: "new", allow: true},
		{name: "forbidden", namespace: "other", queue: "shared", scheduler: "new"},
		{name: "generated-bypass", namespace: "workers", queue: "shared", scheduler: "default-scheduler", generated: true},
		{name: "unchanged-old", namespace: "other", queue: "shared", scheduler: "old", update: true, allow: true},
		{name: "effective-group", namespace: "other", scheduler: "new", group: true},
		{name: "unmanaged", namespace: "other", queue: "default", scheduler: "default-scheduler", allow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/apis/scheduling.simplesilicon.com/v1alpha1/queueaccesspolicies" {
					w.Write([]byte(`{"items":[{"metadata":{"name":"shared"},"spec":{"allowedNamespaces":["workers"]}}]}`))
				} else {
					w.Write([]byte(`{"spec":{"queue":"shared"}}`))
				}
			}))
			defer srv.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
			if err != nil {
				t.Fatal(err)
			}
			metadata := map[string]interface{}{"namespace": tc.namespace, "annotations": map[string]string{"scheduling.volcano.sh/queue-name": tc.queue}}
			if tc.generated {
				metadata["generateName"] = "test-"
			} else {
				metadata["name"] = "test"
			}
			if tc.group {
				metadata["annotations"].(map[string]string)["scheduling.k8s.io/group-name"] = "pg"
			}
			raw, _ := json.Marshal(map[string]interface{}{"metadata": metadata, "spec": map[string]string{"schedulerName": tc.scheduler}})
			req := &admission.AdmissionRequest{UID: "test", Namespace: tc.namespace, Resource: meta.GroupVersionResource{Version: "v1", Resource: "pods"}, Operation: admission.Create, Object: runtime.RawExtension{Raw: raw}}
			if tc.update {
				req.Operation = admission.Update
				req.OldObject = req.Object
			}
			result := Validate(client, []string{"new"}, admission.AdmissionReview{Request: req})
			if result.Allowed != tc.allow {
				t.Fatalf("%+v", result)
			}
		})
	}
}
