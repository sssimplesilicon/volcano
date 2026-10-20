// Package queueaccess provides an explicitly enabled queue namespace admission route.
package queueaccess

import (
	"context"
	"encoding/json"
	"fmt"
	admission "k8s.io/api/admission/v1"
	wh "k8s.io/api/admissionregistration/v1"
	core "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"time"
	qa "volcano.sh/volcano/pkg/scheduler/plugins/queueaccess"
	"volcano.sh/volcano/pkg/webhooks/router"
)

var config = &router.AdmissionServiceConfig{}

func init() {
	fail := wh.Fail
	none := wh.SideEffectClassNone
	router.RegisterAdmission(&router.AdmissionService{Path: "/queueaccess/validate", Config: config, Func: func(r admission.AdmissionReview) *admission.AdmissionResponse {
		return Validate(config.KubeClient, config.SchedulerNames, r)
	}, ValidatingConfig: &wh.ValidatingWebhookConfiguration{Webhooks: []wh.ValidatingWebhook{{Name: "queueaccess.volcano.sh", FailurePolicy: &fail, SideEffects: &none, AdmissionReviewVersions: []string{"v1"}, Rules: []wh.RuleWithOperations{
		{Operations: []wh.OperationType{wh.Create, wh.Update}, Rule: wh.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}}},
		{Operations: []wh.OperationType{wh.Create, wh.Update}, Rule: wh.Rule{APIGroups: []string{"scheduling.volcano.sh"}, APIVersions: []string{"v1beta1"}, Resources: []string{"podgroups"}}},
	}}}}})
}
func Validate(client kubernetes.Interface, schedulers []string, review admission.AdmissionReview) *admission.AdmissionResponse {
	response := &admission.AdmissionResponse{Allowed: true}
	if review.Request == nil {
		response.Allowed = false
		return response
	}
	response.UID = review.Request.UID
	if err := check(client, schedulers, review.Request); err != nil {
		response.Allowed = false
		response.Result = &meta.Status{Message: err.Error(), Code: 403}
	}
	return response
}
func check(client kubernetes.Interface, schedulers []string, r *admission.AdmissionRequest) error {
	if r.SubResource != "" || (r.Operation != admission.Create && r.Operation != admission.Update) {
		return nil
	}
	var queue, advertised string
	var p core.Pod
	isPod := false
	switch {
	case r.Resource.Group == "scheduling.volcano.sh" && r.Resource.Resource == "podgroups":
		var pg struct {
			Spec struct {
				Queue string `json:"queue"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(r.Object.Raw, &pg); err != nil {
			return err
		}
		queue = pg.Spec.Queue
		if queue == "" {
			queue = "default"
		}
		if r.Operation == admission.Update {
			var old struct {
				Spec struct {
					Queue string `json:"queue"`
				} `json:"spec"`
			}
			if err := json.Unmarshal(r.OldObject.Raw, &old); err != nil {
				return err
			}
			if old.Spec.Queue == "" {
				old.Spec.Queue = "default"
			}
			if old.Spec.Queue == queue {
				return nil
			} // existing group's ordinary updates remain possible
		}
	case r.Resource.Group == "" && r.Resource.Resource == "pods":
		isPod = true
		if err := json.Unmarshal(r.Object.Raw, &p); err != nil {
			return err
		}
		p.Namespace = r.Namespace
		advertised = p.Annotations[qa.QueueAnnotation]
		queue = advertised
		if r.Operation == admission.Update {
			var old core.Pod
			if err := json.Unmarshal(r.OldObject.Raw, &old); err != nil {
				return err
			}
			if old.Annotations[qa.QueueAnnotation] == advertised && old.Annotations[qa.GroupAnnotation] == p.Annotations[qa.GroupAnnotation] && old.Spec.SchedulerName == p.Spec.SchedulerName {
				return nil
			}
		}
		if group := p.Annotations[qa.GroupAnnotation]; group != "" {

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			raw, err := client.CoreV1().RESTClient().Get().AbsPath("/apis/scheduling.volcano.sh/v1beta1/namespaces/" + r.Namespace + "/podgroups/" + group).DoRaw(ctx)
			if err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("cannot read PodGroup %s/%s: %w", r.Namespace, group, err)
			}
			if err == nil {
				var pg struct {
					Spec struct {
						Queue string `json:"queue"`
					} `json:"spec"`
				}
				if err = json.Unmarshal(raw, &pg); err != nil {
					return err
				}
				queue = pg.Spec.Queue
				if queue == "" {
					queue = "default"
				}
			}
		}
	default:
		return nil
	}
	if queue == "" {
		queue = "default"
	}
	policies, err := qa.Load(client)
	if err != nil {
		return fmt.Errorf("queue access unavailable: %w", err)
	}
	if err = policies.Check(r.Namespace, queue); err != nil {
		return err
	}
	if isPod {
		if advertised != "" {
			if err = policies.Check(r.Namespace, advertised); err != nil {
				return err
			}
		}
		if policies.Managed(queue) || policies.Managed(advertised) {
			ok := false
			for _, name := range schedulers {
				if name == p.Spec.SchedulerName {
					ok = true
				}
			}
			if !ok {
				return fmt.Errorf("grouped/protected queue Pod requires an authorized scheduler")
			}
		}
	}
	return nil
}
