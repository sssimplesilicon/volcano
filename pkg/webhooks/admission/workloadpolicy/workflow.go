package workloadpolicy

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Check namespaced template references as well as inline templates. Cluster-scoped
// references are explicitly unsupported until a separately reviewed RBAC scope exists.
func (e Engine) workflowReferences(ctx context.Context, o *unstructured.Unstructured) error {
	if o.GetKind() != "Workflow" {
		return nil
	}
	seen := map[string]bool{}
	var walk func(interface{}, int) error
	walk = func(v interface{}, depth int) error {
		if depth > 8 {
			return fmt.Errorf("Workflow template reference depth exceeded")
		}
		switch x := v.(type) {
		case map[string]interface{}:
			for k, v := range x {
				if k == "workflowTemplateRef" || k == "templateRef" {
					ref, ok := v.(map[string]interface{})
					if !ok {
						return fmt.Errorf("invalid workflow template reference")
					}
					if ref["clusterScope"] == true {
						return fmt.Errorf("cluster-scoped Workflow templates are not enabled")
					}
					name, _ := ref["name"].(string)
					if name == "" {
						return fmt.Errorf("template reference name required")
					}
					if seen[name] {
						continue
					}
					seen[name] = true
					if e.Reader == nil {
						return fmt.Errorf("Workflow template resolver unavailable")
					}
					t, err := e.Reader.Get(ctx, "argoproj.io/v1alpha1", "WorkflowTemplate", o.GetNamespace(), name)
					if err != nil {
						return fmt.Errorf("resolve WorkflowTemplate: %w", err)
					}
					if err = workflowSafe(t.Object["spec"]); err != nil {
						return err
					}
					if err = walk(t.Object["spec"], depth+1); err != nil {
						return err
					}
				} else if err := walk(v, depth); err != nil {
					return err
				}
			}
		case []interface{}:
			for _, v := range x {
				if err := walk(v, depth); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(o.Object["spec"], 0)
}
