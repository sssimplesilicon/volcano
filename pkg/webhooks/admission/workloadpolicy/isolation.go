package workloadpolicy

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	admission "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func validatePools(pools []Pool) error {
	key := ""
	values := map[string]bool{}
	for _, p := range pools {
		if mode(p.Spec.Mode) == "Disabled" {
			continue
		}
		if p.DeletionTimestamp != nil {
			return fmt.Errorf("node pool policy %s terminating", p.Name)
		}
		if len(p.Spec.NodeSelector) != 1 {
			return fmt.Errorf("node pool %s requires exactly one label selector", p.Name)
		}
		for k, v := range p.Spec.NodeSelector {
			if k == "" || v == "" || (key != "" && key != k) || values[v] {
				return fmt.Errorf("node pools must use a common label key and distinct values")
			}
			key = k
			values[v] = true
		}
		if len(p.Spec.NamespaceScope.Include) == 0 {
			return fmt.Errorf("node pool namespace scope required")
		}
	}
	return nil
}
func accepts(p Pool, d *Decision) bool {
	if d == nil || d.Target.PoolRef != p.Name {
		return false
	}
	for _, r := range p.Spec.AcceptedRoutes {
		if r.Policy == d.Policy && has(r.Rules, d.Rule) {
			return true
		}
	}
	return false
}
func (e Engine) exempt(ctx context.Context, p Pool, o *unstructured.Unstructured, user string) (bool, error) {
	if !trusted(user, p.Spec.ControllerUsers) {
		return false, nil
	}
	ancestors, err := e.ancestors(ctx, o)
	if err != nil {
		return false, err
	}
	for _, c := range p.Spec.SystemComponents {
		sa, _, _ := unstructured.NestedString(o.Object, "spec", "serviceAccountName")
		if o.GetNamespace() != c.Namespace || sa != c.ServiceAccount {
			continue
		}
		if len(ancestors) > 0 && ancestors[0].GetKind() == c.OwnerKind && ancestors[0].GetName() == c.OwnerName {
			return true, nil
		}
	}
	if p.Spec.LegacyRegistry == nil {
		return false, nil
	}
	ref := p.Spec.LegacyRegistry
	cm, err := e.Reader.Get(ctx, "v1", "ConfigMap", ref.Namespace, ref.Name)
	if err != nil {
		return false, fmt.Errorf("legacy registry unavailable: %w", err)
	}
	raw, _, err := unstructured.NestedString(cm.Object, "data", "legacyOwners")
	if err != nil {
		return false, err
	}
	var owners []string
	if err = decode(raw, &owners); err != nil {
		return false, fmt.Errorf("invalid legacyOwners registry")
	}
	for _, a := range ancestors {
		if has(owners, o.GetNamespace()+"|"+string(a.GetUID())) {
			return true, nil
		}
	}
	return false, nil
}
func stringValues(v interface{}) ([]string, error) {
	a, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid selector values")
	}
	out := []string{}
	for _, x := range a {
		s, ok := x.(string)
		if !ok {
			return nil, fmt.Errorf("invalid selector value")
		}
		out = append(out, s)
	}
	return out, nil
}

// constraints intersects pool rules with every required-affinity OR branch.
// Empty original terms remain impossible (Kubernetes treats them as match-none).
func constraints(o *unstructured.Unstructured, key string, deny []string, allow string) error {
	sel, _, err := unstructured.NestedStringMap(o.Object, "spec", "nodeSelector")
	if err != nil {
		return err
	}
	if v := sel[key]; v != "" && (has(deny, v) || (allow != "" && v != allow)) {
		return fmt.Errorf("nodeSelector conflicts with node pool isolation")
	}
	if allow != "" {
		if sel == nil {
			sel = map[string]string{}
		}
		sel[key] = allow
		unstructured.SetNestedStringMap(o.Object, sel, "spec", "nodeSelector")
	}
	if len(deny) == 0 {
		return nil
	}
	sort.Strings(deny)
	path := []string{"spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms"}
	terms, found, err := unstructured.NestedSlice(o.Object, path...)
	if err != nil {
		return err
	}
	if !found {
		terms = []interface{}{map[string]interface{}{}}
	} else if len(terms) == 0 {
		return fmt.Errorf("empty required nodeSelectorTerms cannot be broadened")
	}
	validTerms := 0
	for i, v := range terms {
		term, ok := v.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid affinity term")
		}
		expr, _ := term["matchExpressions"].([]interface{})
		fields, _ := term["matchFields"].([]interface{})
		if found && len(expr) == 0 && len(fields) == 0 {
			continue
		}
		contradiction := false
		exists := false
		for _, x := range expr {
			m, ok := x.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid matchExpression")
			}
			if m["key"] != key {
				continue
			}
			if m["operator"] == "In" {
				vs, e := stringValues(m["values"])
				if e != nil {
					return e
				}
				possible := false
				for _, v := range vs {
					if !has(deny, v) && (allow == "" || v == allow) {
						possible = true
					}
				}
				if !possible {
					contradiction = true
				}
			}
			if m["operator"] == "NotIn" {
				vs, e := stringValues(m["values"])
				if e != nil {
					return e
				}
				if allow != "" && has(vs, allow) {
					contradiction = true
				}
				all := true
				for _, v := range deny {
					if !has(vs, v) {
						all = false
					}
				}
				exists = exists || all
			}
			if m["operator"] == "DoesNotExist" && allow != "" {
				contradiction = true
			}
		}
		if !contradiction {
			validTerms++
		}
		if !exists {
			vs := []interface{}{}
			for _, v := range deny {
				vs = append(vs, v)
			}
			expr = append(expr, map[string]interface{}{"key": key, "operator": "NotIn", "values": vs})
			term["matchExpressions"] = expr
		}
		terms[i] = term
	}
	if validTerms == 0 {
		return fmt.Errorf("all required affinity terms conflict with node pool isolation")
	}
	return unstructured.SetNestedSlice(o.Object, terms, path...)
}
func (e Engine) Isolation(ctx context.Context, r *admission.AdmissionRequest, validate bool) *admission.AdmissionResponse {
	if r == nil {
		return response(r, fmt.Errorf("missing request"))
	}
	if r.Operation != admission.Create || r.SubResource != "" || r.Resource.Group != "" || r.Resource.Resource != "pods" {
		return response(r, nil)
	}
	o, err := object(r)
	if err != nil {
		return response(r, err)
	}
	s, err := e.Load(ctx)
	if err != nil {
		return response(r, err)
	}
	if err = validatePools(s.Pools); err != nil {
		return response(r, err)
	}
	pools := []Pool{}
	for _, p := range s.Pools {
		if mode(p.Spec.Mode) != "Disabled" && p.Spec.NamespaceScope.Includes(r.Namespace) {
			pools = append(pools, p)
		}
	}
	var d *Decision
	ref, err := owner(o)
	if err != nil {
		return response(r, err)
	}
	if ref != nil {
		// Only resolve routing ownership if the Pod claims enrollment. A random
		// external controller without a decision simply receives pool exclusions.
		claimed := o.GetAnnotations()[DecisionKey] != ""
		for _, policy := range s.Routes {
			claimed = claimed || trusted(r.UserInfo.Username, policy.Spec.ControllerUsers)
		}
		if claimed {
			d, err = e.inherited(ctx, s, o, r.UserInfo.Username)
		}
	} else {
		p, x := selectPolicy(s, r.Namespace, "Pod")
		err = x
		if err == nil && p != nil && mode(p.Spec.Mode) == "Enforce" {
			v, x := choose(s, p, o)
			d = &v
			err = x
		}
	}
	if err != nil {
		return response(r, err)
	}
	if d != nil {
		if err = routePod(o, *d, validate); err != nil {
			return response(r, err)
		}
	}
	original := o.DeepCopy()
	key := ""
	denied := []string{}
	allowed := ""
	active := []string{}
	warnings := []string{}
	poolFound := d == nil || d.Target.PoolRef == ""
	for _, p := range pools {
		var k, v string
		for a, b := range p.Spec.NodeSelector {
			k = a
			v = b
		}
		key = k
		ok := accepts(p, d)
		if ok {
			poolFound = true
		}
		if !ok {
			ok, err = e.exempt(ctx, p, o, r.UserInfo.Username)
			if err != nil {
				return response(r, err)
			}
		}
		if mode(p.Spec.Mode) == "Audit" {
			warnings = append(warnings, fmt.Sprintf("nodepool audit: %s allowed=%t", p.Name, ok))
			continue
		}
		active = append(active, p.Name)
		if ok {
			if accepts(p, d) {
				if allowed != "" && allowed != v {
					return response(r, fmt.Errorf("multiple allowed route pools"))
				}
				allowed = v
			}
		} else {
			denied = append(denied, v)
		}
	}
	if !poolFound {
		return response(r, fmt.Errorf("route references missing, out-of-scope or unauthorized node pool"))
	}
	if len(active) == 0 {
		x := response(r, nil)
		x.Warnings = warnings
		return x
	}
	if len(denied) > 0 {
		n, _, _ := unstructured.NestedString(o.Object, "spec", "nodeName")
		if n != "" {
			return response(r, fmt.Errorf("non-exempt Pod cannot directly bind nodeName"))
		}
	}
	if err = constraints(o, key, denied, allowed); err != nil {
		return response(r, err)
	}
	if validate {
		for _, p := range [][]string{{"spec", "nodeSelector"}, {"spec", "affinity"}} {
			before, _, _ := unstructured.NestedFieldNoCopy(original.Object, p...)
			after, _, _ := unstructured.NestedFieldNoCopy(o.Object, p...)
			if !reflect.DeepEqual(before, after) {
				return response(r, fmt.Errorf("required node pool constraint missing from final Pod"))
			}
		}
		x := response(r, nil)
		x.Warnings = warnings
		return x
	}
	a := o.GetAnnotations()
	if a == nil {
		a = map[string]string{}
	}
	sort.Strings(active)
	a[IsolationKey] = encode(active)
	o.SetAnnotations(a)
	x := patchResponse(r, o)
	x.Warnings = warnings
	return x
}
