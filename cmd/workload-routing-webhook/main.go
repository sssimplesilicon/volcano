// Opt-in standalone workload routing admission server.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	admission "k8s.io/api/admission/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
	"net/http"
	"sync/atomic"
	policy "volcano.sh/volcano/pkg/webhooks/admission/workloadpolicy"

	"time"
	routing "volcano.sh/volcano/pkg/webhooks/admission/workloadrouting"
)

func main() {
	var cert, key string
	flag.StringVar(&cert, "tls-cert", "/tls/tls.crt", "TLS certificate")
	flag.StringVar(&key, "tls-key", "/tls/tls.key", "TLS private key")

	flag.Parse()
	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}
	cfg.Timeout = 5 * time.Second
	cfg.QPS = 50
	cfg.Burst = 100
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	engine := policy.New(dc)
	mux := http.NewServeMux()
	type counters struct{ allowed, denied atomic.Uint64 }
	stats := map[string]*counters{}
	for _, name := range []string{"/mutate-workload", "/validate-workload", "/mutate-isolation", "/validate-isolation"} {
		stats[name] = &counters{}
	}
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for endpoint, c := range stats {
			fmt.Fprintf(w, "workload_policy_admission_total{endpoint=%q,result=\"allowed\"} %d\n", endpoint, c.allowed.Load())
			fmt.Fprintf(w, "workload_policy_admission_total{endpoint=%q,result=\"denied\"} %d\n", endpoint, c.denied.Load())
		}
	})
	for _, endpoint := range []struct {
		path                string
		isolation, validate bool
	}{
		{"/mutate-workload", false, false}, {"/validate-workload", false, true},
		{"/mutate-isolation", true, false}, {"/validate-isolation", true, true},
	} {
		endpoint := endpoint
		mux.HandleFunc(endpoint.path, func(w http.ResponseWriter, r *http.Request) {
			var ar admission.AdmissionReview
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&ar); err != nil {
				http.Error(w, "invalid admission review", 400)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
			defer cancel()
			var answer *admission.AdmissionResponse
			if endpoint.isolation {
				answer = engine.Isolation(ctx, ar.Request, endpoint.validate)
			} else {
				answer = engine.Routing(ctx, ar.Request, endpoint.validate)
			}
			if answer.Allowed {
				stats[endpoint.path].allowed.Add(1)
			} else {
				stats[endpoint.path].denied.Add(1)
			}
			if ar.Request != nil && !answer.Allowed {
				log.Printf("admission denied endpoint=%s namespace=%s kind=%s name=%s reason=%s", endpoint.path, ar.Request.Namespace, ar.Request.Kind.Kind, ar.Request.Name, answer.Result.Message)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(admission.AdmissionReview{TypeMeta: ar.TypeMeta, Response: answer})
		})
	}
	mux.HandleFunc("/mutate", func(w http.ResponseWriter, r *http.Request) {
		var ar admission.AdmissionReview
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&ar); err != nil {
			http.Error(w, "invalid admission review", 400)
			return
		}
		response := admission.AdmissionReview{TypeMeta: ar.TypeMeta, Response: routing.Mutate(ar, routing.Load(client))}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	server := &http.Server{Addr: ":8443", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServeTLS(cert, key))
}
