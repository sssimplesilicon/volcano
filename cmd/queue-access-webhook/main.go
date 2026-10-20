// Optional standalone TLS host for the same validator registered in vc-webhook-manager.
package main

import (
	"encoding/json"
	"flag"
	admission "k8s.io/api/admission/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
	"net/http"
	"strings"
	"time"
	validator "volcano.sh/volcano/pkg/webhooks/admission/queueaccess"
)

func main() {
	var cert, key, names string
	flag.StringVar(&cert, "tls-cert", "/tls/tls.crt", "TLS certificate")
	flag.StringVar(&key, "tls-key", "/tls/tls.key", "TLS private key")
	flag.StringVar(&names, "scheduler-names", "volcano-infra", "allowed schedulers")
	flag.Parse()
	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}
	cfg.Timeout = 5 * time.Second
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/validate", func(w http.ResponseWriter, r *http.Request) {
		var ar admission.AdmissionReview
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&ar); err != nil {
			http.Error(w, "invalid admission review", 400)
			return
		}
		response := admission.AdmissionReview{TypeMeta: ar.TypeMeta, Response: validator.Validate(client, strings.Split(names, ","), ar)}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	server := &http.Server{Addr: ":8443", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServeTLS(cert, key))
}
