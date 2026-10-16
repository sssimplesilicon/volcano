// Local integration-test proxy only; never part of the production plugin.
package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type config struct {
	Mode         string `json:"mode"`
	Nth          int    `json:"nth"`
	DelaySeconds int    `json:"delaySeconds"`
	Count        int    `json:"count"`
	Target       string `json:"target"`
	Repeat       int    `json:"repeat"`
	StatusCode   int    `json:"statusCode"`
}

func main() {
	ca, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	var mu sync.Mutex
	c := config{}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__fault/config" {
			mu.Lock()
			defer mu.Unlock()
			if r.Method == "PUT" {
				var next config
				if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				next.Count = 0
				c = next
			}
			json.NewEncoder(w).Encode(c)
			return
		}
		fault := config{}
		isEviction := r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/worker-type-prototype/pods/") && strings.HasSuffix(r.URL.Path, "/eviction")
		isJournal := r.Method == "PUT" && r.URL.Path == "/api/v1/namespaces/worker-type-prototype/configmaps/worker-type-test-recovery"
		if isEviction {
			log.Printf("EVICTION_REQUEST path=%s", r.URL.Path)
		}
		target := ""
		if isEviction {
			target = "eviction"
		}
		if isJournal {
			target = "journal-write"
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			if journalEmpty(body) {
				target = "journal-clear"
			}
			log.Printf("JOURNAL_REQUEST contentType=%s target=%s", r.Header.Get("Content-Type"), target)
		}
		mu.Lock()
		wanted := c.Target
		if wanted == "" {
			wanted = "eviction"
		}
		mu.Unlock()
		if target != "" && (wanted == target || (wanted == "journal-write" && isJournal)) {
			mu.Lock()
			c.Count++
			repeat := c.Repeat
			if repeat <= 0 {
				repeat = 1
			}
			if c.Count >= c.Nth && c.Count < c.Nth+repeat {
				fault = c
			}
			mu.Unlock()
			log.Printf("MATCHED_REQUEST target=%s path=%s fault=%s", target, r.URL.Path, fault.Mode)
		}
		if fault.Mode == "reject" {
			status := fault.StatusCode
			if status < 400 || status > 599 {
				status = 503
			}
			reason := map[int]string{403: "Forbidden", 409: "Conflict", 429: "TooManyRequests", 503: "ServiceUnavailable"}[status]
			log.Printf("FAULT_REJECT target=%s path=%s code=%d", target, r.URL.Path, status)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(map[string]interface{}{"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": reason, "code": status, "message": "injected local e2e rejection"})
			return
		}
		if fault.Mode == "before" {
			log.Printf("FAULT_BEFORE path=%s", r.URL.Path)
			time.Sleep(time.Duration(fault.DelaySeconds) * time.Second)
			http.Error(w, "injected dropped request", 504)
			return
		}
		// Disconnect from downstream cancellation to model a request already accepted
		// upstream whose response is lost. Only test Eviction paths can inject faults.
		req, err := http.NewRequest(r.Method, "https://kubernetes.default.svc"+r.URL.RequestURI(), r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		req.Header = r.Header.Clone()
		token, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		if fault.Mode == "after" {
			io.Copy(io.Discard, resp.Body)
			log.Printf("FAULT_AFTER path=%s upstreamStatus=%d", r.URL.Path, resp.StatusCode)
			time.Sleep(time.Duration(fault.DelaySeconds) * time.Second)
			http.Error(w, "injected lost response", 504)
			return
		}
		for k, v := range resp.Header {
			for _, s := range v {
				w.Header().Add(k, s)
			}
		}
		w.WriteHeader(resp.StatusCode)
		// Flush watch streams so informers continue to function through this proxy.
		buf := make([]byte, 32768)
		for {
			n, e := resp.Body.Read(buf)
			if n > 0 {
				w.Write(buf[:n])
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			if e != nil {
				return
			}
		}
	})
	log.Fatal(http.ListenAndServe(":18080", nil))
}

// Kubernetes protobuf uses a runtime.Unknown envelope containing the ConfigMap.
// Decode only length-delimited fields required by this test proxy; reject malformed input.
func protoFields(b []byte, field uint64) ([][]byte, bool) {
	var out [][]byte
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 || tag>>3 == 0 {
			return nil, false
		}
		b = b[n:]
		switch tag & 7 {
		case 0:
			_, n = binary.Uvarint(b)
			if n <= 0 {
				return nil, false
			}
			b = b[n:]
		case 1:
			if len(b) < 8 {
				return nil, false
			}
			b = b[8:]
		case 5:
			if len(b) < 4 {
				return nil, false
			}
			b = b[4:]
		case 2:
			size, n := binary.Uvarint(b)
			if n <= 0 {
				return nil, false
			}
			b = b[n:]
			if size > uint64(len(b)) {
				return nil, false
			}
			if tag>>3 == field {
				out = append(out, b[:int(size)])
			}
			b = b[int(size):]
		default:
			return nil, false
		}
	}
	return out, true
}
func emptyObject(s string) bool {
	var entries map[string]json.RawMessage
	return json.Unmarshal([]byte(s), &entries) == nil && entries != nil && len(entries) == 0
}
func journalEmpty(body []byte) bool {
	if !bytes.HasPrefix(body, []byte{'k', '8', 's', 0}) {
		var cm struct {
			Data map[string]string `json:"data"`
		}
		return json.Unmarshal(body, &cm) == nil && emptyObject(cm.Data["intents.json"])
	}
	raw, ok := protoFields(body[4:], 2)
	if !ok || len(raw) != 1 {
		return false
	}
	data, ok := protoFields(raw[0], 2)
	if !ok {
		return false
	}
	for _, entry := range data {
		keys, kok := protoFields(entry, 1)
		vals, vok := protoFields(entry, 2)
		if !kok || !vok {
			return false
		}
		if len(keys) == 1 && string(keys[0]) == "intents.json" && len(vals) == 1 {
			return emptyObject(string(vals[0]))
		}
	}
	return false
}
