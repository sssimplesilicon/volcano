#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
: "${TARGETARCH:=arm64}"
export CGO_ENABLED=0
mkdir -p _output/pipeline
go test ./pkg/webhooks/admission/workloadpolicy ./cmd/workload-policy-webhook ./pkg/webhooks/admission/workloadrouting ./cmd/workload-routing-webhook ./pkg/scheduler/plugins/queueaccess ./pkg/webhooks/admission/queueaccess ./pkg/scheduler/plugins/workerpreemption/... ./cmd/pipeline-budget-controller ./cmd/pipeline-worker-controller ./pkg/controllers/workerscale ./cmd/scheduler/app/options ./pkg/scheduler
GOOS=linux GOARCH="$TARGETARCH" go build -o _output/pipeline/vc-scheduler ./cmd/scheduler
GOOS=linux GOARCH="$TARGETARCH" go build -o _output/pipeline/budget-controller ./cmd/pipeline-budget-controller
GOOS=linux GOARCH="$TARGETARCH" go build -o _output/pipeline/queue-access-webhook ./cmd/queue-access-webhook
GOOS=linux GOARCH="$TARGETARCH" go build -o _output/pipeline/workload-routing-webhook ./cmd/workload-routing-webhook
GOOS=linux GOARCH="$TARGETARCH" go build -o _output/pipeline/workload-policy-webhook ./cmd/workload-policy-webhook

GOOS=linux GOARCH="$TARGETARCH" go build -o _output/pipeline/pipeline-worker-controller ./cmd/pipeline-worker-controller
