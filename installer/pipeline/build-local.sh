#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
: "${TARGETARCH:=arm64}"
export CGO_ENABLED=0
mkdir -p _output/pipeline
go test ./pkg/scheduler/plugins/workerpreemption/... ./cmd/pipeline-budget-controller ./cmd/scheduler/app/options ./pkg/scheduler
GOOS=linux GOARCH="$TARGETARCH" go build -o _output/pipeline/vc-scheduler ./cmd/scheduler
GOOS=linux GOARCH="$TARGETARCH" go build -o _output/pipeline/budget-controller ./cmd/pipeline-budget-controller
