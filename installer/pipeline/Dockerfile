# Build binaries with CGO_ENABLED=0 for TARGETARCH before packaging.
FROM busybox:1.36
COPY vc-scheduler /vc-scheduler
COPY budget-controller /budget-controller
COPY queue-access-webhook /queue-access-webhook
ENTRYPOINT ["/vc-scheduler"]
