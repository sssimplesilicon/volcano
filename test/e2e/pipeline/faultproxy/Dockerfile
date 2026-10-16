# Build faultproxy with CGO_ENABLED=0 for the local k3s architecture first.
FROM alpine:3.22
RUN apk add --no-cache curl
COPY faultproxy /faultproxy
ENTRYPOINT ["/faultproxy"]
