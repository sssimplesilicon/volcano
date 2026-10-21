#!/bin/sh
# Build only committed source. Does not access any Kubernetes cluster.
set -eu
cd "$(dirname "$0")/../.."
test -z "$(git status --porcelain)" || { echo 'Commit changes before release' >&2; exit 1; }
: "${RELEASE_OUT:?Set a new release output directory}"
REGISTRY=ss-container-lib-registry.cn-shanghai.cr.aliyuncs.com/infra/volcano
REV=$(git rev-parse HEAD)
TAG="sq-$(git rev-parse --short=12 HEAD)"
mkdir "$RELEASE_OUT"
export CGO_ENABLED=0
for ARCH in amd64 arm64; do
  mkdir "$RELEASE_OUT/$ARCH"
  GOOS=linux GOARCH="$ARCH" go build -trimpath -o "$RELEASE_OUT/$ARCH/vc-scheduler" ./cmd/scheduler
  GOOS=linux GOARCH="$ARCH" go build -trimpath -o "$RELEASE_OUT/$ARCH/budget-controller" ./cmd/pipeline-budget-controller
  GOOS=linux GOARCH="$ARCH" go build -trimpath -o "$RELEASE_OUT/$ARCH/pipeline-worker-controller" ./cmd/pipeline-worker-controller
  GOOS=linux GOARCH="$ARCH" go build -trimpath -o "$RELEASE_OUT/$ARCH/workload-policy-webhook" ./cmd/workload-policy-webhook
done
cat > "$RELEASE_OUT/Dockerfile.scheduler" <<'EOF'
FROM scratch
ARG TARGETARCH
COPY ${TARGETARCH}/vc-scheduler /vc-scheduler
COPY ${TARGETARCH}/budget-controller /budget-controller
COPY ${TARGETARCH}/pipeline-worker-controller /pipeline-worker-controller
USER 1000:1000
ENTRYPOINT ["/vc-scheduler"]
EOF
cp installer/pipeline/Dockerfile.workload-policy "$RELEASE_OUT/Dockerfile.webhook"
cp installer/pipeline/Dockerfile.worker-controller "$RELEASE_OUT/Dockerfile.worker"
for ROLE in scheduler webhook worker; do
  docker buildx build --builder "${BUILDX_BUILDER:-desktop-linux}" \
    --platform linux/amd64,linux/arm64 --provenance=false \
    --label "org.opencontainers.image.revision=$REV" \
    -f "$RELEASE_OUT/Dockerfile.$ROLE" -t "$REGISTRY:$TAG-$ROLE" \
    --push --metadata-file "$RELEASE_OUT/$ROLE-metadata.json" "$RELEASE_OUT"
done
python3 - "$RELEASE_OUT" "$REGISTRY" "$REV" "$TAG" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]);r={'commit':sys.argv[3],'tag':sys.argv[4]}
for role in ['scheduler','webhook','worker']:
    digest=json.loads((p/(role+'-metadata.json')).read_text())['containerimage.digest']
    assert digest.startswith('sha256:')
    r[role]=sys.argv[2]+'@'+digest
(p/'release.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps(r,indent=2))
PY
