#!/usr/bin/env bash
# Copyright 2026 Mert Öztürk
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$(mktemp -d /tmp/kubectl-cron-kind.XXXXXX)"
CLUSTER="cron-e2e-$$"
NODE_IMAGE="${CRON_E2E_NODE_IMAGE:-kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5}"
WORKLOAD_IMAGE="${CRON_E2E_IMAGE:-busybox:1.37.0}"
CREATED=0
mkdir -p "$RUN_DIR/artifacts"

cleanup() {
  status=$?
  trap - EXIT
  if [[ "$CREATED" == 1 ]]; then
    docker exec "$CLUSTER-control-plane" cat /var/log/cron-audit/audit.log > "$RUN_DIR/artifacts/audit.jsonl" 2>/dev/null || true
    if [[ "${CRON_E2E_KEEP:-0}" != 1 ]]; then
      kind delete cluster --name "$CLUSTER"
    else
      echo "Kept isolated cluster: $CLUSTER"
      echo "Kubeconfig: $RUN_DIR/kubeconfig"
    fi
  fi
  echo "Test artifacts: $RUN_DIR/artifacts"
  exit "$status"
}
trap cleanup EXIT

for command in go kind docker kubectl python3; do command -v "$command" >/dev/null; done
docker info >/dev/null
docker image inspect "$WORKLOAD_IMAGE" >/dev/null 2>&1 || docker pull "$WORKLOAD_IMAGE"
PLATFORM="linux/$(docker version --format '{{.Server.Arch}}')"

# Export a single platform: Docker's cached multi-platform indexes may refer
# to unpulled content that kind's all-platform archive importer cannot resolve.
docker image save --platform="$PLATFORM" -o "$RUN_DIR/workload.tar" "$WORKLOAD_IMAGE"
python3 - "$RUN_DIR" "$NODE_IMAGE" <<'PY'
import json
import sys
from pathlib import Path

root, image = Path(sys.argv[1]), sys.argv[2]
(root / 'audit-policy.json').write_text(json.dumps({
    'apiVersion': 'audit.k8s.io/v1', 'kind': 'Policy',
    'rules': [{'level': 'Metadata'}],
}))
patch = '''apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
apiServer:
  extraArgs:
    - name: audit-policy-file
      value: /etc/kubernetes/cron-audit/audit-policy.json
    - name: audit-log-path
      value: /var/log/cron-audit/audit.log
    - name: audit-log-mode
      value: blocking
  extraVolumes:
    - name: cron-audit-policy
      hostPath: /etc/kubernetes/cron-audit
      mountPath: /etc/kubernetes/cron-audit
      readOnly: true
      pathType: Directory
    - name: cron-audit-log
      hostPath: /var/log/cron-audit
      mountPath: /var/log/cron-audit
      readOnly: false
      pathType: DirectoryOrCreate
'''
(root / 'kind.json').write_text(json.dumps({
    'kind': 'Cluster', 'apiVersion': 'kind.x-k8s.io/v1alpha4',
    'nodes': [{'role': 'control-plane', 'image': image,
               'kubeadmConfigPatches': [patch],
               'extraMounts': [{'hostPath': str(root / 'audit-policy.json'),
                                'containerPath': '/etc/kubernetes/cron-audit/audit-policy.json',
                                'readOnly': True}]}],
}))
PY

CREATED=1
kind create cluster --name "$CLUSTER" --kubeconfig "$RUN_DIR/kubeconfig" --config "$RUN_DIR/kind.json" --wait 120s
kind load image-archive "$RUN_DIR/workload.tar" --name "$CLUSTER"
cd "$REPO_ROOT"
go build -o kubectl-cron .
kubectl --kubeconfig "$RUN_DIR/kubeconfig" --context "kind-$CLUSTER" version -o json > "$RUN_DIR/artifacts/versions.json"
docker image inspect "$WORKLOAD_IMAGE" --format '{{.Id}}' > "$RUN_DIR/artifacts/workload-image.txt"
export KUBECTL_CRON_E2E_BINARY="$REPO_ROOT/kubectl-cron"
export KUBECTL_CRON_E2E_KUBECONFIG="$RUN_DIR/kubeconfig"
export KUBECTL_CRON_E2E_CONTEXT="kind-$CLUSTER"
export KUBECTL_CRON_E2E_ARTIFACTS="$RUN_DIR/artifacts"
export KUBECTL_CRON_E2E_IMAGE="$WORKLOAD_IMAGE"
export KUBECTL_CRON_E2E_NODE="$CLUSTER-control-plane"
go test -race -tags=kind_e2e -count=1 -json ./integration/kind -timeout=20m | tee "$RUN_DIR/artifacts/results.jsonl"
