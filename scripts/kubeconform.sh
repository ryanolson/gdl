#!/usr/bin/env bash
# Validate the Kubernetes objects that `cargo gdl k8s` writes with kubeconform, against the schemas of a pinned
# Kubernetes version, in strict mode, offline. Outside the default suite: the objects' contents are pinned by
# `crates/gdl-gen/tests/deployment/k8s.rs`, and this checks that the Kubernetes API accepts each one.
#
#   bash scripts/kubeconform.sh [dir ...]
#
# Without a directory it validates the pinned objects in crates/gdl-gen/tests/fixtures/k8s/out and
# crates/gdl-gen/tests/fixtures/k8s/dynamodb/out. With
# INSTALL_TOOLS=1 it first downloads the pinned kubeconform and the schemas of the kinds that `cargo gdl k8s`
# writes into TOOLS_DIR (default: .k8s-tools at the repository's root, which git ignores).
set -euo pipefail

KUBECONFORM_VERSION="0.8.0"
KUBERNETES_VERSION="1.35.0"
# The commit of github.com/yannh/kubernetes-json-schema the schemas are read from.
SCHEMAS_COMMIT="47163ead3738a996c3e30be9691bb37b8071d9d3"
# Each kind that `cargo gdl k8s` writes, as the schema repository names its file.
KINDS=(namespace-v1 serviceaccount-v1 configmap-v1 persistentvolumeclaim-v1 service-v1 deployment-apps-v1 job-batch-v1)

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tools="${TOOLS_DIR:-${root}/.k8s-tools}"
schemas="${tools}/schemas/v${KUBERNETES_VERSION}-standalone-strict"

if [[ "${INSTALL_TOOLS:-0}" == "1" ]]; then
  case "$(uname -m)" in
    x86_64) arch=amd64 sha=9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883 ;;
    aarch64 | arm64) arch=arm64 sha=1f53fc8e81258197a35e8603054162a5af1de8c5af13746c71ab680d9534ed87 ;;
    *) echo "kubeconform.sh: no pinned kubeconform for $(uname -m)" >&2; exit 1 ;;
  esac
  mkdir -p "${tools}/bin" "${schemas}"
  archive="$(mktemp)"
  trap 'rm -f "${archive}"' EXIT
  curl -sSfL -o "${archive}" \
    "https://github.com/yannh/kubeconform/releases/download/v${KUBECONFORM_VERSION}/kubeconform-linux-${arch}.tar.gz"
  echo "${sha}  ${archive}" | sha256sum -c --quiet -
  tar -xzf "${archive}" -C "${tools}/bin" kubeconform
  for kind in "${KINDS[@]}"; do
    curl -sSfL -o "${schemas}/${kind}.json" \
      "https://raw.githubusercontent.com/yannh/kubernetes-json-schema/${SCHEMAS_COMMIT}/v${KUBERNETES_VERSION}-standalone-strict/${kind}.json"
  done
fi

kubeconform="${tools}/bin/kubeconform"
if [[ ! -x "${kubeconform}" ]]; then
  echo "kubeconform.sh: ${kubeconform} is missing; run with INSTALL_TOOLS=1" >&2
  exit 1
fi
"${kubeconform}" -v

if [[ $# -eq 0 ]]; then
  set -- "${root}/crates/gdl-gen/tests/fixtures/k8s/out" "${root}/crates/gdl-gen/tests/fixtures/k8s/dynamodb/out"
fi
# A kind with no schema here fails: -ignore-missing-schemas is not given.
"${kubeconform}" -strict -summary -kubernetes-version "${KUBERNETES_VERSION}" \
  -schema-location "${tools}/schemas/{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}/{{.ResourceKind}}{{.KindSuffix}}.json" \
  "$@"
