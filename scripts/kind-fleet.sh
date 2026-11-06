#!/usr/bin/env bash
# Deploy a fleet to a kind cluster with `cargo gdl deploy`, change it, and take it down with `cargo gdl down`,
# outside the default suite. It runs the real kubectl against a cluster of its own. It needs Docker with buildx,
# kubectl, jq, network access for the image builds (crates.io, GitHub, and Debian's packages) and the Prometheus image,
# and kind.
#
# It starts a `registry:2` on a free loopback port and a kind cluster that pulls from it, each with a name of its
# own, and writes the api example's spec in two classes and a fleet of one global pool and two model pools. It makes
# the fleet's namespace first, as a cluster's owner would, with a client-side `kubectl apply` and a label and an
# annotation of its own. It checks that:
#   - `deploy` and `down` leave the owner's label and annotations, the last-applied one among them, as they were:
#     `deploy` applies no Namespace that exists, and only labels it with the fleet's labels, which `down` removes;
#   - `deploy` builds and pushes the images, applies the objects, and waits until each pool's health answers 200;
#   - no container of the fleet restarted during the first deploy: a segment whose pool's hub is not up yet waits
#     for it rather than exiting;
#   - the global pool's health answers 200 through the API server's service proxy, and a request through
#     `kubectl port-forward` is answered;
#   - the cluster's one node is labelled and tainted as a node group of the fleet's own, and every pod of the fleet
#     runs there by its cluster's node selector and toleration, with its requests and limits;
#   - a Prometheus without the operator, configured with the scrape job of docs/src/guides/prometheus.yml, finds
#     every hub and segment of the fleet, each with its pool, class, region and segment, and scrapes each segment.
#     A hub serves its metrics only from a gdl that has its route: with HUB_METRICS_REV, a commit of the spec's
#     `[gdl] git` that does, the images are built from it and every hub must be scraped too. With LOCAL=1, every
#     segment and hub shows its pod's CPU limit as `process_cgroup_cpu_quota_cores`;
#   - a second `deploy`, with one model pool removed from the fleet, drains that pool, which withdraws: its parent's
#     hub no longer holds its record. Its objects are deleted, and the other pools' Deployments and pods are left
#     as they were;
#   - after `down`, no object with the fleet's label is left, and the namespace stays.
#
# With DYNAMODB=1 it then deploys the same pools under a DynamoDB directory in one DynamoDB Local in the cluster, a
# fleet that the relay refuses: the model pools are shared by their parents' class. It checks that the tables Job
# succeeds, that every pool is healthy, which needs the global pool to find and follow both model pools through the
# tables, and that a request is answered; then that `down` leaves no object with the fleet's label. The hub's image
# is then built with the hub's `dynamodb` feature, from a gdl that has it: with LOCAL=1, the working tree; else
# GDL_REV names a commit of the spec's `[gdl] git` that does. The case
# then removes a model pool, which the next deploy drains and deletes once its own hub says that its record is
# withdrawn, and takes the fleet down. kind runs one cluster, so the case covers only
# the hubs of the Local's own cluster, which reach it at its Service, not the pod-IP address of other clusters.
#
# With ETCD=1 it then runs the same case under an etcd directory, in one etcd on a claim in the cluster: the deploy
# waits for etcd before any pool, the claim is bound, and a new etcd pod, on the same claim, still holds the model
# pools' records. `down` also leaves no claim. The hub's image is then built with the hub's `etcd` feature, whose build
# installs protoc, with LOCAL=1 or GDL_REV as for DYNAMODB=1. Both cases run when both are set.
#
# With OPERATOR=1 (and LOCAL=1) it runs the same pools in the cluster the gdl operator runs (`operator = true`)
# instead, as the cluster's owner installs it from this working tree (`gdl-operator manifests`), under a load of
# streamed requests from a pod in the cluster, each on a new connection to the global pool's entry Service, and each
# streaming for about 2 s. A request is admitted once it gets its 200. It checks that:
#   - deploy applies one GdlPool per pool, each ready, and every object of a pool is the operator's;
#   - a second deploy, with model-b removed, deletes its GdlPool alone, in the background: its finalizer drains it,
#     it withdraws (its parent's hub no longer holds its record), and garbage collection deletes its objects; the
#     other pools' Deployments and pods are left as they were, and no request is refused or cut;
#   - a change of the global pool's hub, patched into its GdlPool without allowPoolRestart, is held, with
#     PoolRestartPending in its status and a warning event on the pool, and does not reach the hub's Deployment;
#   - `down`, under the load, stops model-a before global, each segment before its hub, and cuts no admitted request;
#     no object with the fleet's label is left.
#
#   INSTALL_TOOLS=1 bash scripts/kind-fleet.sh      # first downloads the pinned kind into TOOLS_DIR
#   bash scripts/kind-fleet.sh
#   LOCAL=1 DYNAMODB=1 bash scripts/kind-fleet.sh   # every image from this working tree, both cases
#   LOCAL=1 ETCD=1 bash scripts/kind-fleet.sh       # the relay case, then the etcd one
#   LOCAL=1 OPERATOR=1 bash scripts/kind-fleet.sh   # the pools in a cluster the gdl operator runs
#
# LOCAL=1 builds every image from this working tree, with `cargo gdl deploy --local`: the spec is
# examples/api/gdl.toml, which names gdl and the api crates by path, so no gdl is fetched, and a change that is not
# published is what runs.
#
# TOOLS_DIR holds kind (default: .k8s-tools at the repository's root, which git ignores). TIMEOUT bounds each
# `cargo gdl` run in seconds (default 1800: the first deploy builds three release binaries with fat LTO). The images
# are built for the host's platform, which the kind node runs. On exit it stops the port-forward, deletes the
# cluster and the registry, and removes the scratch directory; with KEEP_ON_FAIL=1 a failed run leaves all three, and
# names them, so that its pods can be read. It never reads or changes ~/.kube/config: the
# cluster's kubeconfig is in the scratch directory.
set -euo pipefail

KIND_VERSION="0.33.0"
# The kind release's node image of Kubernetes 1.35, by the digest the release names.
NODE_IMAGE="kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
# registry 2.8.3, by the digest of its multi-architecture index.
REGISTRY_IMAGE="registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
# Prometheus 3.7.3, by the digest of its multi-architecture index.
PROMETHEUS_IMAGE="prom/prometheus:v3.7.3@sha256:49214755b6153f90a597adcbff0252cc61069f8ab69ce8411285cd4a560e8038"
# curl 8.11.1, by the digest of its multi-architecture index: the operator case's load client.
CURL_IMAGE="curlimages/curl:8.11.1@sha256:c1fe1679c34d9784c1b0d1e5f62ac0a79fca01fb6377cdd33e90473c6f9f9a69"
TIMEOUT="${TIMEOUT:-1800}"
DYNAMODB="${DYNAMODB:-0}"
ETCD="${ETCD:-0}"
LOCAL="${LOCAL:-0}"
OPERATOR="${OPERATOR:-0}"
# The operator case's request: its words, and the engine's pace between them, so each streams for about 2 s.
WORDS=20
PACE_MS=100
if [[ "${OPERATOR}" == "1" && "${LOCAL}" != "1" ]]; then
  echo "kind-fleet.sh: OPERATOR=1 needs LOCAL=1: the operator runs pools of the hub this working tree builds" >&2
  exit 1
fi
if [[ "${DYNAMODB}" == "1" && "${LOCAL}" != "1" && -z "${GDL_REV:-}" ]]; then
  echo "kind-fleet.sh: DYNAMODB=1 needs LOCAL=1, or GDL_REV, a commit of the spec's [gdl] git with the hub's dynamodb feature" >&2
  exit 1
fi
if [[ "${ETCD}" == "1" && "${LOCAL}" != "1" && -z "${GDL_REV:-}" ]]; then
  echo "kind-fleet.sh: ETCD=1 needs LOCAL=1, or GDL_REV, a commit of the spec's [gdl] git with the hub's etcd feature" >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"
target="${CARGO_TARGET_DIR:-${root}/target}"
tools="${TOOLS_DIR:-${root}/.k8s-tools}"

fail() {
  echo "kind-fleet.sh: $*" >&2
  # In the operator case, what explains a failure: the operator's pod and log, and the pools.
  if [[ "${OPERATOR}" == "1" && -n "${context:-}" ]]; then
    {
      echo "--- the operator's pod"
      kubectl --context "${context}" --namespace gdl-api describe pod gdl-operator-0 | tail -n 30 || true
      echo "--- the operator's log"
      kubectl --context "${context}" --namespace gdl-api logs statefulset/gdl-operator --tail=80 || true
      echo "--- the pools"
      kubectl --context "${context}" --namespace gdl-api get gdlpools --output json \
        | jq -c '.items[] | {name: .metadata.name, status}' || true
    } >&2 2>&1
  fi
  exit 1
}

case "$(uname -m)" in
  x86_64) arch=amd64 sha=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d ;;
  aarch64 | arm64) arch=arm64 sha=20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2 ;;
  *) fail "no pinned kind for $(uname -m)" ;;
esac
if [[ "${INSTALL_TOOLS:-0}" == "1" ]]; then
  mkdir -p "${tools}/bin"
  download="$(mktemp -d)"
  trap 'rm -rf "${download}"' EXIT
  curl -sSfL -o "${download}/kind" \
    "https://github.com/kubernetes-sigs/kind/releases/download/v${KIND_VERSION}/kind-linux-${arch}"
  echo "${sha}  ${download}/kind" | sha256sum -c --quiet -
  install -m 755 "${download}/kind" "${tools}/bin/kind"
  rm -rf "${download}"
  trap - EXIT
fi
kind="${tools}/bin/kind"
[[ -x "${kind}" ]] || fail "${kind} is missing; run with INSTALL_TOOLS=1"
"${kind}" version | grep -q "v${KIND_VERSION} " || fail "${kind} is not kind ${KIND_VERSION}"
command -v kubectl >/dev/null || fail "kubectl is not on the PATH"
command -v jq >/dev/null || fail "jq is not on the PATH"

cargo build -p gdl-gen --bin cargo-gdl
gdl=("${target}/debug/cargo-gdl" gdl)

scratch="$(mktemp -d "${TMPDIR:-/tmp}/gdl-kind-fleet-XXXXXX")"
name="gdl-kind-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
registry_container=""
forward=""
cleanup() {
  local status=$?
  if [[ -n "${forward}" ]]; then
    kill "${forward}" 2>/dev/null || true
  fi
  if [[ "${status}" -ne 0 && "${KEEP_ON_FAIL:-0}" == "1" ]]; then
    echo "kind-fleet.sh: kept for inspection: KUBECONFIG=${scratch}/kubeconfig kubectl --context kind-${name} ...; remove with: ${kind} delete cluster --name ${name}; docker rm -f ${name}-registry; rm -rf ${scratch}" >&2
    return
  fi
  "${kind}" delete cluster --name "${name}" --kubeconfig "${scratch}/kubeconfig" >/dev/null 2>&1 || true
  if [[ -n "${registry_container}" ]]; then
    docker rm -f "${registry_container}" >/dev/null || true
  fi
  rm -rf "${scratch}"
}
trap cleanup EXIT

# containerd reads a registry's hosts from certs.d: the nodes pull `localhost:<port>` from the registry's container
# on kind's network, and the host pushes to it on loopback.
cat >"${scratch}/kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry]
    config_path = "/etc/containerd/certs.d"
EOF
echo "kind-fleet.sh: creating kind cluster ${name}"
timeout 600 "${kind}" create cluster --name "${name}" --kubeconfig "${scratch}/kubeconfig" \
  --config "${scratch}/kind.yaml" --image "${NODE_IMAGE}" --wait 300s
# Started on kind's network, which kind made: connecting a running container to a network gives its published port
# another host port.
registry_container="$(docker run --detach --name "${name}-registry" --network kind --publish 127.0.0.1::5000 \
  "${REGISTRY_IMAGE}")"
port="$(docker port "${registry_container}" 5000/tcp | head -n 1 | sed 's/.*://')"
registry="localhost:${port}"
for _ in $(seq 1 100); do
  if curl -sf -o /dev/null "http://${registry}/v2/"; then
    break
  fi
  sleep 0.1
done
curl -sf -o /dev/null "http://${registry}/v2/" || fail "the registry at ${registry} does not answer"
for node in $("${kind}" get nodes --name "${name}"); do
  docker exec "${node}" mkdir -p "/etc/containerd/certs.d/${registry}"
  printf '[host."http://%s:5000"]\n' "${name}-registry" \
    | docker exec -i "${node}" cp /dev/stdin "/etc/containerd/certs.d/${registry}/hosts.toml"
done
export KUBECONFIG="${scratch}/kubeconfig"
context="kind-${name}"
k() {
  timeout 60 kubectl --context "${context}" "$@"
}
# A route of a pool's hub's public listener, through the API server's service proxy.
hub() {
  k get --raw "/api/v1/namespaces/gdl-api/services/http:$1:public/proxy$2"
}

if k get namespace gdl-api >/dev/null 2>&1; then
  fail "the new cluster already has namespace gdl-api"
fi
# The fleet's namespace as its owner's setup makes it. A server-side apply of it by gdl would take over the fields
# this client-side apply set, and drop the label and the annotations.
k apply -f - >/dev/null <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: gdl-api
  labels: {example.com/setup: kind-fleet}
  annotations: {example.com/owner: kind-fleet}
EOF
owner() {
  k get namespace gdl-api --output json | jq -c '{
    setup: .metadata.labels["example.com/setup"],
    owner: .metadata.annotations["example.com/owner"],
    applied: (.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"] != null)
  }'
}
owned='{"setup":"kind-fleet","owner":"kind-fleet","applied":true}'
[[ "$(owner)" == "${owned}" ]] || fail "the owner's namespace is not as applied: $(owner)"

# The node as a node group of the fleet's own: labelled, and tainted so that only a pod that tolerates it is
# scheduled there. The fleet places every pod on it by its cluster's node selector and toleration.
node_name="$(k get nodes --output jsonpath='{.items[0].metadata.name}')"
k label node "${node_name}" gdl.dev/node-group=gdl-api >/dev/null
k taint node "${node_name}" dedicated=gdl-api:NoSchedule >/dev/null

# The api example in two classes. The frontend listens on every interface, so that a port-forward reaches it, and
# each segment drains within 20 s.
if [[ "${LOCAL}" == "1" ]]; then
  # The spec that names gdl and the api crates by path, each made absolute, since the spec is written elsewhere.
  sed -e "s#^path = \"api.gdl\"#path = \"${root}/examples/api/api.gdl\"#" \
    -e "s#^crates = \[\".\"\]#crates = [\"${root}/examples/api\"]#" \
    -e "s#^path = \"../../crates/gdl-runtime\"#path = \"${root}/crates/gdl-runtime\"#" \
    examples/api/gdl.toml >"${scratch}/gdl.toml"
  grep -q "^path = \"${root}/crates/gdl-runtime\"" "${scratch}/gdl.toml" || fail "examples/api/gdl.toml names no gdl path"
  [[ "$(grep -c "^crates = \[\"${root}/examples/api\"\]" "${scratch}/gdl.toml")" -eq 2 ]] \
    || fail "examples/api/gdl.toml names its crates otherwise"
  local_flag=(--local)
else
  sed "s#^path = \"api.gdl\"#path = \"${root}/examples/api/api.gdl\"#" examples/api/deploy.toml >"${scratch}/gdl.toml"
  if [[ -n "${HUB_METRICS_REV:-}" ]]; then
    sed -i "s#^rev = \".*\"#rev = \"${HUB_METRICS_REV}\"#" "${scratch}/gdl.toml"
    grep -q "^rev = \"${HUB_METRICS_REV}\"" "${scratch}/gdl.toml" || fail "the spec names no [gdl] rev to replace"
  fi
  local_flag=()
fi
grep -q "^path = \"${root}/examples/api/api.gdl\"" "${scratch}/gdl.toml" || fail "the spec names no api.gdl"
cat >>"${scratch}/gdl.toml" <<'EOF'
[class.global]
segments = ["front"]
[class.model]
segments = ["engine"]
[deployment.segment.front]
ports = ["8080"]
[deployment.segment.front.config.frontend]
listen = "0.0.0.0:8080"
[deployment.segment.front.config.runtime]
drain_ms = 20000
[deployment.segment.engine.config.runtime]
drain_ms = 20000
EOF
cat >"${scratch}/fleet.toml" <<EOF
[fleet]
name = "api"

[cluster.kind]
context = "${context}"
namespace = "gdl-api"
region = "local"
registry = "${registry}/gdl"
platform = "linux/${arch}"
node_selector = { "gdl.dev/node-group" = "gdl-api" }
tolerations = [{ key = "dedicated", operator = "Equal", value = "gdl-api", effect = "NoSchedule" }]

[cluster.kind.resources.default]
requests = { cpu = "50m", memory = "64Mi" }
limits = { cpu = "1", memory = "512Mi" }

[class.global]
ready = { front = 1 }
[class.model]
ready = { engine = 1 }

[pool.global]
class = "global"
cluster = "kind"

[pool.model-a]
class = "model"
cluster = "kind"
home = "global"

[pool.model-a.resources.segment.engine]
limits = { memory = "768Mi" }
EOF
cp "${scratch}/fleet.toml" "${scratch}/smaller.toml"
cat >>"${scratch}/fleet.toml" <<'EOF'

[pool.model-b]
class = "model"
cluster = "kind"
home = "global"
EOF

run() {
  local step="$1"
  shift
  echo "kind-fleet.sh: cargo gdl ${step}"
  # Only deploy builds images: down takes no --local.
  local extra=()
  if [[ "${step}" == "deploy" ]]; then
    extra=("${local_flag[@]}")
  fi
  timeout "${TIMEOUT}" "${gdl[@]}" "${step}" "$@" "${extra[@]}" 2>"${scratch}/${step}.err" \
    || { grep -v '^#\|^ ' "${scratch}/${step}.err" | tail -n 40 >&2; fail "cargo gdl ${step} failed"; }
  grep '^cargo gdl: ' "${scratch}/${step}.err" || true
}

if [[ "${OPERATOR}" == "1" ]]; then
  # The cluster's owner installs the gdl operator, from this working tree, in the fleet's namespace.
  cargo build -p gdl-operator --bin gdl-operator
  image="${registry}/gdl/gdl-operator:kind-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
  echo "kind-fleet.sh: building the operator's image ${image}"
  timeout "${TIMEOUT}" docker build --quiet -f crates/gdl-operator/Dockerfile -t "${image}" . >/dev/null \
    || fail "the operator's image did not build"
  timeout 300 docker push --quiet "${image}" >/dev/null
  # The owner places the operator on the fleet's node group too, which is tainted, before its pod is made: a
  # StatefulSet does not replace a pod that never became ready.
  "${target}/debug/gdl-operator" manifests --namespace gdl-api --image "${image}" | sed '/^---$/d' \
    | jq -c 'if .kind == "StatefulSet" then .spec.template.spec += {
        nodeSelector: {"gdl.dev/node-group": "gdl-api"},
        tolerations: [{key: "dedicated", operator: "Equal", value: "gdl-api", effect: "NoSchedule"}]} else . end' \
    | sed 's/^/---\n/' | k apply --server-side -f - >/dev/null
  for crd in gdlpools gdlscalers; do
    k wait --for=condition=Established "crd/${crd}.gdl.dev" --timeout=50s >/dev/null || fail "CRD ${crd} not established"
  done
  timeout 300 kubectl --context "${context}" --namespace gdl-api rollout status statefulset/gdl-operator \
    --timeout=240s >/dev/null || fail "the operator did not start"
  echo "kind-fleet.sh: the operator runs"

  # The fleet with the operator in its one cluster, and an engine that paces its words: each response streams for
  # about 2 s, so that requests are in flight while a pool stops.
  for file in fleet smaller; do
    sed 's/^platform = \(.*\)$/platform = \1\noperator = true/' "${scratch}/${file}.toml" >"${scratch}/operated-${file}.toml"
    grep -q '^operator = true$' "${scratch}/operated-${file}.toml" || fail "the fleet does not opt in"
  done
  cp "${scratch}/gdl.toml" "${scratch}/operated.gdl.toml"
  cat >>"${scratch}/operated.gdl.toml" <<EOF
[deployment.segment.engine.config.node.Engine]
pace_ms = "${PACE_MS}"
EOF
  run deploy --spec "${scratch}/operated.gdl.toml" --fleet "${scratch}/operated-fleet.toml" --target-dir "${target}"
  [[ "$(owner)" == "${owned}" ]] || fail "deploy changed the owner's label or annotations: $(owner)"
  pools="$(k --namespace gdl-api get gdlpools --output json \
    | jq -r '.items[] | "\(.metadata.name) \(.status.phase)"' | sort)"
  [[ "${pools}" == $'global Ready\nmodel-a Ready\nmodel-b Ready' ]] || fail "the GdlPools are not ready: ${pools}"
  # Every object of a pool is the operator's, by its GdlPool's controller reference.
  unowned="$(k --namespace gdl-api get deployments,services,configmaps,gdlscalers --selector gdl.dev/fleet=api \
    --output json | jq -r '.items[] | select(.metadata.labels["gdl.dev/pool"] != null)
      | select([.metadata.ownerReferences[]? | select(.kind == "GdlPool" and .controller)] | length != 1)
      | "\(.kind)/\(.metadata.name)"')"
  [[ -z "${unowned}" ]] || fail "objects of the pools that no GdlPool controls: ${unowned}"
  hub global /v1/ext/gdl-pool/health >/dev/null || fail "the global pool's health does not answer 200"
  echo "kind-fleet.sh: deploy applied a GdlPool per pool, and the operator made and owns every pool's objects"

  # The load: workers in a pod, each sending one streamed chat request after another, each on a new connection to
  # the global pool's entry Service. A request is admitted once it gets its 200: before that the frontend answers
  # 502, or the connection is refused. One line per request: `ok`, `refused <curl exit> <status>` for one never
  # admitted, or `cut <curl exit> <status> <words> <body>` for one admitted and not answered whole.
  prompt="$(seq -s ' ' 1 "${WORDS}" | sed 's/[0-9]*/w&/g')"
  cat >"${scratch}/load.sh" <<EOF
worker() {
  while [ ! -f /tmp/stop ]; do
    out="\$(curl -sS -m 60 -w '\n%{http_code}' -X POST -H 'content-type: application/json' \\
      -d '{"messages":["${prompt}"]}' http://global-front:8080/v1/chat/completions 2>/dev/null)"
    code=\$?
    status="\$(printf '%s\n' "\${out}" | tail -n 1)"
    words="\$(printf '%s\n' "\${out}" | grep -c '"delta"')"
    errors="\$(printf '%s\n' "\${out}" | grep -c '"error"')"
    if [ "\${status}" != 200 ]; then
      echo "refused \$(date +%s) \${code} \${status}"
      sleep 0.2
    elif [ "\${code}" -eq 0 ] && [ "\${words}" -eq ${WORDS} ] && [ "\${errors}" -eq 0 ]; then
      echo "ok \$(date +%s)"
    else
      echo "cut \$(date +%s) \${code} \${status} \${words} \$(printf '%s' "\${out}" | tr '\n' ' ' | head -c 200)"
    fi
  done
}
for w in 1 2 3 4 5 6; do worker & done
wait
echo done
EOF
  k --namespace gdl-api create configmap load --from-file=load.sh="${scratch}/load.sh" >/dev/null
  k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: load, namespace: gdl-api}
spec:
  restartPolicy: Never
  nodeSelector: {gdl.dev/node-group: gdl-api}
  tolerations: [{key: dedicated, operator: Equal, value: gdl-api, effect: NoSchedule}]
  containers:
  - name: load
    image: ${CURL_IMAGE}
    command: [sh, /load/load.sh]
    volumeMounts: [{name: load, mountPath: /load}]
  volumes: [{name: load, configMap: {name: load}}]
EOF
  # The load's counts so far: `<ok> <refused> <cut>`.
  load_counts() {
    k --namespace gdl-api logs load \
      | awk '$1 == "ok" {ok++} $1 == "refused" {no++} $1 == "cut" {cut++} END {print ok + 0, no + 0, cut + 0}'
  }
  for _ in $(seq 1 180); do
    [[ "$(load_counts | cut -d' ' -f1)" -gt 0 ]] && break
    sleep 1
  done
  [[ "$(load_counts | cut -d' ' -f1)" -gt 0 ]] || fail "the load did not start: $(k --namespace gdl-api logs load | tail -n 5)"
  echo "kind-fleet.sh: the load runs: $(load_counts)"

  # A pool removed from the fleet: deploy deletes its GdlPool alone, in the background, and the finalizer drains it
  # and waits for its record's withdrawal, then stops its engine and its hub, before garbage collection.
  operated_kept() {
    k --namespace gdl-api get deployments,pods --selector 'gdl.dev/pool in (global,model-a)' \
      --output jsonpath='{range .items[*]}{.kind}/{.metadata.name} {.metadata.uid} {.metadata.generation}{"\n"}{end}' \
      | grep -v '^Pod/load ' | sort
  }
  before="$(operated_kept)"
  counts_before="$(load_counts)"
  run deploy --spec "${scratch}/operated.gdl.toml" --fleet "${scratch}/operated-smaller.toml" --target-dir "${target}"
  grep -q 'stop: pool(s) model-b in cluster `kind` stopped and deleted' "${scratch}/deploy.err" \
    || fail "the second deploy did not delete model-b's GdlPool"
  k --namespace gdl-api get gdlpool model-b >/dev/null 2>&1 && fail "model-b's GdlPool is left"
  if hub global /v1/ext/gdl-pool/report/model/model-b >/dev/null 2>"${scratch}/held.err"; then
    fail "the global pool's hub still holds model-b: it did not withdraw"
  fi
  grep -q '(NotFound)' "${scratch}/held.err" || fail "reading model-b's record failed: $(cat "${scratch}/held.err")"
  for _ in $(seq 1 60); do
    left="$(k --namespace gdl-api get deployments,services,configmaps,gdlscalers,pods --selector gdl.dev/pool=model-b \
      --output name)"
    [[ -z "${left}" ]] && break
    sleep 1
  done
  [[ -z "${left}" ]] || fail "garbage collection left model-b's objects: ${left}"
  [[ "$(operated_kept)" == "${before}" ]] || fail "the second deploy changed the other pools:
before:
${before}
after:
$(operated_kept)"
  counts_after="$(load_counts)"
  read -r ok_before refused_before cut_before <<<"${counts_before}"
  read -r ok_after refused_after cut_after <<<"${counts_after}"
  ((cut_after == 0)) || fail "admitted requests were cut: $(k --namespace gdl-api logs load | grep '^cut' | head -n 5)"
  ((refused_after == refused_before)) \
    || fail "requests were refused while model-b stopped: $(k --namespace gdl-api logs load | grep '^refused' | head -n 5)"
  ((ok_after > ok_before)) || fail "no request was answered while model-b stopped"
  echo "kind-fleet.sh: model-b's finalizer drained it, it withdrew, and it was deleted; no request was refused or cut (${counts_before} -> ${counts_after}); global and model-a are as they were"

  # A change that replaces the global pool's hub, which would drain the pool, is held without allowPoolRestart.
  hub_generation="$(k --namespace gdl-api get deployment global --output jsonpath='{.metadata.generation}')"
  k --namespace gdl-api patch gdlpool global --type=json \
    -p '[{"op": "add", "path": "/spec/hub/env/-", "value": {"name": "GDL_KIND_PROBE", "value": "1"}}]' >/dev/null
  pool_generation="$(k --namespace gdl-api get gdlpool global --output jsonpath='{.metadata.generation}')"
  held=""
  for _ in $(seq 1 60); do
    held="$(k --namespace gdl-api get gdlpool global --output json | jq -r --argjson g "${pool_generation}" \
      '.status.conditions[]? | select(.type == "PoolRestartPending" and .status == "True" and .observedGeneration == $g)
        | .message')"
    [[ -n "${held}" ]] && break
    sleep 1
  done
  [[ -n "${held}" ]] || fail "the hub change was not held: $(k --namespace gdl-api get gdlpool global --output json | jq -c .status)"
  [[ "$(k --namespace gdl-api get gdlpool global --output jsonpath='{.status.held}')" == "${pool_generation}" ]] \
    || fail "the GdlPool does not name the held generation"
  [[ "$(k --namespace gdl-api get deployment global --output jsonpath='{.metadata.generation}')" == "${hub_generation}" ]] \
    || fail "the held change reached the hub's Deployment"
  # And it says so in an event on the pool, as a person reading `kubectl describe` sees it.
  event=""
  for _ in $(seq 1 30); do
    event="$(k --namespace gdl-api get events.events.k8s.io --output json | jq -r '.items[]
      | select(.regarding.kind == "GdlPool" and .regarding.name == "global" and .reason == "HubChange")
      | "\(.type): \(.note)"' | head -n 1)"
    [[ -n "${event}" ]] && break
    sleep 1
  done
  [[ "${event}" == Warning:* ]] || fail "no HubChange event on GdlPool global: ${event}"
  echo "kind-fleet.sh: a hub change without allowPoolRestart is held: ${held}"

  # down, under the load: model-a's GdlPool first, then global's, each in the background, each stopped by its
  # finalizer: the engine before the hub, and the child pool before its parent. A sampler records which pods run.
  (
    while :; do
      k --namespace gdl-api get pods --selector gdl.dev/fleet=api --output json 2>/dev/null \
        | jq -r --arg t "$(date +%s%3N)" '.items[] | select(.metadata.deletionTimestamp == null)
            | "\($t) \(.metadata.labels["gdl.dev/pool"]) \(.metadata.labels["gdl.dev/segment"])"' || true
      sleep 0.5
    done
  ) >"${scratch}/pods.samples" &
  sampler=$!
  counts_before="$(load_counts)"
  run down --spec "${scratch}/operated.gdl.toml" --fleet "${scratch}/operated-smaller.toml" --target-dir "${target}"
  kill "${sampler}" 2>/dev/null || true
  wait "${sampler}" 2>/dev/null || true
  stops="$(grep -o 'stop: pool(s) [a-z-]* in cluster' "${scratch}/down.err" | cut -d' ' -f3 | tr '\n' ' ')"
  [[ "${stops}" == "model-a global " ]] || fail "down did not stop model-a before global: ${stops}"
  k --namespace gdl-api get gdlpools --output name | grep -q . && fail "GdlPools are left after down"
  # The last time each pool's segment and hub were seen running.
  last() {
    awk -v pool="$1" -v segment="$2" '$2 == pool && $3 == segment {t = $1} END {print t + 0}' "${scratch}/pods.samples"
  }
  (($(last model-a engine) <= $(last model-a hub))) || fail "model-a's hub stopped before its engine"
  (($(last global front) <= $(last global hub))) || fail "global's hub stopped before its frontend"
  (($(last model-a hub) <= $(last global front))) || fail "global stopped before model-a"
  k --namespace gdl-api exec load -- touch /tmp/stop
  for _ in $(seq 1 120); do
    k --namespace gdl-api logs load | grep -q '^done$' && break
    sleep 1
  done
  k --namespace gdl-api logs load | grep -q '^done$' || fail "the load's workers did not stop within 120 s"
  read -r ok_after refused_after cut_after <<<"$(load_counts)"
  ((cut_after == 0)) || fail "admitted requests were cut: $(k --namespace gdl-api logs load | grep '^cut' | head -n 5)"
  echo "kind-fleet.sh: down stopped model-a, then global, each engine before its hub, and cut no admitted request ($(load_counts): answered, refused once no pool could admit them, cut)"
  k --namespace gdl-api delete pod load --wait=false >/dev/null
  k --namespace gdl-api delete configmap load >/dev/null
  labelled="$(k get serviceaccounts,configmaps,persistentvolumeclaims,services,deployments,replicasets,gdlscalers \
    --all-namespaces --selector gdl.dev/fleet=api --output name)"
  [[ -z "${labelled}" ]] || fail "objects with the fleet's label are left: ${labelled}"
  [[ "$(owner)" == "${owned}" ]] || fail "down changed the owner's label or annotations: $(owner)"
  echo "kind-fleet.sh: no object with the fleet's label is left, and the namespace stays as its owner made it"
  echo "kind-fleet.sh: passed"
  exit 0
fi

run deploy --spec "${scratch}/gdl.toml" --fleet "${scratch}/fleet.toml" --target-dir "${target}"
[[ "$(owner)" == "${owned}" ]] || fail "deploy changed the owner's label or annotations: $(owner)"
fleet_labels="$(k get namespace gdl-api --output json \
  | jq -c '.metadata.labels | {fleet: .["gdl.dev/fleet"], cluster: .["gdl.dev/cluster"]}')"
[[ "${fleet_labels}" == '{"fleet":"api","cluster":"kind"}' ]] || fail "deploy did not label the namespace: ${fleet_labels}"
hub global /v1/ext/gdl-pool/health >/dev/null || fail "the global pool's health does not answer 200"
for pool in model-a model-b; do
  hub global "/v1/ext/gdl-pool/report/model/${pool}" >/dev/null || fail "the global pool's hub does not hold ${pool}"
done
# Every pod of the fleet runs on the tainted node by the cluster's selector and toleration, and asks for its
# requests and limits: the default, and model-a's engine its own memory limit.
placed="$(k --namespace gdl-api get pods --selector gdl.dev/fleet=api --output json | jq -r '.items[]
  | [.metadata.labels["gdl.dev/pool"], .metadata.labels["gdl.dev/segment"],
     .spec.nodeSelector["gdl.dev/node-group"],
     ([.spec.tolerations[] | select(.key == "dedicated" and .effect == "NoSchedule")] | length),
     (.spec.containers[0].resources | [.requests.cpu, .requests.memory, .limits.cpu, .limits.memory] | join("/"))]
  | map(tostring) | join(" ")' | sort)"
expected_placed="global front gdl-api 1 50m/64Mi/1/512Mi
global hub gdl-api 1 50m/64Mi/1/512Mi
model-a engine gdl-api 1 50m/64Mi/1/768Mi
model-a hub gdl-api 1 50m/64Mi/1/512Mi
model-b engine gdl-api 1 50m/64Mi/1/512Mi
model-b hub gdl-api 1 50m/64Mi/1/512Mi"
[[ "${placed}" == "${expected_placed}" ]] || fail "the fleet's pods are not placed as the fleet says:
${placed}
expected:
${expected_placed}"
echo "kind-fleet.sh: every pod runs on the tainted node group, with its requests and limits"
# The hubs and segments start together: a segment that exited before its hub answered would show a restart.
restarts="$(k --namespace gdl-api get pods --selector gdl.dev/fleet=api --output json | jq -r '.items[]
  | [.metadata.name, ([.status.containerStatuses[].restartCount] | add)] | map(tostring) | join(" ")' | sort)"
echo "kind-fleet.sh: restarts after the first deploy:"
echo "${restarts}"
if awk '$2 != 0 { found = 1 } END { exit !found }' <<<"${restarts}"; then
  fail "a container of the fleet restarted during the first deploy"
fi
echo "kind-fleet.sh: every pool is healthy, and the global pool holds both model pools"

# A request through a port-forward of the global pool's entry Service.
k --namespace gdl-api port-forward service/global-front :8080 >"${scratch}/forward.out" 2>&1 &
forward=$!
for _ in $(seq 1 100); do
  grep -q '^Forwarding from 127.0.0.1:' "${scratch}/forward.out" && break
  sleep 0.1
done
local_port="$(sed -n 's/^Forwarding from 127.0.0.1:\([0-9]*\) .*/\1/p' "${scratch}/forward.out" | head -n 1)"
[[ -n "${local_port}" ]] || fail "the port-forward did not start: $(cat "${scratch}/forward.out")"
answer="$(curl -sS -m 30 -X POST -H 'content-type: application/json' -d '{"messages":["chat words"]}' \
  "http://127.0.0.1:${local_port}/v1/chat/completions")"
[[ "${answer}" == $'{"delta":"chat"}\n{"delta":"words"}' ]] || fail "the request through the port-forward got: ${answer}"
kill "${forward}"
forward=""
echo "kind-fleet.sh: a request through the port-forward is answered"

# A Prometheus without the operator, in a namespace of its own, with the chapter's scrape job: it must find each
# of the six pods (three pools, each a hub and one segment) by their annotations, with their labels.
k create namespace gdl-metrics >/dev/null
k --namespace gdl-metrics create configmap prometheus \
  --from-file=prometheus.yml="${root}/docs/src/guides/prometheus.yml" >/dev/null
k apply --filename - >/dev/null <<YAML
apiVersion: v1
kind: ServiceAccount
metadata: {name: prometheus, namespace: gdl-metrics}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: gdl-kind-prometheus}
rules:
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: gdl-kind-prometheus}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: gdl-kind-prometheus}
subjects:
- {kind: ServiceAccount, name: prometheus, namespace: gdl-metrics}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: prometheus, namespace: gdl-metrics}
spec:
  selector: {matchLabels: {app: prometheus}}
  template:
    metadata: {labels: {app: prometheus}}
    spec:
      serviceAccountName: prometheus
      nodeSelector: {gdl.dev/node-group: gdl-api}
      tolerations: [{key: dedicated, operator: Equal, value: gdl-api, effect: NoSchedule}]
      containers:
      - name: prometheus
        image: ${PROMETHEUS_IMAGE}
        args: [--config.file=/etc/prometheus/prometheus.yml]
        ports: [{name: web, containerPort: 9090}]
        volumeMounts: [{name: config, mountPath: /etc/prometheus}]
      volumes: [{name: config, configMap: {name: prometheus}}]
---
apiVersion: v1
kind: Service
metadata: {name: prometheus, namespace: gdl-metrics}
spec:
  selector: {app: prometheus}
  ports: [{name: web, port: 9090, targetPort: 9090}]
YAML
timeout 300 kubectl --context "${context}" --namespace gdl-metrics rollout status deployment/prometheus \
  --timeout=240s >/dev/null || fail "Prometheus did not start"
# Each active target of job gdl as one line: pool, class, region, segment, and health.
targets() {
  k get --raw "/api/v1/namespaces/gdl-metrics/services/http:prometheus:9090/proxy/api/v1/targets?state=active" \
    | jq -r '.data.activeTargets[] | select(.labels.job == "gdl")
        | [.labels.in_pool, .labels.in_class, .labels.in_region, .labels.segment, .health] | join(" ")' \
    | sort
}
expected="global global local front
global global local hub
model-a model local engine
model-a model local hub
model-b model local engine
model-b model local hub"
# Whether `found` holds every target with its labels, each segment scraped, and with HUB_METRICS_REV each hub too.
complete() {
  [[ "$(cut -d' ' -f1-4 <<<"$1")" == "${expected}" ]] || return 1
  ! grep -v ' hub ' <<<"$1" | grep -qv ' up$' || return 1
  [[ -z "${HUB_METRICS_REV:-}" ]] || ! grep ' hub ' <<<"$1" | grep -qv ' up$'
}
# A scrape every 5 s: 120 s is ample once Prometheus runs.
found=""
for _ in $(seq 1 120); do
  found="$(targets || true)"
  complete "${found}" && break
  sleep 1
done
complete "${found}" || fail "Prometheus found these targets (pool, class, region, segment, health):
${found}
expected each of:
${expected}
with each segment up$([[ -n "${HUB_METRICS_REV:-}" ]] && echo ", and each hub")"
echo "kind-fleet.sh: Prometheus finds every hub and segment with its labels, and scrapes each segment$([[ -n "${HUB_METRICS_REV:-}" ]] && echo " and hub"):"
echo "${found}"

# With LOCAL=1 every image has the process's cgroup figures: each pod's limit of 1 CPU must show as the quota of its
# segment and hub. An image from the spec's pinned gdl may predate them.
if [[ "${LOCAL}" == "1" ]]; then
  quota() {
    k get --raw "/api/v1/namespaces/gdl-metrics/services/http:prometheus:9090/proxy/api/v1/query?query=process_cgroup_cpu_quota_cores" \
      | jq -r '.data.result[] | select(.metric.job == "gdl")
          | [.metric.in_pool, .metric.in_class, .metric.in_region, .metric.segment, .value[1]] | join(" ")' \
      | sort
  }
  expected_quota="$(sed 's/$/ 1/' <<<"${expected}")"
  quotas=""
  for _ in $(seq 1 60); do
    quotas="$(quota || true)"
    [[ "${quotas}" == "${expected_quota}" ]] && break
    sleep 1
  done
  [[ "${quotas}" == "${expected_quota}" ]] || fail "process_cgroup_cpu_quota_cores (pool, class, region, segment, cores):
${quotas}
expected:
${expected_quota}"
  echo "kind-fleet.sh: every segment and hub shows its CPU limit as process_cgroup_cpu_quota_cores 1"
fi

# What a second deploy must leave alone: the other pools' Deployments (uid and generation) and pods (uid).
kept() {
  k --namespace gdl-api get deployments,pods --selector 'gdl.dev/pool in (global,model-a)' \
    --output jsonpath='{range .items[*]}{.kind}/{.metadata.name} {.metadata.uid} {.metadata.generation}{"\n"}{end}' \
    | sort
}
before="$(kept)"
[[ "$(grep -c '^Pod/' <<<"${before}")" -eq 4 ]] || fail "expected 4 pods of global and model-a, got:
${before}"

run deploy --spec "${scratch}/gdl.toml" --fleet "${scratch}/smaller.toml" --target-dir "${target}"
grep -q "pool \`model-b\` has withdrawn" "${scratch}/deploy.err" || fail "the second deploy did not drain model-b"
if hub global /v1/ext/gdl-pool/report/model/model-b >/dev/null 2>"${scratch}/held.err"; then
  fail "the global pool's hub still holds model-b: it did not withdraw"
fi
grep -q '(NotFound)' "${scratch}/held.err" || fail "reading model-b's record failed: $(cat "${scratch}/held.err")"
left="$(k --namespace gdl-api get deployments,services,configmaps,pods --selector gdl.dev/pool=model-b --output name)"
[[ -z "${left}" ]] || fail "model-b's objects are left: ${left}"
[[ "$(kept)" == "${before}" ]] || fail "the second deploy changed the other pools:
before:
${before}
after:
$(kept)"
hub global /v1/ext/gdl-pool/health >/dev/null || fail "the global pool's health does not answer 200 after model-b left"
echo "kind-fleet.sh: model-b drained, withdrew, and was deleted; global and model-a are as they were"

run down --spec "${scratch}/gdl.toml" --fleet "${scratch}/smaller.toml" --target-dir "${target}"
labelled="$(k get namespaces,serviceaccounts,configmaps,persistentvolumeclaims,services,deployments,replicasets,pods \
  --all-namespaces --selector gdl.dev/fleet=api --output name)"
[[ -z "${labelled}" ]] || fail "objects with the fleet's label are left: ${labelled}"
k get namespace gdl-api >/dev/null || fail "down deleted the namespace"
[[ "$(owner)" == "${owned}" ]] || fail "down changed the owner's label or annotations: $(owner)"
echo "kind-fleet.sh: no object with the fleet's label is left, and the namespace stays as its owner made it"

# The same pools under a directory that every hub reads, which the relay refuses: the model pools are shared by
# their parents' class. $1 names it in messages, $2 is the fleet's `[directory]` table, and $3 a function that checks
# the directory's own objects after the first deploy.
directory_case() {
  local name="$1" directory="$2" check="$3" spec="${scratch}/$1.gdl.toml"
  if [[ "${LOCAL}" == "1" ]]; then
    cp "${scratch}/gdl.toml" "${spec}"
  else
    sed "s#^rev = \".*\"#rev = \"${GDL_REV}\"#" "${scratch}/gdl.toml" >"${spec}"
    grep -q "^rev = \"${GDL_REV}\"" "${spec}" || fail "the spec names no [gdl] rev to replace"
  fi
  sed 's/^home = "global"$/parents = "global"/' "${scratch}/fleet.toml" >"${scratch}/$1.toml"
  printf '%s' "${directory}" >>"${scratch}/$1.toml"
  sed 's/^home = "global"$/parents = "global"/' "${scratch}/smaller.toml" >"${scratch}/$1-smaller.toml"
  printf '%s' "${directory}" >>"${scratch}/$1-smaller.toml"
  run deploy --spec "${spec}" --fleet "${scratch}/$1.toml" --target-dir "${target}"
  "${check}"
  hub global /v1/ext/gdl-pool/health >/dev/null \
    || fail "the global pool's health does not answer 200: it does not follow both model pools through ${name}"
  k --namespace gdl-api port-forward service/global-front :8080 >"${scratch}/forward.out" 2>&1 &
  forward=$!
  for _ in $(seq 1 100); do
    grep -q '^Forwarding from 127.0.0.1:' "${scratch}/forward.out" && break
    sleep 0.1
  done
  local_port="$(sed -n 's/^Forwarding from 127.0.0.1:\([0-9]*\) .*/\1/p' "${scratch}/forward.out" | head -n 1)"
  [[ -n "${local_port}" ]] || fail "the port-forward did not start: $(cat "${scratch}/forward.out")"
  answer="$(curl -sS -m 30 -X POST -H 'content-type: application/json' -d '{"messages":["chat words"]}' \
    "http://127.0.0.1:${local_port}/v1/chat/completions")"
  [[ "${answer}" == $'{"delta":"chat"}\n{"delta":"words"}' ]] || fail "the request under ${name} got: ${answer}"
  kill "${forward}"
  forward=""
  echo "kind-fleet.sh: under ${name} every pool is healthy, and a request is answered"
  run deploy --spec "${spec}" --fleet "${scratch}/$1-smaller.toml" --target-dir "${target}"
  grep -q "pool \`model-b\` has withdrawn" "${scratch}/deploy.err" || fail "the deploy did not drain model-b under ${name}"
  left="$(k --namespace gdl-api get deployments,services,configmaps,pods --selector gdl.dev/pool=model-b --output name)"
  [[ -z "${left}" ]] || fail "model-b's objects are left under ${name}: ${left}"
  hub global /v1/ext/gdl-pool/health >/dev/null \
    || fail "the global pool's health does not answer 200 under ${name} after model-b left"
  echo "kind-fleet.sh: under ${name}, model-b drained, said that it withdrew, and was deleted"
  run down --spec "${spec}" --fleet "${scratch}/$1-smaller.toml" --target-dir "${target}"
  labelled="$(k get serviceaccounts,configmaps,persistentvolumeclaims,services,deployments,replicasets,pods,jobs \
    --all-namespaces --selector gdl.dev/fleet=api --output name)"
  [[ -z "${labelled}" ]] || fail "objects with the fleet's label are left under ${name}: ${labelled}"
  echo "kind-fleet.sh: under ${name}, no object with the fleet's label is left"
}

dynamodb_objects() {
  grep -q "tables: the DynamoDB directory's tables are ready" "${scratch}/deploy.err" \
    || fail "the deploy ran no tables Job"
  [[ "$(k --namespace gdl-api get job dynamodb-tables --output jsonpath='{.status.succeeded}')" == "1" ]] \
    || fail "the tables Job did not succeed"
}

# etcd keeps the directory on its claim: a new etcd pod, on the same claim, still holds every pool's record, so the
# global pool's health answers 200 again without a deploy. The hubs reach etcd at its Service, which follows the pod.
etcd_objects() {
  grep -q "base: the etcd runs at" "${scratch}/deploy.err" || fail "the deploy did not wait for etcd"
  [[ "$(k --namespace gdl-api get persistentvolumeclaim etcd-data --output jsonpath='{.status.phase}')" == "Bound" ]] \
    || fail "etcd's claim is not bound"
  hub global /v1/ext/gdl-pool/health >/dev/null \
    || fail "the global pool's health does not answer 200 before etcd restarts"
  k --namespace gdl-api delete pod --selector gdl.dev/directory=etcd --wait=true >/dev/null
  k --namespace gdl-api rollout status deployment/etcd --timeout=120s >/dev/null || fail "etcd did not come back"
  k --namespace gdl-api exec deployment/etcd -- etcdctl get --prefix --keys-only /gdl/pools/model/ \
    >"${scratch}/etcd-keys.out" || fail "etcdctl did not answer after the restart"
  grep -q '^/gdl/pools/model/model-a$' "${scratch}/etcd-keys.out" \
    || fail "the new etcd pod does not hold model-a's record: $(cat "${scratch}/etcd-keys.out")"
  echo "kind-fleet.sh: a new etcd pod keeps the directory on its claim"
}

if [[ "${DYNAMODB}" == "1" ]]; then
  directory_case DynamoDB '
[directory]
backend = "dynamodb"
records_table = "gdl-pools"
lists_table = "gdl-pool-lists"
local = true
cluster = "kind"
' dynamodb_objects
fi
if [[ "${ETCD}" == "1" ]]; then
  directory_case etcd '
[directory]
backend = "etcd"
cluster = "kind"
' etcd_objects
fi
echo "kind-fleet.sh: passed"
