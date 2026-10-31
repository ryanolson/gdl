#!/usr/bin/env bash
# Deploy a fleet to a kind cluster with `cargo gdl deploy`, change it, and take it down with `cargo gdl down`,
# outside the default suite. It runs the real kubectl against a cluster of its own. It needs Docker with buildx,
# kubectl, jq, network access for the image builds (crates.io, GitHub, and Debian's packages) and the Prometheus image,
# and kind.
#
# It starts a `registry:2` on a free loopback port and a kind cluster that pulls from it, each with a name of its
# own, and writes the api example's spec in two classes and a fleet of one global pool and two model pools. From a
# cluster with no fleet namespace, it checks that:
#   - `deploy` builds and pushes the images, applies the objects, and waits until each pool's health answers 200;
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
# GDL_REV names a commit of the spec's `[gdl] git` that does (examples/api/deploy.toml pins an older one). The case
# then removes a model pool, which the next deploy drains and deletes once its own hub says that its record is
# withdrawn, and takes the fleet down. kind runs one cluster, so the case covers only
# the hubs of the Local's own cluster, which reach it at its Service, not the pod-IP address of other clusters.
#
#   INSTALL_TOOLS=1 bash scripts/kind-fleet.sh      # first downloads the pinned kind into TOOLS_DIR
#   bash scripts/kind-fleet.sh
#   LOCAL=1 DYNAMODB=1 bash scripts/kind-fleet.sh   # every image from this working tree, both cases
#
# LOCAL=1 builds every image from this working tree, with `cargo gdl deploy --local`: the spec is
# examples/api/gdl.toml, which names gdl and the api crates by path, so no gdl is fetched, and a change that is not
# published is what runs.
#
# TOOLS_DIR holds kind (default: .k8s-tools at the repository's root, which git ignores). TIMEOUT bounds each
# `cargo gdl` run in seconds (default 1800: the first deploy builds three release binaries with fat LTO). The images
# are built for the host's platform, which the kind node runs. On exit it stops the port-forward, deletes the
# cluster and the registry, and removes the scratch directory. It never reads or changes ~/.kube/config: the
# cluster's kubeconfig is in the scratch directory.
set -euo pipefail

KIND_VERSION="0.33.0"
# The kind release's node image of Kubernetes 1.35, by the digest the release names.
NODE_IMAGE="kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
# registry 2.8.3, by the digest of its multi-architecture index.
REGISTRY_IMAGE="registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
# Prometheus 3.7.3, by the digest of its multi-architecture index.
PROMETHEUS_IMAGE="prom/prometheus:v3.7.3@sha256:49214755b6153f90a597adcbff0252cc61069f8ab69ce8411285cd4a560e8038"
TIMEOUT="${TIMEOUT:-1800}"
DYNAMODB="${DYNAMODB:-0}"
LOCAL="${LOCAL:-0}"
if [[ "${DYNAMODB}" == "1" && "${LOCAL}" != "1" && -z "${GDL_REV:-}" ]]; then
  echo "kind-fleet.sh: DYNAMODB=1 needs LOCAL=1, or GDL_REV, a commit of the spec's [gdl] git with the hub's dynamodb feature" >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"
target="${CARGO_TARGET_DIR:-${root}/target}"
tools="${TOOLS_DIR:-${root}/.k8s-tools}"

fail() {
  echo "kind-fleet.sh: $*" >&2
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
  if [[ -n "${forward}" ]]; then
    kill "${forward}" 2>/dev/null || true
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

run deploy --spec "${scratch}/gdl.toml" --fleet "${scratch}/fleet.toml" --target-dir "${target}"
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
echo "kind-fleet.sh: no object with the fleet's label is left, and the namespace stays"

if [[ "${DYNAMODB}" == "1" ]]; then
  if [[ "${LOCAL}" == "1" ]]; then
    cp "${scratch}/gdl.toml" "${scratch}/dynamodb.gdl.toml"
  else
    sed "s#^rev = \".*\"#rev = \"${GDL_REV}\"#" "${scratch}/gdl.toml" >"${scratch}/dynamodb.gdl.toml"
    grep -q "^rev = \"${GDL_REV}\"" "${scratch}/dynamodb.gdl.toml" || fail "the spec names no [gdl] rev to replace"
  fi
  directory='
[directory]
backend = "dynamodb"
records_table = "gdl-pools"
lists_table = "gdl-pool-lists"
local = true
cluster = "kind"
'
  sed 's/^home = "global"$/parents = "global"/' "${scratch}/fleet.toml" >"${scratch}/dynamodb.toml"
  printf '%s' "${directory}" >>"${scratch}/dynamodb.toml"
  sed 's/^home = "global"$/parents = "global"/' "${scratch}/smaller.toml" >"${scratch}/dynamodb-smaller.toml"
  printf '%s' "${directory}" >>"${scratch}/dynamodb-smaller.toml"
  run deploy --spec "${scratch}/dynamodb.gdl.toml" --fleet "${scratch}/dynamodb.toml" --target-dir "${target}"
  grep -q "tables: the DynamoDB directory's tables are ready" "${scratch}/deploy.err" \
    || fail "the deploy ran no tables Job"
  [[ "$(k --namespace gdl-api get job dynamodb-tables --output jsonpath='{.status.succeeded}')" == "1" ]] \
    || fail "the tables Job did not succeed"
  hub global /v1/ext/gdl-pool/health >/dev/null \
    || fail "the global pool's health does not answer 200: it does not follow both model pools through DynamoDB"
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
  [[ "${answer}" == $'{"delta":"chat"}\n{"delta":"words"}' ]] || fail "the request under DynamoDB got: ${answer}"
  kill "${forward}"
  forward=""
  echo "kind-fleet.sh: under DynamoDB every pool is healthy, and a request is answered"
  run deploy --spec "${scratch}/dynamodb.gdl.toml" --fleet "${scratch}/dynamodb-smaller.toml" --target-dir "${target}"
  grep -q "pool \`model-b\` has withdrawn" "${scratch}/deploy.err" || fail "the deploy did not drain model-b under DynamoDB"
  left="$(k --namespace gdl-api get deployments,services,configmaps,pods --selector gdl.dev/pool=model-b --output name)"
  [[ -z "${left}" ]] || fail "model-b's objects are left under DynamoDB: ${left}"
  hub global /v1/ext/gdl-pool/health >/dev/null \
    || fail "the global pool's health does not answer 200 under DynamoDB after model-b left"
  echo "kind-fleet.sh: under DynamoDB, model-b drained, said that it withdrew, and was deleted"
  run down --spec "${scratch}/dynamodb.gdl.toml" --fleet "${scratch}/dynamodb-smaller.toml" --target-dir "${target}"
  labelled="$(k get serviceaccounts,configmaps,services,deployments,replicasets,pods,jobs \
    --all-namespaces --selector gdl.dev/fleet=api --output name)"
  [[ -z "${labelled}" ]] || fail "objects with the fleet's label are left under DynamoDB: ${labelled}"
  echo "kind-fleet.sh: under DynamoDB, no object with the fleet's label is left"
fi
echo "kind-fleet.sh: passed"
