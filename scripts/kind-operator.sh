#!/usr/bin/env bash
# Prove the gdl operator's scale-down on a kind cluster, outside the default suite: a GdlScaler removes the instance
# it chooses, under streamed load, with no request lost. It needs Docker with buildx, kubectl, jq, kind, and network
# access for the image builds (crates.io, GitHub, Debian's packages) and the curl image.
#
# It starts a `registry:2` on a free loopback port and a kind cluster that pulls from it, each with a name of its
# own. As the cluster's owner, it makes the fleet's namespace, builds the operator's image from this working tree,
# and applies `gdl-operator manifests`. It writes the api example's spec in two classes, with an engine that paces
# its chunks so that each response streams for about 2 s, and a fleet of one cluster that the operator runs
# (`operator = true`): a global pool (`front`, with an entry Service) and a model pool (`engine`). `cargo gdl deploy
# --local` builds every image from this working tree and applies a GdlPool for each pool. It then checks that:
#   - the operator made a GdlScaler per segment, wrote each one's replicas once at the quorum, under its own manager,
#     and the entry Service selects on gdl.dev/routable;
#   - scaled to 3 engines, under a load of streamed requests from a pod in the cluster, each on a new connection to
#     the entry Service's ClusterIP, and then to 2: the pod the scaler chose (status.retiring) is the pod deleted,
#     and the others stay; the victim's `requests_total` does not rise once its `instance_avoided` reads 1, so no
#     offer reached it after its parent's acknowledgement; its leave hook reads `pending`, then `done`, on /metrics
#     before its pod is gone; its removal ends inside its drain bound and the exit margin (status.last.outcome is
#     `removed`); and no request failed;
#   - the same for the frontend, scaled 3 to 2: its victim leaves the Service's EndpointSlices before its pod is
#     deleted, and no request failed, a new connection included;
#   - an HPA on the engine's GdlScaler, with no metrics server, scales it up to its minimum and down to its maximum
#     through /scale, and the scaler removes the victim it chose.
#
#   INSTALL_TOOLS=1 bash scripts/kind-operator.sh   # first downloads the pinned kind into TOOLS_DIR
#   bash scripts/kind-operator.sh
#
# TOOLS_DIR holds kind (default: .k8s-tools at the repository's root, which git ignores). TIMEOUT bounds the
# `cargo gdl deploy` and the operator's image build, in seconds (default 2400: each builds release binaries with fat
# LTO). On exit it deletes the cluster and the registry and removes the scratch directory. It never reads or changes
# ~/.kube/config: the cluster's kubeconfig is in the scratch directory.
set -euo pipefail

KIND_VERSION="0.33.0"
# The kind release's node image of Kubernetes 1.35, by the digest the release names.
NODE_IMAGE="kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
# registry 2.8.3, by the digest of its multi-architecture index.
REGISTRY_IMAGE="registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
# curl 8.11.1, by the digest of its multi-architecture index: the load's client.
CURL_IMAGE="curlimages/curl:8.11.1@sha256:c1fe1679c34d9784c1b0d1e5f62ac0a79fca01fb6377cdd33e90473c6f9f9a69"
TIMEOUT="${TIMEOUT:-2400}"
# Each segment's drain bound, in ms: the bound a removal must end inside, with the runtime's exit margin.
DRAIN_MS=20000
EXIT_MARGIN_MS=10000
# Each request's words, and the engine's pace between them: a response streams for about 2 s.
WORDS=20
PACE_MS=100

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"
target="${CARGO_TARGET_DIR:-${root}/target}"
tools="${TOOLS_DIR:-${root}/.k8s-tools}"

fail() {
  echo "kind-operator.sh: $*" >&2
  # Once the cluster runs, what explains a failure: the operator's log, the scalers, the pods, and the samples.
  if [[ -n "${context:-}" ]]; then
    {
      echo "--- the operator's log"
      kubectl --context "${context}" --namespace "${ns}" logs statefulset/gdl-operator --tail=80 || true
      echo "--- the scalers"
      kubectl --context "${context}" --namespace "${ns}" get gdlscalers --output json | jq '.items[] | {name: .metadata.name, spec, status}' || true
      echo "--- the pods"
      kubectl --context "${context}" --namespace "${ns}" get pods --output wide --show-labels || true
      for samples in "${scratch}"/*.samples; do
        [[ -f "${samples}" ]] && { echo "--- ${samples##*/}"; tail -n 40 "${samples}"; }
      done
    } >&2 2>&1
  fi
  exit 1
}
say() {
  echo "kind-operator.sh: $*"
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

cargo build -p gdl-gen --bin cargo-gdl -p gdl-operator --bin gdl-operator
gdl=("${target}/debug/cargo-gdl" gdl)
operator="${target}/debug/gdl-operator"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/gdl-kind-operator-XXXXXX")"
name="gdl-op-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
registry_container=""
cleanup() {
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
say "creating kind cluster ${name}"
timeout 600 "${kind}" create cluster --name "${name}" --kubeconfig "${scratch}/kubeconfig" \
  --config "${scratch}/kind.yaml" --image "${NODE_IMAGE}" --wait 300s
registry_container="$(docker run --detach --name "${name}-registry" --network kind --publish 127.0.0.1::5000 \
  "${REGISTRY_IMAGE}")"
port="$(docker port "${registry_container}" 5000/tcp | head -n 1 | sed 's/.*://')"
registry="localhost:${port}"
for _ in $(seq 1 100); do
  curl -sf -o /dev/null "http://${registry}/v2/" && break
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
ns=gdl-api
k() {
  timeout 60 kubectl --context "${context}" --namespace "${ns}" "$@"
}
# A route of a pool's hub's control listener, through the API server's service proxy.
control() {
  k get --raw "/api/v1/namespaces/${ns}/services/http:$1:control/proxy$2"
}
# A pod's /metrics on its system port, through the API server's pod proxy.
metrics() {
  k get --raw "/api/v1/namespaces/${ns}/pods/$1:9090/proxy/metrics"
}
# Wait, bounded by $1 seconds, until the command after it succeeds; name $2 on a timeout.
until_true() {
  local seconds="$1" what="$2"
  shift 2
  local deadline=$((SECONDS + seconds))
  until "$@"; do
    ((SECONDS < deadline)) || fail "${what} within ${seconds} s"
    sleep 0.5
  done
}

# The owner's setup: the namespace, and the operator, from this working tree.
timeout 60 kubectl --context "${context}" create namespace "${ns}" >/dev/null
tag="kind-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
image="${registry}/gdl/gdl-operator:${tag}"
say "building the operator's image ${image}"
timeout "${TIMEOUT}" docker build --quiet -f crates/gdl-operator/Dockerfile -t "${image}" . >/dev/null
timeout 300 docker push --quiet "${image}" >/dev/null
"${operator}" manifests --namespace "${ns}" --image "${image}" \
  | timeout 60 kubectl --context "${context}" apply --server-side -f - >/dev/null
for crd in gdlpools gdlscalers; do
  timeout 120 kubectl --context "${context}" wait --for=condition=Established "crd/${crd}.gdl.dev" --timeout=90s >/dev/null
done
k rollout status statefulset/gdl-operator --timeout=240s >/dev/null || fail "the operator did not start"
say "the operator runs"

# The api example in two classes, every image from this working tree. The frontend listens on every interface, and
# the engine paces its chunks.
sed -e "s#^path = \"api.gdl\"#path = \"${root}/examples/api/api.gdl\"#" \
  -e "s#^crates = \[\".\"\]#crates = [\"${root}/examples/api\"]#" \
  -e "s#^path = \"../../crates/gdl-runtime\"#path = \"${root}/crates/gdl-runtime\"#" \
  examples/api/gdl.toml >"${scratch}/gdl.toml"
grep -q "^path = \"${root}/crates/gdl-runtime\"" "${scratch}/gdl.toml" || fail "examples/api/gdl.toml names no gdl path"
cat >>"${scratch}/gdl.toml" <<EOF
[class.global]
segments = ["front"]
[class.model]
segments = ["engine"]
[deployment.segment.front]
ports = ["8080"]
[deployment.segment.front.config.frontend]
listen = "0.0.0.0:8080"
[deployment.segment.front.config.runtime]
drain_ms = ${DRAIN_MS}
[deployment.segment.engine.config.runtime]
drain_ms = ${DRAIN_MS}
[deployment.segment.engine.config.node.Engine]
pace_ms = "${PACE_MS}"
leave_hook = "flush"
EOF
cat >"${scratch}/fleet.toml" <<EOF
[fleet]
name = "api"

[cluster.kind]
context = "${context}"
namespace = "${ns}"
region = "local"
registry = "${registry}/gdl"
platform = "linux/${arch}"
operator = true

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
EOF

say "cargo gdl deploy --local"
timeout "${TIMEOUT}" "${gdl[@]}" deploy --spec "${scratch}/gdl.toml" --fleet "${scratch}/fleet.toml" \
  --target-dir "${target}" --local 2>"${scratch}/deploy.err" \
  || { grep -v '^#\|^ ' "${scratch}/deploy.err" | tail -n 40 >&2; fail "cargo gdl deploy failed"; }
grep '^cargo gdl: ' "${scratch}/deploy.err" || true

# The operator's scalers: one per segment, each written once at the quorum under its own manager, and the
# Deployment's replicas under gdl-scaler; gdl owns neither.
for scaler in global-front model-a-engine; do
  owners="$(k get gdlscaler "${scaler}" --show-managed-fields --output json \
    | jq -c '[.spec.replicas, ([.metadata.managedFields[] | select(.fieldsV1["f:spec"]["f:replicas"]) | .manager])]')"
  [[ "${owners}" == '[1,["gdl-bootstrap"]]' ]] || fail "GdlScaler ${scaler}'s replicas and their owners: ${owners}"
  owners="$(k get deployment "${scaler}" --show-managed-fields --output json \
    | jq -c '[.spec.replicas, ([.metadata.managedFields[] | select(.fieldsV1["f:spec"]["f:replicas"]) | .manager])]')"
  [[ "${owners}" == '[1,["gdl-scaler"]]' ]] || fail "Deployment ${scaler}'s replicas and their owners: ${owners}"
done
[[ "$(k get service global-front --output jsonpath='{.spec.selector.gdl\.dev/routable}')" == "true" ]] \
  || fail "the entry Service does not select on gdl.dev/routable"
say "the operator made each GdlScaler, wrote its replicas once, and the entry Service selects on gdl.dev/routable"

# The pods of `segment`, each as `name uid`, of those not being deleted.
live_pods() {
  k get pods --selector "gdl.dev/pool=$1,gdl.dev/segment=$2" --output json \
    | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | "\(.metadata.name) \(.metadata.uid)"' | sort
}
# Whether `pool`'s hub lists `count` instances of `segment`, each ready.
listed_ready() {
  local listed
  listed="$(control "$1" /v1/ext/gdl-pool/instances 2>/dev/null \
    | jq -r --arg s "$2" '[.instances[] | select(.segment == $s)] | "\(length) \([.[] | select(.phase == "ready")] | length)"')" \
    || return 1
  [[ "${listed}" == "$3 $3" ]]
}
# Whether `segment`'s `count` pods are ready, and its hub lists them ready.
scaled_to() {
  local pool="$1" segment="$2" count="$3" ready
  ready="$(k get pods --selector "gdl.dev/pool=${pool},gdl.dev/segment=${segment}" --output json | jq '[.items[]
    | select(.metadata.deletionTimestamp == null)
    | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length')"
  [[ "${ready}" == "${count}" && "$(live_pods "${pool}" "${segment}" | wc -l)" == "${count}" ]] \
    && listed_ready "${pool}" "${segment}" "${count}"
}

for segment in "model-a engine" "global front"; do
  read -r pool seg <<<"${segment}"
  k scale gdlscaler "${pool}-${seg}" --replicas=3 >/dev/null
  until_true 300 "${pool}'s ${seg} did not run 3 ready instances" scaled_to "${pool}" "${seg}" 3
done
say "3 engines and 3 frontends run, each listed ready by its hub"

# The load: workers in a pod, each sending one streamed chat request after another, each on a new connection to the
# entry Service's ClusterIP. A line per request: `ok <epoch s>`, or `fail <epoch s> <curl exit> <words> <body>`.
prompt="$(seq -s ' ' 1 "${WORDS}" | sed 's/[0-9]*/w&/g')"
cat >"${scratch}/load.sh" <<EOF
worker() {
  while [ ! -f /tmp/stop ]; do
    out="\$(curl -sS -f -m 60 -X POST -H 'content-type: application/json' \\
      -d '{"messages":["${prompt}"]}' http://global-front:8080/v1/chat/completions 2>&1)"
    code=\$?
    words="\$(printf '%s\n' "\${out}" | grep -c '"delta"')"
    if [ "\${code}" -eq 0 ] && [ "\${words}" -eq ${WORDS} ]; then
      echo "ok \$(date +%s)"
    else
      echo "fail \$(date +%s) \${code} \${words} \$(printf '%s' "\${out}" | tr '\n' ' ' | head -c 200)"
    fi
  done
}
for w in 1 2 3 4 5 6; do worker & done
wait
echo done
EOF
k create configmap load --from-file=load.sh="${scratch}/load.sh" >/dev/null
k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: load}
spec:
  restartPolicy: Never
  containers:
  - name: load
    image: ${CURL_IMAGE}
    command: [sh, /load/load.sh]
    volumeMounts: [{name: load, mountPath: /load}]
  volumes: [{name: load, configMap: {name: load}}]
EOF
until_true 180 "the load did not start" sh -c "kubectl --context '${context}' -n '${ns}' logs load 2>/dev/null | grep -q '^ok '"
say "the load runs"
# The load's counts so far: `<ok> <fail>`.
load_counts() {
  k logs load | awk '$1 == "ok" {ok++} $1 == "fail" {bad++} END {print ok + 0, bad + 0}'
}

# Scale `pool`'s `segment` from 3 to 2 and prove the removal: the victim the scaler chose is the pod deleted, the
# others stay, it ends inside the bound, and no request failed. While it runs, it samples the victim's metrics (an
# engine's `instance_avoided`, `requests_total`, and leave hook), and for a frontend the Service's EndpointSlices and the victim's
# deletion: each sample a line in `${scratch}/<segment>.samples`.
scale_down() {
  local pool="$1" segment="$2" scaler="$1-$2" before victim uid chosen_at started
  before="$(live_pods "${pool}" "${segment}")"
  local load_before
  load_before="$(load_counts)"
  started="$(date +%s%3N)"
  k scale gdlscaler "${scaler}" --replicas=2 >/dev/null
  until_true 60 "the scaler chose no victim" sh -c \
    "kubectl --context '${context}' -n '${ns}' get gdlscaler '${scaler}' -o json | jq -e '.status.retiring | length == 1' >/dev/null"
  victim="$(k get gdlscaler "${scaler}" --output jsonpath='{.status.retiring[0].pod}')"
  uid="$(k get gdlscaler "${scaler}" --output jsonpath='{.status.retiring[0].uid}')"
  grep -q "^${victim} ${uid}$" <<<"${before}" || fail "the victim ${victim} ${uid} is not one of the pods: ${before}"
  say "${scaler}: the scaler chose ${victim}"
  : >"${scratch}/${segment}.samples"
  local deadline=$((SECONDS + (DRAIN_MS + EXIT_MARGIN_MS) / 1000 + 60))
  while k get pod "${victim}" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fail "${victim} was not gone in time"
    local now text avoided requests hook deleting listed
    now="$(date +%s%3N)"
    deleting="$(k get pod "${victim}" --output jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true)"
    if [[ "${segment}" == "engine" ]]; then
      text="$(metrics "${victim}" 2>/dev/null || true)"
      # Every series carries the segment's identity labels.
      avoided="$(awk '$1 ~ /^dynamo_component_instance_avoided(\{|$)/ {print $2}' <<<"${text}")"
      requests="$(awk '$1 ~ /^dynamo_component_requests_total(\{|$)/ {sum += $2} END {print sum + 0}' <<<"${text}")"
      # The outcome that reads 1.
      hook="$(awk '$1 ~ /^dynamo_component_leave_outcome\{/ && $1 ~ /hook="flush"/ && $2 == 1 {
        sub(/.*outcome="/, "", $1); sub(/".*/, "", $1); print $1 }' <<<"${text}")"
      [[ -n "${text}" ]] && echo "${now} avoided=${avoided:-?} requests=${requests} hook=${hook:-?} deleting=${deleting:+yes}" \
        >>"${scratch}/${segment}.samples"
    else
      listed="$(k get endpointslices --selector "kubernetes.io/service-name=${scaler}" --output json \
        | jq --arg uid "${uid}" '[.items[].endpoints[]? | select(.targetRef.uid == $uid)] | length')"
      echo "${now} listed=${listed} deleting=${deleting:+yes}" >>"${scratch}/${segment}.samples"
    fi
    sleep 0.2
  done
  until_true 60 "the scaler did not record the removal" sh -c \
    "kubectl --context '${context}' -n '${ns}' get gdlscaler '${scaler}' -o json | jq -e --arg uid '${uid}' '.status.last.chosen == \$uid and (.status.retiring // [] | length) == 0' >/dev/null"
  local last
  last="$(k get gdlscaler "${scaler}" --output json | jq -c '.status.last')"
  say "${scaler}: status.last ${last}"
  [[ "$(jq -r '.deleted' <<<"${last}")" == "${uid}" ]] || fail "${scaler}: the pod deleted is not the one chosen: ${last}"
  [[ "$(jq -r '.outcome' <<<"${last}")" == "removed" ]] || fail "${scaler}: the removal did not end inside its bound: ${last}"
  local took=$(($(date +%s%3N) - started))
  ((took <= DRAIN_MS + EXIT_MARGIN_MS + 30000)) || fail "${scaler}: the scale-down took ${took} ms"
  local after
  after="$(live_pods "${pool}" "${segment}")"
  [[ "${after}" == "$(grep -v "^${victim} " <<<"${before}")" ]] || fail "${scaler}: the other pods changed:
before:
${before}
after:
${after}"
  until_true 120 "${pool}'s ${segment} did not settle at 2" scaled_to "${pool}" "${segment}" 2
  say "${scaler}: ${victim} was the pod deleted, the others stay, and it took ${took} ms"
  local load_after
  load_after="$(load_counts)"
  [[ "${load_after#* }" == "0" ]] || fail "requests failed: $(k logs load | grep '^fail' | head -n 5)"
  ((${load_after% *} > ${load_before% *})) || fail "no request was answered while ${segment} scaled down"
  say "${scaler}: no request failed (${load_before% *} answered before, ${load_after% *} after)"
}

scale_down model-a engine
# No offer reached the victim after its parent's acknowledgement: its requests_total did not rise once its
# instance_avoided read 1.
samples="${scratch}/engine.samples"
grep -q 'avoided=1' "${samples}" || fail "the victim never read instance_avoided 1:
$(cat "${samples}")"
awk '/avoided=1/ { split($3, r, "="); if (seen == "") seen = r[2]; else if (r[2] != seen) bad = 1 }
  END { exit bad }' "${samples}" || fail "an offer reached the victim after its acknowledgement:
$(cat "${samples}")"
say "model-a-engine: the victim's requests_total held at $(awk '/avoided=1/ {split($3, r, "="); print r[2]; exit}' "${samples}") from its acknowledgement on"
# Its leave hook started at the stop, and said how it ended while the pod still answered.
grep -q 'hook=pending' "${samples}" || fail "the victim's hook never read pending:
$(cat "${samples}")"
grep -q 'hook=done' "${samples}" || fail "the victim's hook never read done before its pod went:
$(cat "${samples}")"
say "model-a-engine: the victim's leave hook read done, $(grep -c 'hook=done' "${samples}") samples before its pod went"

scale_down global front
# The frontend's victim left the Service's EndpointSlices before its pod was deleted.
samples="${scratch}/front.samples"
awk '/deleting=yes/ && !/listed=0/ { bad = 1 } END { exit bad }' "${samples}" \
  || fail "the frontend's victim was listed by its Service while it was deleted:
$(cat "${samples}")"
grep -q 'listed=0 deleting=$' "${samples}" || fail "the samples never saw the victim unlisted before its delete:
$(cat "${samples}")"
say "global-front: the victim left its Service's endpoints before its pod was deleted"

# An HPA on the engine's GdlScaler, with no metrics server: it holds the scaler between its bounds through /scale.
hpa() {
  k apply -f - >/dev/null <<EOF
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: model-a-engine}
spec:
  scaleTargetRef: {apiVersion: gdl.dev/v1alpha1, kind: GdlScaler, name: model-a-engine}
  minReplicas: $1
  maxReplicas: $1
  behavior:
    scaleDown: {stabilizationWindowSeconds: 0}
EOF
}
hpa 3
until_true 300 "the HPA did not scale the engine to 3" scaled_to model-a engine 3
manager="$(k get gdlscaler model-a-engine --show-managed-fields --output json \
  | jq -r '[.metadata.managedFields[] | select(.fieldsV1["f:spec"]["f:replicas"]) | "\(.manager)/\(.subresource)"] | join(",")')"
[[ "${manager}" == "kube-controller-manager/scale" ]] || fail "the scaler's replicas are owned by ${manager}"
say "the HPA scaled the engine to 3 through /scale"
before="$(k get gdlscaler model-a-engine --output jsonpath='{.status.last.chosen}')"
hpa 2
until_true 120 "the HPA did not scale the scaler to 2" sh -c \
  "[ \"\$(kubectl --context '${context}' -n '${ns}' get gdlscaler model-a-engine -o jsonpath='{.spec.replicas}')\" = 2 ]"
until_true 120 "the scaler recorded no removal for the HPA" sh -c \
  "kubectl --context '${context}' -n '${ns}' get gdlscaler model-a-engine -o json | jq -e --arg before '${before}' '.status.last.chosen != \$before and .status.last.chosen == .status.last.deleted and .status.last.outcome == \"removed\"' >/dev/null"
until_true 120 "the engine did not settle at 2 under the HPA" scaled_to model-a engine 2
say "the HPA scaled the engine down to 2, and the scaler removed the pod it chose"

counts="$(load_counts)"
# Stop the load, then count: each worker finishes the request it is sending, and the pod prints `done` once all
# have, so a request that fails as the load stops is counted too. Only external commands, `k`, `load_counts`,
# `fail` and `say` here: crates/gdl-operator/tests/proof.rs runs this tail against a mock.
k exec load -- touch /tmp/stop
for _ in $(seq 1 240); do
  k logs load | grep -q '^done$' && break
  sleep 0.5
done
k logs load | grep -q '^done$' || fail "the load's workers did not stop within 120 s"
stopped="$(load_counts)"
[[ "${stopped#* }" == "0" ]] || fail "requests failed: $(k logs load | grep '^fail' | head -n 5)"
say "no request failed: ${stopped% *} answered"
say "passed"
