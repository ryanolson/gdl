#!/usr/bin/env bash
# Build a fleet's images with `cargo gdl images`, push them to a local registry, and check them, outside the
# default suite. It needs Docker with buildx, uv, and network access for the image builds (crates.io, GitHub, and
# Debian's packages).
#
# It starts a `registry:2` on a free loopback port and writes the api example's spec in two classes, and a fleet
# of one cluster that pulls from that registry for PLATFORM (default linux/amd64; on an aarch64 host the build
# cross-compiles). Then it checks that:
#   - the first run pushes the hub's image and each segment's, and prints each digest;
#   - each digest is the registry's own (`Docker-Content-Digest`), and each image is of PLATFORM;
#   - each segment's binary is built for PLATFORM and links mimalloc, and, on the host's own platform, starts;
#   - `cargo gdl k8s --images` names exactly the pushed images, by digest;
#   - a second run with no change builds and pushes nothing, and leaves the registry's files as they were.
#
#   bash scripts/images-local.sh
#
# TIMEOUT bounds each `cargo gdl images` run in seconds (default 1800: the first run builds three release
# binaries with fat LTO). On exit it removes the scratch directory, and the images it built or pulled from the
# local image store, and stops the registry.
set -euo pipefail

# registry 2.8.3, by the digest of its multi-architecture index.
REGISTRY_IMAGE="registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373"
PLATFORM="${PLATFORM:-linux/amd64}"
TIMEOUT="${TIMEOUT:-1800}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"
target="${CARGO_TARGET_DIR:-${root}/target}"

fail() {
  echo "images-local.sh: $*" >&2
  exit 1
}

command -v uv >/dev/null || fail "uv is not on the PATH; install it from https://docs.astral.sh/uv/getting-started/installation/"

case "${PLATFORM}" in
  linux/amd64) machine="3e00" arch=amd64 ;;   # ELF e_machine, little-endian: EM_X86_64
  linux/arm64) machine="b700" arch=arm64 ;;   # EM_AARCH64
  *) fail "PLATFORM is linux/amd64 or linux/arm64, not ${PLATFORM}" ;;
esac
case "$(uname -m)" in
  x86_64) host=linux/amd64 ;;
  aarch64 | arm64) host=linux/arm64 ;;
  *) host="" ;;
esac

cargo build -p gdl-gen --bin cargo-gdl
gdl=("${target}/debug/cargo-gdl" gdl)

scratch="$(mktemp -d "${TMPDIR:-/tmp}/gdl-images-local-XXXXXX")"
container=""
pulled=()
cleanup() {
  if [[ ${#pulled[@]} -gt 0 ]]; then
    docker rmi "${pulled[@]}" >/dev/null || true
  fi
  if [[ -n "${container}" ]]; then
    docker stop "${container}" >/dev/null || true
  fi
  rm -rf "${scratch}"
}
trap cleanup EXIT

container="$(docker run --detach --rm --publish 127.0.0.1::5000 "${REGISTRY_IMAGE}")"
port="$(docker port "${container}" 5000/tcp | head -n 1 | sed 's/.*://')"
registry="127.0.0.1:${port}"
for _ in $(seq 1 100); do
  if curl -sf -o /dev/null "http://${registry}/v2/"; then
    break
  fi
  sleep 0.1
done
curl -sf -o /dev/null "http://${registry}/v2/" || fail "the registry at ${registry} does not answer"

# The api example in two classes: a global class that runs `front`, and a model class that runs `engine`.
sed "s#^path = \"api.gdl\"#path = \"${root}/examples/api/api.gdl\"#" examples/api/deploy.toml >"${scratch}/gdl.toml"
grep -q "^path = \"${root}/examples/api/api.gdl\"" "${scratch}/gdl.toml" || fail "examples/api/deploy.toml names no api.gdl"
cat >>"${scratch}/gdl.toml" <<'EOF'
[class.global]
segments = ["front"]
[class.model]
segments = ["engine"]
EOF
cat >"${scratch}/fleet.toml" <<EOF
[fleet]
name = "api"

[cluster.local]
context = "unused"
namespace = "gdl-api"
region = "local"
registry = "${registry}/gdl"
platform = "${PLATFORM}"

[class.global]
ready = { front = 1 }
[class.model]
ready = { engine = 1 }

[pool.global]
class = "global"
cluster = "local"

[pool.model]
class = "model"
cluster = "local"
home = "global"
EOF

# The registry's files, by path and checksum: a push of anything changes them.
stored() {
  docker exec "${container}" sh -c 'cd /var/lib/registry && find . -type f -exec sha256sum {} + | sort'
}

images() {
  local run="$1"
  timeout "${TIMEOUT}" "${gdl[@]}" images --spec "${scratch}/gdl.toml" --fleet "${scratch}/fleet.toml" \
    --target-dir "${target}" --push --out "${scratch}/${run}.json" >"${scratch}/${run}.out" 2>"${scratch}/${run}.err" \
    || { tail -n 40 "${scratch}/${run}.err" >&2; fail "${run}: cargo gdl images failed"; }
}

echo "images-local.sh: first run, for ${PLATFORM}, to ${registry}"
images first
mapfile -t pushed <"${scratch}/first.out"
[[ ${#pushed[@]} -eq 3 ]] || fail "first run: expected 3 images (hub, front, engine), got: ${pushed[*]}"
for image in "${pushed[@]}"; do
  reference="${image%@*}"
  digest="${image#*@}"
  repository="${reference#"${registry}"/}"
  tag="${repository##*:}"
  repository="${repository%:*}"
  grep -qF "${reference}: pushed" "${scratch}/first.err" || fail "first run did not push ${reference}"
  served="$(curl -sfI \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "http://${registry}/v2/${repository}/manifests/${tag}" | tr -d '\r' | sed -n 's/^[Dd]ocker-[Cc]ontent-[Dd]igest: //p')"
  [[ "${served}" == "${digest}" ]] || fail "${reference}: printed ${digest}, the registry serves ${served}"
  architecture="$(docker buildx imagetools inspect "${image}" --format '{{json .Image}}' \
    | uv run -q --no-project --python '>=3.8' python -I -c 'import json, sys; print(json.load(sys.stdin)["architecture"])')"
  [[ "${architecture}" == "${arch}" ]] || fail "${reference}: architecture ${architecture}, not ${arch}"
  # The binary itself: its ELF machine, and, for a segment, mimalloc. A container that is created and never
  # started runs nothing, whatever its platform.
  docker pull --quiet --platform "${PLATFORM}" "${image}" >/dev/null
  pulled+=("${reference}")
  holder="$(docker create --platform "${PLATFORM}" "${image}")"
  docker cp "${holder}:/usr/local/bin/segment" "${scratch}/binary" >/dev/null
  docker rm "${holder}" >/dev/null
  found="$(od -An -t x1 -j 18 -N 2 "${scratch}/binary" | tr -d ' \n')"
  [[ "${found}" == "${machine}" ]] || fail "${reference}: ELF machine ${found}, not ${machine}"
  if [[ "${repository}" != */api-hub ]]; then
    grep -aq "mimalloc" "${scratch}/binary" || fail "${reference}: the binary does not link mimalloc"
  fi
  # On the host's own platform the binary can run: with no arguments it starts, finds the runtime image's C
  # library, and refuses to go on. A binary of another platform would need emulation to run.
  if [[ "${PLATFORM}" == "${host}" ]]; then
    started="$(docker run --rm "${image}" 2>&1 || true)"
    if grep -qiE "exec format|error while loading shared libraries|GLIBC_|no such file" <<<"${started}"; then
      fail "${reference}: the binary does not start: ${started}"
    fi
    if [[ "${repository}" != */api-hub ]] && ! grep -q "usage:" <<<"${started}"; then
      fail "${reference}: the binary printed no usage: ${started}"
    fi
  fi
  rm -f "${scratch}/binary"
  echo "images-local.sh: ${image}: ${architecture}, the registry's digest"
done

# The objects name exactly the pushed images, by digest.
"${gdl[@]}" k8s --spec "${scratch}/gdl.toml" --fleet "${scratch}/fleet.toml" --target-dir "${target}" \
  --images "${scratch}/first.json" --out "${scratch}/k8s"
named="$(grep -rhoE '"image": "[^"]+"' "${scratch}/k8s" | sed 's/"image": "//; s/"$//' | sort -u)"
expected="$(printf '%s\n' "${pushed[@]}" | sort -u)"
[[ "${named}" == "${expected}" ]] || fail "the objects name:
${named}
and the first run pushed:
${expected}"
echo "images-local.sh: the objects name the pushed digests"

before="$(stored)"
echo "images-local.sh: second run, with no change"
images second
cmp -s "${scratch}/first.out" "${scratch}/second.out" || fail "the second run printed other digests"
cmp -s "${scratch}/first.json" "${scratch}/second.json" || fail "the second run recorded other digests"
if grep -q ": pushed$" "${scratch}/second.err"; then
  fail "the second run pushed: $(grep ': pushed$' "${scratch}/second.err")"
fi
[[ "$(grep -c ': already in its registry$' "${scratch}/second.err")" -eq 3 ]] \
  || fail "the second run did not find each image in the registry"
grep -q "buildx build\|^#[0-9]" "${scratch}/second.err" && fail "the second run built an image"
[[ "$(stored)" == "${before}" ]] || fail "the second run changed the registry's files"
echo "images-local.sh: the second run built nothing and pushed nothing"
echo "images-local.sh: passed"
