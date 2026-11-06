#!/usr/bin/env bash
# Run the etcd pool directory's tests against a real etcd, outside the default suite: the `etcd` test target of
# gdl-pools-etcd (the directory conformance suite and the backend's own cases) and the `etcd` test target of gdl-hub
# (the stock hub binary, built with its `etcd` feature, finding a child pool through etcd). Each test starts its own
# etcd from the binary GDL_ETCD names, on OS-assigned ports, over a temporary data directory: a case that restarts or
# replaces its store disturbs no other. nextest runs each test in a process group of its own and stops the group when
# the test ends or times out, so no etcd outlives its test.
#
#   bash scripts/etcd.sh [nextest arguments ...]
#
# Needs:
#   - protoc: etcd-client compiles etcd's protos when it builds (`apt-get install protobuf-compiler`, or PROTOC naming
#     the binary).
#   - etcd v3.5.21, the version srt-slurm installs: with INSTALL_TOOLS=1 the script downloads the pinned release into
#     TOOLS_DIR (default: .etcd-tools at the repository's root, which git ignores) and checks its checksum. Or set
#     GDL_ETCD to an etcd binary yourself.
#
# TIMEOUT bounds each test run in seconds (default 300). Arguments after the script's name go to nextest, for both runs.
set -euo pipefail

ETCD_VERSION="3.5.21"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"
tools="${TOOLS_DIR:-${root}/.etcd-tools}"

if [[ -z "${PROTOC:-}" ]] && ! command -v protoc >/dev/null; then
  echo "etcd.sh: protoc is not on the PATH; etcd-client needs it to build (apt-get install protobuf-compiler, or set PROTOC)" >&2
  exit 1
fi

if [[ "${INSTALL_TOOLS:-0}" == "1" ]]; then
  case "$(uname -m)" in
    x86_64) arch=amd64 sha=adddda4b06718e68671ffabff2f8cee48488ba61ad82900e639d108f2148501c ;;
    aarch64 | arm64) arch=arm64 sha=95bf6918623a097c0385b96f139d90248614485e781ec9bee4768dbb6c79c53f ;;
    *) echo "etcd.sh: no pinned etcd for $(uname -m)" >&2; exit 1 ;;
  esac
  mkdir -p "${tools}/bin"
  archive="$(mktemp)"
  trap 'rm -f "${archive}"' EXIT
  curl -sSfL -o "${archive}" \
    "https://github.com/etcd-io/etcd/releases/download/v${ETCD_VERSION}/etcd-v${ETCD_VERSION}-linux-${arch}.tar.gz"
  echo "${sha}  ${archive}" | sha256sum -c --quiet -
  tar -xzf "${archive}" -C "${tools}/bin" --strip-components=1 "etcd-v${ETCD_VERSION}-linux-${arch}/etcd"
fi

etcd="${GDL_ETCD:-${tools}/bin/etcd}"
if [[ ! -x "${etcd}" ]]; then
  echo "etcd.sh: ${etcd} is missing; run with INSTALL_TOOLS=1, or set GDL_ETCD to an etcd binary" >&2
  exit 1
fi
version="$("${etcd}" --version | head -n 1)"
if [[ "${version}" != "etcd Version: ${ETCD_VERSION}" ]]; then
  echo "etcd.sh: ${etcd} says '${version}'; the tests pin etcd ${ETCD_VERSION}" >&2
  exit 1
fi

# Build first, so that the limit covers the tests and not the compiler. The default suite never builds the `etcd-local`
# features, so lint them here.
cargo nextest run -p gdl-pools-etcd --features etcd-local --test etcd --no-run
cargo nextest run -p gdl-hub --features etcd-local --test etcd --no-run
cargo clippy -p gdl-pools-etcd -p gdl-hub --features gdl-pools-etcd/etcd-local,gdl-hub/etcd-local --all-targets -- -D warnings

GDL_ETCD="${etcd}" timeout "${TIMEOUT:-300}" \
  cargo nextest run -p gdl-pools-etcd --features etcd-local --test etcd "$@"
GDL_ETCD="${etcd}" timeout "${TIMEOUT:-300}" \
  cargo nextest run -p gdl-hub --features etcd-local --test etcd "$@"
