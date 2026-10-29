#!/usr/bin/env bash
# Run the DynamoDB pool directory's tests against DynamoDB Local, outside the
# default suite: start DynamoDB Local in Docker on a free loopback port, run
# the `local` test target of gdl-pools-dynamodb and the `dynamodb` test target
# of gdl-hub against it, and stop it. The gdl-hub target runs the stock hub
# binary, built with its `dynamodb` feature: the tables step, and a parent
# pool's hub that finds two child pools in two regions.
#
# DynamoDB Local runs with -sharedDb: without it, Local keeps a database for
# each region and access key, and hubs of two regions would see two
# directories. With it, every region sees one, as the regions of a global
# table do.
#
# The image is pinned by the digest of its multi-architecture index (3.3.2,
# linux/amd64 and linux/arm64). Without Docker, run the jar of the same
# version on Java 17, as the image does, and set GDL_DYNAMODB_LOCAL yourself:
#   java -jar DynamoDBLocal.jar -inMemory -sharedDb -port 8000
#   GDL_DYNAMODB_LOCAL=http://127.0.0.1:8000 cargo nextest run -p gdl-pools-dynamodb --features dynamodb-local --test local
#   GDL_DYNAMODB_LOCAL=http://127.0.0.1:8000 cargo nextest run -p gdl-hub --features dynamodb-local --test dynamodb
#
# TIMEOUT bounds each test run in seconds (default 300). Arguments after the
# script's name go to nextest, for both runs.
set -euo pipefail

IMAGE="amazon/dynamodb-local@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root}"

# Build first, so that the limit covers the tests and not the compiler. The
# default suite never builds the hub's `dynamodb` feature, so lint it here.
cargo nextest run -p gdl-pools-dynamodb --features dynamodb-local --test local --no-run
cargo nextest run -p gdl-hub --features dynamodb-local --test dynamodb --no-run
cargo clippy -p gdl-hub --features dynamodb-local --all-targets -- -D warnings

container="$(docker run --detach --rm --publish 127.0.0.1::8000 "${IMAGE}" -jar DynamoDBLocal.jar -inMemory -sharedDb -port 8000)"
trap 'docker stop "${container}" >/dev/null' EXIT
port="$(docker port "${container}" 8000/tcp | head -n 1 | sed 's/.*://')"
endpoint="http://127.0.0.1:${port}"

# DynamoDB Local answers any HTTP request once it listens.
for _ in $(seq 1 100); do
  if curl -s -o /dev/null "${endpoint}"; then
    break
  fi
  sleep 0.1
done

GDL_DYNAMODB_LOCAL="${endpoint}" timeout "${TIMEOUT:-300}" \
  cargo nextest run -p gdl-pools-dynamodb --features dynamodb-local --test local "$@"
GDL_DYNAMODB_LOCAL="${endpoint}" timeout "${TIMEOUT:-300}" \
  cargo nextest run -p gdl-hub --features dynamodb-local --test dynamodb "$@"
