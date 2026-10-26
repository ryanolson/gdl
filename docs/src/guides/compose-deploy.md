# Deploy with Docker Compose

This guide turns a graph spec into a Docker Compose deployment: one hub and one container per segment. A spec with classes deploys by cell instead ([Deploy a fleet of cells](fleet.md)). The composer's Deploy tab writes the same bundle, and can apply it on its own host ([Compose in the browser](composer.md)).

## Before you start

A bundle builds on a Docker host from the bundle alone, so Cargo must be able to fetch every crate:

- The spec is a graph spec. Its message and node crates come from Git or crates.io. A local `path` crate is refused, with the names of the crates to import again.
- `[gdl]` names `gdl-runtime` by `git` and `rev`, or by `version`. The hub is installed from the same source. The revision must carry the runtime that the generator emits for, so move the pin when the generator and the runtime change together.
- The source is pinned to a `Frontend`. A source without one reads requests from stdin, which a container does not have.

## 1. Set the images

Image settings go in the spec's `[deployment]` table. [Spec and fleet reference](../appendix/spec-reference.md) lists every key.

```toml
[deployment]
base_image = "gcr.io/distroless/cc-debian12:nonroot"   # the default
build_image = "rust:1.93.1-slim-bookworm"               # the default
hub_image = "gcr.io/distroless/cc-debian12:nonroot"    # defaults to base_image

[deployment.segment.front]
ports = ["8082:8082"]

[deployment.segment.front.config.frontend]
listen = "0.0.0.0:8082"

[deployment.segment.engine]
base_image = "nvcr.io/nvidia/cuda:12.8.1-runtime-ubuntu24.04"
```

A runtime image must run the binary built in `build_image` and supply its libraries. The generator installs no packages into it. The default runtime image holds glibc and the C runtime, and no shell, and runs as a user that is not root (uid 65532). The default build image is the toolchain that `rust-toolchain.toml` pins, on Debian 12, the release that the runtime image is built from, so a binary finds the glibc that it was linked against. A runtime image that runs as root, such as a CUDA image, works too: nothing in the image or the bundle depends on the user. A `config` table adds to the segment's settings ([Segment settings](../operations/configuration.md)).

## 2. Write the bundle

```bash
cargo gdl compose --spec examples/api/deploy.toml --out /tmp/deployment
```

The output directory must not exist. The bundle holds:

- `deployment.json`: the manifest and placement the hub admits.
- `hub/Dockerfile`: installs `gdl-hub` from the `[gdl]` source.
- `<segment>/`: the segment's generated crate, its `Dockerfile`, and its `config.toml`.
- `compose.yaml` and a `README.md`.

## 3. Start it

Copy the directory to the Docker host, then:

```bash
cd /path/to/deployment
docker compose up --build
```

The hub starts first. Every segment starts once the hub's health check passes, and `depends_on` nothing else. No segment waits on another's health: segments whose paths return wait for each other, and an order over them deadlocks. [Segment lifecycle](../concepts/lifecycle.md) describes the barrier.

Each segment's health check runs `segment --healthcheck /etc/gdl/config` every 5 s, with a start period of 1800 s to cover loading models. The hub's health check asks its own `/v1/health` every 2 s.

## 4. Scale and stop

- **Add instances** with `docker compose up --scale <segment>=N`. Each new instance joins once it is ready. A segment that publishes a fixed, nonzero host port cannot scale: two containers cannot bind one port. A port Docker chooses (`CONTAINER`, `0:CONTAINER`, or `IP::CONTAINER`) scales.
- **Stop one instance** with `docker stop <container>`. It retires: its peers stop routing to it, and it leaves once it has drained.
- **Stop everything** with `docker compose down`. Every segment drains, within its drain bound.

Each segment's `stop_grace_period` is its drain bound plus 30 s. Docker otherwise kills a container 10 s after it stops it. The bound is read from the segment's settings when the bundle is written, so a `GDL_RUNTIME__DRAIN_MS` set at run time does not change the grace period.

A hub restart ends every segment's lease. Start the segments again after the hub ([Discovery: rosters and the hub](../concepts/hub.md)).
