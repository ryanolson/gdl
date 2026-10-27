# Deploy with Docker Compose

This guide turns a graph spec into a Docker Compose deployment: one hub and one container per segment. A spec with classes deploys by cell instead ([Deploy a fleet of cells](fleet.md)). The composer's Deploy tab writes the same bundle, and can apply it on its own host ([Compose in the browser](composer.md)).

## Before you start

A bundle builds on a Docker host from the bundle alone, so Cargo must be able to fetch every crate:

- The spec is a graph spec. Its message and node crates come from Git or crates.io. A local `path` crate is refused, with the names of the crates to import again. To test crates that are not pushed, use `--local` ([Local sources](#local-sources)).
- `[gdl]` names `gdl-runtime` by `git` and `rev`, or by `version`. The hub is installed from the same source. The revision must carry the runtime that the generator emits for, so move the pin when the generator and the runtime change together.
- The source is pinned to a `Frontend`, or its binary reads JSON requests on stdin. A binary reads stdin when its graph has one source, the source's request is one value, and both its request and its item cross segments ([Code generation](../concepts/generation.md)). A source with neither is refused, since its segment serves nothing that a container can reach.

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

### Secrets

A segment can mount a secret: a file on the Docker host that Compose mounts read-only at `/run/secrets/<name>` in each of the segment's containers. Declare it once, name it in each segment that reads it, and point the node's setting at the mount:

```toml
[deployment.secrets.fingerprint]
file = "/etc/gdl/secrets/fingerprint"   # on the Docker host

[deployment.segment.front]
secrets = ["fingerprint"]

[deployment.segment.front.config.node.Fingerprint]
secret_file = "/run/secrets/fingerprint"
```

The bundle names the file and never holds it: the generator does not open the file, so it need not exist when the bundle is written, and its contents appear in no generated file and no output. `compose.yaml` gets a top-level `secrets` entry for each secret a service mounts, and each service of the segment a `secrets` list. The bundle's README lists them.

- **Names.** A secret's name is letters, digits, `_`, `.`, and `-`, and not `.` or `..`, so that it names one file in `/run/secrets`. A segment may name only a declared secret, and each once.
- **Paths.** `file` must be an absolute path. Compose reads a relative one from the bundle's directory, which is the build context of a bundle's images: every `docker compose build` would copy the file into a build stage. For the same reason a file in the build context is refused: in the bundle, or with `--local`, anywhere in the wider context. A `$` in the path is written `$$`, which Compose reads as `$`.
- **Who can read it.** Compose mounts the file as it is on the host, with its owner and mode. It ignores a service's `uid`, `gid`, and `mode` for a secret outside Swarm. The default runtime image runs as uid 65532, so that user must be able to read the file. For a file only you can read:

  ```bash
  setfacl -m u:65532:r /etc/gdl/secrets/fingerprint
  ```

  The file must exist before `docker compose up`, or Compose refuses to start the service.

A runtime image must run the binary built in `build_image` and supply its libraries. The generator installs no packages into it. The default runtime image holds glibc and the C runtime, and no shell, and runs as a user that is not root (uid 65532). The default build image is the toolchain that `rust-toolchain.toml` pins, on Debian 12, the release that the runtime image is built from, so a binary finds the glibc that it was linked against. A runtime image that runs as root, such as a CUDA image, works too: nothing in the image or the bundle depends on the user. A `config` table adds to the segment's settings ([Segment settings](../operations/configuration.md)).

## 2. Write the bundle

```bash
cargo gdl compose --spec examples/api/deploy.toml --out /tmp/deployment
```

The output directory must not exist, and Compose must be able to run a project from it ([limitations](../operations/limitations.md#compose-runs-a-bundle-only-from-some-directory-names)). The bundle holds:

- `deployment.json`: the manifest and placement the hub admits.
- `hub/Dockerfile`: installs `gdl-hub` from the `[gdl]` source.
- `<segment>/`: the segment's generated crate, its `Dockerfile`, and its `config.toml`.
- `compose.yaml` and a `README.md`.

### Local sources

To test code that is not pushed, write the bundle with `--local`. Its images build the crates that the spec names by path, and gdl and its hub from `[gdl] path`, as they are on this machine:

```bash
cargo gdl compose --local --spec examples/http/gdl.toml --out target/http-bundle
cd target/http-bundle
docker compose up --build
```

The build context is the nearest directory that holds the bundle, every crate that the generated manifests name by path, and the Cargo workspace of each. Write the bundle inside that directory, for example under the repository's `target/`. If the bundle and the crates share no directory but `/`, the command fails. The bundle names the context only by a relative path. `compose.yaml` says, in its field `x-gdl-local-sources`, that the bundle builds only on this machine, and the command says so on stderr, with the context's path. Each Dockerfile has an ignore file beside it, which leaves build output and Git history out of the context. The command writes each segment's `Cargo.lock` on this machine, from the lock of gdl's workspace, so this machine must reach every source that the segments name. Each image builds with `cargo build --locked`, and fetches no Git source again. A node crate can take gdl from Git or crates.io, as a crate in another repository does. Each segment then patches that source with the gdl crates of `[gdl] path`, so it builds one copy of gdl. The bundle's files escape the characters that Compose or Docker read as syntax in its path, such as `$`, `*`, `?`, and `[`. A path that holds a line break is refused ([limitations](../operations/limitations.md#a-bundle-of-local-sources-builds-only-on-the-machine-that-wrote-it)). A spec that names gdl by Git or version builds without `--local`: with `--local` and without `--fleet`, the command refuses it before it fetches any crate. With `--fleet`, the fleet check comes first, and it fetches the spec's crates. The composer does not offer this mode. With `--fleet`, each cell's hub and segments build the same way ([Deploy a fleet of cells](fleet.md#run-local-sources)).

## 3. Start it

Copy the directory to the Docker host, then:

```bash
cd /path/to/deployment
docker compose up --build
```

The hub starts first. Every segment starts once the hub's health check passes, and `depends_on` nothing else. No segment waits on another's health: segments whose paths return wait for each other, and an order over them deadlocks. [Segment lifecycle](../concepts/lifecycle.md) describes the barrier.

Each segment's health check runs `segment --healthcheck /etc/gdl/config` every 5 s, with a start period of 1800 s to cover loading models. The hub's health check asks its own `/v1/health` every 2 s.

### A source that reads stdin

When no frontend is pinned, the source's service keeps its stdin open (`stdin_open: true`), and the other segments start without it (`[min_replicas] <source> = 0` in their settings). Start the other services, then run the source with the requests as its input:

```bash
docker compose up --build --detach --scale front=0
docker compose run --rm -T front < requests.jsonl
```

Each run is one more instance of the source. It joins through the hub, writes each answer's items as JSON lines, and leaves at the end of its input. So repeated runs need no `docker compose down` between them. The bundle's `README.md` names the source's service.

## 4. Scale and stop

- **Add instances** with `docker compose up --scale <segment>=N`. Each new instance joins once it is ready. A segment that publishes a fixed, nonzero host port cannot scale: two containers cannot bind one port. A port Docker chooses (`CONTAINER`, `0:CONTAINER`, or `IP::CONTAINER`) scales.
- **Stop one instance** with `docker stop <container>`. It retires: its peers stop routing to it, and it leaves once it has drained.
- **Stop everything** with `docker compose down`. Every segment drains, within its drain bound.

Each segment's `stop_grace_period` is its drain bound plus 30 s. Docker otherwise kills a container 10 s after it stops it. The bound is read from the segment's settings when the bundle is written, so a `GDL_RUNTIME__DRAIN_MS` set at run time does not change the grace period.

A hub restart ends every segment's lease. Start the segments again after the hub ([Discovery: rosters and the hub](../concepts/hub.md)).
