# Deploy a fleet on Kubernetes

This guide writes the Kubernetes objects of a fleet: plain Namespaces, ServiceAccounts, ConfigMaps, Deployments, Services, and PersistentVolumeClaims, one directory per cluster. It uses the `api` example's fleet of [Deploy a fleet of pools](fleet.md), over two clusters. `cargo gdl images` builds the images for each cluster's platform and pushes them to each cluster's registry. `cargo gdl k8s` needs no Docker and no cluster. `cargo gdl deploy` runs both and applies the objects to each cluster, and `cargo gdl down` takes the fleet off its clusters.

## 1. Name the clusters

Give the fleet a name, and each pool a cluster. Each cluster needs a `registry` and a `platform`:

```toml
[fleet]
name = "api"                      # labels every object: gdl.dev/fleet=api

[cluster.use1]
context = "kind-use1"
namespace = "gdl-api"
region = "us-east-1"
registry = "registry.example/gdl"
platform = "linux/amd64"
service_account_annotations = { "eks.amazonaws.com/role-arn" = "arn:aws:iam::000000000000:role/gdl-api" }
secrets = { fingerprint = "api-fingerprint" }   # the spec's secret -> an existing Kubernetes Secret
env = { RUST_LOG = "info" }

[cluster.euw1]
context = "kind-euw1"
namespace = "gdl-api"
region = "eu-west-1"
registry = "registry.example/gdl-eu"
platform = "linux/arm64"
secrets = { fingerprint = "api-fingerprint-eu" }

[class.global]
ready = { front = 2 }
[class.model_x]
ready = { engine = 1 }

[pool.global-us]
class = "global"
cluster = "use1"
[pool.model-x-us]
class = "model_x"
cluster = "use1"
home = "global-us"
ready = { engine = 2 }

[pool.global-eu]
class = "global"
cluster = "euw1"
[pool.model-x-eu]
class = "model_x"
cluster = "euw1"
home = "global-eu"
```

Each child runs in its parent's cluster. The hubs publish under the `relay` directory: a child's hub reports to its parent's hub by the parent's Service name, and that name does not resolve in another cluster.

## 2. Build and push the images

```bash
cargo gdl images --spec gdl.toml --fleet fleet.toml --push --out target/images.json
```

`images` makes the same checks as `k8s` (step 3). Then it builds the hub's image, and one image for each layout and segment that a pool runs, with `docker buildx build --platform <platform>`. Each cluster gets the images of its own pools, for its `platform`, in its `registry`. Two clusters with the same registry and platform share each image. An image's tag is the sha256 of what it is built from (see [Images](#images)). Before it builds an image, `images` asks the registry for its tag. When the registry has it, `images` does not build or push that image again. So a second run with no change pushes nothing.

`images` prints each image as `<registry>/<repository>:<tag>@sha256:<digest>`, and says on stderr whether it pushed the image or found it in the registry. `--out` writes the digests to a JSON file, keyed by reference, for `k8s --images`. Without `--push`, it asks no registry: it builds each image into the local image store, and `--out` is refused.

The command uses the Docker login you already have. It never reads, writes, or prints a registry credential. Log in to each registry first, for example with `docker login` or a credential helper. A registry that makes no repository on push, such as Amazon ECR, must already have each repository: `<graph>-hub`, and each segment's package.

## 3. Write the objects

```bash
cargo gdl k8s --spec gdl.toml --fleet fleet.toml --images target/images.json --out target/k8s
```

`k8s` runs the same check as `cargo gdl check --fleet` first, and then refuses a fleet that cannot run on Kubernetes. It names each problem at its table:

- The fleet has no `[fleet] name`.
- A cluster has no `registry` or no `platform`, a registry that is not `host[:port][/path]` (no scheme, tag, or digest), or a platform that is not `<os>/<arch>`.
- A pool has no `cluster`.
- A key of a cluster's `secrets` is not a secret of the spec's `[deployment.secrets]`.
- A segment that a pool runs mounts a secret that the pool's cluster does not map.
- A spec secret's name starts with `..`, which a projected volume refuses as a file's path.
- An `env` key names a static AWS credential: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_SECURITY_TOKEN`, or `AWS_CONTAINER_AUTHORIZATION_TOKEN`, in any case. Use the service account's role, or a Secret.
- An `env` key is a variable that the render sets: `GDL_POD_IP`, `GDL_POOL`, `GDL_HUB`, or one under `GDL_POOL__` or `GDL_HUB__`, in any case.
- A child runs in another cluster than its parent.
- A segment's name is not a DNS label, or a port is a range.
- The graph's source reads its requests on stdin, which a pod cannot give it.

The output must not exist. It holds a `README.md`, and for each cluster:

```text
target/k8s/
  README.md
  use1/base.yaml                  # Namespace gdl-api, ServiceAccount api
  use1/pools/global-us.yaml
  use1/pools/model-x-us.yaml
  euw1/base.yaml
  euw1/pools/global-eu.yaml
  euw1/pools/model-x-eu.yaml
```

Each file is a YAML stream of JSON documents. The pinned output of this fleet is in `crates/gdl-gen/tests/fixtures/k8s/out`.

## What a pool gets

| Object | Name | What it holds |
|---|---|---|
| ConfigMap | `<pool>` | `deployment.json`, which the hub admits, and `<segment>.toml` for each segment of the pool's class |
| hub Deployment | `<pool>` | 1 replica, strategy `Recreate`, so that two hubs never write the pool's record at once |
| hub Service | `<pool>` | ClusterIP, ports 1337 (public) and 8337 (control) |
| segment Deployment | `<pool>-<segment>` | the pool's quorum of the segment as replicas |
| entry Service | `<pool>-<segment>` | ClusterIP, for a segment with `[deployment.segment.<name>] ports` only |
| PersistentVolumeClaim | `<pool>-state` | 1Gi, for a parent's hub only |

Every object carries `gdl.dev/fleet` and `gdl.dev/cluster`, the fleet's name for its cluster. Every object of a pool also carries `gdl.dev/pool` and `gdl.dev/class`. Every object of a pool but the ConfigMap also carries `gdl.dev/segment`, which is `hub` for the hub's objects. No segment can be named `hub`. A Deployment or a Service selects pods only by `gdl.dev/fleet`, `gdl.dev/pool`, and `gdl.dev/segment`, which no two pools' objects share. The class and the cluster are not in a selector: Kubernetes refuses a change to a Deployment's selector, and a fleet can rename a class. A label's value is at most 63 characters. A longer value, such as a class's name, is labelled by its first 52 characters, `-`, and 10 hex digits of its hash, and the object keeps the whole value in an annotation of the same key.

- **Settings.** Each pod reads its settings from the pool's ConfigMap, mounted at `/etc/gdl/fleet`: the hub runs `/usr/local/bin/segment /etc/gdl/fleet/deployment.json`, and a segment `/usr/local/bin/segment /etc/gdl/fleet/<segment>.toml`. A pod template carries `gdl.dev/config`, a checksum of the ConfigMap's data. A change to the settings changes it, and so replaces the pods. A ConfigMap's change alone restarts no pod.
- **Variables.** A pool's hub and segments get the `GDL_POOL__*` variables that its Compose services get, from the same function. Only the addresses differ. A hub's `GDL_POOL__HUB` is `$(GDL_POD_IP):1337`, and `GDL_POD_IP` comes from `status.podIP`, so the pool's record names the hub's pod. A segment's `GDL_POOL__HUB` and `GDL_HUB__HOST` name the hub's Service. A child's `GDL_POOL__PARENT_HUB` is `<parent>:1337`, the parent hub's Service. The cluster's `env` comes after these. Kubernetes reads `$(NAME)` in a value as another variable, so the render writes each `$` of a cluster's value as `$$`.
- **Probes.** A hub's live and ready probes ask its own `/v1/health` on port 1337. They never ask the pool's health: a parent answers that with 503 until its children are ready, and a parent that is never ready has no Service endpoint for its children to report to. A segment's probes ask its system listener, port 9090 unless its settings say otherwise: `/live` for liveness, and `/health` for readiness. The kubelet asks at the pod's IP, so the render sets `[system] host = "0.0.0.0"` in each segment's settings, whatever the spec sets. A segment whose settings turn the listener off, or give it port 0, is refused.
- **Stop.** A segment's `terminationGracePeriodSeconds` is its drain bound and 30 s, read from its settings as generated, as the Compose bundle's `stop_grace_period` is.
- **State.** A parent's hub keeps its children's records in its claim, mounted at `/var/lib/gdl-hub` (`GDL_POOL__STATE_DIR`). Its pod sets `fsGroup: 65532`, the default image's user, since a new claim is usually root's.
- **Secrets.** A segment's secrets are one projected volume at `/run/secrets`, where Compose mounts them. Secret `<name>` is the file `/run/secrets/<name>`, taken from key `<name>` of the Kubernetes Secret that the cluster's `secrets` names. The objects name the Secret and hold no value. The spec's `file` is not read. Make the Secret with that key:

  ```bash
  kubectl --context kind-use1 -n gdl-api create secret generic api-fingerprint --from-file=fingerprint=/path/to/file
  ```

- **Ports.** An entry Service publishes each container port of `[deployment.segment.<name>] ports`, in Compose's short form. The host port is Compose's alone.
- **Pods.** A pod runs as the fleet's ServiceAccount, which carries the cluster's `service_account_annotations`, on nodes of the cluster's platform (`kubernetes.io/os` and `kubernetes.io/arch`). Service links are off, so that no Service of the namespace adds variables to a container.

## Images

An image holds only its binary, at `/usr/local/bin/segment`. It holds no settings: each pod reads them from its pool's ConfigMap. So a change to the settings alone changes no image.

| Image | Repository | Built from |
|---|---|---|
| hub | `<graph>-hub` | `cargo install gdl-hub`, from the spec's `[gdl]` Git revision or version, with `--locked` |
| segment | its package: `<graph>-<segment>`, or `<graph>-<layout>-<segment>` for a spec of several layouts | the contract crate and the segment's crate, as `cargo gdl generate` writes them |

One hub image serves every layout, because its pod reads the deployment from the ConfigMap.

- **Tag.** An image's tag is the sha256, as 64 hex digits, of its platform and of every file of its build context, its `Dockerfile` included, each by its path in the context. The context names no path outside itself: a generated crate's `Regenerate with:` comment, which names the spec's file, names `cargo gdl images` instead. So one spec gives the same tags in any checkout, on any host. `k8s` computes the same tags with no Docker, so its objects name the images that `images` pushes. `--tag <tag>` gives every image that tag instead, for images built some other way.
- **Digest.** With `--images <images.json>`, each container names its image as `<registry>/<repository>:<tag>@sha256:<digest>`, so a node pulls exactly the pushed image. `k8s` refuses an image that the file does not name.
- **Platform.** The build stage runs on the build host's platform, and cargo builds for the image's platform with `--target`. When the two differ, the build stage installs the target's Rust standard library and Debian's C cross compiler for it, which links the binary and builds mimalloc. Only the final stage, which copies the binary and runs nothing, is of the image's platform. So no compiler runs under emulation. `images` builds for `linux/amd64` and `linux/arm64`, and refuses any other platform.
- **Allocator.** Each segment links mimalloc. jemalloc fixes the page size when it is built, so a binary built on a host of 4 KB pages does not start on a node of 64 KB pages. mimalloc reads the page size when it runs. `images` and `k8s` refuse a spec that sets `[binary] allocator = "jemalloc"`, and use mimalloc when it sets `system` or nothing. The hub uses the system allocator, as `gdl-hub` builds it.
- **Base images.** The build stage uses `[deployment] build_image`, and the final stage `base_image`, or `hub_image` for the hub. A build image of your own must be Debian, with `apt-get` and `rustup`, as the default one is.

## 4. Deploy the fleet

```bash
cargo gdl deploy --spec gdl.toml --fleet fleet.toml
```

`deploy` runs these steps in order. It stops at the first step that fails, and runs nothing after it:

1. **Check.** It checks the spec and the fleet, as `cargo gdl check --fleet` does. Then it runs `kubectl auth can-i create deployments.apps` with each cluster's context and namespace, and refuses a cluster that does not answer `yes`.
2. **Images.** It builds and pushes the images, as `cargo gdl images --push` does (step 2).
3. **Objects.** It renders the objects, as `cargo gdl k8s` does (step 3), with each image named by the digest it pushed. `--out <dir>` also writes them there. The directory must not exist.
4. **Base.** For each cluster, it applies `base.yaml`, the Namespace and the ServiceAccount, and waits until the namespace is `Active`.
5. **Tables.** No fleet renders a tables job yet, so this step does nothing.
6. **Apply.** For each cluster, it applies the pools' objects with `kubectl apply --server-side --field-manager=gdl`.
7. **Prune.** For each cluster, it lists the objects in the namespace that carry `gdl.dev/fleet=<fleet>` and `gdl.dev/cluster=<cluster>`, and deletes those that this render did not write ([Prune](#prune)).
8. **Wait.** It waits until each pool's health answers 200, through the API server's service proxy, for up to 600 s.

Each `kubectl` command names its cluster's `--context` first, so that no command uses the current context. `deploy` reads no kubeconfig, token, key, or Secret. It uses the contexts you already have. It does not check up front that the Secrets the cluster's `secrets` names exist, because `kubectl get secret` sends the Secret's data to the client. A missing Secret shows when the health wait fails: for each pod of an unhealthy pool that is not ready, the error names its phase, why its container waits, and its latest events, such as `secret "api-fingerprint" not found`.

A second `deploy` with no change pushes no image and changes no object, so no pod restarts.

### Prune

kubectl refuses `--prune` together with `--server-side`, so `deploy` prunes by itself. A pool that the fleet no longer places in a cluster is drained first, when it has a parent:

1. `deploy` reads the pool's parent from `GDL_POOL__HOME` in its hub's Deployment. A pool with no parent, such as a global pool, is not drained.
2. It sends `POST /v1/ext/gdl-pool/drain` to the pool's hub, at its Service's `control` port, through the service proxy.
3. It waits until the parent's hub answers 404 for the pool's record (`GET /v1/ext/gdl-pool/report/<class>/<pool>`). The pool's hub withdraws the record only once its drain has ended. A pool that stops without a drain stays in its parent's records, as missing. The wait lasts for the longest `terminationGracePeriodSeconds` of the pool's Deployments, and 60 s more.
4. It deletes the objects it did not render: first the segments' Deployments, then the hubs' Deployments, then the Services, ConfigMaps, PersistentVolumeClaims, and ServiceAccounts. Each delete uses `--cascade=foreground` and waits until the pods are gone.

Prune never deletes a Namespace. It looks only in the namespaces of the fleet's clusters, and only at the objects rendered for that cluster, by `gdl.dev/cluster`. So two clusters of the fleet whose contexts reach one namespace never prune each other's objects. As a second guard, `deploy` stops, and deletes nothing, when a cluster's prune would delete an object that this run applied in any cluster, by its uid. A cluster removed from the fleet keeps its objects: run `cargo gdl down` with the old fleet first.

## 5. Take the fleet down

```bash
cargo gdl down --spec gdl.toml --fleet fleet.toml
```

For each cluster, `down` drains each pool that has a parent and waits until it withdraws, as the prune does. Then it deletes every object in the namespace that carries `gdl.dev/fleet=<fleet>` and `gdl.dev/cluster=<cluster>`. Last, it removes both labels from the Namespace and keeps the Namespace, so the Secrets in it remain. `down` needs only the fleet file. With `--spec`, it first checks the spec and the fleet, as `cargo gdl check --fleet` does, and stops on a problem before it touches a cluster.

To reach a global pool's entry while the fleet runs, forward its Service:

```bash
kubectl --context kind-use1 -n gdl-api port-forward service/global-us-front 8080
```

The `README.md` that `k8s` writes lists the `kubectl apply` commands for each cluster, and the Secrets each cluster must hold, for applying the objects without `deploy`.

## Validate the objects

`scripts/kubeconform.sh` validates objects with kubeconform 0.8.0, in strict mode, against the schemas of Kubernetes 1.35.0. It runs offline. `INSTALL_TOOLS=1` downloads kubeconform and the schemas into `TOOLS_DIR`, which is `.k8s-tools` at the repository's root by default. Without a directory, it validates the pinned objects:

```bash
INSTALL_TOOLS=1 bash scripts/kubeconform.sh
bash scripts/kubeconform.sh target/k8s
```

A kind that has no schema there fails the run.

## Test the images on one host

`scripts/images-local.sh` runs `cargo gdl images` against a local `registry:2`, outside the default suite. It needs Docker with buildx, and network access for the builds. It writes the `api` example in two classes, and a fleet of one cluster whose registry is the local one and whose platform is `PLATFORM` (`linux/amd64` by default, so an aarch64 host cross-builds):

```bash
bash scripts/images-local.sh
PLATFORM=linux/arm64 TIMEOUT=1200 bash scripts/images-local.sh
```

It checks that the first run pushes the hub's image and each segment's, and that each printed digest is the one the registry serves. It checks that each image and its binary are of `PLATFORM`, that each segment links mimalloc, that each binary starts when `PLATFORM` is the host's own, and that `k8s --images` names exactly the pushed images, by digest. A second run must find every image in the registry, build nothing, and leave the registry's files unchanged. `TIMEOUT` bounds each run of `images`, in seconds.

## Test a fleet on kind

`scripts/kind-fleet.sh` runs `cargo gdl deploy` and `cargo gdl down` with the real `kubectl`, against a kind cluster of its own, outside the default suite. It needs Docker with buildx, `kubectl`, and network access for the image builds. `INSTALL_TOOLS=1` downloads kind 0.33.0 into `TOOLS_DIR` and checks its sha256:

```bash
INSTALL_TOOLS=1 bash scripts/kind-fleet.sh
```

The script starts a local `registry:2` and a kind cluster of Kubernetes 1.35 that pulls from it. It writes the `api` example in two classes, and a fleet of one global pool and two model pools, built for the host's platform. It checks that:

- the cluster has no fleet namespace before the first `deploy`;
- after the first `deploy`, the global pool's health answers 200, and its hub holds both model pools;
- a request through `kubectl port-forward` to the global pool's entry is answered;
- a second `deploy`, with one model pool removed from the fleet, drains that pool, and the global pool's hub then answers 404 for its record;
- that pool's objects are deleted, and the other pools' Deployments and pods keep their uids and generations;
- after `down`, no object with the fleet's label is left, and the namespace remains.

The kubeconfig is in the script's scratch directory, so the script never reads or changes `~/.kube/config`. On exit it deletes the cluster and the registry. `TIMEOUT` bounds each `cargo gdl` run, in seconds (1800 by default).
