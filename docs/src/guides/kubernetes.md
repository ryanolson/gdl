# Deploy a fleet on Kubernetes

This guide writes the Kubernetes objects of a fleet: plain Namespaces, ServiceAccounts, ConfigMaps, Deployments, Services, and PersistentVolumeClaims, one directory per cluster. It uses the `api` example's fleet of [Deploy a fleet of pools](fleet.md), over two clusters. `cargo gdl k8s` needs no Docker and no cluster. It does not build or push images, and it does not apply the objects.

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

## 2. Write the objects

```bash
cargo gdl k8s --spec gdl.toml --fleet fleet.toml --out target/k8s
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

Every object carries `gdl.dev/fleet`, `gdl.dev/pool`, and `gdl.dev/class`. Every object but the ConfigMap also carries `gdl.dev/segment`, which is `hub` for the hub's objects. No segment can be named `hub`. A Deployment or a Service selects pods only by `gdl.dev/fleet`, `gdl.dev/pool`, and `gdl.dev/segment`, which no two pools' objects share. The class is not in a selector: Kubernetes refuses a change to a Deployment's selector, and a fleet can rename a class. A label's value is at most 63 characters. A longer value, such as a class's name, is labelled by its first 52 characters, `-`, and 10 hex digits of its hash, and the object keeps the whole value in an annotation of the same key.

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

Each image is `<registry>/<repository>:<tag>`, in its cluster's registry. A segment's repository is its package (`<graph>-<segment>`, or `<graph>-<layout>-<segment>` for a spec of several layouts). The hub's is `<graph>-hub` for every layout, since its pod reads the deployment from the ConfigMap. `--tag <tag>` gives the tag. Without it, each layout's images are tagged `<contract hash>-<placement hash>`, each 16 hex digits. That tag does not change when a node crate changes. The images must be in each registry before the pods start: `k8s` does not build or push them.

## 3. Apply the objects

Apply each cluster's base first, since every other object is in its Namespace:

```bash
kubectl --context kind-use1 apply --server-side -f target/k8s/use1/base.yaml
kubectl --context kind-use1 apply --server-side -R -f target/k8s/use1/pools
```

The `README.md` in the output lists these commands for each cluster, and the Secrets each cluster must hold. To reach a global pool's entry, forward its Service:

```bash
kubectl --context kind-use1 -n gdl-api port-forward service/global-us-front 8080
```

## Validate the objects

`scripts/kubeconform.sh` validates objects with kubeconform 0.8.0, in strict mode, against the schemas of Kubernetes 1.35.0. It runs offline. `INSTALL_TOOLS=1` downloads kubeconform and the schemas into `TOOLS_DIR`, which is `.k8s-tools` at the repository's root by default. Without a directory, it validates the pinned objects:

```bash
INSTALL_TOOLS=1 bash scripts/kubeconform.sh
bash scripts/kubeconform.sh target/k8s
```

A kind that has no schema there fails the run.
