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

[metrics]
pod_monitor = true                # optional: a PodMonitor in each cluster, for the Prometheus operator
```

Each child runs in its parent's cluster. The hubs publish under the `relay` directory: a child's hub reports to its parent's hub by the parent's Service name, and that name does not resolve in another cluster. With a DynamoDB `[directory]`, a child may run in any cluster ([The DynamoDB directory](#the-dynamodb-directory)).

## 2. Build and push the images

```bash
cargo gdl images --spec gdl.toml --fleet fleet.toml --push --out target/images.json
```

`images` makes the same checks as `k8s` (step 3). Then it builds the hub's image, and one image for each layout and segment that a pool runs, with `docker buildx build --platform <platform>`. Each cluster gets the images of its own pools, for its `platform`, in its `registry`. Two clusters with the same registry and platform share each image. An image's tag is the sha256 of what it is built from (see [Images](#images)). Before it builds an image, `images` asks the registry for its tag. When the registry has it, `images` does not build or push that image again. So a second run with no change pushes nothing.

`images` prints each image as `<registry>/<repository>:<tag>@sha256:<digest>`, and says on stderr whether it pushed the image or found it in the registry. `--out` writes the digests to a JSON file, keyed by reference, for `k8s --images`. Without `--push`, it asks no registry: it builds each image into the local image store, and `--out` is refused.

### Build from local sources

`--local` builds every image from this machine's sources, with no fetch of gdl: gdl and its hub from the workspace of the spec's `[gdl] path`, and the node, message, and policy crates the spec names by path, as `compose --local` does ([Local sources](compose-deploy.md#local-sources)). The spec must name gdl by `path`. So a change to gdl or to a node that is not published is what runs, which is how a policy is developed against a fleet's metrics. `k8s`, `images`, and `deploy` take `--local`, and a run of `images --local` is named by `k8s --local --images`.

```bash
cargo gdl deploy --local --spec examples/api/gdl.toml --fleet fleet.toml
```

Every image of the spec builds from one tree, which `--local` makes in a temporary directory, and removes after. `compose --local` builds the same tree with the same code, and keeps it in the bundle ([Local sources](compose-deploy.md#local-sources)).

- **Sources.** gdl's workspace, each crate the spec names by path, every crate those reach by path, and the workspace of each, as Git holds them. Git decides what is in the tree. Each repository's directories come from `git archive` of one commit: `HEAD`, or, when tracked files have changes, the commit that `git stash create` makes of them. That command runs on a copy of the index, so it changes neither the checkout, its index, nor any ref, and another command that holds the index's lock does not stop it. So the tree holds each committed file and each change to a tracked file, staged or not. An untracked file is not built: `git add` a new file to build it. A file that Git ignores never reaches an image. A link stays a link, and a name stays the bytes Git holds. Each directory goes in the tree at its path from the directory that holds them all, and nothing else of that directory goes in.
- **Refusals.** The tree is a trust boundary, so it fails closed. Each refusal names the entry:
  - a directory that Git cannot archive: one outside a Git checkout, one with no file that Git tracks, or a checkout that Git refuses, such as for its ownership, with Git's words;
  - a submodule, whose files the archive leaves out;
  - a link to an absolute path, or one that leaves the tree, read as text, or through other links on the tree as built: a link that names the bundle's place resolves only once the bundle is written;
  - a file or a link of the sources on the bundle's path, such as a `target` link: the generator writes through no link, and makes each directory it writes to itself;
  - a `path` in any `Cargo.toml` of the sources, at any depth, or a `package.workspace`, that is absolute or leaves the tree. Cargo would read this machine's sources there, and the image would not hold them. The manifests the generator writes are not checked: they name each path relative to themselves, within the tree;
  - any Cargo configuration, `.cargo/config.toml` or `.cargo/config`, with a key other than `[net] git-fetch-with-cli`, or that does not parse as TOML, naming the file and the key: one the archive holds, at any depth; one in a directory between a source and the directory that holds them all, which Cargo on this machine reads; and one above the temporary directory the tree is built in. The image build reads only the configuration the generator writes, which is none, so a build with such a key would quietly differ from this machine's. A file whose only key is `[net] git-fetch-with-cli` is accepted and left out of the tree: a local build fetches no gdl over Git, so the key changes nothing an image builds. A project whose build needs any other Cargo configuration builds without `--local`. Cargo's own home configuration, such as `~/.cargo/config.toml`, is read on this machine when the locks are settled, and not in the image.
- **Root.** The sources' `Dockerfile`, `.dockerignore`, and `Dockerfile.dockerignore` at the tree's root are left out. The root is the build's context, and `images` writes its Dockerfile there, so no ignore rule of the sources, such as `target/`, can leave the bundle out.
- **Bundle.** The generated crates are placed in the tree at `target/gdl-images` of gdl's workspace. This is the same place on every machine, so their manifests name the sources by the same relative paths. Each segment's manifest is patched to build gdl from the tree. Its lock is settled from gdl's lock as the archive holds it, with Cargo run at the tree's root, as the image build runs it at `/app`, so the image build resolves nothing. When Git does not hold gdl's lock, such as when it ignores it, each lock is settled from scratch: only bytes the archive holds reach the tree.
- **Dockerfiles.** Each image copies the tree to `/app`. The hub installs with `cargo install --path /app/crates/gdl-hub --locked`, with its `dynamodb` feature under a DynamoDB directory. A segment builds with `cargo build --locked` from its crate in the bundle.
- **Tags.** An image's tag is the sha256 of its platform, its Dockerfile, and the tree. Each entry of the tree adds four fields, each with its length: its kind (a file of the sources, a link, or a file the generator or Cargo wrote), its mode, its path, and the sha256 of its content or of its link's target. So a file and a link never read alike, and no commit's id or time is in a tag. A change to the spec alone, which changes the generated code, moves the tags. A change to any file of the sources moves every tag. The same sources give the same tags wherever the checkout is, and sources that did not change push nothing.

A walk of the working tree, filtered by hand, was the design before this one, and files got past its filter. A tracked directory replaced by a link let its tracked names reach the link's target. A name that Git lists as bytes, read as UTF-8, named another file, which Git ignored. A parent step in an absolute path passed a check of its prefix. And a file and a link of the same bytes gave the same tag. So Git now decides what is in the tree, and the checks run on what `git archive` gives.

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
  use1/pools/global-us.yaml       # its GdlPool: the operator runs use1
  use1/pools/model-x-us.yaml
  euw1/base.yaml
  euw1/pools/global-eu.yaml
  euw1/pools/model-x-eu.yaml
```

Each file is a YAML stream of JSON documents. In a cluster the gdl operator runs, a pool's file holds its one `GdlPool` ([The operator](#the-operator)). The pinned output of this fleet is in `crates/gdl-gen/tests/fixtures/k8s/out`.

## What a pool gets

| Object | Name | What it holds |
|---|---|---|
| ConfigMap | `<pool>` | `deployment.json`, which the hub admits, and `<segment>.toml` for each segment of the pool's class |
| hub Deployment | `<pool>` | 1 replica, strategy `Recreate`, so that two hubs never write the pool's record at once |
| hub Service | `<pool>` | ClusterIP, ports 1337 (public) and 8337 (control) |
| segment Deployment | `<pool>-<segment>` | the pool's quorum of the segment as replicas; under the operator, no `replicas` ([The operator](#the-operator)) |
| entry Service | `<pool>-<segment>` | ClusterIP, for a segment with `[deployment.segment.<name>] ports` only |
| PersistentVolumeClaim | `<pool>-state` | 1Gi, for a parent's hub only, under `relay` |
| `GdlScaler` | `<pool>-<segment>` | under the operator only: the segment's quorum as its `minReplicas`, its drain bound, and its entry Service |

Every object carries `gdl.dev/fleet` and `gdl.dev/cluster`, the fleet's name for its cluster. Every object of a pool also carries `gdl.dev/pool`, `gdl.dev/class`, and `gdl.dev/region`, the pool's region. Every object of a pool but the ConfigMap also carries `gdl.dev/segment`, which is `hub` for the hub's objects. No segment can be named `hub`. A Deployment or a Service selects pods only by `gdl.dev/fleet`, `gdl.dev/pool`, and `gdl.dev/segment`, which no two pools' objects share. The class and the cluster are not in a selector: Kubernetes refuses a change to a Deployment's selector, and a fleet can rename a class. A label's value is at most 63 characters. A longer value, such as a class's name, is labelled by its first 52 characters, `-`, and 10 hex digits of its hash, and the object keeps the whole value in an annotation of the same key.

- **Settings.** Each pod reads its settings from the pool's ConfigMap, mounted at `/etc/gdl/fleet`: the hub runs `/usr/local/bin/segment /etc/gdl/fleet/deployment.json`, and a segment `/usr/local/bin/segment /etc/gdl/fleet/<segment>.toml`. A pod template carries `gdl.dev/config`, a checksum of what its pods read from the ConfigMap: for a segment, the whole of its data; for the hub, the pool's deployment alone. A change to the settings changes it, and so replaces the pods. A change to one segment's settings replaces the segments' pods and never the hub, since a hub restart drains its pool ([Known limitations](../operations/limitations.md)). A ConfigMap's change alone restarts no pod.
- **Variables.** A pool's hub and segments get the `GDL_POOL__*` variables that its Compose services get, from the same function. Only the addresses differ. A hub's `GDL_POOL__HUB` is `$(GDL_POD_IP):1337`, and `GDL_POD_IP` comes from `status.podIP`, so the pool's record names the hub's pod. A segment's `GDL_POOL__HUB` and `GDL_HUB__HOST` name the hub's Service. A child's `GDL_POOL__PARENT_HUB` is `<parent>:1337`, the parent hub's Service. The cluster's `env` comes after these, on every hub and segment in the cluster. A `GDL_NODE__<name>__*` variable there is read only by the segments that host the node; the others ignore it ([Segment settings](../operations/configuration.md)). Kubernetes reads `$(NAME)` in a value as another variable, so the render writes each `$` of a cluster's value as `$$`.
- **Probes.** A hub's live and ready probes ask its own `/v1/health` on port 1337. They never ask the pool's health: a parent answers that with 503 until its children are ready, and a parent that is never ready has no Service endpoint for its children to report to. A segment's probes ask its system listener, port 9090 unless its settings say otherwise: `/live` for liveness, and `/health` for readiness. The kubelet asks at the pod's IP, so the render sets `[system] host = "0.0.0.0"` in each segment's settings, whatever the spec sets. A segment whose settings turn the listener off, or give it port 0, is refused.
- **Stop.** A segment's `terminationGracePeriodSeconds` is its drain bound and 30 s, read from its settings as generated, as the Compose bundle's `stop_grace_period` is.
- **State.** Under `relay`, a parent's hub keeps its children's records in its claim, mounted at `/var/lib/gdl-hub` (`GDL_POOL__STATE_DIR`). Its pod sets `fsGroup: 65532`, the default image's user, since a new claim is usually root's.
- **Secrets.** A segment's secrets are one projected volume at `/run/secrets`, where Compose mounts them. Secret `<name>` is the file `/run/secrets/<name>`, taken from key `<name>` of the Kubernetes Secret that the cluster's `secrets` names. The objects name the Secret and hold no value. The spec's `file` is not read. Make the Secret with that key:

  ```bash
  kubectl --context kind-use1 -n gdl-api create secret generic api-fingerprint --from-file=fingerprint=/path/to/file
  ```

- **Ports.** An entry Service publishes each container port of `[deployment.segment.<name>] ports`, in Compose's short form. The host port is Compose's alone. The system port is gdl's: every segment's container declares it as port `system`, so `check` refuses it bare in `ports` (`"9090"`), naming the segment. Kubernetes refuses a pod that declares one port twice. A host publication of it (`"127.0.0.1:9091:9090"`) is Compose's only route from the host to the segment's `/metrics` and `/health`, so it passes, and the render leaves it out of the pod and the entry Service.
- **Pods.** A pod runs as the fleet's ServiceAccount, which carries the cluster's `service_account_annotations`, on nodes of the cluster's platform (`kubernetes.io/os` and `kubernetes.io/arch`). Service links are off, so that no Service of the namespace adds variables to a container.

## The operator

A cluster runs its pools in one of two ways. With `[cluster.<name>] operator = true`, the gdl operator runs them ([Scale a segment with the operator](operator.md)): `cargo gdl` writes one `GdlPool` per pool there, and the operator makes the pool's objects from it, scales each segment through its `GdlScaler`, and stops the pool in order when its `GdlPool` is deleted. Without it, `cargo gdl` writes the pool's objects itself, and `deploy` and `down` stop each pool from the deploying machine, as below. Use the operator wherever it is installed. A cluster runs without it when its owner has not installed the operator there: the operator's CRDs are cluster-scoped, and a shared cluster's owner installs them. A fleet may mix both kinds of cluster.

In a cluster the operator runs:

- **Each pool is one `GdlPool`**, named after the pool, in `<cluster>/pools/<pool>.yaml`. Its spec holds every value the pool's objects are rendered from, and the operator renders the same objects `cargo gdl k8s` writes in a cluster without it, with the same `gdl-k8s` library. The base holds no PodMonitor: each pool has its own, named after it.
- **The operator owns the pool's objects** by a controller reference, and changes no object it did not make. `cargo gdl deploy` and `down` never list, change, or delete them. A cluster that sets `operator` while it holds the fleet's objects from an earlier deploy without it is refused before anything is applied: take the fleet down there first. A cluster that drops `operator` while the operator still runs pools there, or still owns a segment Deployment's `replicas`, is refused the same way.
- **Each segment gets a `GdlScaler`**, which an HPA or a person scales through `/scale`, and whose `replicas` no deploy writes again ([How a pool is kept](operator.md#how-a-pool-is-kept)). Pods carry `gdl.dev/routable: "true"`, and entry Services select on it from their start, so that the operator can take a pod out of its Service before it goes.
- **A change that replaces a pool's hub is held** until the pool's `allowPoolRestart` is set, since a hub restart drains the pool. The hub's pods are replaced when its image or the pool's deployment changes; a segment's settings replace only that segment's pods. `cargo gdl deploy --allow-pool-restart` sets it on every `GdlPool` it applies, and a deploy without it leaves it out.
- **A pool's presence** is `serving` unless its `GdlPool` says `presence: scaledToZero`, which the operator tells the pool's hub before it holds each segment at 0. `cargo gdl` writes the field only when it is not `serving`, so a redeploy leaves a `kubectl patch` of it in place.

In a cluster without the operator, a pool's segments run the pool's quorum as `replicas`, under the manager `gdl`, and nothing else changes the count.

## Node groups and resources

On a shared cluster, a fleet can keep its pods on a node group of its own and give each pod its requests and limits, so that the scheduler sees what it uses and nothing of the fleet runs on another team's nodes:

```toml
[cluster.use1]
# ...
node_selector = { "gdl.dev/node-group" = "gdl-api" }
tolerations = [{ key = "dedicated", operator = "Equal", value = "gdl-api", effect = "NoSchedule" }]

[cluster.use1.resources.default]          # every pod of the cluster: all four are required
requests = { cpu = "100m", memory = "128Mi" }
limits = { cpu = "500m", memory = "512Mi" }

[cluster.use1.resources.segment.engine]   # a segment's own pods, or `hub` for the hubs
requests = { cpu = "250m" }
limits = { cpu = "1", memory = "1Gi" }

[pool.model-x-us.resources.segment.engine]   # one pool's own, over its cluster's
limits = { memory = "2Gi" }
```

- **Every pod.** The selector, beside the OS and architecture of the cluster's `platform`, and the tolerations go on every Pod template the render writes in the cluster: each pool's hub and segments, and the DynamoDB directory's Local and tables Job. With `resources`, every container gets CPU and memory requests and limits. A pod's own come from its pool's `resources.segment.<segment>`, then its cluster's, then the cluster's default, field by field. The Local and the Job take the default. Without `resources`, no pod gets any, as before.
- **Checked first.** `cargo gdl check` refuses a value Kubernetes would refuse: a selector key that is not a label key, or `kubernetes.io/os` or `kubernetes.io/arch`, which the `platform` sets; a toleration operator other than `Equal` or `Exists`, an effect other than `NoSchedule`, `PreferNoSchedule`, or `NoExecute`, a value with `Exists`, and `toleration_seconds` without `NoExecute`; a quantity that does not parse, or that Kubernetes would round (finer than `1m`) or cap (above 2^63 - 1 of its unit); a default without all four; a limit below its request in any pod's merged resources; and a `resources.segment` key that is not a segment or `hub`. Write quantities in quotes, as Kubernetes does.
- **CPU limits.** Every pod gets a CPU limit, so a busy segment is throttled at it. Give a segment a limit above its request where it bursts.
- **A change replaces the pods.** The selector, the tolerations, and the resources are in the pod templates, so a change to them replaces the pods at the next `deploy`.

## Metrics

A segment serves its metrics on its system port at `/metrics`, and a hub on its public port, 1337, at `/v1/ext/gdl-pool/metrics` ([Metrics reference](../appendix/metrics.md)). Every pod template says so in the annotations that a Prometheus's pod discovery reads by convention: `prometheus.io/scrape: "true"`, `prometheus.io/port`, and `prometheus.io/path`. The ports are named, `system` on a segment's container and `public` on a hub's.

A series that Prometheus scrapes should say which pool it comes from. gdl's own labels are taken: a chooser labels its pool series with `class`, `pool`, and `region` of the pool it offered to, and a hub labels its follow series with the followed pool's. So the pod's pool, class, region, and segment become `in_pool`, `in_class`, `in_region`, and `segment` on every series, read from the pod's `gdl.dev/*` labels, or whole from their annotations when a label holds a shortened value.

**With the Prometheus operator,** set `pod_monitor = true` in the fleet's `[metrics]` table. In a cluster the gdl operator runs, each pool then has a PodMonitor named after it, which the gdl operator makes and deletes with the pool, and which selects the pool's pods. In a cluster without it, each cluster's `base.yaml` holds a PodMonitor named `<fleet>-<cluster>-<hash>`, in the fleet's namespace: its start cut to fit, and always 10 hex digits of a hash of the fleet and the cluster, so that two clusters whose contexts reach one namespace, or two fleets' clusters there, each keep their own, whatever their names spell. It selects the pods with `gdl.dev/fleet` and `gdl.dev/cluster`, scrapes port `system` at `/metrics` and port `public` at the hub's route, and adds the four labels, and the pod's fleet and cluster as `fleet` and `cluster`. The operator adds `namespace` and `pod` itself. The operator's Prometheus must select the PodMonitor, by its `podMonitorSelector` and `podMonitorNamespaceSelector`. `deploy` asks each cluster once whether it has the CRD (`kubectl api-resources --api-group=monitoring.coreos.com`), and refuses a fleet that asks for a PodMonitor in a cluster that does not, before it builds anything. In each cluster that has the CRD, `deploy` lists the fleet's PodMonitors with its other objects, and prunes one it did not render, as after `pod_monitor` is turned off, and `down` deletes them. A cluster without the CRD is never asked for PodMonitors, since `kubectl get` fails on a kind the cluster does not know. `kubectl api-resources` discovers every API group, and exits 1 when one fails, after it prints what it found. When the failed groups do not include `monitoring.coreos.com`, as with a stale metrics server, `deploy` and `down` go on with what it printed. When they do, whether the cluster has the CRD is not known, and both stop before they build, apply, or delete anything.

**Without the operator,** give a Prometheus a scrape job of role `pod`, whose service account may get, list, and watch pods. This one finds every gdl fleet's hubs and segments in the cluster, and adds the same labels and the pod's fleet and cluster:

```yaml
{{#include prometheus.yml}}
```

Pod discovery makes one target for each container port. The job keeps the one named `system` or `public`, and so does not need `prometheus.io/port`, which is there for other scrape jobs that read it.

## The DynamoDB directory

A fleet with a DynamoDB `[directory]` ([Use the DynamoDB directory](fleet.md#use-the-dynamodb-directory)) publishes every pool's record to two DynamoDB tables, which every hub reads, whatever its cluster. So no hub reports to a parent's hub or keeps a claim, a child may run in another cluster than its parents, and a shared pool (`parents`) is taken. Every hub and segment gets `GDL_POOL__DIRECTORY=dynamodb`, the two table names, and `GDL_POOL__AWS_REGION`, its cluster's region. The hub's image is built with the hub's `dynamodb` feature.

**With `local = true`,** one DynamoDB Local runs in the cluster that `[directory] cluster` names, shared by every region, as the regions of a global table share one directory. A Local in each cluster would give each region a directory of its own.

| Object | In | What it holds |
|---|---|---|
| Deployment and Service `dynamodb-local` | the directory's `base.yaml` | DynamoDB Local 3.3.2, pinned by digest, in memory, with `-sharedDb`, on port 8000; ready once its port answers |
| Job `dynamodb-tables` | the directory's `tables.yaml` | `gdl-hub --tables` from the hub's image, against the Local: it makes the tables, and checks the keys of those that exist. `backoffLimit: 0`, since the step tries again itself while the Local does not answer |

The directory's cluster's hubs reach the Local at its Service, `http://dynamodb-local:8000`. A Service name does not resolve in another cluster, so the other clusters' hubs reach it at its pod's IP, which every cluster routes to, as every pool's data path already needs. A pod's IP exists only once the pod runs, so `cargo gdl k8s` writes `http://DYNAMODB_LOCAL_POD_IP:8000` there, a host that resolves nowhere, and its README says so. `cargo gdl deploy` writes the pod's IP in its place ([Deploy the fleet](#4-deploy-the-fleet)). The value is in the pod template, so a new IP replaces the hubs. An internal load balancer was the other choice. It needs a cloud's controller and annotations of that cloud, kind has none, and its address too exists only after the apply. The Local keeps the tables in memory: a new Local pod starts empty, with a new IP, so deploy again.

**On AWS (without `local`),** each hub takes its credentials from its service account's role. No Local and no Job are rendered: a Job that made an absent table in one region would make a regional table there, and that region would see a directory of its own. Before the first deploy, the owner of the AWS account makes the tables as global tables, with a replica in every region of the fleet, from `dynamodb/tables.json`, which the render writes: each table's keys, the lists table's TTL attribute, and the regions.

## Images

An image holds only its binary, at `/usr/local/bin/segment`. It holds no settings: each pod reads them from its pool's ConfigMap. So a change to the settings alone changes no image.

| Image | Repository | Built from |
|---|---|---|
| hub | `<graph>-hub` | `cargo install gdl-hub`, from the spec's `[gdl]` Git revision or version, with `--locked` |
| segment | its package: `<graph>-<segment>`, or `<graph>-<layout>-<segment>` for a spec of several layouts | the contract crate and the segment's crate, as `cargo gdl generate` writes them |

One hub image serves every layout, because its pod reads the deployment from the ConfigMap.

- **Tag.** An image's tag is the sha256, as 64 hex digits, of its platform and of every file of its build context, its `Dockerfile` included, each by its path in the context. The context names no path outside itself: a generated crate's `Regenerate with:` comment, which names the spec's file, names `cargo gdl images` instead. So one spec gives the same tags in any checkout, on any host. `k8s` computes the same tags with no Docker, so its objects name the images that `images` pushes. `--tag <tag>` gives every image that tag instead, for images built some other way.
- **Digest.** With `--images <images.json>`, each container names its image as `<registry>/<repository>:<tag>@sha256:<digest>`, so a node pulls exactly the pushed image. `k8s` refuses an image that the file does not name.
- **Platform.** The build stage runs on the build host's platform, and cargo builds for the image's platform with `--target`. When the two differ, the build stage installs the target's Rust standard library and Debian's C and C++ cross compilers for it, and names them to cargo and to the `cc` crate (`CC_<target>`, `CXX_<target>`). The C compiler links the binary. Together they build mimalloc and any crate that compiles C or C++ through the `cc` crate, such as the tokenizer crate `esaxx-rs`. A crate that needs a native library of the target, such as OpenSSL through `pkg-config`, or a build tool such as CMake, finds neither in the build stage: build its image on a host of its platform. Only the final stage, which copies the binary and runs nothing, is of the image's platform. So no compiler runs under emulation. `images` builds for `linux/amd64` and `linux/arm64`, and refuses any other platform.
- **Allocator.** Each segment links mimalloc. jemalloc fixes the page size when it is built, so a binary built on a host of 4 KB pages does not start on a node of 64 KB pages. mimalloc reads the page size when it runs. `images` and `k8s` refuse a spec that sets `[binary] allocator = "jemalloc"`, and use mimalloc when it sets `system` or nothing. The hub uses the system allocator, as `gdl-hub` builds it.
- **Base images.** The build stage uses `[deployment] build_image`, and the final stage `base_image`, or `hub_image` for the hub. A build image of your own must be Debian, with `apt-get` and `rustup`, as the default one is.

## 4. Deploy the fleet

```bash
cargo gdl deploy --spec gdl.toml --fleet fleet.toml
```

`deploy` runs these steps in order. It stops at the first step that fails, and runs nothing after it:

1. **Check.** It checks the spec and the fleet, as `cargo gdl check --fleet` does. Then it runs `kubectl auth can-i create deployments.apps` with each cluster's context and namespace, and refuses a cluster that does not answer `yes`. It asks each cluster whether it has the PodMonitor CRD, and refuses a fleet that asks for a PodMonitor where it does not ([Metrics](#metrics)). It asks each cluster whether it has the `GdlPool` and `GdlScaler` CRDs, and refuses a cluster that sets `operator` without them ([The operator](#the-operator)).
2. **Images.** It builds and pushes the images, as `cargo gdl images --push` does (step 2).
3. **Objects.** It renders the objects, as `cargo gdl k8s` does (step 3), with each image named by the digest it pushed. Then it refuses a pool that moves between clusters ([Prune](#prune)), and a cluster that changes between running its pools with the operator and without it over live pools ([The operator](#the-operator)), before anything is applied.
4. **Base.** For each cluster, it asks whether the namespace exists (`kubectl get namespace/<namespace> --ignore-not-found`). It never applies the Namespace. Where the namespace does not exist, it creates it with `kubectl create`, which fails on one that exists. Where it exists, or where another makes it after the question so that the create fails as `AlreadyExists`, it adds `gdl.dev/fleet` and `gdl.dev/cluster` with `kubectl label --overwrite`, a patch of those labels alone. It then applies the rest of `base.yaml`. A server-side apply of a namespace that its owner made with a client-side apply takes over the fields that apply set, and removes those that gdl's object does not list: the owner's labels and its `last-applied-configuration` annotation. A failed question, or a create that fails for another reason, stops the deploy. It then waits until the namespace is `Active`. Under a DynamoDB directory in DynamoDB Local, the directory's cluster goes first, and its base holds the Local: `deploy` waits, for up to 300 s, until the Local's pod is ready, and writes its pod IP in place of `DYNAMODB_LOCAL_POD_IP` in the other clusters' objects before it applies their base ([The DynamoDB directory](#the-dynamodb-directory)). `--out <dir>` then writes the objects, so complete, there. The directory must not exist.
5. **Tables.** Where the render wrote a tables Job (`<cluster>/tables.yaml`), `deploy` deletes the Job's last run, since a Job's pod template cannot change and a Job that once succeeded would not make the tables of a new Local, applies it, and waits, for up to 300 s, until it succeeds. A Job that fails stops the deploy before any pool is applied, and the error carries the Job's log, which names the table. A fleet on AWS has no Job: the owner made the tables.
6. **Apply.** For each cluster, it applies the pools' objects with `kubectl apply --server-side --field-manager=gdl`: a `GdlPool` for each pool where the operator runs them. A cluster in which the fleet now places no pool has nothing to apply, and is still pruned.
7. **Prune.** For each cluster, it lists the objects in the namespace that carry `gdl.dev/fleet=<fleet>` and `gdl.dev/cluster=<cluster>`, but those the operator made, and the `GdlPool`s there, and deletes those that this render did not write, child pools first across the fleet ([Prune](#prune)).
8. **Wait.** It waits until each pool's health answers 200, through the API server's service proxy, for up to 600 s. Where the operator runs the pools, it reads each `GdlPool` instead: the pool is ready once its status says `Applied` for its generation and its phase is `Ready`: the operator stores the generation it acts on before it applies it. A generation the operator holds (`PoolRestartPending`) or refuses (`NotOwned`) stops the wait at once, naming the pool and the operator's reason, and a pool the operator never acted on is named at the wait's end. A pool's hub and segments start together. A segment whose hub is not up yet waits for it, for up to `hub_wait_ms` (5 minutes by default), rather than exiting, so a first deploy restarts no pod ([Segment lifecycle](../concepts/lifecycle.md)).

Each `kubectl` command names its cluster's `--context` first, so that no command uses the current context. Each also carries `--request-timeout`: the time left in its step's wait, and at least 100 ms. A `kubectl` that runs 500 ms past it is killed, and one whose output a process it started holds open that long is given up on, so a command that hangs cannot outlive its step. `deploy` reads no kubeconfig, token, key, or Secret. It uses the contexts you already have. It does not check up front that the Secrets the cluster's `secrets` names exist, because `kubectl get secret` sends the Secret's data to the client. A missing Secret shows when the health wait fails: for each pod of an unhealthy pool that is not ready, the error names its phase, why its container waits, and its latest events, such as `secret "api-fingerprint" not found`.

A second `deploy` with no change pushes no image and changes no object, so no pod restarts.

### Prune

kubectl refuses `--prune` together with `--server-side`, so `deploy` prunes by itself. Pools go children first across the whole fleet, whatever kind of cluster each runs in: every pool at one depth below a top pool before any above it, since a child's drain needs its parent's hub. In a cluster the operator runs, a pool the fleet no longer places goes as its `GdlPool`, deleted alone, with background propagation, Kubernetes' default, and only while it is the object `deploy` listed, by its uid (a pool made again under its name since, before or after its delete, is left in place; the rest of its level still stops in every cluster, and then `deploy` or `down` fails, naming each such pool and both uids, before any pool above it stops): the pool's finalizer stops it in order while its objects still exist ([How a pool stops](operator.md#how-a-pool-stops)), and `deploy` waits until the `GdlPool` is gone, bounded by the pool's drain, its segments' and its hub's stop, and a margin. A wait that runs out names the step of the pool's stop and why it waits. Foreground propagation would delete the pool's objects while it still stops. A renamed segment's objects there are the operator's to delete. In a cluster without the operator, `deploy` stops each pool from the deploying machine, in the same fleet-wide order: a removed child pool is drained and deleted whole, its segments and then its hub, before a removed parent's segments are touched, so a parent keeps answering while its children stop. A cluster without the operator and without the gdl CRDs needs no right on `GdlPool`s: nothing asks for them. Where a cluster has the `GdlPool` kind, `deploy` and `down` list the fleet's `GdlPool`s there, and refuse before anything changes when the deployer may not, naming the right: only the list shows that the operator runs no pool of the fleet there. A pool that the fleet still places, in any cluster, keeps serving: only its objects that this render no longer writes, such as a renamed segment's, are deleted, with no drain and no question to its parent. A pool that the fleet places in no cluster is drained first, when it has a parent:

1. `deploy` reads the pool's parent from `GDL_POOL__HOME`, or, for a shared pool, `GDL_POOL__PARENTS`, in any of its Deployments. A pool with no parent, such as a global pool, is not drained: its hub refuses a drain. A pool whose hub's Deployment is already gone cannot drain. Under `relay`, `deploy` asks its parent's hub for its record once, before anything is drained or deleted. A 404 from that hub means the pool withdrew, and the prune goes on. Otherwise `deploy` stops, deletes nothing, and names the parent's withdraw route (`POST /v1/ext/gdl-pool/children/<class>/<pool>/withdraw` on the control port). Under the DynamoDB directory no parent's hub holds the record, so `deploy` stops, deletes nothing, and names that route and the delete to run by hand.
2. It sends `POST /v1/ext/gdl-pool/drain` to the pool's hub, at its Service's `control` port, through the service proxy.
3. It waits until the pool has withdrawn its record. The pool's hub withdraws the record only once its drain has ended. A pool that stops without a drain stays in the directory, as missing. Under `relay`, `deploy` waits until the parent's hub answers 404 for the pool's record (`GET /v1/ext/gdl-pool/report/<class>/<pool>`). Only the hub's own 404 counts. When the parent's Service does not exist, `deploy` stops at once. Under the DynamoDB directory no parent's hub holds the record, and it answers that route 404 at once, which proves nothing: `deploy` asks the pool's own hub, through its own Service, until it answers `{"withdrawn": true}` (`GET /v1/ext/gdl-pool/withdrawn`), which it does once the directory has stored the withdrawal. The pool's health says the same, but it answers 503 for a drained pool, and `kubectl get --raw` through the service proxy shows no body of a 503, only that the service is unavailable: so the hub answers this one route 200 either way. When the hub that is asked does not answer, or does not say the pool withdrew, `deploy` stops when the wait ends. Either way it names the pool and deletes nothing. The wait lasts for the longest `terminationGracePeriodSeconds` of the pool's Deployments, and 60 s more.
4. It deletes the objects it did not render: first the segments' Deployments, then the hubs' Deployments, then the rest: Services, ConfigMaps, PersistentVolumeClaims, ServiceAccounts, PodMonitors and `GdlScaler`s where the cluster has their CRD, and the DynamoDB directory's Local and Job, after every hub that uses them. Each delete uses `--cascade=foreground` and waits until the pods are gone. Each names its object by the resource the listing named, qualified by its API group (`deployments.apps/<name>`, `jobs.batch/<name>`, `podmonitors.monitoring.coreos.com/<name>`), since `podmonitor/<name>` alone could resolve to another group's kind of that name.

A pool cannot move between clusters. It keeps one name and one record wherever it runs. Draining its old copy would withdraw the record that its new copy serves, and deleting the old copy without a drain would cut its requests. So when the fleet places a pool in one cluster while any of its Deployments, its hub's or a segment's, runs in another, `deploy` stops before it applies anything, and names the pool and both clusters. To move a pool, give it a new name in its new cluster and deploy. Then remove the old name from the fleet and deploy again, which drains it.

Prune never deletes a Namespace. It looks only in the namespaces of the fleet's clusters, and only at the objects rendered for that cluster, by `gdl.dev/cluster`. So two clusters of the fleet whose contexts reach one namespace never prune each other's objects. As a second guard, `deploy` stops, and deletes nothing, when a cluster's prune would delete an object that this run applied in any cluster, by its uid. An object that carries `gdl.dev/fleet=<fleet>` but no `gdl.dev/cluster`, as one rendered before that label, belongs to no known cluster. The prune and `down` stop when the namespace holds one, delete nothing there, and name each such object with the `kubectl label` command that gives it to the cluster. Label it, or delete it, and run again. A cluster removed from the fleet keeps its objects: run `cargo gdl down` with the old fleet first.

## 5. Take the fleet down

```bash
cargo gdl down --spec gdl.toml --fleet fleet.toml
```

`down` stops every pool of the fleet, children first across every cluster, as the prune does: it deletes the `GdlPool` of each pool in a cluster the operator runs, and waits until it is gone; in a cluster without the operator, it drains each pool that has a parent and waits until it withdraws, and then deletes the pool's objects. Under the DynamoDB directory it waits until the pool's own hub says that its record is withdrawn, as the prune does. Only once every cluster's pools are gone does it delete, in each cluster, the rest of the objects that carry `gdl.dev/fleet=<fleet>` and `gdl.dev/cluster=<cluster>`, but any the operator made: the ServiceAccount, the PodMonitor where the cluster has its CRD, and the DynamoDB directory's Local and Job, which the hubs of every cluster use. Last, it removes both labels from each Namespace with `kubectl label`, and keeps the Namespace, so the Secrets in it remain. Neither `deploy` nor `down` changes anything else of a namespace that exists. `down` needs only the fleet file. With `--spec`, it first checks the spec and the fleet, as `cargo gdl check --fleet` does, and stops on a problem before it touches a cluster.

To reach a global pool's entry while the fleet runs, forward its Service:

```bash
kubectl --context kind-use1 -n gdl-api port-forward service/global-us-front 8080
```

The `README.md` that `k8s` writes lists the `kubectl apply` commands for each cluster, and the Secrets each cluster must hold, for applying the objects without `deploy`.

## Validate the objects

`scripts/kubeconform.sh` validates objects with kubeconform 0.8.0, in strict mode, against the schemas of Kubernetes 1.35.0, a PodMonitor against the Prometheus operator's CRD schema, from the CRDs catalog of datreeio at a pinned commit, and a `GdlPool` and a `GdlScaler` against the gdl operator's pinned CRDs. It runs offline. `INSTALL_TOOLS=1` downloads kubeconform and the schemas into `TOOLS_DIR`, which is `.k8s-tools` at the repository's root by default. Without a directory, it validates the pinned objects, and the pinned objects the operator makes from their `GdlPool`s (`crates/gdl-gen/tests/fixtures/k8s/operated`):

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

`scripts/kind-fleet.sh` runs `cargo gdl deploy` and `cargo gdl down` with the real `kubectl`, against a kind cluster of its own, outside the default suite. It needs Docker with buildx, `kubectl`, `jq`, and network access for the image builds and the Prometheus image. `INSTALL_TOOLS=1` downloads kind 0.33.0 into `TOOLS_DIR` and checks its sha256:

```bash
INSTALL_TOOLS=1 bash scripts/kind-fleet.sh
```

The script starts a local `registry:2` and a kind cluster of Kubernetes 1.35 that pulls from it. It writes the `api` example in two classes, and a fleet of one global pool and two model pools, built for the host's platform. It checks that:

- the fleet's namespace, made first with a client-side `kubectl apply` and a label and an annotation of the owner's, keeps that label and both annotations, the last-applied one among them, after `deploy` and after `down`. `deploy` labels it with the fleet's labels, and `down` removes them. A deploy into a namespace that does not exist is tested against the fake `kubectl` only;
- after the first `deploy`, the global pool's health answers 200, and its hub holds both model pools;
- a request through `kubectl port-forward` to the global pool's entry is answered;
- with the cluster's one node labelled and tainted as a node group of the fleet's own, every pod of the fleet runs there by its cluster's `node_selector` and `tolerations`, with the cluster's default requests and limits, and one pool's engine with its own memory limit;
- a Prometheus 3.7.3 without the operator, given the scrape job above (`docs/src/guides/prometheus.yml`), finds each hub and segment of the three pools, each with its `in_pool`, `in_class`, `in_region`, and `segment`, and scrapes each segment. A hub serves its metrics only from a gdl that has the route. `HUB_METRICS_REV` names a commit of the spec's `[gdl] git` that does: the images are then built from it, and every hub must be scraped too. The PodMonitor is not run here, since the operator is too large to install for this check. `kubeconform.sh` checks it against the operator's schema instead;
- a second `deploy`, with one model pool removed from the fleet, drains that pool, and the global pool's hub then answers 404 for its record;
- that pool's objects are deleted, and the other pools' Deployments and pods keep their uids and generations;
- after `down`, no object with the fleet's label is left, and the namespace remains.

The kubeconfig is in the script's scratch directory, so the script never reads or changes `~/.kube/config`. On exit it deletes the cluster and the registry. `TIMEOUT` bounds each `cargo gdl` run, in seconds (1800 by default).

`DYNAMODB=1` then deploys the same pools under a DynamoDB directory in one DynamoDB Local in the cluster, with the model pools shared by their parents' class, which `relay` refuses. It checks that the tables Job succeeds, that every pool is healthy, which needs the global pool to find and follow both model pools through the tables, and that a request is answered, and that `down` then leaves no object with the fleet's label. The hub's image is then built with the hub's `dynamodb` feature, so `GDL_REV` must name a commit of the spec's `[gdl] git` that has it:

```bash
DYNAMODB=1 GDL_REV=<commit> bash scripts/kind-fleet.sh
```

`LOCAL=1` builds every image from this working tree, with `cargo gdl deploy --local` and the spec `examples/api/gdl.toml`, so the DynamoDB case needs no `GDL_REV`: `LOCAL=1 DYNAMODB=1 bash scripts/kind-fleet.sh`. The DynamoDB case also removes a model pool, which the next deploy drains and deletes once its own hub says that its record is withdrawn. kind runs one cluster, so the case covers only the hubs in the Local's own cluster, which reach it at its Service. The address of the other clusters' hubs, the Local's pod IP, is covered by the fake-kubectl tests in `crates/gdl-gen/tests/deployment/deploy/dynamodb.rs`.

`OPERATOR=1`, with `LOCAL=1`, runs the same pools in a cluster the gdl operator runs (`operator = true`) instead. As the cluster's owner, the script builds the operator's image from this working tree, applies `gdl-operator manifests`, and places the operator on the fleet's node group. A pod in the cluster then sends a load of streamed requests, each on a new connection to the global pool's entry Service, each streaming for about 2 s. A request is admitted once it gets its 200: before that the frontend answers 502, or the connection is refused. It checks that:

- `deploy` applies one `GdlPool` per pool, each `Ready`, and the operator owns every object of every pool by a controller reference;
- a second `deploy`, with one model pool removed, deletes that pool's `GdlPool` alone, in the background: its finalizer drains the pool, the global pool's hub then answers 404 for its record, and garbage collection deletes its objects. The other pools' Deployments and pods keep their uids and generations, and no request is refused or cut;
- a change of the global pool's hub, patched into its `GdlPool` without `allowPoolRestart`, is held, with `PoolRestartPending` naming the held generation and a warning event on the pool, and does not reach the hub's Deployment;
- `down`, under the load, deletes the model pool's `GdlPool` before the global pool's, and a sampler of the running pods shows each pool's segment gone before its hub, and the model pool gone before the global pool's frontend. No admitted request is cut; requests that arrive once no pool can admit them are refused;
- no object with the fleet's label is left, and the namespace remains as its owner made it.

```bash
LOCAL=1 OPERATOR=1 bash scripts/kind-fleet.sh
```
