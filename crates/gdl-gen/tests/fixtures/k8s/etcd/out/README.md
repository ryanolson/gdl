# Kubernetes objects of fleet `api`

One directory per cluster. `base.yaml` holds the cluster's Namespace and the fleet's ServiceAccount; `pools/<pool>.yaml` holds each pool's ConfigMap, its hub's Deployment and Service, a Deployment for each segment of its class, and a Service for each segment that publishes ports. Every object is labelled `gdl.dev/fleet=api`.

Apply the base first, since every other object is in its Namespace. Where a cluster's namespace already exists, leave its Namespace out of the apply and label the namespace instead, with `kubectl label namespace/<namespace> gdl.dev/fleet=<fleet> gdl.dev/cluster=<cluster>`: an apply would take over the fields its owner set, and drop the labels and annotations this Namespace does not list. `cargo gdl deploy` does so itself.

```bash
kubectl --context kind-euw1 apply --server-side -f euw1/base.yaml
kubectl --context kind-euw1 apply --server-side -R -f euw1/pools
```

Before its pods start, namespace `gdl-api` of cluster `euw1` must hold: Secret `api-fingerprint-eu`, with key `fingerprint`. The objects name each Secret and hold no value.

```bash
kubectl --context kind-use1 apply --server-side -f use1/base.yaml
kubectl --context kind-use1 apply --server-side -R -f use1/pools
```

Before its pods start, namespace `gdl-api` of cluster `use1` must hold: Secret `api-fingerprint`, with key `fingerprint`. The objects name each Secret and hold no value.

Each image must be in its cluster's registry before its pods start: `cargo gdl images --push` builds and pushes them. A pool's hub's Service is named for the pool, and its segments' objects `<pool>-<segment>`.

Every pod says where it serves its metrics, in its `prometheus.io/scrape`, `prometheus.io/port`, and `prometheus.io/path` annotations.

Every pool's hub publishes its record to one etcd that cluster `use1` runs (`base.yaml`: claim `etcd-data`, Deployment and Service `etcd`), shared by every region. Its hubs reach it at its Service. The other clusters' hubs reach it at its pod's IP, which exists only once the pod runs: their objects name it `ETCD_POD_IP` (`GDL_POOL__ENDPOINTS=["http://ETCD_POD_IP:2379"]`), and `cargo gdl deploy` writes the pod's IP in its place. To apply these objects by hand, replace `ETCD_POD_IP` with the etcd pod's IP first, and apply them again when a new pod takes another IP. etcd runs with `--auto-compaction-mode=periodic --auto-compaction-retention=1h` and `--quota-backend-bytes=8589934592`: the directory never compacts, so without them etcd would keep each heartbeat until its quota refused every write. Its data is on the claim, from the cluster's default storage class, so a new pod keeps the directory. etcd needs no step before the hubs start: each writes its record on its first heartbeat.
