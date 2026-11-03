# Kubernetes objects of fleet `api`

One directory per cluster. `base.yaml` holds the cluster's Namespace and the fleet's ServiceAccount; `pools/<pool>.yaml` holds each pool's ConfigMap, its hub's Deployment and Service, a Deployment for each segment of its class, and a Service for each segment that publishes ports. Every object is labelled `gdl.dev/fleet=api`.

Apply the base first, since every other object is in its Namespace. Where a cluster's namespace already exists, leave its Namespace out of the apply and label the namespace instead, with `kubectl label namespace/<namespace> gdl.dev/fleet=<fleet> gdl.dev/cluster=<cluster>`: an apply would take over the fields its owner set, and drop the labels and annotations this Namespace does not list. `cargo gdl deploy` does so itself.

```bash
kubectl --context kind-use1 apply --server-side -f use1/base.yaml
kubectl --context kind-use1 apply --server-side -R -f use1/pools
```

The gdl operator runs the pools of cluster `use1`: it needs the `GdlPool` and `GdlScaler` CRDs and the operator in namespace `gdl-api` (`gdl-operator manifests`). Each `pools/<pool>.yaml` there holds the pool's one `GdlPool`, from which the operator makes the pool's objects, and which it stops in order when it is deleted. Delete a `GdlPool` with background propagation, the default of `kubectl delete`, child pools first: `kubectl --context kind-use1 --namespace gdl-api delete gdlpools.gdl.dev/<pool>`. A change that replaces a pool's hub is held until its `allowPoolRestart` is set.

The `GdlPool` of `dgd-use1` holds its DynamoGraphDeployment, which dynamo's operator runs: the cluster must serve `dynamographdeployments.nvidia.com` at `nvidia.com/v1beta1`, with dynamo's platform installed, before the gdl operator starts, or the pool applies nothing.

Before its pods start, namespace `gdl-api` of cluster `use1` must hold: Secret `api-fingerprint`, with key `fingerprint`. The objects name each Secret and hold no value.

Each image must be in its cluster's registry before its pods start: `cargo gdl images --push` builds and pushes them. A pool's hub's Service is named for the pool, and its segments' objects `<pool>-<segment>`.

Every pod says where it serves its metrics, in its `prometheus.io/scrape`, `prometheus.io/port`, and `prometheus.io/path` annotations.
