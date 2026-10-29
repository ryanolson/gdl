# Kubernetes objects of fleet `api`

One directory per cluster. `base.yaml` holds the cluster's Namespace and the fleet's ServiceAccount; `pools/<pool>.yaml` holds each pool's ConfigMap, its hub's Deployment and Service, a Deployment for each segment of its class, and a Service for each segment that publishes ports. Every object is labelled `gdl.dev/fleet=api`.

Apply the base first, since every other object is in its Namespace:

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
