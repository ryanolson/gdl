# Scale a segment with the operator

The gdl operator scales the segments of a fleet's pools on Kubernetes. A `GdlScaler` holds how many instances of one segment of one pool run. An HPA, or a person, writes that count through the scaler's `/scale` subresource, and the operator is the one writer of the segment Deployment's `replicas`. On a scale-down it chooses which instance to remove by its load, and makes sure that exactly that pod goes.

Without the operator, a Deployment scaled down by hand or by an HPA lets Kubernetes choose the pod. That choice is correct, since every instance drains on its SIGTERM ([Stopping](../concepts/lifecycle.md#stopping)), but it is blind to load: it may stop the busiest instance, whose drain is the longest.

The operator is a Rust binary, `gdl-operator`, that runs in the fleet's namespace. It reaches the Kubernetes API and each pool hub's HTTP routes, and nothing else. It never links velo.

## Install it

The operator needs one cluster-scoped object, the `GdlScaler` CRD (`gdlscalers.gdl.dev`), and four objects in the fleet's namespace: its ServiceAccount, a Role, a RoleBinding, and its Deployment. A cluster's owner applies them, since a deployer cannot grant rights it does not hold. `gdl-operator manifests` prints all five, each labelled `gdl.dev/setup=gdl-operator`:

```bash
gdl-operator manifests --namespace gdl-api --image registry.example/gdl/gdl-operator:<tag> \
  | kubectl --context <context> apply --server-side -f -
```

The image is built from `crates/gdl-operator/Dockerfile`, at the repository's root:

```bash
docker build -f crates/gdl-operator/Dockerfile -t registry.example/gdl/gdl-operator:<tag> .
```

The Role grants only what the scaler uses, in its namespace:

| Resource | Verbs | Why |
|---|---|---|
| `gdl.dev` `gdlscalers` | get, list, watch, update, patch | Read each scaler |
| `gdl.dev` `gdlscalers/status` | get, update, patch | Report what it observed and did |
| `apps` `deployments` | get, list, watch, patch | Read the target, and write its `replicas` |
| core `pods` | get, list, watch, patch | Read the pods, and label and annotate the victim |
| `discovery.k8s.io` `endpointslices` | get, list, watch | See that a victim has left its entry Service |
| core `events` | create, patch | Events |

It grants no delete, and nothing on Secrets, ServiceAccounts, or Namespaces. It has no `services/proxy` right: the operator runs in the cluster, and reaches hubs and pods at their addresses.

The operator runs as one replica, which its Deployment's `Recreate` strategy stops before it starts another, so two operators never act on one scaler. It does not elect a leader.

## The `GdlScaler` resource

Group `gdl.dev`, version `v1alpha1`, namespaced. One scaler per segment of a pool.

| Field | Meaning |
|---|---|
| `spec.pool`, `spec.segment` | The pool and the segment. The pool's hub's Service has the pool's name |
| `spec.targetRef` | `{apiVersion: apps/v1, kind: Deployment, name}`: the segment's Deployment |
| `spec.replicas` | How many instances to run. An HPA writes it through `/scale`. Until it is set, the scaler does nothing |
| `spec.minReplicas` | The fewest it scales to: the segment's quorum, never below 1, since the last instance of a segment is not avoided ([Known limitations](../operations/limitations.md)) |
| `spec.suspend` | `zero` holds the target at 0, whatever `replicas` and `minReplicas` say |
| `spec.maxConcurrentDrains` | How many victims drain at once: 1 by default |
| `spec.victim` | The victim policy: `fewestAdmitted` by default, or `newest` |
| `spec.retire` | `signal`: the victim retires on its pod's SIGTERM. The only mode |
| `spec.drainBound` | The segment's drain bound, such as `20s`. The scaler reports a removal that took longer |
| `spec.routingGrace` | How long after a victim leaves its entry Service's endpoints the scaler waits before the pod goes: `10s` by default |
| `spec.service` | The segment's entry Service, when it publishes ports |
| `spec.metricsPrefix` | The segment's `[system] metrics_prefix`: `dynamo_component` by default |
| `status.replicas`, `status.selector` | The target's count and its pods' selector, which `/scale` reports to an HPA |
| `status.readyReplicas` | Its ready pods |
| `status.retiring` | The victims chosen and not gone yet, each by its pod's uid |
| `status.last` | The last removal: the pod chosen, the pod deleted, why, how long it took, and how it ended |
| `status.conditions` | `Scaling`, `Blocked`, and `Suspended` |

A duration is a count and one unit: `ms`, `s`, `m`, or `h`. The schema refuses any other, a `retire` other than `signal`, and a `minReplicas` below 1.
