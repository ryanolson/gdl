# Scale a segment with the operator

The gdl operator scales the segments of a fleet's pools on Kubernetes. A `GdlScaler` holds how many instances of one segment of one pool run. An HPA, or a person, writes that count through the scaler's `/scale` subresource, and the operator is the one writer of the segment Deployment's `replicas`. On a scale-down it chooses which instance to remove by its load, and makes sure that exactly that pod goes.

Without the operator, a Deployment scaled down by hand or by an HPA lets Kubernetes choose the pod. That choice is correct, since every instance drains on its SIGTERM ([Stopping](../concepts/lifecycle.md#stopping)), but it is blind to load: it may stop the busiest instance, whose drain is the longest.

The operator is a Rust binary, `gdl-operator`, that runs in the fleet's namespace. It reaches the Kubernetes API and each pool hub's HTTP routes, and nothing else. It never links velo.

## Install it

The operator needs one cluster-scoped object, the `GdlScaler` CRD (`gdlscalers.gdl.dev`), and five objects in the fleet's namespace: its ServiceAccount, a Role, a RoleBinding, a StatefulSet of one replica, and the StatefulSet's headless Service. A cluster's owner applies them, since a deployer cannot grant rights it does not hold. `gdl-operator manifests` prints all six, each labelled `gdl.dev/setup=gdl-operator`:

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

It grants no delete, and nothing on Secrets, ServiceAccounts, or Namespaces. It has no `services/proxy` right: the operator runs in the cluster, and reaches hubs and pods at their addresses.

The operator runs as a StatefulSet of one replica. A StatefulSet never runs two pods of one identity: it starts the replacement of its pod only once the old one is confirmed gone. So at most one operator acts at a time, through a rollout, a restart, or a node failure. While the operator's node does not answer, its pod is not confirmed gone, and no replacement starts: scaling pauses, the safe way, until the node answers again or Kubernetes removes it. Scaling pauses, and nothing else does: the segments serve on, and a pod that leaves meanwhile drains on its own SIGTERM.

**Never force-delete the operator's pod** (`kubectl delete --force --grace-period=0`), and never remove its node by hand while the pod may still run. Either tells Kubernetes the pod is gone when it may not be, and a second operator can then start beside the first ([Known limitations](../operations/limitations.md)).

### Why a StatefulSet

The rejected alternative is an election of the operator's own, by a Kubernetes Lease. Three review passes each found a new way for it to let two operators act at once: a renewal that hung left the old holder acting after another took the lapsed lease; a delayed first read let a successor take over inside the holder's hold; and a write that passed the holder's check could land after the handoff. Kubernetes already keeps one pod of a StatefulSet's identity, and the operator needs nothing more.

## Let `deploy` make the scalers

Set `scaler = true` on a cluster in the fleet file. `cargo gdl deploy` then gives each segment of that cluster a `GdlScaler` named after its Deployment, writes each one's `replicas` once at the segment's quorum, and labels the pods for their entry Service ([The operator](kubernetes.md#the-operator) in the Kubernetes guide). It refuses a cluster that sets `scaler` and has no `GdlScaler` CRD. A cluster without `scaler` keeps its segments at the quorum, with no scaler.

```toml
[cluster.use1]
context = "kind-use1"
namespace = "gdl-api"
region = "us-east-1"
registry = "registry.example/gdl"
platform = "linux/amd64"
scaler = true
```

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
| `status.retiring` | The victims chosen and not gone yet: each one's pod and its uid, its Deployment's uid, the count it is removed at (`target`), and its phase: `unrouting`, `committing`, or `removing` |
| `status.last` | The last removal: the pod chosen, the pod deleted, why, how long it took, and how it ended |
| `status.conditions` | `Scaling`, `Blocked`, and `Suspended` |

A duration is a count and one unit: `ms`, `s`, `m`, or `h`. The schema refuses any other, a `retire` other than `signal`, and a `minReplicas` below 1.

## How a scale-down runs

```mermaid
sequenceDiagram
    participant A as HPA or person
    participant O as gdl operator
    participant K as Kubernetes (API, ReplicaSet, EndpointSlices)
    participant H as Pool hub
    participant V as Victim instance
    A->>K: GdlScaler /scale: replicas 3 to 2
    K-->>O: watch event
    O->>O: check: no rollout, no pod terminating, every pod ready
    O->>H: GET /v1/ext/gdl-pool/instances
    O->>V: GET /metrics on each pod
    O->>O: victim: the lowest rank of the policy, ties to the newest pod
    opt the segment has an entry Service
        O->>K: label V gdl.dev/routable=false
        K-->>O: the EndpointSlices no longer list V
        O->>O: wait routingGrace
    end
    O->>K: annotate V pod-deletion-cost = -2147483648
    O->>K: Deployment replicas 3 to 2
    K->>V: SIGTERM (V has the lowest cost)
    V->>H: withdraw ready and loaded, announce retired
    V->>V: drain, then leave
    K-->>O: V is gone
    O->>O: status.last: chosen, deleted, duration, outcome
```

The scaler keeps its target at `max(spec.replicas, spec.minReplicas)`. A scale-up first takes back any victim not yet committed, and raises the target's `replicas` once every committed victim's pod is being deleted ([Why a deletion cost](#why-a-deletion-cost)). A scale-down takes one victim at a time to its commit, and up to `maxConcurrentDrains` drain together, in these steps.

1. **Wait until it is safe.** The scaler removes pods while more live, not terminating and not already leaving, than the desired count. It chooses a victim only while no rollout of the target is in progress, no other pod of it is terminating, and every other pod is ready. Otherwise its `Blocked` condition says why (`RolloutInProgress`, `PodTerminating`, `PodNotReady`), and it looks again every 5 s. Every pod being ready is also what makes the ReplicaSet's choice predictable, below.
2. **Choose.** The scaler reads each pod's `/metrics`, at its IP, on its container port named `system`, and the hub's instance list (`GET /v1/ext/gdl-pool/instances`, [Listing the instances](../concepts/pools.md#listing-the-instances)), which maps a pod to its instance by the pod's IP. A pod's load is the larger of `admission_admitted` and its `inflight_requests`, summed over its entries: a source's own requests are only in the second, and an entry's admitted requests stay in the first while velo still holds their output ([Reading a drain](../operations/observability.md#reading-a-drain)). When a pod's `/metrics` does not answer, the hub's latest load report of it stands in. The names carry the segment's `metricsPrefix`, and are read by their whole name. The victim policy ranks each pod, and the lowest rank goes. A tie goes to the newest pod, then the greater uid. The last ready pod is never chosen.
3. **Take it out of routing.** For a segment with an entry Service, the scaler stores the victim as `unrouting`, then sets the victim's label `gdl.dev/routable` to `false`. Entry Services select on `gdl.dev/routable: "true"`, so the EndpointSlice controller drops the pod. The label is not in the Deployment's selector, so the ReplicaSet still owns the pod. The scaler waits until no EndpointSlice of the Service lists the pod, and then `routingGrace`, for kube-proxy and load balancers to stop sending to it.
4. **Delete exactly that pod.** The scaler stores the victim as `committing`, at a count one below the target's, then sets the victim's `controller.kubernetes.io/pod-deletion-cost` to `-2147483648`, the lowest, and then writes the target's `replicas` one lower. Once the count is written, the victim is `removing`: committed. The ReplicaSet deletes the pod, which retires and drains on its SIGTERM ([Stopping](../concepts/lifecycle.md#stopping)), bounded by the pod's grace: the drain bound and 30 s.
5. **Record it.** Once the pod is gone and the hub no longer lists its instance, `status.last` holds the pod chosen, the pod deleted, the time from the choice, and the outcome: `removed`, or `overran` when the pod went more than `drainBound` and 10 s after the scale-down. The 10 s are the runtime's margin past its bound ([The bound](../concepts/lifecycle.md#the-bound)). An instance that left cleanly leaves its hub as it exits. One that crashed stays listed until its lease lapses, so the scaler waits for the hub at most a minute after the pod is gone, and then says in the reason that the hub still listed it. A hub that does not answer cannot say, and the pod's exit alone ends the removal.

Each victim is in `status.retiring` from its choice until its pod is gone, with what its `/metrics` last said it holds. The status is all the scaler remembers, so an operator that restarts carries on. It is written ahead: the scaler stores each phase before the step it names, so a step that a lost write or a restart leaves half done is finished or undone from what the status says, never guessed. A victim whose pod is terminating, or whose count is at or below its target, is committed, whatever its phase says. One that is not committed and whose pod went ends as `lost`, with nothing else changed. A live pod out of its Service that no victim names gets its Service back. Only one victim at a time is not yet committed; committed victims drain together, up to `maxConcurrentDrains`.

Every write carries the read it was decided on. A status write carries the scaler's resourceVersion, a write of the count the Deployment's uid and resourceVersion, and a write to a pod the pod's uid and resourceVersion. The API server refuses a write whose read is out of date, a write that changes nothing included, and the reconcile stops; the next one reads again and decides again. A step that puts a pod back is stored before it is taken, as every step is, and each reconcile stores its status, conditionally, before its first write to a pod: a reconcile whose read of the scaler predates another's stored step writes no pod. So even two operators at once on one scaler, which only a force-delete makes possible, cannot both commit a victim, both lower the count, undo a newer scale, or undo each other's label or cost on a pod. So a reconcile that waited on a scrape cannot undo a newer scale, and a Deployment made again under the same name is not changed by a decision about the old one. A victim of the old one is abandoned: its pod gets its Service and its cost back, and `status.last` says `abandoned`. Each reconcile reads the scaler from the API, not from its watch's cache.

### Victim policies

| Policy | Removes first |
|---|---|
| `fewestAdmitted` (the default) | The pod with the least load: the shortest drain. A pod whose load could not be read ranks last |
| `newest` | The newest pod |

A policy is registered by name, as pool policies are: `Victims::add(name, policy)` with a `VictimPolicy`, which ranks one candidate. A scaler that names a policy no one registered is blocked (`UnknownVictimPolicy`).

### Why a deletion cost

A ReplicaSet deletes, in order: unscheduled pods, then Pending, Unknown, and Running pods, then pods that are not ready, then pods of a lower `controller.kubernetes.io/pod-deletion-cost`. Only after those does it look at nodes, readiness time, restarts, and age. The scaler acts only while every other pod runs and is ready, so the victim's cost decides. The rejected alternatives: a StatefulSet always removes the highest ordinal, so it cannot choose; and pods the operator owns directly would mean writing a ReplicaSet again, with adoption, template hashes, and rollout pacing.

The cost is a hint: the ReplicaSet reads pods from its own cache, and a pod that fails at that moment comes first. If it deletes another pod, nothing is lost. The victim never retired, since only its SIGTERM retires it. The pod that went drains on its own SIGTERM. The scaler sees that the ReplicaSet has nothing left to delete: the victim still runs, the count is at or below its target, and no more pods live than the count. It clears the victim's cost, sets its `gdl.dev/routable` back to `true`, and records `kubernetesChoseAnother` with the pod that went. A victim left out of its Service for good would stay counted in the replicas and take no connection. Any other committed victim stays out of its Service until its pod is gone: a ReplicaSet that has not acted yet still deletes it. A scale-up takes back a victim not yet committed the same way, before any new pod. A committed one cannot be taken back, so the scale-up raises the count only once its pod is being deleted: raised before, the count would leave the ReplicaSet nothing to delete, and the victim out of its Service for good.

### Frontends

A frontend takes its requests from its Service, not from a gdl chooser. So "out of the routing table" means out of the Service's endpoints. Leaving the endpoints first means the pod stops getting new connections before its SIGTERM stops it taking requests, so a new connection is never refused once routing has converged.

That promise has two limits ([Known limitations](../operations/limitations.md)):

- **A load balancer is best effort.** Nothing in Kubernetes acknowledges that a load balancer has stopped sending to a pod: `routingGrace` is a wait, not a signal. With the AWS load balancer controller, use IP targets, and set `routingGrace` to the controller's reconcile time plus the target group's deregistration delay.
- **Open connections stay.** Leaving the endpoints moves no open connection. A keep-alive client keeps sending on its connection, and the frontend learns that it retires only at its SIGTERM. Closing keep-alive connections while it drains is the frontend's own drain policy ([Hosted frontends](frontends.md)).

## With an HPA

Point the HPA at the scaler, never at the Deployment, whose `replicas` the scaler owns:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: model-a-engine, namespace: gdl-api}
spec:
  scaleTargetRef: {apiVersion: gdl.dev/v1alpha1, kind: GdlScaler, name: model-a-engine}
  minReplicas: 1
  maxReplicas: 4
```

Set the HPA's `minReplicas` to at least the segment's quorum. The scaler drains `maxConcurrentDrains` at a time, so a larger step down drains in waves, and the HPA's stabilization window (300 s by default) keeps it from flapping. A drain cannot be undone: a scale-up while a victim drains starts a new pod.

## Prove it on kind

`scripts/kind-operator.sh` runs the scale-down on a kind cluster, outside the default suite. It installs the operator as a cluster's owner would, deploys the api example's fleet with `scaler = true` and `cargo gdl deploy --local`, and runs a load of streamed requests from a pod in the cluster, each on a new connection to the frontend's Service. Each response streams for about 2 s: the engine waits `pace_ms` before each word. It then proves, for the engine and for the frontend, each scaled from 3 to 2:

- the pod the scaler chose is the pod deleted, and the others stay;
- no request fails, a new connection to the frontend included;
- no offer reaches the engine's victim after its acknowledgement: its `requests_total` does not rise once its `instance_avoided` reads 1;
- the frontend's victim leaves its Service's EndpointSlices before its pod is deleted;
- the removal ends inside the drain bound and the runtime's exit margin.

Last, an HPA on the engine's `GdlScaler` scales it to 3 and back to 2 through `/scale`. kind runs no metrics server, so the HPA moves the scaler by its `minReplicas` and `maxReplicas`, which it applies before it reads any metric.

```bash
INSTALL_TOOLS=1 bash scripts/kind-operator.sh
```
