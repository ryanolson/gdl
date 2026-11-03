# Spec and fleet reference

This appendix lists every table and key of `gdl.toml` (the spec), `fleet.toml` (the fleet file), and a pool's settings. It is derived from the parsing code in `gdl-gen` and `gdl-model`. Every table refuses a key it does not know. A table whose keys are names (`ready`, `requires`, `[config]`, `pin`) refuses a name that is not in the spec. Paths in a spec are relative to the spec file. [Specs, segments, and layouts](../concepts/placement.md) explains the placement. [Split, generate, and draw](../guides/split-and-generate.md) shows a spec at work.

## gdl.toml

A spec names the graph with `[contract]` or with `[graph]`, never both. `[messages]` goes with `[graph]`. `[nodes]` goes with `[graph]`, and `[bind.*]` goes with `[contract]`. `[pool_policy.*]`, `[selector.*]`, and `[binary]` go with either. `[gdl]` is always required.

### Graph source

| Key | Meaning | Default |
|---|---|---|
| `[contract] path` | a crate that holds a `graph!` invocation | required |
| `[contract] module` | the Rust path of the module that holds the invocation | the crate's library name |
| `[graph] path` | a graph file | required |
| `[messages] crates` | crates that offer the graph file's messages. Each entry is a directory, `name@version`, or `{ package, git, rev, lib? }`. At least one. | required |
| `[messages] pin` | message name to the public path of its type, to break a tie or to rename | none |
| `[nodes] crates` | crates that offer the graph's nodes, in the same forms as `[messages] crates` | none |
| `[nodes] pin` | node name to the path of its implementing type | none |
| `[bind.<Node>] crate` | the implementation's package (contract spec) | required |
| `[bind.<Node>] path` | the crate's directory | required |
| `[bind.<Node>] type` | the implementing type's path, which starts in the crate's library | required |
| `[pool_policy.<name>] crate` | the package of a pool policy that a `[pools.<class>] policy` names | required |
| `[pool_policy.<name>] path` | the crate's directory | one source is required |
| `[pool_policy.<name>] version` | an exact version on crates.io, as `name@version` in `[nodes] crates` | one source is required |
| `[pool_policy.<name>] git`, `rev` | a repository and a revision. Both are required together. | one source is required |
| `[pool_policy.<name>] lib` | the crate's library name, with `git` only, when it differs from the package's | optional |
| `[pool_policy.<name>] type` | the type that implements `gdl::PoolPolicy` and `gdl::Build`, at the root of the crate's library | required |
| `[selector.<name>] crate` | the package of a selector that a `[routing]` router names | required |
| `[selector.<name>] path`, `version`, `git`, `rev`, `lib` | the crate's source, as for `[pool_policy.<name>]` | one source is required |
| `[selector.<name>] type` | the type that implements `gdl::Selector` and `gdl::Build`, at the root of the crate's library | required |
| `[gdl] path` | a `gdl-runtime` directory | |
| `[gdl] version` | a registry version | |
| `[gdl] git`, `rev` | a repository and a revision. Both are required together. | |

`[gdl]` takes exactly one of `path`, `version`, and `git` with `rev`. `cargo gdl build` takes any of them. `compose --local`, and `k8s`, `images`, and `deploy` with `--local`, take only `path`: their images build gdl and its hub from that workspace, and the crates the spec names by path, as Git holds them on this machine. A listed package appears once, and two listed message libraries cannot share a name. A `name@version` entry is fetched, and its library must be named for its package.

A `[pool_policy.<name>]` table binds a policy name to its crate and type. It names the crate by exactly one of `path`, `version`, and `git` with `rev`, as `[nodes] crates` can. A crate named by `version` must name its library for its package. The name follows the rule for a registered pool policy below. It must not be the name of a node or an entry adapter, or a `[config]` key, since the segment builds the policy under it. Some layout's `[pools.<class>] policy` must name it. `check` reads the crate and refuses a type that the crate does not offer as a pool policy. In a spec with a `[graph]` file, it also refuses a policy whose `Head` does not fit the request that crosses into a class that names it. Each kind the policy's `NEEDS` names must be supplied to each class whose policy it is. Each segment that chooses pools of a class whose policy the table binds depends on the crate, builds the policy from its `[pool_policy.<name>]` settings table, and registers it. No other segment depends on the crate. [Write a pool policy](../guides/write-a-pool-policy.md) shows one.

A `[selector.<name>]` table binds a selector name to its crate and type, as a `[pool_policy.<name>]` table binds a policy's, and takes the same sources. The name follows the rule for a selector below, and is not `affinity` or `least_loaded`, gdl's own. It must not be the name of a node, an entry adapter, a `[config]` key, or a bound pool policy. Some layout's `[routing]` must name it. `check` reads the crate and refuses a type that the crate does not offer as a selector. In a spec with a `[graph]` file, it also refuses a selector whose `Head` does not fit the request of a crossing that names it. Each segment that sends through such a crossing depends on the crate, builds the selector from its `[selector.<name>]` settings table, and registers it. No other segment depends on the crate. [Write a selector](../guides/write-a-selector.md) shows one.

### Placement

A spec of one layout has these tables at its top level. A spec with `[layout.<name>]` tables has them only inside each layout.

| Key | Meaning | Default |
|---|---|---|
| `[segment.<name>] nodes` | the nodes the segment hosts | required |
| `[routing] <Node>` | the router of the boundary that enters the node: `"round_robin"`, `"random"`, or `{ selector = "<name>" }` | `"round_robin"` |
| `[class.<name>] segments` | the segments that deploy together | no classes |
| `[pools.<class>] policy` | how a chooser picks a pool of the class | `"round_robin"` |
| `[pools.<class>] provides` | the property kinds the class's pools serve from their settings (`[pool.properties]`), not from a node; each must be declared in the graph | none |
| `[layout.<name>.segment\|routing\|class\|pools]` | the same tables, for one layout | one layout, `default` |

| Name | Rule |
|---|---|
| segment | lowercase letters, digits, `-`, `_`; at most 58 characters |
| class, layout, selector, policy | lowercase letters and digits in words joined by single `_`; start with a letter; at most 64 characters |
| selector | never `round_robin` or `random` |
| registered pool policy | never `home_first` or `round_robin` |

A value equal to its default is not recorded. The placement hash covers each class's effective pool policy, the default included, and each class's `provides`.

### Configuration and deployment

| Key | Meaning | Default |
|---|---|---|
| `[config.<Node>] <field>` | a value for a graph-affecting field of the node's `Build::Config`: a bool, an integer, or a string. The field must exist and have that type. | the field's declared default |
| `[settings.<segment>]` | the segment's own settings: top-level keys of its settings file, such as `[node.<Node>]`, `[runtime]`, and `[system]`, which `run`, `compose`, and `k8s` write over the generated ones. Unlike `[config.<Node>]`, which sets graph-affecting `Build::Config` fields, it holds what a segment reads when it starts, and is in neither hash. Unlike `[deployment.segment.<name>] config`, it implies no deployment. It must name a segment of some layout, and cannot set `hub`, `roster`, `instances`, or `pool` | none |
| `[deployment] base_image` | the image a segment runs in | `gcr.io/distroless/cc-debian12:nonroot` |
| `[deployment] build_image` | the image every binary is built in | `rust:1.93.1-slim-bookworm` |
| `[deployment] hub_image` | the image the hub runs in | `base_image` |
| `[deployment.segment.<name>] base_image` | one segment's run image | `base_image` |
| `[deployment.segment.<name>] ports` | the ports the segment publishes, as Compose writes them. The segment's system port (9090 unless its `config` sets `[system] port`) is gdl's: `check` refuses it bare (`"9090"`). A host publication of the system port (`"127.0.0.1:9091:9090"`) is for Compose; Kubernetes declares the port as `system` and publishes nothing | none |
| `[deployment.segment.<name>] config` | top-level keys of the segment's settings file, which replace the generated ones. Laid over the segment's `[settings.<segment>]` table field by field: a value here wins, and a table in both is merged. `run` does not read it | none |
| `[deployment.segment.<name>] secrets` | the declared secrets the segment mounts, each at `/run/secrets/<secret>` | none |
| `[deployment.secrets.<secret>] file` | the secret's file on the Docker host, by absolute path. The generator never opens it. | required |
| `[entry.<name>] forward` | the forward nodes of a service entry arm, by type path | none |
| `[entry.<name>] operator` | the entry arm's operator, by type path | none |
| `[entry.<name>] target` | the named pipeline the arm ends at. An operator owns a nested arm, not a target. | none |
| `[entry.<name>] nested` | the arm under an operator, in the same keys | none |
| `[entry.<name>] backward` | the backward nodes of the arm, by type path | none |
| `[entry.<name>] needs` | the property kinds the entry's adapters read, each with the class whose pools they read it from: `needs = { model = "sets" }`. Only in the entry's own table, not a nested arm's. The source's segment must choose pools of the class, and an entry segment of the class must provide each kind, or `[pools.<class>] provides` name it. Each adapter of the entry, in nested arms too, reads the class with `FollowedPools::needed_class` ([What a node reads](../concepts/pool-routing.md#what-a-node-reads)). In neither hash: a change of it changes only what the hosting segment reads. | none |
| `[pipeline_names] <Source>` | a label for a pipeline root. Nonempty and unique, and the graph must have that source. | none |

A `[deployment]` table, or the `compose` command, makes the generated crates ask for hub discovery. A `[settings.<segment>]` table does not. A `[deployment.segment.<name>]` table must name a segment that some layout has. Image names are validated: letters, digits, and `/ _ . : @ -`. A secret's name is letters, digits, `_`, `.`, and `-`, and not `.` or `..`. Its file must be absolute and outside the bundle's build context ([Secrets](../guides/compose-deploy.md#secrets)). [Deploy with Docker Compose](../guides/compose-deploy.md) covers the deployment, and [Hosted frontends](../guides/frontends.md) covers entries.

### Binary

| Key | Meaning | Default |
|---|---|---|
| `[binary] allocator` | each segment binary's global allocator: `"system"`, `"mimalloc"`, or `"jemalloc"` | `"system"` |

The generated `main.rs` sets `#[global_allocator]` to the chosen allocator, and the segment's manifest depends on its crate and on no other allocator's: `mimalloc` 0.1 with its `no_thp` feature, or `tikv-jemallocator` 0.6. A segment's lock therefore holds only the chosen crate. `"system"` sets nothing, so a spec that names it generates the same crates as a spec that does not. Only the binary sets the allocator. A program that deploys a segment through its library keeps its own, since a library that set one would not link into a program that sets another. A node crate may not take the chosen crate's package or library name. The setting is not part of the placement or its hash: it changes how a binary is built, not what crosses a cut. The hub's binary keeps the system allocator. The images that `cargo gdl images` builds, and that `cargo gdl k8s` names, link mimalloc into each segment when the spec sets `"system"`, `"mimalloc"`, or nothing, and refuse `"jemalloc"`: jemalloc fixes the page size when it is built, and an image runs on nodes whose page size the build host does not know ([Deploy a fleet on Kubernetes](../guides/kubernetes.md#images)).

Both crates build C code with the build image's compiler.

- jemalloc's build also runs `make`, which the slim Rust images lack: the default build image, and the one an export's `Dockerfile` names. Set `[deployment] build_image` to an image that has it, such as `rust:1.93.1-bookworm`.
- jemalloc fixes its page size when it is built, to the build host's. A binary built on a host of 4 KB pages does not start on a host of 64 KB pages, such as some arm64 servers. Build it with `JEMALLOC_SYS_WITH_LG_PAGE=16` in the build's environment, which runs on both.

mimalloc has neither limit. [Known limitations](../operations/limitations.md#jemalloc-needs-make-and-the-run-hosts-page-size-at-build-time) has the workarounds.

mimalloc is built with its `no_thp` feature, so it does not ask for transparent huge pages. Without the feature, mimalloc calls `madvise(MADV_HUGEPAGE)` on its 1 GiB arena. On a host where transparent huge pages are set to `madvise` and memory is fragmented, each first touch of a 2 MB block of the arena can then run compaction in the kernel before the page fault returns. On Linux and Android, `no_thp` removes that call when mimalloc is compiled. It adds no crate to the lock. `crates/gdl-gen/tests/film.rs` checks that no mapping of a running mimalloc segment has the `hg` flag in `/proc/<pid>/smaps`.

The measurement used the serving binaries of an HTTP endpoint service, which link mimalloc 0.1.52, not generated segments. It ran on an aarch64 host under load, with transparent huge pages set to `madvise` and 19 free 2 MB blocks (order 9) in the Normal zone. The two variants started 40 times each, in alternation. Without load, a start took 85 ms. Under load, the slow starts were all system time in the engine before it joined the roster.

| mimalloc | Start to ready, median / max | Engine system time, median / max | Host compaction stalls per start, median / max |
|---|---|---|---|
| asks for huge pages (its default) | 156 ms / 6,892 ms | 30 ms / 3,070 ms | 5 / 129 |
| does not ask (`MIMALLOC_ALLOW_THP=0`) | 128 ms / 199 ms | max 10 ms | 0 / 0 |

The second row used mimalloc's runtime variable, which turns off the same `madvise` call that `no_thp` removes. Rejected: letting mimalloc ask for huge pages. A start stalled for up to 6.9 s. The throughput that huge pages gain through fewer TLB misses was not measured, and neither was whether the arena's growth after start stalls requests in the same way. Rejected: setting `MIMALLOC_ALLOW_THP=0` where the binary runs. It works only where each deployment sets it, and a binary started without it asks for huge pages again.

## fleet.toml

The fleet file lists a deployment's pools. It is not part of the placement's identity. [Classes and pools](../concepts/pools.md) and [Deploy a fleet of pools](../guides/fleet.md) explain it.

| Key | Meaning | Default |
|---|---|---|
| `[fleet] name` | the fleet's name, which labels the objects it deploys | none |
| `[cluster.<name>] context` | the kube context that reaches the cluster, not empty | required |
| `[cluster.<name>] namespace` | the namespace the fleet's objects go in | required |
| `[cluster.<name>] region` | the region of every pool in the cluster | required |
| `[cluster.<name>] registry` | the registry the cluster pulls images from, as `host[:port][/path]` with no scheme, tag, or digest, which `cargo gdl images --push` pushes to; `cargo gdl k8s` requires it | none |
| `[cluster.<name>] platform` | the platform of the cluster's nodes, as `docker buildx --platform` takes it, such as `linux/amd64`; `cargo gdl k8s` requires it, and `cargo gdl images` builds for `linux/amd64` and `linux/arm64` | none |
| `[cluster.<name>] service_account_annotations` | annotations of the pools' service account, such as a cloud role | none |
| `[cluster.<name>] secrets` | spec secret name to the name of an existing Kubernetes Secret that holds it in this cluster, under a key of the spec secret's name | none |
| `[cluster.<name>] env` | variables that every container of the cluster's pools gets | none |
| `[cluster.<name>] pins_local` | whether the cluster's pin table runs in a DynamoDB Local of its own | `false` |
| `[cluster.<name>] operator` | whether the gdl operator runs the cluster's pools: each pool is one `GdlPool`, from which the operator makes its objects, scales each segment through a `GdlScaler`, and stops the pool in order when the `GdlPool` is deleted. `cargo gdl deploy` refuses a cluster that sets it and has no `gdlpools.gdl.dev` or `gdlscalers.gdl.dev` CRD, and a cluster that changes it over live pools ([The operator](../guides/kubernetes.md#the-operator)) | `false` |
| `[cluster.<name>] node_selector` | node labels every pod of the cluster is scheduled by, beside the OS and architecture its `platform` gives | none |
| `[cluster.<name>] tolerations` | the taints every pod of the cluster tolerates, each `{ key, operator, value, effect, toleration_seconds }` as Kubernetes reads one | none |
| `[cluster.<name>.resources.default]` | every pod's `requests` and `limits`, each `{ cpu, memory }` as Kubernetes quantities in quotes (`"250m"`, `"512Mi"`): all four. Required when the cluster has `resources` | none: no pod gets resources |
| `[cluster.<name>.resources.segment.<segment>]` | the requests and limits of the pods of `<segment>`, a segment of the spec, or `hub` for the hubs, over the default, field by field | the default |
| `[pool.<name>.resources.segment.<segment>]` | the pool's own pods of `<segment>` or `hub`, over its cluster's, field by field. Refused when its cluster has no `resources` | its cluster's |
| `[metrics] pod_monitor` | each cluster's objects include a PodMonitor of the fleet's pods, for the Prometheus operator. Compose does not read it | `false` |
| `[class.<name>] ready` | segment to the usable instances a pool of the class needs to be ready | 1 per segment |
| `[class.<name>] requires` | class to the number of ready children of it that a pool's health needs | 1 for each class the pool's class sends to |
| `[pool.<name>] class` | the pool's class, a class of its layout | required |
| `[pool.<name>] cluster` | the cluster that runs the pool | none |
| `[pool.<name>] region` | the pool's region | its cluster's, and required without a cluster |
| `[pool.<name>] layout` | the spec layout the pool runs | the only layout, when there is one; required otherwise |
| `[pool.<name>] home` | the parent pool | this or `parents` is required for a class that another class sends to; refused otherwise |
| `[pool.<name>] parents` | in place of `home`, the parent class: every pool of it follows this pool | none; refused without a `[directory]` table, while the fleet's pools publish through `relay` |
| `[pool.<name>] ready` | overrides the class's quorum for a segment | the class's, else 1 |
| `[directory] backend` | where every pool's hub publishes its record: `dynamodb`, the one value. Without the table, each child's hub reports to its parent's (`relay`) | no table |
| `[directory] records_table`, `lists_table` | the DynamoDB tables' names: 3 to 255 letters, digits, `_`, `-`, and `.` | required |
| `[directory] local` | the deployment runs one DynamoDB Local for the tables, which every hub uses. Compose requires it | `false` |
| `[directory] cluster` | the cluster that runs the DynamoDB Local, which every cluster reaches. Required with `local = true` when the fleet names clusters; refused without `local = true`, and when it names no cluster of the fleet. Compose does not read it | none |

- A pool's quorum for a segment is its own `ready`, else its class's, else 1.
- An explicit `requires` replaces the default whole. It can raise a count or name a subset of the classes sent to. A count of 0, and a class that the class does not send to, are refused.
- A pool or region name is a DNS label: lowercase letters, digits, and `-`, starting with a letter, ending with a letter or digit, at most 63 characters. So are the fleet's name and a cluster's name. A cluster's `namespace` is a Kubernetes namespace, an RFC 1123 label: lowercase letters, digits, and `-`, starting and ending with a letter or digit, at most 63 characters.
- A pool's `cluster` names a `[cluster.<name>]` table, and the pool takes the cluster's region. A pool may still write `region`, and then it must be the cluster's. A pool without a cluster writes its `region`.
- No two clusters have the same `context` and `namespace`.
- The fleet file holds no secret value and no credential. A cluster's `secrets` holds only Kubernetes Secret names: lowercase letters, digits, `-`, and `.`, starting and ending with a letter or digit, at most 253 characters. `check` does not judge its keys against the spec's `[deployment.secrets]` tables. `cargo gdl k8s` refuses a key that no such table declares, and a secret that a pool's segment mounts and the pool's cluster does not map. A kube context's credentials stay in the kubeconfig. Do not put a secret value in `env`. An `env` key is letters, digits, and `_`, not starting with a digit.
- A `service_account_annotations` key is a Kubernetes annotation key: an optional DNS subdomain and `/`, then at most 63 letters, digits, `-`, `_`, and `.`, starting and ending with a letter or digit.
- A cluster's placement is checked as Kubernetes checks it. A `node_selector` key is a label key, as an annotation key is, and a value a label value: at most 63 letters, digits, `-`, `_`, and `.`, starting and ending with a letter or digit. `kubernetes.io/os` and `kubernetes.io/arch` are refused: the cluster's `platform` sets them. A toleration's `operator` is `Equal` or `Exists`, `Equal` when unsaid; `Exists` takes no `value`, and an empty `key` needs `Exists`. Its `effect` is `NoSchedule`, `PreferNoSchedule`, `NoExecute`, or unsaid, for every effect, and `toleration_seconds` goes with `NoExecute` only.
- A quantity is a non-negative decimal number and a suffix: binary (`Ki` to `Ei`), decimal (`n`, `u`, `m`, `k`, `M` to `E`), or an exponent (`e3`). Quantities compare by value, exactly, so `"1"` is `"1000m"` and `"0.0009765625Gi"` is `"1Mi"`. A quantity that Kubernetes would change is refused: one finer than `1m` of its unit, which it rounds up, and one above 9223372036854775807 of its unit, such as `"8Ei"` or `"10E"`, which it caps, since either could put a limit below its request unseen. Every pod's merged requests and limits, its pool's over its cluster's for its segment over the default, has no limit below its request. A `resources.segment` key that names no segment of the spec, or of the pool's class for a pool, and is not `hub`, is refused. The DynamoDB directory's Local and tables Job take the default.
- `cargo gdl k8s` requires `[fleet] name`, a `cluster` for every pool, and a `registry` and `platform` for every cluster. It refuses an `env` key that names a static AWS credential (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_SECURITY_TOKEN`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`), an `env` key that is `GDL_POD_IP`, `GDL_POOL`, or `GDL_HUB`, or starts with `GDL_POOL__` or `GDL_HUB__`, in any case, and a child in another cluster than its parent. See [Deploy a fleet on Kubernetes](../guides/kubernetes.md).
- `cargo gdl compose` reads only a cluster's region: not its placement or resources. A fleet whose pools take their regions from clusters writes the same bundle as one whose pools write the same regions.
- A `home` names a pool of a parent class that runs the same layout. A `parents` names a class that sends to the pool's class in its layout. A pool names one of the two. Every pool whose class sends to another class needs a child of each class it sends to, in its layout: a pool whose `home` it is, or whose `parents` is its class.
- Without a `[directory]` table a fleet's pools publish through `relay`, so `check` refuses a pool with `parents`. With one, every parent's hub reads every record, and `check` takes it.
- A `ready` key must be a segment of the class, and a count is at least 1.
- A `[class.<name>]` table applies to the class's pools in every layout with the class, and is checked against each.
- With `[directory]`, each pool's settings name the `dynamodb` backend, with the pool's `region` as `aws_region`. A Compose bundle adds the endpoint of its DynamoDB Local.

## A pool's settings

The fleet file fills in each pool's `[pool]` table in its segments' settings. The same keys are read from `GDL_POOL__<KEY>` variables, with `__` between nested keys. [Segment settings](../operations/configuration.md) covers the rest of a segment's settings.

| Key | Meaning | Default |
|---|---|---|
| `name`, `class`, `region` | the pool's name, class, and region | required |
| `layout` | the layout the pool runs. Read and checked for its shape. Nothing else in an instance or hub reads it. | none |
| `home` | the parent pool | none |
| `parents` | in place of `home`, the parent class: every pool of it follows this pool. Refused with `home`. A hub that publishes through `relay` refuses it. | none |
| `directory` | where pool records are published: `relay`, `memory` (one directory per process, for tests), `dir`, or `dynamodb` (a hub built with its `dynamodb` feature) | `relay` |
| `parent_hub` | the parent pool's hub, `host:port`, where the pool's hub reports its record. With `relay`, a hub of a class that another class sends to needs it. Refused for a class that nothing sends to, and with the other backends. A fleet bundle sets it to `<home>:1337`. | none |
| `state_dir` | where the pool's hub keeps the records its children report, across its restart. With `relay`, a hub of a class that sends to another class needs it. Refused for a class that sends to none, and with the other backends. Only the hub reads it. A fleet bundle sets it to `/var/lib/gdl-hub`, on a volume of the hub's own. | none |
| `dir` | the shared directory. Required with `directory = "dir"` and refused with the others. | none |
| `records_table`, `lists_table` | the DynamoDB tables. Required with `directory = "dynamodb"` and refused with the others. Only the hub reads them. | none |
| `aws_region` | the AWS region whose copy of the tables the hub uses. Required with `directory = "dynamodb"` and refused with the others. Only the hub reads it. | none |
| `endpoint` | a DynamoDB Local, `http://host:port`, which the hub uses instead of AWS, with fixed test credentials. Only with `directory = "dynamodb"`. Only the hub reads it. | none |
| `hub` | the address, `host:port`, at which other pools' hubs reach this pool's hub. Only the hub reads it. | none |
| `summary_ms` | the load summary's period. Least 1000. | 10000 |
| `ready.<segment>` | the quorum. A segment it does not name needs 1. | 1 |
| `requires.<class>` | the ready pools of a class that the pool's health needs. Only the hub reads it. | 1 for each class sent to |
| `hold_ms` | how long every quorum must hold before the pool is `ready`. Only the hub reads it. | 2000 |
| `heartbeat_ms` | how often the hub publishes the pool's record while its state does not change. At least 1. Only the hub reads it. | 10000 |
| `lease_ms` | how long a reader keeps the record without seeing it published again. At least 1, longer than `heartbeat_ms`, and at least 300 under the `relay` backend. Only the hub reads it. | 30000 |
| `watch_buffer` | events a watcher may fall behind by before its watch ends. From 1 to 65536. Only the hub reads it. | 1024 |
| `lost_chooser_ms` | how long a chooser may have no watch of the hub's relay open before the hub stops waiting for its acknowledgements. At least 1. Only the hub reads it. | 30000 |
| `watch_caps.pool`, `watch_caps.relay`, `watch_caps.follow` | how many watches of the pool, of the relay, and of one follow the hub serves at once. At least 1. Only the hub reads them. | 2048, 2048, 16384 |
| `keyed_capacity` | how many records the hub's keyed record table holds. At least 1. Only the hub reads it. | 1000000 |
| `[properties]` | the properties the pool serves when its entry instances report none of a kind: for each kind, a list of keys, each a table of the kind's own fields, `shard = [{ name = "a" }, { name = "b", partition = "p" }]`. A kind is a settings key, each key is a table whose numbers are integers, and no key is declared twice in a kind, also with its fields in another order. gdl does not check a key's fields against its type: a reader that decodes the kind leaves out a key that does not decode. Refused for a class that nothing sends to, and for a kind that the class's `[pools.<class>] provides` does not name. Only the hub reads it. As a variable for each kind: `GDL_POOL__PROPERTIES__SHARD='[{name = "a"}]'`. | none |

An instance refuses to start when its `[pool]` does not fit its deployment: the placement has no classes, the class is not one of them, the instance's segment is not in it, a `ready` key is not a segment of it, or a `requires` key is not a class it sends to or is 0. An instance and a hub both refuse a hub timing out of its bounds, though only the hub runs with it. Each problem names where the key was set, the file and key or the variable.

## Constants

| Constant | Value | In |
|---|---|---|
| `HUB_PUBLIC_PORT` | 1337 | `gdl_model` |
| `HUB_CONTROL_PORT` | 8337 | `gdl_model` |
| `DEFAULT_DEADLINE` | 600 s: a request's deadline when it has none, and so the default drain bound | `gdl_model` |
| `SEGMENT_NAME_LIMIT` | 58 | `gdl_model` |

### Defaults

Every default of the spec's placement and deployment tables, the fleet file, and a pool's settings is a named constant. A test in `gdl-gen` checks this table against the constants.

| Key | Default | Constant |
|---|---|---|
| spec `[routing] <node>` | `"round_robin"` | `RequestRouter::DEFAULT` |
| spec `[pools.<class>] policy` | `"round_robin"` | `PoolPolicy::DEFAULT_POLICY` |
| spec `[deployment] base_image` | `"gcr.io/distroless/cc-debian12:nonroot"` | `DEFAULT_BASE_IMAGE` |
| spec `[deployment] build_image` | `"rust:1.93.1-slim-bookworm"` | `DEFAULT_BUILD_IMAGE` |
| spec `[binary] allocator` | `"system"` | `Allocator::DEFAULT` |
| fleet `ready.<segment>`, `[pool] ready.<segment>` | `1` | `PoolSettings::DEFAULT_QUORUM` |
| fleet `requires.<class>`, `[pool] requires.<class>`, for each class the pool's class sends to, when `requires` is unsaid | `1` | `PoolSettings::DEFAULT_REQUIRES` |
| fleet `[cluster.<name>] pins_local` | `false` | `Cluster::DEFAULT_PINS_LOCAL` |
| `[pool] directory` | `"relay"` | `DirectoryBackend::DEFAULT` |
| `[pool] summary_ms` | `10000` | `PoolSettings::DEFAULT_SUMMARY` |
| `[pool] summary_ms`, its least | `1000` | `PoolSettings::MIN_SUMMARY` |
| `[pool] hold_ms` | `2000` | `HubTiming::DEFAULT_HOLD` |
| `[pool] heartbeat_ms` | `10000` | `HubTiming::DEFAULT_HEARTBEAT` |
| `[pool] lease_ms` | `30000` | `HubTiming::DEFAULT_LEASE` |
| `[pool] lease_ms`, its least under `relay` | `300` | `HubTiming::MIN_RELAY_LEASE` |
| `[pool] watch_buffer` | `1024` | `HubTiming::DEFAULT_WATCH_BUFFER` |
| `[pool] lost_chooser_ms` | `30000` | `HubTiming::DEFAULT_LOST_CHOOSER` |
| `[pool] watch_caps.pool` | `2048` | `WatchCaps::DEFAULT_POOL` |
| `[pool] watch_caps.relay` | `2048` | `WatchCaps::DEFAULT_RELAY` |
| `[pool] watch_caps.follow` | `16384` | `WatchCaps::DEFAULT_FOLLOW` |
| `[pool] keyed_capacity` | `1000000` | `HubTiming::DEFAULT_KEYED_CAPACITY` |

The constants are `gdl_model`'s, and the two images are the generator's (`gdl-gen`). Every other optional key has no value unless it is set.
