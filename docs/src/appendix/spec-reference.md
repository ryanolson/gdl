# Spec and fleet reference

This appendix lists every table and key of `gdl.toml` (the spec), `fleet.toml` (the fleet file), and a pool's settings. It is derived from the parsing code in `gdl-gen` and `gdl-model`. Every table refuses a key it does not know. A table whose keys are names (`ready`, `requires`, `[config]`, `pin`) refuses a name that is not in the spec. Paths in a spec are relative to the spec file. [Specs, segments, and layouts](../concepts/placement.md) explains the placement. [Split, generate, and draw](../guides/split-and-generate.md) shows a spec at work.

## gdl.toml

A spec names the graph with `[contract]` or with `[graph]`, never both. `[messages]` goes with `[graph]`. `[nodes]` goes with `[graph]`, and `[bind.*]` goes with `[contract]`. `[pool_policy.*]` and `[binary]` go with either. `[gdl]` is always required.

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
| `[gdl] path` | a `gdl-runtime` directory | |
| `[gdl] version` | a registry version | |
| `[gdl] git`, `rev` | a repository and a revision. Both are required together. | |

`[gdl]` takes exactly one of `path`, `version`, and `git` with `rev`. `cargo gdl build` takes any of them. `compose --local` takes only `path`. A listed package appears once, and two listed message libraries cannot share a name. A `name@version` entry is fetched, and its library must be named for its package.

A `[pool_policy.<name>]` table binds a policy name to its crate and type. It names the crate by exactly one of `path`, `version`, and `git` with `rev`, as `[nodes] crates` can. A crate named by `version` must name its library for its package. The name follows the rule for a registered pool policy below. It must not be the name of a node or an entry adapter, or a `[config]` key, since the segment builds the policy under it. Some layout's `[pools.<class>] policy` must name it. `check` reads the crate and refuses a type that the crate does not offer as a pool policy. In a spec with a `[graph]` file, it also refuses a policy whose `Head` does not fit the request that crosses into a class that names it. Each segment that chooses pools of a class whose policy the table binds depends on the crate, builds the policy from its `[pool_policy.<name>]` settings table, and registers it. No other segment depends on the crate. [Write a pool policy](../guides/write-a-pool-policy.md) shows one.

### Placement

A spec of one layout has these tables at its top level. A spec with `[layout.<name>]` tables has them only inside each layout.

| Key | Meaning | Default |
|---|---|---|
| `[segment.<name>] nodes` | the nodes the segment hosts | required |
| `[routing] <Node>` | the router of the boundary that enters the node: `"round_robin"`, `"random"`, or `{ selector = "<name>" }` | `"round_robin"` |
| `[class.<name>] segments` | the segments that deploy together | no classes |
| `[pools.<class>] policy` | how a chooser picks a pool of the class | `"round_robin"` |
| `[layout.<name>.segment\|routing\|class\|pools]` | the same tables, for one layout | one layout, `default` |

| Name | Rule |
|---|---|
| segment | lowercase letters, digits, `-`, `_`; at most 58 characters |
| class, layout, selector, policy | lowercase letters and digits in words joined by single `_`; start with a letter; at most 64 characters |
| selector | never `round_robin` or `random` |
| registered pool policy | never `home_first` or `round_robin` |

A value equal to its default is not recorded. The placement hash covers each class's effective pool policy, the default included.

### Configuration and deployment

| Key | Meaning | Default |
|---|---|---|
| `[config.<Node>] <field>` | a value for a graph-affecting field of the node's `Build::Config`: a bool, an integer, or a string. The field must exist and have that type. | the field's declared default |
| `[deployment] base_image` | the image a segment runs in | `gcr.io/distroless/cc-debian12:nonroot` |
| `[deployment] build_image` | the image every binary is built in | `rust:1.93.1-slim-bookworm` |
| `[deployment] hub_image` | the image the hub runs in | `base_image` |
| `[deployment.segment.<name>] base_image` | one segment's run image | `base_image` |
| `[deployment.segment.<name>] ports` | the ports the segment publishes, as Compose writes them | none |
| `[deployment.segment.<name>] config` | top-level keys of the segment's settings file, which replace the generated ones | none |
| `[deployment.segment.<name>] secrets` | the declared secrets the segment mounts, each at `/run/secrets/<secret>` | none |
| `[deployment.secrets.<secret>] file` | the secret's file on the Docker host, by absolute path. The generator never opens it. | required |
| `[entry.<name>] forward` | the forward nodes of a service entry arm, by type path | none |
| `[entry.<name>] operator` | the entry arm's operator, by type path | none |
| `[entry.<name>] target` | the named pipeline the arm ends at. An operator owns a nested arm, not a target. | none |
| `[entry.<name>] nested` | the arm under an operator, in the same keys | none |
| `[entry.<name>] backward` | the backward nodes of the arm, by type path | none |
| `[pipeline_names] <Source>` | a label for a pipeline root. Nonempty and unique, and the graph must have that source. | none |

A `[deployment]` table, or the `compose` command, makes the generated crates ask for hub discovery. A `[deployment.segment.<name>]` table must name a segment that some layout has. Image names are validated: letters, digits, and `/ _ . : @ -`. A secret's name is letters, digits, `_`, `.`, and `-`, and not `.` or `..`. Its file must be absolute and outside the bundle's build context ([Secrets](../guides/compose-deploy.md#secrets)). [Deploy with Docker Compose](../guides/compose-deploy.md) covers the deployment, and [Hosted frontends](../guides/frontends.md) covers entries.

### Binary

| Key | Meaning | Default |
|---|---|---|
| `[binary] allocator` | each segment binary's global allocator: `"system"`, `"mimalloc"`, or `"jemalloc"` | `"system"` |

The generated `main.rs` sets `#[global_allocator]` to the chosen allocator, and the segment's manifest depends on its crate and on no other allocator's: `mimalloc` 0.1, or `tikv-jemallocator` 0.6. A segment's lock therefore holds only the chosen crate. `"system"` sets nothing, so a spec that names it generates the same crates as a spec that does not. Only the binary sets the allocator. A program that deploys a segment through its library keeps its own, since a library that set one would not link into a program that sets another. A node crate may not take the chosen crate's package or library name. The setting is not part of the placement or its hash: it changes how a binary is built, not what crosses a cut. The hub's binary keeps the system allocator.

Both crates build C code with the build image's compiler.

- jemalloc's build also runs `make`, which the slim Rust images lack: the default build image, and the one an export's `Dockerfile` names. Set `[deployment] build_image` to an image that has it, such as `rust:1.93.1-bookworm`.
- jemalloc fixes its page size when it is built, to the build host's. A binary built on a host of 4 KB pages does not start on a host of 64 KB pages, such as some arm64 servers. Build it with `JEMALLOC_SYS_WITH_LG_PAGE=16` in the build's environment, which runs on both.

mimalloc has neither limit. [Known limitations](../operations/limitations.md#jemalloc-needs-make-and-the-run-hosts-page-size-at-build-time) has the workarounds.

## fleet.toml

The fleet file lists a deployment's pools. It is not part of the placement's identity. [Classes and pools](../concepts/pools.md) and [Deploy a fleet of pools](../guides/fleet.md) explain it.

| Key | Meaning | Default |
|---|---|---|
| `[class.<name>] ready` | segment to the usable instances a pool of the class needs to be ready | 1 per segment |
| `[class.<name>] requires` | class to the number of ready children of it that a pool's health needs | 1 for each class the pool's class sends to |
| `[pool.<name>] class` | the pool's class, a class of its layout | required |
| `[pool.<name>] region` | the pool's region | required |
| `[pool.<name>] layout` | the spec layout the pool runs | the only layout, when there is one; required otherwise |
| `[pool.<name>] home` | the parent pool | required for a class that another class sends to; refused otherwise |
| `[pool.<name>] ready` | overrides the class's quorum for a segment | the class's, else 1 |

- A pool's quorum for a segment is its own `ready`, else its class's, else 1.
- An explicit `requires` replaces the default whole. It can raise a count or name a subset of the classes sent to. A count of 0, and a class that the class does not send to, are refused.
- A pool or region name is a DNS label: lowercase letters, digits, and `-`, starting with a letter, ending with a letter or digit, at most 63 characters.
- A `home` names a pool of a parent class that runs the same layout. Every pool whose class sends to another class needs a child of each class it sends to, in its layout.
- A `ready` key must be a segment of the class, and a count is at least 1.
- A `[class.<name>]` table applies to the class's pools in every layout with the class, and is checked against each.

## A pool's settings

The fleet file fills in each pool's `[pool]` table in its segments' settings. The same keys are read from `GDL_POOL__<KEY>` variables, with `__` between nested keys. [Segment settings](../operations/configuration.md) covers the rest of a segment's settings.

| Key | Meaning | Default |
|---|---|---|
| `name`, `class`, `region` | the pool's name, class, and region | required |
| `layout` | the layout the pool runs. Read and checked for its shape. Nothing else in an instance or hub reads it. | none |
| `home` | the parent pool | none |
| `directory` | where pool records are published: `relay`, `memory` (one directory per process, for tests), or `dir` | `relay` |
| `parent_hub` | the parent pool's hub, `host:port`, where the pool's hub reports its record. With `relay`, a hub of a class that another class sends to needs it. Refused for a class that nothing sends to, and with the other backends. A fleet bundle sets it to `<home>:1337`. | none |
| `state_dir` | where the pool's hub keeps the records its children report, across its restart. With `relay`, a hub of a class that sends to another class needs it. Refused for a class that sends to none, and with the other backends. Only the hub reads it. A fleet bundle sets it to `/var/lib/gdl-hub`, on a volume of the hub's own. | none |
| `dir` | the shared directory. Required with `directory = "dir"` and refused with the others. | none |
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
| `models` | the models the pool serves when its entry instances report none of their own: a list of `{ model = "<name>", routing_key = "<key>" }`, `routing_key` optional. A model needs a name, a routing key is not empty, and no model is named twice. Refused for a class that nothing sends to. Only the hub reads it. As a variable: `GDL_POOL__MODELS='[{model = "llama-70b"}]'`. | none |

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
