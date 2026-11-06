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
| `[selector.<name>] crate` | the package of a selector that a `[routing]` router names, for a crossing or for a call that leaves its caller's segment | required |
| `[selector.<name>] path`, `version`, `git`, `rev`, `lib` | the crate's source, as for `[pool_policy.<name>]` | one source is required |
| `[selector.<name>] type` | the type that implements `gdl::Selector` and `gdl::Build`, at the root of the crate's library | required |
| `[gdl] path` | a `gdl-runtime` directory | |
| `[gdl] version` | a registry version | |
| `[gdl] git`, `rev` | a repository and a revision. Both are required together. | |

`[gdl]` takes exactly one of `path`, `version`, and `git` with `rev`. `cargo gdl build` takes any of them. `compose --local`, and `k8s`, `images`, and `deploy` with `--local`, take only `path`: their images build gdl and its hub from that workspace, and the crates the spec names by path, as Git holds them on this machine. A listed package appears once, and two listed message libraries cannot share a name. A `name@version` entry is fetched, and its library must be named for its package.

A `[pool_policy.<name>]` table binds a policy name to its crate and type. It names the crate by exactly one of `path`, `version`, and `git` with `rev`, as `[nodes] crates` can. A crate named by `version` must name its library for its package. The name follows the rule for a registered pool policy below. It must not be the name of a node or an entry adapter, or a `[config]` key, since the segment builds the policy under it. Some layout's `[pools.<class>] policy` must name it. `check` reads the crate and refuses a type that the crate does not offer as a pool policy. In a spec with a `[graph]` file, it also refuses a policy whose `Head` does not fit the request that crosses into a class that names it. Each kind the policy's `NEEDS` names must be supplied to each class whose policy it is. Each segment that chooses pools of a class whose policy the table binds depends on the crate, builds the policy from its `[pool_policy.<name>]` settings table, and registers it. No other segment depends on the crate. [Write a pool policy](../guides/write-a-pool-policy.md) shows one.

A `[selector.<name>]` table binds a selector name to its crate and type, as a `[pool_policy.<name>]` table binds a policy's, and takes the same sources. The name follows the rule for a selector below, and is not `affinity` or `least_loaded`, gdl's own. It must not be the name of a node, an entry adapter, a `[config]` key, or a bound pool policy. Some layout's `[routing]` must name it. `check` reads the crate and refuses a type that the crate does not offer as a selector. In a spec with a `[graph]` file, it also refuses a selector whose `Head` does not fit the request of a crossing that names it, or the request of a unary node whose calls it routes. Each segment that sends through such a crossing, or makes such calls to another segment, depends on the crate, builds the selector from its `[selector.<name>]` settings table, and registers it. No other segment depends on the crate. [Write a selector](../guides/write-a-selector.md) shows one.

### Placement

A spec of one layout has these tables at its top level. A spec with `[layout.<name>]` tables has them only inside each layout.

| Key | Meaning | Default |
|---|---|---|
| `[segment.<name>] nodes` | the nodes the segment hosts. A node of the topology is in exactly one segment; a unary node may be in several | required |
| `[routing] <Node>` | the router of the boundary that enters the node, or of the calls to a unary node from other segments: `"round_robin"`, `"random"`, or `{ selector = "<name>" }` | `"round_robin"` |
| `[class.<name>] segments` | the segments that deploy together | no classes |
| `[pools.<class>] policy` | how a chooser picks a pool of the class | `"round_robin"` |
| `[pools.<class>] provides` | the property kinds the class's pools serve from their settings (`[pool.properties]`), not from a node; each must be declared in the graph | none |
| `[codec] "<from> -> <to>"` | the codec of the edge from `<from>` to `<to>`, in the direction its data flows, for the messages on it that pin none: `"msgpack_zstd"`, or a table of `codec` (every message on the edge), or `head` and `item` (a streamed request's parts), and `floor` (bytes) | no codec |
| `[codec] <Unary>` | the codec of the calls to a unary node that cross segments, from every caller: a table of `request`, `reply`, or both, and `floor` (bytes), for both parts | no codec |
| `[layout.<name>.segment\|routing\|class\|pools\|codec]` | the same tables, for one layout | one layout, `default` |

| Name | Rule |
|---|---|
| segment | lowercase letters, digits, `-`, `_`; at most 58 characters |
| class, layout, selector, policy | lowercase letters and digits in words joined by single `_`; start with a letter; at most 64 characters |
| selector | never `round_robin` or `random` |
| registered pool policy | never `home_first` or `round_robin` |

A value equal to its default is not recorded. The placement hash covers each class's effective pool policy, the default included, each class's `provides`, and each `[codec]` entry without its `floor`.

A `[codec]` entry names one edge, and each direction of a pair of nodes is its own edge: `"Engine -> Detokenize"` carries the engine's items, and `"Truncate -> Engine"` its request. `msgpack_zstd` sends a message as named MessagePack, and one at least `floor` bytes long (16 KiB by default) as one zstd frame at level 1, when that is smaller. `check` refuses an entry:

- on a message that pins its codec with `#[gdl(codec = "...")]`, `msgpack` included, naming the edge, the message, and the pin;
- in string form on a streamed request whose head or items are pinned: name the other part alone, `{ item = "msgpack_zstd" }`;
- with `head` or `item` on an edge whose request is not streamed;
- whose key names no edge of the graph, or an edge that the layout does not encode, because it does not cross segments and is not carried by an anchor ([The codec rule](../concepts/crossings.md#the-codec-rule)).

A key without an arrow names a unary node, and its entry is for the node's calls: `Tokenize = { request = "msgpack_zstd", reply = "msgpack_zstd" }`. Each part is optional, and one `floor` covers both. `check` refuses a unary node's entry:

- in string form, or with `codec`, `head`, or `item`: it names `request` and `reply`. An edge's entry cannot name them;
- on a part whose message pins its codec: the request's `Req` or the reply's `Resp`, naming the unary node, the part, the message, and the pin. An entry that names both parts says which one could be named alone;
- whose key names no unary node of the graph;
- for a unary node that no caller calls in another segment. A call to the copy in the caller's own segment never encodes.

[Edge codecs](../concepts/crossings.md#edge-codecs) says how a compressed message crosses, and [A call's codec](../concepts/unary.md#a-calls-codec) how a compressed call does.

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
| `[deployment.codegen.<arch>] target_features` | CPU features that `cargo gdl images` builds the segment images of architecture `<arch>` (`amd64` or `arm64`) for, as `-C target-feature` takes them. Only `["+lse"]` for `arm64` | none |
| `[entry.<name>] forward` | the forward nodes of a service entry arm, by type path | none |
| `[entry.<name>] operator` | the entry arm's operator, by type path | none |
| `[entry.<name>] target` | the named pipeline the arm ends at. An operator owns a nested arm, not a target. | none |
| `[entry.<name>] nested` | the arm under an operator, in the same keys | none |
| `[entry.<name>] backward` | the backward nodes of the arm, by type path | none |
| `[entry.<name>] needs` | the property kinds the entry's adapters read, each with the class whose pools they read it from: `needs = { model = "sets" }`. Only in the entry's own table, not a nested arm's. The source's segment must choose pools of the class, and an entry segment of the class must provide each kind, or `[pools.<class>] provides` name it. Each adapter of the entry, in nested arms too, reads the class with `FollowedPools::needed_class` ([What a node reads](../concepts/pool-routing.md#what-a-node-reads)). In neither hash: a change of it changes only what the hosting segment reads. | none |
| `[pipeline_names] <Source>` | a label for a pipeline root. Nonempty and unique, and the graph must have that source. | none |

A `[deployment]` table, or the `compose` command, makes the generated crates ask for hub discovery. A `[settings.<segment>]` table does not. A `[deployment.segment.<name>]` table must name a segment that some layout has. Each image a key names, here or on a Slurm site, is an image reference, `[host[:port]/]path[:tag][@digest]`, which `check` reads as Docker, containerd, and Kubernetes do: gdl's rule is a port of the parser they share, `github.com/distribution/reference` v0.6.0, with its digests checked as `github.com/opencontainers/go-digest` v1.0.0 checks them, and is tested with those packages' own cases. Moving to a later version of either changes this rule. The first part, before a `/`, names the host when it has a `.` or a `:`, is `localhost`, or has an uppercase letter; otherwise the host is Docker Hub's, `docker.io`, and a path of one part, such as `debian`, is `library/debian`. A host is a name of letters, digits, and `-` joined by `.`, or an IPv6 address in `[]`, with an optional port. A first part that is not one, such as `registry_dev.example.com`, is read as the first part of the path. The path is one or more parts joined by `/`, each lowercase letters and digits joined by `.`, `_`, `__`, or runs of `-`, and is at most 255 characters, not counting the host but counting Docker Hub's `library/`. A tag is 1 to 128 letters, digits, `_`, `.`, and `-`, not starting with `.` or `-`. A digest is `sha256:`, `sha384:`, or `sha512:` and its 64, 96, or 128 lowercase hex digits. `check` refuses an image of another form, such as `:`, `debian:`, or `Debian`, and names the key and why. A secret's name is letters, digits, `_`, `.`, and `-`, and not `.` or `..`. Its file must be absolute and outside the bundle's build context ([Secrets](../guides/compose-deploy.md#secrets)). A `[deployment.codegen.<arch>]` table is off unless a spec names it, and is chosen per deployment: a binary built for a CPU feature faults on a CPU without it. Its key is an architecture as a cluster's `platform` names it, `amd64` or `arm64`. It sets `target_features`. A feature is one `+` or `-` and a name of letters, digits, `.`, `_`, and `-` that starts with a letter or digit. A build image that sets `RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS` cannot build an image with a table: the build step stops, since it cannot tell which of rustc's flags set LTO. Drop the table, or move the flags into the build image's `RUSTC_WRAPPER` or `RUSTC_WORKSPACE_WRAPPER`, which the build trusts, as it trusts `RUSTC`. `check` refuses another key, an empty table, a name of another shape, and any feature but `+lse` for `arm64`: a new feature needs its own row in `scripts/qemu-startcheck.sh` ([A table names only proven features](../development/codegen-design.md#a-table-names-only-proven-features)). The table has no `target_cpu` ([Target features design](../development/codegen-design.md)). Only `cargo gdl images` builds with the table. `compose` and `export` refuse a spec that has one ([Target features](../guides/kubernetes.md#target-features)). [Deploy with Docker Compose](../guides/compose-deploy.md) covers the deployment, and [Hosted frontends](../guides/frontends.md) covers entries.

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

glibc's malloc asks for huge pages too, on AArch64 from glibc 2.43, under every allocator. Its release notes say that it turns on 2 MB transparent huge pages by default there, as the tunable `glibc.malloc.hugetlb=1` does. malloc then calls `madvise(MADV_HUGEPAGE)` on its heap (`[heap]`) and on each thread's arena. A mimalloc segment has these arenas too, since its threads make them. On an aarch64 host with Linux 7.0, transparent huge pages and defrag both set to `madvise`, and glibc 2.43, each film segment held this much in huge pages once it was ready:

| Film segment | Allocator | `AnonHugePages` at ready, glibc's default | With `glibc.malloc.hugetlb=0` |
|---|---|---|---|
| `front` | system | 108,544 kB | 0 kB |
| `engine` | system | 108,544 kB | 0 kB |
| `accounting` | system | 108,544 kB | 0 kB |
| `all` | mimalloc | 98,304 kB | 0 kB |

That is about 50 faults of a 2 MB page in each start, on the same path in the kernel as mimalloc's arena: each fault can compact memory before it returns. Stalls were not timed on fragmented memory.

glibc reads its tunables from the environment once, before `main`, so a running process cannot turn this off. So each launcher that gdl renders gives a segment process `GLIBC_TUNABLES` with `glibc.malloc.hugetlb=0`:

- `cargo gdl run`, for each segment it starts;
- `cargo gdl compose`, through each segment image's `ENV` line alone: Compose ranks a user's `env_file` and `environment` above an image's `ENV`, so a service of gdl's that set the variable would replace the user's value;
- `cargo gdl k8s`, in each segment container's `env`, and the gdl operator, in the pods it makes from a `GdlPool`;
- a pool's DGD, in the `main` container of each component that runs a segment, where the pool's objects are rendered: by `cargo gdl k8s`, and by the operator from a `GdlPool`, also one written by hand;
- `cargo gdl slurm`, in the preamble of each segment service of a pool's job and a joining job, which sets it only when the job's environment has none: a variable of the recipe's would replace the user's;
- the image of each segment that `images`, `compose`, and `export` build, as an `ENV` line.

A user who sets `GLIBC_TUNABLES` owns it. When the launcher's environment has no `GLIBC_TUNABLES`, the launcher sets it to exactly `glibc.malloc.hugetlb=0`. When the user set it, in any form, the launcher passes it on byte for byte and adds nothing. The user's value can reach a launcher as a cluster's `env`, as the `env` of a DGD's `main` container, or in the environment of `cargo gdl run`. An entry that takes its value from elsewhere (`valueFrom`) counts as set. A null `valueFrom` beside a `value` counts as that value. The image's `ENV` line is replaced by a launcher's own variable, as Docker and Kubernetes replace it. `gdl_model::segment_tunables` holds the rule, and every launcher calls it. A hub gets only what its cluster gives it. The setting goes on every architecture. glibc before 2.35 ignores the tunable, and glibc's default on x86_64 is already 0.

`cargo gdl k8s` and `cargo gdl deploy` print one warning for each user's `GLIBC_TUNABLES` that they can read and that does not mention `glibc.malloc.hugetlb`: a cluster's `env`, and the `main` container of each DGD component that runs a segment. The warning says that gdl added nothing, and that the value must include `glibc.malloc.hugetlb=0` to keep the start from stalling. It checks the text only, since it is advice. An entry with a `valueFrom` gets none.

Rejected: adding `glibc.malloc.hugetlb=0` to a user's own list. Three review passes each found a list on which the edit went wrong, because it had to read the list as glibc's loader does. glibc refuses some lists as a whole and applies none of their entries, as for a `:` at the end, so an added entry was lost with the rest. Dropping malformed entries also dropped an empty value that resets a tunable. Numbers such as `+1` and ` 1`, and numbers that overflow, read differently in gdl and in glibc. On glibc 2.39, adding one entry to a list of 32 valid repeated entries crashed the loader, while the same list without the entry ran.

`crates/gdl-gen/tests/film.rs` starts its segments with the variable from the same function, and checks that no mapping of a running segment has the `hg` flag in `/proc/<pid>/smaps`. `every_launcher_gives_a_segment_the_glibc_tunables` in `crates/gdl-gen/tests/deployment/k8s.rs` checks that the launchers set it.

Rejected: a segment binary that runs itself again with the tunable set, when the variable does not name it. A review found six defects in it:

- Under secure execution, such as a binary with a file capability like `cap_net_bind_service`, glibc removes `GLIBC_TUNABLES` before `main`. The second run cannot see that the first set the variable, so the binary runs itself again without end.
- A binary replaced on disk between the check and the second run is not the binary that started.
- A binary started through a symbolic link takes the link target's name in `/proc/<pid>/comm`, so `pkill -x` and other stops by name miss it.
- When the second run fails, the binary runs on with `SIGPIPE` reset to its default action.
- A bare tunable name stopped the second run, although glibc reads no value from it.
- The code compiled on other targets but did nothing there, so `-D warnings` refused it.

On a host with glibc 2.39, which does not ask for huge pages, it still ran the binary twice, at a cost of about 11 ms a start. Rejected: `prctl(PR_SET_THP_DISABLE)`. It turns off huge pages for the whole process, so a node that asks for them with `madvise` gets none. The process's children inherit it too.

## fleet.toml

The fleet file lists a deployment's pools. It is not part of the placement's identity. [Classes and pools](../concepts/pools.md) and [Deploy a fleet of pools](../guides/fleet.md) explain it.

| Key | Meaning | Default |
|---|---|---|
| `[fleet] name` | the fleet's name, which labels the objects it deploys | none |
| `[cluster.<name>] kind` | which kind of cluster runs the pools: `"kubernetes"` or `"slurm"`. Each kind takes only its own keys, below, and refuses the other kind's by name. A Kubernetes cluster is written without it | `"kubernetes"` |
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
| `[cluster.<name>] region`, on a Slurm site | the region of every pool on the site | required |
| `[cluster.<name>] srtctl`, on a Slurm site | the srt-slurm checkout that submits the jobs, an absolute path on the login node. Its `srtslurm.yaml` holds the account, the default partition, and the network interface, which stay out of the fleet file | required |
| `[cluster.<name>] run_dir`, on a Slurm site | the directory of the rendered recipes and settings, an absolute path on a file system the login node and the compute nodes share | required |
| `[cluster.<name>] registry`, on a Slurm site | the registry the site's nodes pull images from, as a Kubernetes cluster's | required: `check` refuses a site without it |
| `[cluster.<name>] platform`, on a Slurm site | the platform of the site's nodes, as a Kubernetes cluster's. A `[deployment.codegen.<arch>]` table for its architecture reaches its segment images, as a cluster's | required: `check` refuses a site without it |
| `[cluster.<name>] base_image`, on a Slurm site | the image the site's segment images are built on, in place of `[deployment] base_image`: srt-slurm runs each command under `bash -c`, so it must hold bash. An image reference, as the spec's images are | `"debian:bookworm-slim"` |
| `[cluster.<name>] hub_image`, on a Slurm site | the image the site's hub image is built on | its `base_image` |
| `[cluster.<name>] network_interface`, on a Slurm site | the compute nodes' interface for traffic between nodes, which must match `srtslurm.yaml`'s: at most 15 characters, without `/`, `:`, or white space | `srtslurm.yaml`'s |
| `[cluster.<name>] signal_lead_s`, on a Slurm site | how long before a job's time limit Slurm sends it SIGTERM, in seconds (`sbatch --signal=TERM@<lead>`). At most 65535, the longest lead `sbatch` takes. `check` refuses less than the longest stop wait of a pool on the site (its `stop_wait_ms`, 660 s by default) plus 120 s, naming both | `900` |
| `[cluster.<name>] no_kill`, on a Slurm site | a job keeps running when one of its nodes fails (`sbatch --no-kill`), so its pool degrades rather than ends | `true` |
| `[cluster.<name>] time_limit`, on a Slurm site | every job's time limit, in Slurm's format: minutes, `minutes:seconds`, `hours:minutes:seconds`, `days-hours`, `days-hours:minutes`, or `days-hours:minutes:seconds`, above 0, at most 365 days, and longer than `signal_lead_s` | the partition's |
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
| `[pool.<name>.dgd] file` | the DynamoGraphDeployment (DGD) the pool owns, YAML or JSON, relative to the fleet file. Only for a pool of a cluster with `operator = true`. Compose refuses it ([A pool with a DGD](../guides/operator.md#a-pool-with-a-dgd)) | no DGD |
| `[pool.<name>.dgd] components` | segment of the pool's class to the DGD component that runs it. A segment it does not name stays one of gdl's own Deployments | required with `file` |
| `[pool.<name>] slurm` | how the pool's job runs on its Slurm site: `{ partition, time_limit, spare_nodes }`. Refused on a pool of any other cluster | none |
| `[pool.<name>] slurm.partition` | the partition the pool's job runs in | `srtslurm.yaml`'s |
| `[pool.<name>] slurm.time_limit` | the pool's job's time limit, over its site's, in the same format | its site's `time_limit` |
| `[pool.<name>] slurm.spare_nodes` | nodes the pool's job holds beyond its segments, for segments that join it later inside the job | `0` |
| `[directory] backend` | where every pool's hub publishes its record: `dynamodb`, the one value. Without the table, each child's hub reports to its parent's (`relay`) | no table |
| `[directory] records_table`, `lists_table` | the DynamoDB tables' names: 3 to 255 letters, digits, `_`, `-`, and `.` | required |
| `[directory] local` | the deployment runs one DynamoDB Local for the tables, which every hub uses. Compose requires it | `false` |
| `[directory] cluster` | the cluster that runs the DynamoDB Local, which every cluster reaches. Required with `local = true` when the fleet names clusters; refused without `local = true`, and when it names no cluster of the fleet. Compose does not read it | none |

- A pool's quorum for a segment is its own `ready`, else its class's, else 1.
- An explicit `requires` replaces the default whole. It can raise a count or name a subset of the classes sent to. A count of 0, and a class that the class does not send to, are refused.
- A pool or region name is a DNS label: lowercase letters, digits, and `-`, starting with a letter, ending with a letter or digit, at most 63 characters. So are the fleet's name and a cluster's name. A cluster's `namespace` is a Kubernetes namespace, an RFC 1123 label: lowercase letters, digits, and `-`, starting and ending with a letter or digit, at most 63 characters.
- A pool's `cluster` names a `[cluster.<name>]` table, and the pool takes the cluster's region. A pool may still write `region`, and then it must be the cluster's. A pool without a cluster writes its `region`.
- No two Kubernetes clusters have the same `context` and `namespace`.
- A Slurm site (`kind = "slurm"`) is checked by `cargo gdl check`, built for by `cargo gdl images`, and rendered by `cargo gdl slurm` ([Deploy a fleet on Slurm](../guides/slurm.md)). No command runs one yet: `cargo gdl k8s`, `deploy`, and `down` refuse a fleet with a Slurm site, naming it. Compose reads only a site's region, as it reads a Kubernetes cluster's. `cargo gdl slurm` refuses a fleet with a pool on a Slurm site unless its `[directory]` has `local = true` and runs on that site, a segment of such a pool that mounts a secret, a `run_dir` with `:`, `,`, `$`, `"`, `\`, a backquote, or white space, which srt-slurm's or Pyxis's mounts would split, expand, or unquote, and a pool with no `cluster`. A pool on a Slurm site takes no `dgd` table, which only dynamo's operator on Kubernetes runs, and no `resources`, since each instance takes its nodes whole. Every gdl job on a Slurm site takes its nodes whole, so a site has no key to share them: a pool's record carries only its hub's public port, and two jobs on one node would bind the same ports. A problem in a `[cluster.<name>]` table that does not read names the table and the key, with the table's line: the table is read whole, since its `kind` says which keys it takes.
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
- A pool's `dgd` table has no class default. `check` reads its file and refuses a file that is missing or is not one YAML document, or that has a YAML merge key (`<<`), which gdl does not expand, or a plain scalar that YAML 1.1, which kubectl reads, and YAML 1.2, which gdl reads, give two values: a number with a leading zero such as `0400`, a `0o`, `0b` or `_` number, a base-60 number such as `1:30`, or `yes`, `no`, `on`, `off`, `y` or `n` in any case. Quote such a value, or write it in decimal or as `true` or `false`. a DGD that is not `nvidia.com/v1beta1`, a `metadata.name` or `metadata.namespace` that is not the pool's name or its cluster's namespace, a component the DGD does not have, a component without `scalingAdapter`, a component of type `epp`, `frontend`, `worker`, `prefill`, `decode` or `planner` without `runtimeVersionOverride` (dynamo's webhook refuses such a typed component, since `cargo gdl`'s image tags are not semantic versions, and a component without a type may omit it), a component with a `runtimeVersionOverride` that is not a string `MAJOR.MINOR.PATCH` as dynamo's schema takes it, each part `0` or 1 to 4 digits without a leading `0`, a component that two segments name, a key of `components` that is not a segment of the pool's class, and a component whose pod templates do not hold exactly one container named `main`. It also refuses the table for a pool whose cluster does not set `operator = true`: the gdl operator owns the DGD, its stop, and its scaling, and without it `deploy`'s prune and `down` cannot find the DGD.
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
| `lease_ms` | how long a reader keeps the record without seeing it published again. At least 1, longer than `heartbeat_ms`, at least 300 under the `relay` backend, and longer than `heartbeat_ms` plus the poll period, 50 under `dir` and 1000 under `dynamodb`. Only the hub reads it. | 30000 |
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
| spec `[codec] "<from> -> <to>"` `floor`, `[codec] <Unary>` `floor` | `16384` | `EdgeCodec::DEFAULT_FLOOR` |
| spec `[deployment] base_image` | `"gcr.io/distroless/cc-debian12:nonroot"` | `DEFAULT_BASE_IMAGE` |
| spec `[deployment] build_image` | `"rust:1.93.1-slim-bookworm"` | `DEFAULT_BUILD_IMAGE` |
| spec `[binary] allocator` | `"system"` | `Allocator::DEFAULT` |
| fleet `ready.<segment>`, `[pool] ready.<segment>` | `1` | `PoolSettings::DEFAULT_QUORUM` |
| fleet `requires.<class>`, `[pool] requires.<class>`, for each class the pool's class sends to, when `requires` is unsaid | `1` | `PoolSettings::DEFAULT_REQUIRES` |
| fleet `[cluster.<name>] pins_local` | `false` | `KubernetesCluster::DEFAULT_PINS_LOCAL` |
| fleet `[cluster.<name>] base_image`, on a Slurm site | `"debian:bookworm-slim"` | `SlurmSite::DEFAULT_BASE_IMAGE` |
| fleet `[cluster.<name>] signal_lead_s`, on a Slurm site | `900` | `SlurmSite::DEFAULT_SIGNAL_LEAD_S` |
| fleet `[cluster.<name>] no_kill`, on a Slurm site | `true` | `SlurmSite::DEFAULT_NO_KILL` |
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
