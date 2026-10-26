# Spec and fleet reference

This appendix lists every table and key of `gdl.toml` (the spec), `fleet.toml` (the fleet file), and a cell's settings. It is derived from the parsing code in `gdl-gen` and `gdl-model`. Every table refuses a key it does not know. A table whose keys are names (`ready`, `requires`, `[config]`, `pin`) refuses a name that is not in the spec. Paths in a spec are relative to the spec file. [Specs, segments, and layouts](../concepts/placement.md) explains the placement. [Split, generate, and draw](../guides/split-and-generate.md) shows a spec at work.

## gdl.toml

A spec names the graph with `[contract]` or with `[graph]`, never both. `[messages]` goes with `[graph]`. `[nodes]` goes with `[graph]`, and `[bind.*]` goes with `[contract]`. `[cell_policy.*]` goes with either. `[gdl]` is always required.

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
| `[cell_policy.<name>] crate` | the package of a cell policy that a `[cells.<class>] policy` names | required |
| `[cell_policy.<name>] path` | the crate's directory | one source is required |
| `[cell_policy.<name>] version` | an exact version on crates.io, as `name@version` in `[nodes] crates` | one source is required |
| `[cell_policy.<name>] git`, `rev` | a repository and a revision. Both are required together. | one source is required |
| `[cell_policy.<name>] lib` | the crate's library name, with `git` only, when it differs from the package's | optional |
| `[cell_policy.<name>] type` | the type that implements `gdl::CellPolicy` and `gdl::Build`, at the root of the crate's library | required |
| `[gdl] path` | a `gdl-runtime` directory | |
| `[gdl] version` | a registry version | |
| `[gdl] git`, `rev` | a repository and a revision. Both are required together. | |

`[gdl]` takes exactly one of `path`, `version`, and `git` with `rev`. A listed package appears once, and two listed message libraries cannot share a name. A `name@version` entry is fetched, and its library must be named for its package.

A `[cell_policy.<name>]` table binds a policy name to its crate and type. It names the crate by exactly one of `path`, `version`, and `git` with `rev`, as `[nodes] crates` can. A crate named by `version` must name its library for its package. The name follows the rule for a registered cell policy below. It must not be the name of a node or an entry adapter, or a `[config]` key, since the segment builds the policy under it. Some layout's `[cells.<class>] policy` must name it. `check` reads the crate and refuses a type that the crate does not offer as a cell policy. In a spec with a `[graph]` file, it also refuses a policy whose `Head` does not fit the request that crosses into a class that names it. Each segment that chooses cells of a class whose policy the table binds depends on the crate, builds the policy from its `[cell_policy.<name>]` settings table, and registers it. No other segment depends on the crate. [Write a cell policy](../guides/write-a-cell-policy.md) shows one.

### Placement

A spec of one layout has these tables at its top level. A spec with `[layout.<name>]` tables has them only inside each layout.

| Key | Meaning | Default |
|---|---|---|
| `[segment.<name>] nodes` | the nodes the segment hosts | required |
| `[routing] <Node>` | the router of the boundary that enters the node: `"round_robin"`, `"random"`, or `{ selector = "<name>" }` | `"round_robin"` |
| `[class.<name>] segments` | the segments that deploy together | no classes |
| `[cells.<class>] policy` | how a chooser picks a cell of the class | `"round_robin"` |
| `[layout.<name>.segment\|routing\|class\|cells]` | the same tables, for one layout | one layout, `default` |

| Name | Rule |
|---|---|
| segment | lowercase letters, digits, `-`, `_`; at most 58 characters |
| class, layout, selector, policy | lowercase letters and digits in words joined by single `_`; start with a letter; at most 64 characters |
| selector | never `round_robin` or `random` |
| registered cell policy | never `home_first` or `round_robin` |

A value equal to its default is not recorded. The placement hash covers each class's effective cell policy, the default included.

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

## fleet.toml

The fleet file lists a deployment's cells. It is not part of the placement's identity. [Classes and cells](../concepts/cells.md) and [Deploy a fleet of cells](../guides/fleet.md) explain it.

| Key | Meaning | Default |
|---|---|---|
| `[class.<name>] ready` | segment to the usable instances a cell of the class needs to be ready | 1 per segment |
| `[class.<name>] requires` | class to the number of ready children of it that a cell's health needs | 1 for each class the cell's class sends to |
| `[cell.<name>] class` | the cell's class, a class of its layout | required |
| `[cell.<name>] region` | the cell's region | required |
| `[cell.<name>] layout` | the spec layout the cell runs | the only layout, when there is one; required otherwise |
| `[cell.<name>] home` | the parent cell | required for a class that another class sends to; refused otherwise |
| `[cell.<name>] ready` | overrides the class's quorum for a segment | the class's, else 1 |

- A cell's quorum for a segment is its own `ready`, else its class's, else 1.
- An explicit `requires` replaces the default whole. It can raise a count or name a subset of the classes sent to. A count of 0, and a class that the class does not send to, are refused.
- A cell or region name is a DNS label: lowercase letters, digits, and `-`, starting with a letter, ending with a letter or digit, at most 63 characters.
- A `home` names a cell of a parent class that runs the same layout. Every cell whose class sends to another class needs a child of each class it sends to, in its layout.
- A `ready` key must be a segment of the class, and a count is at least 1.
- A `[class.<name>]` table applies to the class's cells in every layout with the class, and is checked against each.

## A cell's settings

The fleet file fills in each cell's `[cell]` table in its segments' settings. The same keys are read from `GDL_CELL__<KEY>` variables, with `__` between nested keys. [Segment settings](../operations/configuration.md) covers the rest of a segment's settings.

| Key | Meaning | Default |
|---|---|---|
| `name`, `class`, `region` | the cell's name, class, and region | required |
| `layout` | the layout the cell runs. Read and checked for its shape. Nothing else in an instance or hub reads it. | none |
| `home` | the parent cell | none |
| `directory` | where cell records are published: `relay`, `memory` (one directory per process, for tests), or `dir` | `relay` |
| `parent_hub` | the parent cell's hub, `host:port`, where the cell's hub reports its record. With `relay`, a hub of a class that another class sends to needs it. Refused for a class that nothing sends to, and with the other backends. A fleet bundle sets it to `<home>:1337`. | none |
| `dir` | the shared directory. Required with `directory = "dir"` and refused with the others. | none |
| `hub` | the address, `host:port`, at which other cells' hubs reach this cell's hub. Only the hub reads it. | none |
| `summary_ms` | the load summary's period. Least 1000. | 10000 |
| `ready.<segment>` | the quorum. A segment it does not name needs 1. | 1 |
| `requires.<class>` | the ready cells of a class that the cell's health needs. Only the hub reads it. | 1 for each class sent to |

An instance refuses to start when its `[cell]` does not fit its deployment: the placement has no classes, the class is not one of them, the instance's segment is not in it, a `ready` key is not a segment of it, or a `requires` key is not a class it sends to or is 0. Each problem names where the key was set, the file and key or the variable.

## Constants

| Constant | Value | In |
|---|---|---|
| `HUB_PUBLIC_PORT` | 1337 | `gdl_model` |
| `HUB_CONTROL_PORT` | 8337 | `gdl_model` |
| `DEFAULT_DEADLINE` | 600 s: a request's deadline when it has none, and so the default drain bound | `gdl_model` |
| `SEGMENT_NAME_LIMIT` | 58 | `gdl_model` |

### Defaults

Every default of the spec's placement and deployment tables, the fleet file, and a cell's settings is a named constant. A test in `gdl-gen` checks this table against the constants.

| Key | Default | Constant |
|---|---|---|
| spec `[routing] <node>` | `"round_robin"` | `RequestRouter::DEFAULT` |
| spec `[cells.<class>] policy` | `"round_robin"` | `CellPolicy::DEFAULT_POLICY` |
| spec `[deployment] base_image` | `"gcr.io/distroless/cc-debian12:nonroot"` | `DEFAULT_BASE_IMAGE` |
| spec `[deployment] build_image` | `"rust:1.93.1-slim-bookworm"` | `DEFAULT_BUILD_IMAGE` |
| fleet `ready.<segment>`, `[cell] ready.<segment>` | `1` | `CellSettings::DEFAULT_QUORUM` |
| fleet `requires.<class>`, `[cell] requires.<class>`, for each class the cell's class sends to, when `requires` is unsaid | `1` | `CellSettings::DEFAULT_REQUIRES` |
| `[cell] directory` | `"relay"` | `DirectoryBackend::DEFAULT` |
| `[cell] summary_ms` | `10000` | `CellSettings::DEFAULT_SUMMARY` |
| `[cell] summary_ms`, its least | `1000` | `CellSettings::MIN_SUMMARY` |

The constants are `gdl_model`'s, and the two images are the generator's (`gdl-gen`). Every other optional key has no value unless it is set.
