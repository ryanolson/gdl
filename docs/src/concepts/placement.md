# Specs, segments, and layouts

A graph says what the nodes are. A placement says where they run. The spec file, `gdl.toml`, holds the placement and the binding of each node to its implementation. [The spec reference](../appendix/spec-reference.md) lists every key. [Split, generate, and draw](../guides/split-and-generate.md) gives the steps.

## Segments

A segment is a set of nodes that deploy as one program. Every node is in exactly one segment. A segment runs as many instances as the deployment wants. A `[segment.<name>]` table lists its `nodes`. Cutting an edge between two nodes in different segments makes it a crossing: the request leaves the program and travels over velo. [Crossing segments](crossings.md) says how.

A segment name is lowercase letters, digits, `-`, and `_`, at most 58 characters. The limit is there because a segment is named in settings keys, where an environment variable arrives lowercased, and in the hub's service names, which are the name in hex under velo-hub's 128-character limit. Each segment becomes a package named `<graph>-<segment>`. A segment cannot take the name of the generated contract crate.

A source's segment hosts the graph's entry. [Frontends](../guides/frontends.md) covers a hosted frontend and several roots.

## Bindings

A graph node binds to the implementation of the same name that a listed node crate offers. A graph spec lists the crates in `[nodes]`, and the generator reads their source. The node must have the role the graph gives it, and at each port the message its name resolves to. A contract spec binds each node in a `[bind.<Node>]` table instead. [Write nodes](../guides/write-nodes.md) shows how a crate offers a node.

A segment crate depends only on the implementation crates of the nodes it hosts. That is how a node that needs CUDA stays out of every other segment's build. [Generation design](../development/generation-design.md) says why this uses crates and not Cargo features.

## Identity

A deployment is identified by two hashes. Every envelope carries both, and an instance rejects an envelope whose hashes differ from its own. An old engine segment therefore cannot serve a new placement.

| Hash | Covers |
|---|---|
| contract hash | the graph's name and `schema`; each node's name and role; the hops; each edge's message type as written, with its encoding and the encoding's wire revision |
| placement hash | each segment's name and node set; the non-default `[routing]` entries; the classes and their segment sets; each class's effective cell policy |

- Order does not matter. Node order inside a segment and segment order inside a class do not change a hash.
- Defaults are not recorded. A placement that spells out `round_robin` for a boundary has the hash of one that leaves it unsaid. For cell policies the exception is deliberate: each class's effective policy is hashed, default included, so that a future change of default changes the hash and not what a placement means.
- A placement without classes keeps the hash it had before classes existed.
- A segment's binary carries the hashes of the placement it was generated for. A hub admits only registrations whose hashes match its deployment: [The hub](hub.md).
- Cells are runtime topology and are not part of the placement. [Classes and cells](cells.md) covers them.

## What is checked, and when

The checks live in `gdl-model`, so the generator, the composer, and the runtime apply one copy of each rule.

| Check | When it fails |
|---|---|
| Names agree on every edge of the graph file, and each name resolves to one message | `check`, `generate` |
| Every node is in one segment; no segment is empty; no segment names an unknown node | `check`, `generate`, and segment start |
| Segment names, and package and library names do not collide | `check`, `generate` |
| An implementation has its node's role and port types | the segment's build, naming the node |
| Every cut edge, and every anchor-carried edge, has a codec | `check`, `generate`, and segment start |
| Every cut or anchor-carried message is a wire type | the segment's build ([assertions](generation.md#the-crossing-assertions)) |
| A `[routing]` key names a node that starts a remote request run | `check`, `generate`, and segment start |
| Classes: each segment in exactly one, the boundary rule, no cycle, a cell policy only for a class a boundary enters | `check`, `generate`, and segment start |
| The deployment was made for this contract | `assemble`, at segment start |
| A hosted node has a binding | `assemble`, at segment start |
| The envelope's hashes match the receiver's | every request |

The checks report every problem they find, each with the file, the table, and the key, and the line when the parser has one. The codec rule that decides which edges need a codec, including the edge whose items return through an anchor although both its ends share a segment, is stated once, in [Crossing segments](crossings.md).

## Runs, hops, and the crossing report

For each hop the placement splits the forward path and the backward path into runs: maximal sequences of stages in one segment. A hop with a single run each way, in one segment, is local and costs a nested call. Any other hop crosses once for each pair of neighbouring runs.

The generator writes the cost of one request under a placement to `crossings.txt`:

```text
hop 0: 0 crossings, 0 anchors
hop 1: 3 crossings, 2 anchors
per request: 3 crossings, 2 anchors
```

A streamed request adds one anchor at each forward crossing it passes, for the receiver's input anchor. `cargo gdl diagram` draws the same placement with its crossing edges marked.

## Routing a boundary

A crossing into a segment with several instances picks one. `[routing]` names the policy for the boundary that enters a node:

```toml
[routing]
Engine = "random"                        # "round_robin" (the default) or "random"
Detokenize = { selector = "affinity" }   # a named selector
```

The key must be the first stage of a remote forward run. A selector name has the shape of a settings key and is never `round_robin` or `random`. gdl provides `affinity` and `least_loaded`, and the sending segment can register others. [Routing](routing.md) covers selectors and offers.

## Classes

A class is a set of segments that deploy together, in a `[class.<name>]` table with a `segments` list. A placement either has no classes, or puts every segment in exactly one.

A hop crosses a class boundary when one of its stages is in a class other than its outer end's. The boundary rule: such a hop crosses only at its inner end. Its inner end is in the other class, every other stage of the hop is in the outer end's segment, and every hop nested under the inner end stays in the inner end's class. Class boundaries must not form a cycle. The usual fix for a placement that breaks the rule is to put the boundary at an operator. [Known limitations](../operations/limitations.md) explains why the rule exists and what lifts it.

With classes, the crossing report also counts the WAN round trips that each boundary adds when a request spills to a cell in another region: 1 for a request value, and 2 for a streamed request, whose items wait for the input anchor.

A class that a boundary enters can set how the hop's outer end picks a cell, in `[cells.<class>] policy` (`home_first`, for example).

The spec does not check the name against a list, because a custom policy can be registered later. A chooser that cannot run the policy refuses to start. The runtime provides `home_first` and `round_robin`.

## Layouts

One graph can be split differently in different places. A layout is one split: its own segments, `[routing]`, classes, and cell policies. A spec holds several in `[layout.<name>]` tables, each with the same `segment`, `routing`, `class`, and `cells` tables a spec has at its top level, as in `layout.regional.segment.front`.

- Every layout places the same graph under the same contract hash. Nothing else is shared.
- Each layout is checked as a complete graph on its own. A problem names its tables (``[layout.compact.class]``), or the layout (`[layout.compact]`) when it belongs to no table.
- Each layout has its own placement hash. Two layouts that place the graph alike have equal hashes whatever they are called.
- A spec with `[layout]` tables has no top-level `[segment]`, `[routing]`, `[class]`, or `[cells]`. A spec without them has one layout, named `default`.
- A layout name has a class name's shape.
- With several layouts, each layout's crates go in `<out>/<layout>/<segment>` and are named `<graph>-<layout>-<segment>`. `[deployment.segment.<name>]` names a segment of any layout and serves it in each layout that has it.

A cell runs one layout. A hub follows only cells whose contract and placement hashes equal its own, so cells of different layouts never follow or offer to each other, and the runtime needs no layout name to keep them apart. `export` and `diagram` take a spec of one layout. `compose` deploys several layouts only by cell. See [Known limitations](../operations/limitations.md).
