# Routing design

This chapter records why routing works as [Routing, selectors, and offers](../concepts/routing.md) describes, and what was rejected. The constraints on the selector seam were these: `round_robin` and `random` keep their cost, and the dispatch to a selector adds nothing to their path. There is no task or timer per request or per stream, and no global lock on the hot path. A selector sees membership changes, and it can never return an instance that the routing table excludes. A selector failure is a gdl error. Generated crates do not change.

The seam was shaped by what dynamo's KV router needs: the request's tokens, the workers it may choose, each worker's cache overlap from an indexer, and each worker's load from its own book of active requests, which it adds to when it chooses and frees when the request ends. Its choice is asynchronous: it asks an indexer on its own thread, and its scheduler can queue a request until a worker has room. It also returns a data-parallel rank inside the worker.

## The trait is `async`

`select` returns a future. **Rejected: a synchronous trait.** It cannot host the KV router's indexer query or its queue. Because a selector can queue, its wait is bounded by the request's deadline and cancel, not by `setup_timeout_ms`. Every request that crosses has a deadline.

A selector is built through `gdl::Build`, as a node is. Its settings, its metrics, and its readiness then enter as a node's do, with no second mechanism.

## The head is decoded again

A selector that reads a typed head gets it decoded from the bytes the sender just encoded. The typed value is gone by then. The pick must follow the wait for a held roster and stay just before the send, where the retirement grace expects it. **Rejected: carrying the typed head to the pick.** It would add a field and a branch to every crossing's walk, built-in policies included. One decode per request is the selector's cost, and `gdl::Encoded` avoids even that.

The head type is checked when the segment starts, not per request. Each forward position's codec carries the `TypeId` and name of its head type. An `Absent` slot still has its edge's codec, so the check does not depend on what the segment hosts.

## Choosing from the snapshot, by construction

A selector gets `Instances`, a view of one table's eligible instances, and returns a `Choice` built from one of them. Neither has a public constructor, so an out-of-table answer cannot be written. The runtime maps the choice back by its index in the same table.

A selector can wait, so the table can change before it returns. The runtime then checks the choice: a pointer comparison when the table did not change, else a search of the new eligible list. If the instance is no longer eligible, the runtime drops the lease and asks again. It asks at most 8 times in all (`CHOICES` in `crates/gdl-runtime/src/dist/roster.rs`), then fails with `Routing`. Each further ask follows a membership change that made the last choice ineligible, and membership changes seconds apart. That check is the selection's pick. Nothing waits after it but the encoding, so a selector is under the same retirement rules as a built-in pick: an instance acknowledged before the check is never chosen, and a hand-off chosen just before an acknowledgement is covered by the retirement grace.

## The dispatch resolves once

Each forward position gets its router when its hop is built: a policy, or the selector, already checked against the edge. `send_forward` takes it directly. Before selectors, `send_forward` looked the router up in the layout's map on every hand-off. A policy still goes to the same `pick`. The only addition on its path is a match on the router's kind. A selector's path is boxed, so a policy's hand-off does not carry the selector's future state.

As measured on the laptop (provisional, Apple M5, shared machine), `round_robin` and `random` stayed within the spread of their base runs, and the root call future grew by 16 bytes, the lease beside the attempt slot. [Runtime feature costs](../operations/feature-costs.md) has the tables and commands.

## The lease

A selector keeps its own state. The runtime takes no lock for it and spawns nothing for it. Its inputs are its own choices and a lease, an owned value that the runtime drops when the sending segment stops holding the request. Where the response returns through the sender, the lease rides the holder that already keeps the attempt slot, so no stream gains a wrapper.

A forward run that hands on, with nothing of the response passing it, drops the lease at the acknowledgement. It has no later event to end the lease on without a task per request.

- **Rejected: a notice to the lease at the response's first item** (dynamo's prefill-completed signal). It costs a branch per item on every response.

`least_loaded` keeps every count it ever makes. One selector serves every crossing that names it, into any segment. A view of one segment cannot tell that another segment's idle count is unused, and a pick that loaded a count may be about to add to it. Dropping idle counts lost counts that another pick was using.

## Names

A selector name is words of lowercase letters and digits joined by single `_`s, at most 64 characters. The name is also the key of its settings table, so `GDL_SELECTOR__<NAME>__<KEY>` must reach it. `__` is the variables' separator, so `foo__bar` and `foo_` are refused. The name cannot be a built-in policy's name.

## Selectors from other crates

A generated binary routes only with gdl's own selectors. A selector from a crate that the segment does not otherwise link needs code that names its type. **Rejected: link-time registration (`linkme`).** The linker does not reliably keep a registration from a crate that nothing references. Emitted code can name the type: the spec would name the crate and the type, and the generator would emit the dependency and one `builder.selector::<T>("<name>")` line. That is not built.

## Offers

Every routed forward hand-off is an offer, and a declined one is chosen again in the same call.

- **The same instance may be chosen again.** A decline can come from a state that the router's view did not show yet, and the view catches up. A policy's turn moves on as for any pick.
- **gdl's selectors skip decliners while another instance remains.** Without that, `affinity` would offer every failover to the same instance, the key's top score, which wastes the bound. `affinity` takes the highest score among those that have not declined, which is the same next instance from every sender.
- **Only a decline before admission, or an unsent hand-off, moves a request on.** An entry declines with `Overloaded` or `Placement` only before it admits anything, so the request has not run there. An unknown outcome (no answer within `setup_timeout_ms`, or a broken connection after the handshake) may have run the request, so offering it again could run it twice. It fails with `OutcomeUnknown`, a kind of its own, so a caller does not take it for a retryable `Transport` error.
- **The first offer costs what a hand-off did before.** The failover's state (the declined instances, the last miss) is a local of a call that is boxed only on the miss path.

The opening of a backward run keeps its own round robin. A chooser's hand-off into another class keeps its failover between cells ([Cells design](cells-design.md)).
