# Requests, attempts, and deadlines

Every node receives a request context, `gdl::Cx`. The context carries the request's identity and metadata, its deadline, and the stop and cancel signals that end its work. This chapter describes the context, the attempts an operator makes, how a deadline crosses segments, and how stop, cancel, and failure reach each stage. [Runtime design](../development/runtime-design.md) gives the reasons and the rejected alternatives.

## The context

| Part | Crosses segments | Purpose |
|---|---|---|
| `id()`, a UUID v7 | yes | Logs and traces. Admission counts root requests by it. The runtime never uses it to route a response. |
| `meta()`, a string map | yes | User metadata and trace context. `insert_meta` changes only this context and contexts derived from it later. |
| `deadline()` | yes, as wall-clock time | Bounds all work for the request, including work that no cancel reaches. |
| stop (`request_stop`, `stopping`) | through anchors | A graceful stop: finish buffered output, then end. |
| cancel (`cancel`, `cancelled`) | through anchors | A hard cancel: stop now. |
| extensions (`ext`, `insert_ext`) | no | Values stored by type, local to one segment. |

Stop and cancel are separate signals. A stop does not imply a cancel, because a stopped producer still delivers its buffered output. A cancel is also a stop: `is_stopping` is true after either. `check` returns the reason a request must end, and reports a passed deadline ahead of a cancel. `done` resolves with that reason.

## The request id

`Cx::new` mints the request id: an RFC 9562 UUID, version 7, written in the hyphenated form (`0199f5a2-7c3e-7a01-9f2b-4c1d8e6a3b70`). Every segment the request crosses gets the same id, because the envelope carries it as a `u128` and the receiver restores it.

| Bits | Holds |
|---|---|
| 48 | Unix time in milliseconds, from the kernel's coarse clock on Linux and the precise clock elsewhere |
| 4 | the version, 7 |
| 12 | a counter |
| 2 | the RFC variant |
| 62 | random bits from a per-thread generator |

- Ids sort by time, as integers and as text.
- The ids that one process mints sort in the order it minted them, across threads too. A mint that happens after another gets the higher id.
- When more than 4,096 ids are minted in one millisecond, the counter carries into the time, and the time runs ahead of the clock until the clock catches up. When the clock steps back, the time does not follow it.
- The time can lag the precise clock by one kernel tick, 1 to 10 ms. On the shared aarch64 host of [Feature costs](../operations/feature-costs.md#request-ids), the precise clock costs 23 to 30 ns a read, and the coarse one 6 ns.
- The random bits keep ids from different processes apart, except in a process that forks without exec ([Known limitations](../operations/limitations.md#a-process-that-forks-without-exec-repeats-request-ids)). The id is not a secret.

Minting takes no lock and allocates nothing. [Feature costs](../operations/feature-costs.md#request-ids) gives its cost.

## Calls and attempts

A call is one call of `next.generate(cx, req)`. By default, a call that an operator makes with its own context runs under the request's scope: the hop below adds no scope, check, or boundary of its own, which is what makes it cheap ([Feature costs](../operations/feature-costs.md#stages-in-callgrind-ir)). An attempt is a call with a scope of its own. These rules hold for every call:

1. Dropping a call's stream, or its pending `generate` future, ends that call: its futures and streams are dropped.
2. Calls can run at the same time, in no fixed order. Each remote call mints its own anchors.
3. A `Next` can be cloned and kept inside the returned stream, for a retry or a migration.
4. A returned stream is `'static`.
5. An around node's value belongs to its call. Its forward half keeps it, and the backward half of the same call receives it. A retry, or a new call of `next`, runs the forward half again and gets a new value. No other call can reach it ([The graph model](model.md#around-nodes)).
6. Each call spends one unit of tokio's cooperative budget. So an operator that retries calls which fail at once still yields to other tasks.

A call is an attempt when:

- the operator sets `Operator::CALL_SCOPE`;
- the operator calls with a context it derived: a child scope (`cx.child()`) or another deadline;
- the hop below is remote. Its watcher, its admission slot, and its default deadline belong to the attempt, so a call over a remote hop always has one. Scope therefore depends on placement.

An attempt runs in a child scope of `cx`. Cancelling it, by its own scope or by a drop, does not affect the parent or other calls. A call made after `cx` is cancelled or past its deadline fails at once. Its cancel and deadline are enforced at its boundary, which is described below.

Any other call runs under the request's scope:

- Its nodes see the request's cancel, stop, and deadline. Their `cx.cancelled()` resolves when the request's inner stream ends, not when their own call's stream ends.
- A node's own `cx.cancel()` or `cx.request_stop()` reaches every node under the request's edge.
- A call that finds its request over ends quietly, with an empty stream, and builds no further backward node. The edge reports why. An error here would let a retry or a migration send the failure that caused it, after the request was cancelled.
- Nothing below the edge checks the cancel per item, ends a stream at its first `Err`, or reports a clean end after a cancel. A node that reads such a call's stream sees what the producer sends.

An operator sets `CALL_SCOPE` when it needs a call's own cancel: it hedges or abandons calls and expects the dropped call's nodes to see a cancel while the request goes on, it turns its `next`'s end into output and must stop reading at once on a kill (for output at the end alone, a `cx.check()` on the clean end is enough), it filters or batches an always-ready local source, or it calls `next` from a task it spawned. A backward node or an around node that may take items without passing one on (a filter, a batcher) sets `FILTERS`: its input then yields every 1,024 ready items, as a node boundary's does, so a cancel or a client's detach reaches the edge.

The request's edge is always an attempt: `Source`, a service entry, a hop that is called directly, and a segment's received run. It races the scope it hands the stage below it, the request scope, while the request dispatches. A child sees its parent's cancel, so the race also covers the attempt and the root. When the stream ends, the edge reports a cancel of that scope, including one made in a producer's cleanup. While the stream runs, the edge's boundary watches its attempt: a cancel from above (the caller, the deadline, a drop) is enforced as in any attempt, and a node's own cancel of the request scope is cooperative, and reported when the stream ends.

The runtime limits the outstanding remote attempts of one root request on one instance (`attempt_limit`). Past the limit, `generate` returns `Err(Overloaded)`. [Admission and egress](admission.md) gives the other limits.

## Deadlines across segments

A source sets an absolute deadline. `Deadline` holds it on the local monotonic clock and as wall-clock nanoseconds. The wall-clock form crosses segments unchanged. Each receiver maps it onto its own monotonic clock:

```text
local_deadline = mono_now + max(0, deadline_wall - wall_now - skew)
```

`skew` is `clock_skew`, the assumed largest clock difference between two segments (50 ms by default). A deadline within `skew` of now counts as passed, and the receiver refuses the request with `DeadlineExceeded` before it takes anything. If the clocks of all segments are within `skew` of each other, no receiver runs past the source's deadline. A receiver can stop up to two times `skew` early. The receiver reads the monotonic clock before the wall clock, so a delay between the two reads shortens the budget.

No attempt crosses segments without a deadline. An attempt with none gets `default_deadline` (600 s) before it leaves its segment. A purely local attempt keeps no deadline if it has none.

## Where cancel and deadline are checked

An attempt boundary watches the cancel and deadline futures while its producer is pending. When the producer is always ready, those futures never get polled. So the boundary also checks the cancel flag and the clock every 64 ready items (`CHECK_INTERVAL`). A wait checks the cancel itself, and polls the deadline's timer, but the count runs on across it: the timer fires on a whole-millisecond tick, and can still be waiting after the deadline has passed. With no deadline, the check reads no clock. A ready producer can overrun a cancel or a deadline by at most 64 items. A pending producer is interrupted at once.

These checks are an attempt's. A call under the request scope has none of its own: the edge's boundary checks for it.

A node boundary inside an attempt ends the stream after the first `Err`, and cancels the producing node's scope when the stream ends or is dropped. It also yields after 1,024 consecutive ready items. Without that, a node that consumes without producing (a filter) would loop inside one poll and never return to the attempt boundary. A node that ends its stream cleanly after its scope was cancelled, or after the deadline, is reported as interrupted, not as a success. The boundary checks that after it drops the producer's stream, so a cancel made in the producer's cleanup is reported too.

So, at the edge and in an attempt, the cancel is also the cleanup at the end of the stream. A node's `cx.cancelled()` resolves after its stream ends normally, not only when the caller leaves, and that ends whatever the node runs under its scope. Under the request scope, it resolves when the request's inner stream ends, and a node whose cleanup must run when its own stream ends puts it in that stream's `Drop`. A node cannot tell a hang-up from a normal end by the cancel. To learn that the caller left, a node checks whether its stream was dropped before its last item, for example with a guard inside the stream that it disarms after it yields the last item.

In an attempt, the inner end runs in its own scope. So a node that cancels its own context gets the same outcome whatever follows it, with one exception in timing: a node that cancels its own context and then waits, before it returns its stream, is dropped at once by the attempt's race, with `Cancelled` and no items. One that returns at once still delivers its ready items, then `Cancelled`.

A remote attempt enforces its deadline whether or not anyone reads its stream: a watcher at the outer end drops the response anchor at the deadline or cancel, which cancels the producer and releases its buffer. A local scope, including a local hop that encloses a remote attempt, enforces its deadline only at its boundaries, when its stream is read. A caller that holds a stream unread keeps its own process's scopes until it reads or drops the stream.

## Stop, cancel, deadline, and failure

| Event | What happens |
|---|---|
| The caller drops the response stream | The outer end's anchor is dropped and cancels its producer. Each pump cancels the anchor it reads. The cancel reaches the inner end. |
| A stream ends normally | At the edge and in an attempt, the node boundary cancels the producing node's scope, which ends what the node runs under it. The node's `cx.cancelled()` resolves, though the caller did not leave. Under the request scope, nothing is cancelled until the request's inner stream ends. |
| The caller asks to stop | The outer end asks its anchor's producer to stop. Each pump passes the stop to the anchor it reads. The inner end sees `cx.stopping()`, finishes, and ends its stream. |
| The deadline passes | Every stage stops on its own clock, including work that no cancel reaches. |
| A cancel or a deadline reaches an operator's call under the request scope | The call ends quietly, with an empty stream or with the producer's clean end. Only the request's edge reports the cancel or the deadline. An operator that turns its `next`'s end into output checks `cx` on the clean end to tell a kill from a real end ([Write nodes](../guides/write-nodes.md#pick-a-role)). |
| A producer dies | velo reports the dropped sender or a transport error after its heartbeat check, or the deadline fires first. Either way the consumer gets a terminal `Err`. |
| A receiver refuses, or is full | The sending stage gets the refusal. It offers the request to another instance, or returns the error or writes it to its reply. See [Routing, selectors, and offers](routing.md). |

A forward node in another segment cannot see a cancel in the middle of its transform, because no stream is attached for it yet. Its deadline bounds its work. When it finishes, the onward send finds the reply gone, and the next stage stops at its attach. Operators and sinks attach as soon as they accept a request, so they see a cancel at once. velo delivers a remote stop or cancel on a best-effort basis. That is a second reason every stage also holds the deadline.

A streamed request adds an input direction, with its own stop rules. [Streamed requests](streamed-requests.md) describes them.
