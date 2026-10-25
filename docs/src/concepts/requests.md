# Requests, attempts, and deadlines

Every node receives a request context, `gdl::Cx`. The context carries the request's identity and metadata, its deadline, and the stop and cancel signals that end its work. This chapter describes the context, the attempts an operator makes, how a deadline crosses segments, and how stop, cancel, and failure reach each stage. [Runtime design](../development/runtime-design.md) gives the reasons and the rejected alternatives.

## The context

| Part | Crosses segments | Purpose |
|---|---|---|
| `id()`, a random `u128` | yes | Logs and traces only. The runtime never uses it to route a response. |
| `meta()`, a string map | yes | User metadata and trace context. `insert_meta` changes only this context and contexts derived from it later. |
| `deadline()` | yes, as wall-clock time | Bounds all work for the request, including work that no cancel reaches. |
| stop (`request_stop`, `stopping`) | through anchors | A graceful stop: finish buffered output, then end. |
| cancel (`cancel`, `cancelled`) | through anchors | A hard cancel: stop now. |
| extensions (`ext`, `insert_ext`) | no | Values stored by type, local to one segment. |

Stop and cancel are separate signals. A stop does not imply a cancel, because a stopped producer still delivers its buffered output. A cancel is also a stop: `is_stopping` is true after either. `check` returns the reason a request must end, and reports a passed deadline ahead of a cancel. `done` resolves with that reason.

## Attempts

An attempt is one call of `next.generate(cx, req)`. These rules hold in every placement:

1. Each attempt runs in a child scope of `cx`. Cancelling the parent cancels every attempt. Cancelling one attempt (by dropping its stream or its pending `generate` future) does not affect the parent or other attempts.
2. Attempts can run at the same time, in no fixed order. Each remote attempt mints its own anchors.
3. A `Next` can be cloned and kept inside the returned stream, for a retry or a migration. After the parent scope is cancelled or its deadline passes, `generate` fails at once.
4. A returned stream is `'static`.

Each attempt spends one unit of tokio's cooperative budget. So an operator that retries attempts which fail at once still yields to other tasks.

The runtime limits the outstanding remote attempts of one root request on one instance (`attempt_limit`). Past the limit, `generate` returns `Err(Overloaded)`. [Admission and egress](admission.md) gives the other limits.

## Deadlines across segments

A source sets an absolute deadline. `Deadline` holds it on the local monotonic clock and as wall-clock nanoseconds. The wall-clock form crosses segments unchanged. Each receiver maps it onto its own monotonic clock:

```text
local_deadline = mono_now + max(0, deadline_wall - wall_now - skew)
```

`skew` is `clock_skew`, the assumed largest clock difference between two segments (50 ms by default). A deadline within `skew` of now counts as passed, and the receiver refuses the request with `DeadlineExceeded` before it takes anything. If the clocks of all segments are within `skew` of each other, no receiver runs past the source's deadline. A receiver can stop up to two times `skew` early. The receiver reads the monotonic clock before the wall clock, so a delay between the two reads shortens the budget.

No attempt crosses segments without a deadline. An attempt with none gets `default_deadline` (600 s) before it leaves its segment. A purely local attempt keeps no deadline if it has none.

## Where cancel and deadline are checked

An attempt boundary watches the cancel and deadline futures while its producer is pending. When the producer is always ready, those futures never get polled. So the boundary also checks the cancel flag and the clock every 64 ready items (`CHECK_INTERVAL`). A ready producer can overrun a cancel or a deadline by at most 64 items. A pending producer is interrupted at once.

A node boundary inside an attempt ends the stream after the first `Err`, and cancels the producing node's scope when the stream ends or is dropped. It also yields after 1,024 consecutive ready items. Without that, a node that consumes without producing (a filter) would loop inside one poll and never return to the attempt boundary. A node that ends its stream cleanly after its scope was cancelled, or after the deadline, is reported as interrupted, not as a success.

The inner end always runs in its own scope. So a node that cancels its own context gets the same outcome whatever follows it.

A remote attempt enforces its deadline whether or not anyone reads its stream: a watcher at the outer end drops the response anchor at the deadline or cancel, which cancels the producer and releases its buffer. A local scope, including a local hop that encloses a remote attempt, enforces its deadline only at its boundaries, when its stream is read. A caller that holds a stream unread keeps its own process's scopes until it reads or drops the stream.

## Stop, cancel, deadline, and failure

| Event | What happens |
|---|---|
| The caller drops the response stream | The outer end's anchor is dropped and cancels its producer. Each pump cancels the anchor it reads. The cancel reaches the inner end. |
| The caller asks to stop | The outer end asks its anchor's producer to stop. Each pump passes the stop to the anchor it reads. The inner end sees `cx.stopping()`, finishes, and ends its stream. |
| The deadline passes | Every stage stops on its own clock, including work that no cancel reaches. |
| A producer dies | velo reports the dropped sender or a transport error after its heartbeat check, or the deadline fires first. Either way the consumer gets a terminal `Err`. |
| A receiver refuses, or is full | The sending stage gets the refusal. It offers the request to another instance, or returns the error or writes it to its reply. See [Routing, selectors, and offers](routing.md). |

A forward node in another segment cannot see a cancel in the middle of its transform, because no stream is attached for it yet. Its deadline bounds its work. When it finishes, the onward send finds the reply gone, and the next stage stops at its attach. Operators and sinks attach as soon as they accept a request, so they see a cancel at once. velo delivers a remote stop or cancel on a best-effort basis. That is a second reason every stage also holds the deadline.

A streamed request adds an input direction, with its own stop rules. [Streamed requests](streamed-requests.md) describes them.
