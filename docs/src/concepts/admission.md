# Admission and egress

Each segment instance limits how much work it takes. Admission limits the root requests it serves at once. Egress limits the streams it produces at once. A per-root limit bounds the remote attempts of one request. A full instance refuses new work with `Overloaded` instead of buffering for it. [Runtime design](../development/runtime-design.md#admission-and-egress) gives the reasons and a design that was tried and removed. [Segment settings](../operations/configuration.md) gives the settings and their defaults.

## Admission

`admission_limit` counts root requests, not entries. The root is the request id, which every envelope carries. A request can enter one instance several times: a provisioned backward run and a forward stage of the same hop, or two hops of a nested graph. The first entry takes a permit, and each later entry of the same root shares it.

Without this, one request can block itself. With placement `{A,B,D,G} {C,F,E}` and a limit of one, the provisioned F/E run would hold the second segment's only permit while C waits for it. `an_admission_limit_of_one_serves_a_request_that_enters_a_segment_twice` covers this.

An entry (a forward hand-off or a run opening) takes admission before it acknowledges. If no permit becomes free within `admission_wait`, it refuses with `Overloaded`. The wait is bounded, so admission can refuse but never deadlock. The sender can then offer the request to another instance ([Routing, selectors, and offers](routing.md)). An arriving entry rebuilds its context first. So an expired request is refused before it takes anything, and every wait ends at the request's deadline.

The ledger of admitted roots is sharded by root. A request touches it only at entry and at retirement.

## When a share is released

| Holder | Releases its share |
|---|---|
| A forward run that hands the request on | at the acknowledged hand-off |
| A forward run whose onward hand-off failed | after it has delivered the failure to its reply |
| A relay of a streamed request | after it has sent all of its input on |
| The inner end's run, and a backward run | when its stream retires (below) |

The permit returns when the last share of its root is released.

## Egress

`egress_limit` counts the streams an instance is producing at once. A producing stream takes its permit before it attaches, and before the entry acknowledges, so a full instance refuses. The egress wait uses the same bound as admission, `admission_wait`. These take a permit:

- the inner end's run and each backward run;
- a relay, before it acknowledges;
- an outer end's input pump, before it sends the hand-off;
- an inner end or a backward run that the outer end runs in its own segment.

A stage that delivers only a failure takes no permit. It writes one frame, bounded by the request and `setup_timeout` before it attaches and by 100 ms after.

An outer end that holds an input pump's permit while its local inner end waits for another can be refused with `Overloaded` at the limit. The wait is bounded, so it is refused, never hung. `egress_hold_and_wait_at_the_limit_is_refused_not_hung` covers this.

## Holding until velo retires a stream

`finalize` only queues a stream's end. A consumer that does not read keeps the rest of the output in velo on the producer's node: its withheld queue, up to the slot byte budget, plus its credit window. So a stream that finished cleanly keeps its admission share and its egress permit until velo reports the stream retired. velo reports `Sent` when it holds none of the stream's output, or `Discarded` when the consumer left while it held some. The wait is raced against the request's deadline and cancel, so no request holds anything past its end, and no timer or task is added for it. A failure delivery waits for retirement within its 100 ms bound. A sender dropped without a finish (the consumer left, or a failure was not delivered) is not waited for.

So, once its source's body has returned, an instance whose ledger is empty has handed on all of its output, except output that a deadline, a cancel, or the failure bound cut. The condition covers the producers that hold no share (an inner end or a backward run that the outer end runs in its own segment, and the outer end's input pump). They wait for retirement too, and they run under an attempt whose response ends before the body returns or before its visit's share drops. [Segment lifecycle](lifecycle.md) relies on this when a retiring instance exits. `a_finished_stream_holds_its_share_until_its_output_is_read` covers it. `Sent` means that the output left this node, not that the consumer read it. [Known limitations](../operations/limitations.md) lists the case of an item larger than velo's eager budget.

Every gdl consumer holds the request's deadline and cancels its anchor when the deadline passes, read or not. That bounds how long a finished stream can hold its permit.

## Attempts per root

Concurrent attempts each mint anchors and share their root's admission, so admission alone does not bound streams. `attempt_limit` bounds the outstanding remote attempts of one root on one instance. `next.generate` beyond it returns `Err(Overloaded)`. A local attempt does not count.

A zero admission, attempt, or egress limit is refused when the segment starts.
