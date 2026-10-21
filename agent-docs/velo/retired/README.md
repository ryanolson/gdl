# A retirement signal for velo streams

A prototype of ruling 120 (`agent-docs/plan.md`; the reasoning is in `agent-docs/dynamo.md`), made 2026-10-20 against velo `1bbeb88` in a scratch copy. Nothing was committed to velo.

- `prototype.diff`: the change, made with `diff -ru velo-base velo` from the repository root. Apply it in a velo checkout at `1bbeb88` with `patch -p1`.
- `tests.rs`: the five tests the prototype added as `lib/velo/tests/streaming/retired.rs`: a short stream retires without a reply, a slow consumer holds retirement until it reads, a consumer that leaves retires the stream as discarded, producer shutdown discards withheld output, and the shutdown hazard itself.
- `bench.rs`: the lifecycle benchmark, two velo nodes in one process over loopback TCP, 200,000 streams of four 16-byte items, 64 at once, in modes `plain`, `retired`, and `ack` (a consumer acknowledgement, the alternative).
- `results.txt`: the raw runs. The machine was loaded by other sessions, so compare instruction counts (`/usr/bin/time -l`), not wall times.

With it, 501 velo streaming unit tests and 71 streaming integration tests passed. The graceful-shutdown change ruling 120 also needs (writing admitted frames before a TCP or UDS writer closes) is not in the prototype.

## Staged items: a fix not taken

`staged-drain.patch` is a fix written during velo#141's review and set aside (the owner, 2026-10-20: document large staged items as an exception, and solve it later if needed). An item larger than velo's eager budget (256 KiB by default) leaves the node as a handle, and its bytes stay staged until the consumer pulls them, so `retired()` can report `Sent` while they are still staged. The patch makes `Velo::graceful_shutdown` also wait for staged payloads to be pulled, on the shutdown policy's budget (at most 30 s under `WaitForever`, as the RDMA sweep does). It touches `lib/velo/src/lib.rs` and `lib/velo/src/rendezvous/`, and applies with `git apply` at velo#141's head `36bb0aa`. velo's `LIMITATIONS.md` records the exception; gdl's `LIMITATIONS.md` notes it under "Output velo still holds".
