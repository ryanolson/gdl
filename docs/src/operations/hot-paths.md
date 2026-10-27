# Hot paths

This register lists the code paths that a profile shows to be hot, and the check that guards each one. A change to a path listed here runs the guard before and after, on the same host and build. A result outside the noise band needs its numbers and a reason in the change, and the owner's decision. Each entry point carries a `// hot path: <name>` comment, so a search finds the entry from the code.

The profiled workload is one stream of 100,000 items of 16 B through the lean two-segment graph, both ends in one process, on the gb300 bench host, with gdl at main `0ff3a6b`, after the in-place encode. [Where an item's cycles go](crossing-performance.md#where-an-items-cycles-go) has the setup, the command, and the step split. Shares are of all sampled cycles: every thread, user and kernel. The local item boundary comes from an endpoint's profile, which its entry names.

## crossing-item-send

- **Entry point:** `crates/gdl-runtime/src/dist/hop.rs`, `pump_watching`. It takes each item, already encoded in place into a velo frame, from the producer's stream and hands it to `deliver`, which calls `Outbound::send` (`dist/fabric.rs`) and velo's `send_frame`.
- **Why it is hot:** it runs once for every item that leaves a segment.
- **Profile:** `python3 scripts/bench/items.py profile --ref main=<commit> --out <new directory> --shape lean-16 --count 100000 --period 250000 --stack 8192 --cpus 16-23`, gdl `0ff3a6b`, 2026-10-26, gb300 (load 2.8). 8.4% of 7,356 samples, callees included. `deliver` is 5.6% of it, and velo's `send_item_frame` 4.8%. At `d1fa15a` the entry held 7.0%.
- **Guard:** `python3 scripts/bench/items.py count --ref base=<base commit> --ref change=<change commit> --out <new directory> --cpus 16-23 --rounds 7`: at `0ff3a6b`, 8,856 user instructions per item at 16 B, 10,336 at 1 KiB, and 28,942 at 16 KiB. Over seven rounds the range was -4% to +4% at 16 B and within 1.5% at the larger sizes. Repeated runs of one build gave medians within 1.2%. `timeout 120 cargo nextest run -p gdl-runtime --test item_allocs --no-capture` fails when a crossed item makes more than half an allocation over velo direct.
- **Sensitive to:** a per-item poll of the stop, cancel, or deadline futures, a timer per item, a lock or an allocation per send.

## crossing-item-receive

- **Entry point:** `crates/gdl-runtime/src/dist/hop.rs`, `InboundItems::poll_next`. At the outer end it reads `SharedInbound::poll_next` (`dist/fabric.rs`), which locks a mutex on each poll and reads `Inbound::poll_next`. That takes each item from velo's `poll_next_bytes` as velo's view of the frame.
- **Why it is hot:** it runs once for every item that enters a segment. Its share is under 5%, and it is listed because it runs on every item and is the other end of the same path.
- **Profile:** the same run as above. 3.9% of 7,356 samples, callees included. The `poll_next` functions of `dist/fabric.rs` are 3.9% of it, and velo's `poll_next_bytes` 3.4%. At `d1fa15a` the entry held 4.1%.
- **Guard:** the same instruction count and allocation test as `crossing-item-send`, which count both ends. For the codec steps alone: `cargo bench -p gdl-runtime --bench remote -- 'codec-' --warm-up-time 1 --measurement-time 3`. In place, the whole encode and decode was 47.6 ns at 16 B, 105.9 ns at 1 KiB, and 456.6 ns at 16 KiB ([Codec cost](crossing-performance.md#codec-cost) has the setup). velo direct, the same function in each run, measured 66.8 to 69.1 ns at 16 B and 133.2 to 144.1 ns at 1 KiB, so the band is about 3% at 16 B and 8% at 1 KiB.
- **Sensitive to:** a copy of the payload out of velo's buffer, an owned `Vec` per item, a lock or an allocation per item.

## local-item-boundary

- **Entry point:** `crates/gdl/src/items.rs`, `Fuse::poll_next` (a node boundary) and `Bounded::poll_next` (an attempt boundary). Each item that leaves a local hop passes one of each.
- **Why it is hot:** it runs once for every item at every local hop. An endpoint's streamed chunk crosses two hops.
- **Profile:** dynamo-http-endpoints' `crates/http-endpoints/examples/loopback_profile.sh chat-stream` (`perf record -F 1999 -g`, frame pointers), with gdl at `523afc0`, 2026-10-27, gb300. On the 256-chunk streams of three APIs, `Fuse` held 12.7% to 13.8% and `Bounded` 1.8% to 3.1% of the server threads' samples, callees included. `__memcpy_sve` was the top leaf. With the copies cut in that gdl, `Bounded` held 0.8%, and `Fuse` 13.3%, most of it the endpoint's own stream below it ([Copies of an item](feature-costs.md#copies-of-an-item)).
- **Guard:** `timeout 120 cargo nextest run -p gdl --test codegen` fails if either boundary's release code copies an item. `cargo bench -p gdl-runtime --bench hop -- 'chunks/' --warm-up-time 1 --measurement-time 3`: at `4931dcaf`, 2.07 to 2.10 µs for one request and 16.8 to 17.2 µs for 256 items, three runs on gb300 within 2.6%.
- **Sensitive to:** a call while the poll is held, an item moved into a new poll, a lock, a clock read, or an allocation per item.

## roster-stream-retirement

- **Entry point:** `crates/gdl-runtime/src/dist/fabric.rs`, `Outbound::finish`. Each stream a segment sends waits there for velo's `retired()` before it gives back its admission share and egress permit.
- **Why it is guarded:** it runs once for every stream sent across, so it scales with the crossings of a request. It is not listed from a profile. It is listed because its cost is over the 1% per-request gate and the owner accepts it for now, on condition that it is watched.
- **Cost:** on gb300, the commit before live rosters (`1ca7eba`) against the commit that added them (`83e9588`): three segments 546,726 → 558,658 user instructions per request (+2.18%), two segments +1.10%, one segment (the control) within the spread ([Live rosters](benchmarking.md#live-rosters)). On brev-x86-cpu: pending.
- **Guard:** the `requests` bench, setup `three`, by user instructions, base and change built in one target directory and run in turn for ten rounds, as [Claims checked on gb300](benchmarking.md#setup) shows. Compare only numbers from one host. A change to rosters or to stream retirement runs it and reports the three-segment row with its numbers. Growth beyond the range of the base goes to the owner before merge.
- **Sensitive to:** a lock, a waker, or an allocation per stream sent; a wake added to the pump's task; a second wait per stream.
