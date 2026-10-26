# Hot paths

This register lists the code paths that a profile shows to be hot, and the check that guards each one. A change to a path listed here runs the guard before and after, on the same host and build. A result outside the noise band needs its numbers and a reason in the change, and the owner's decision. Each entry point carries a `// hot path: <name>` comment, so a search finds the entry from the code.

The profiled workload is one stream of 100,000 items of 16 B through the lean two-segment graph, both ends in one process, on the gb300 bench host, with gdl at main `0ff3a6b`, after the in-place encode. [Where an item's cycles go](crossing-performance.md#where-an-items-cycles-go) has the setup, the command, and the step split. Shares are of all sampled cycles: every thread, user and kernel.

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
