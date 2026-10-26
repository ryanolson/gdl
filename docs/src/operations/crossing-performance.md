# Crossing performance

This chapter reports what a crossing costs: a request, an item, a streamed request, and the codec. A crossing is one request or stream that leaves a segment for another over velo. [Concepts: Crossings](../concepts/crossings.md) says what one is. [Benchmarking](benchmarking.md) says what these shared-machine numbers may claim, and the measurement traps that apply to all of them.

The result: a two-segment request costs about what the same exchange costs on velo alone, within the run-to-run spread. A crossing's codec work per item is 21 to 38 ns under velo's at 16 B to 16 KiB. Both are bounded by velo's single-stream rate of about 11 K items/s. At `0ff3a6b`, a crossed item costs 7% more user instructions than velo carrying it at 16 B, 4% more at 1 KiB, and no measurable amount more at 16 KiB. Most of an item's cycles go to velo's transport and tokio's scheduler.

A graph that is not cut costs no encoding. Nodes of one segment pass values directly. `crates/gdl-runtime/tests/crossings.rs` counts the encodes of one message type: none in a local graph, and one encode and one decode per message per crossing.

## Crossing a segment

**Setup.** One aarch64 workstation, release profile, one process, 4 tokio workers, velo 0.19.0 nodes on loopback TCP with default settings. gdl at `0e7afdd` (2026-10-15). Latency, throughput, and CPU come from `cargo bench -p gdl-runtime --bench remote`. Allocations and tasks come from `--bench footprint`, with a counting allocator, and with `RUSTFLAGS="--cfg tokio_unstable"` for the task count.

**Compared.** The lean graph `S => D` over two segments, against the same request written directly on velo. The direct baseline opens its stream as gdl does, on velo's ticket path (`prebind_anchor` and `open_anchor_stream`). Velo's plain `attach_anchor` is reported beside it and is not the baseline. The reference graph is the seven-node graph of the test suites, local and over two and three segments.

| One request, one item of 16 B | Latency | Allocations | Tasks spawned |
|---|---|---|---|
| Reference, local | 26 us | 67 | 1 |
| Reference, 2 segments | 223 us | 214 | 7 |
| Reference, 3 segments | 612 us | 350 | 12 |
| Lean, 2 segments | 231 us | 146 | 6 |
| velo direct (ticket) | 216 us | 98 | 4 |
| velo direct, plain attach | 258 us | 130 | 5 |

| 1,000 items per request | Lean, 2 segments | velo direct | Difference |
|---|---|---|---|
| 16 B | 10.8 K items/s | 12.0 K items/s | -11% |
| 1 KiB | 10.8 K items/s | 11.0 K items/s | -1% |
| 16 KiB | 10.0 K items/s | 10.3 K items/s | -2% |
| 16 B, 32 streams at once | 234 K items/s | 233 K items/s | 0% |

Across runs, direct velo measured 11.0 to 12.1 K items/s at 16 B and gdl 10.6 to 11.5 K. The reference graph moves 360 K items/s locally and 10.6 K and 10.7 K items/s over two and three segments. CPU time per item (32 streams of 1,000 items, 20 rounds) was 3.08 us for gdl and 2.56 us for velo direct. Differences under about 0.5 us are below what this setup resolves.

**Reading.**
- **A two-segment hop costs what velo costs**, plus 2 spawned tasks and 48 allocations. One task is the outer end's watcher, which releases a held, unread stream at its deadline or cancel. The other is velo's unattached-anchor timer, which gdl sets on every anchor.
- **The third segment adds one round trip and one crossing**, because the outer end opens the intermediate run before dispatch.
- **Per item, gdl is within 0% to 11% of velo**, about the spread between runs. The producer's pump takes an item that is already there without polling its watchers, and polls them every 64 items or when it must wait. No speedup is claimed. The change removes a per-item lock on tokio's shared timer that every stream of a runtime would contend on.
- **A held stream holds velo's buffer, and gdl adds 5 to 10 KiB.** Of 256 requests of 1,000 items, read once and held, every producer sends the whole stream into velo at 16 B and 1 KiB. At 16 KiB every producer stops at 131 items, about 2 MiB, velo's per-stream budget. Size velo's `MuxConfig` for a deployment that expects many held streams. A stream past its deadline holds under 1 KiB and no tasks.

## Streamed requests

**Locally.** `cargo bench -p gdl-runtime --bench hop`, one aarch64 workstation, one current-thread runtime, gdl at `93fc3fe` against `f345c65`, run alternately, two rounds each. A unary request's response items cost 24.9 ns per item and a streamed request's items 28.4 ns. The 3.5 ns is the request stream's fuse and one more boxed stream. The unary path did not change (under 1%).

**Across segments.** `--bench remote`, same machine and setup as above, gdl at `df0d506`. The comparison is the exchange as rhino-asr writes it on velo alone: the client mints the response anchor, the server mints the input anchor, and the client opens the input by ticket.

| 1,000 items per request | Upload, 2 segments | velo direct upload | Difference |
|---|---|---|---|
| 16 B | 13.8 K items/s | 13.8 K items/s | 0% |
| 1 KiB | 13.6 K items/s | 13.9 K items/s | -2% |
| 16 KiB | 11.1 K items/s | 11.6 K items/s | -5% |

One item of 16 B, median of five runs: upload 524 us, velo direct upload 403 us, unary 240 us, direct unary 210 us. A one-item upload costs about 120 us more than the same exchange on velo alone. That gap is not explained, and it needs a profile. The steps gdl adds are the admission and egress checks, two spawned tasks, and the `Open` frame, which a sink sends only after its `generate` returns.

## Reply renewal

Each remote request and forward hand-off registers a reservation in a sharded `DashMap` while request-only work runs. One task per runtime renews them in batches every `reservation_renew_interval` (30 s).

**Setup.** Apple silicon, macOS, 2026-10-20. gdl at `ad11b37` (before) against `b42ed3d` (after), each in its own target directory. Velo `d793db4` and `1bbeb88`, where `velo-direct` is the control. Medians of the second alternating run.

```bash
cargo bench -p gdl-runtime --bench hop --bench remote -- --warm-up-time 2 --measurement-time 5 <filter>
```

| Bench | Before | After |
|---|---|---|
| `request/gdl` (one hop, in process) | 1.30 us | 1.29 us |
| `request/reference-2-segments` | 57.4 us | 56.9 us |
| `request/upload-2-segments` | 85.3 us | 85.4 us |
| `reference-items/2-segments` (1,000 items) | 131.6 ms | 131.0 ms |
| `request/reference-3-segments` | 117.1 us | 117.1 us |
| `request/velo-direct` (control) | 40.2 us | 40.3 us |

The cost per request is below the noise of a 57 us crossing. The renewal task did not appear in any request's time.

## Velo deadline sweeps

Velo moved anchors' unattached deadlines and senders' heartbeats from one timer per stream to one shared queue per manager. **Setup.** Apple M5, macOS, loopback, shared machine, rustc 1.93.1, metrics off. Base is gdl `8c76017` with velo `e512ba0`. Change is the same gdl with velo `cee2834`. Instructions per request as [Benchmarking](benchmarking.md#method) defines, seven rounds, load average 5.7 falling to 3.0.

| Setup | Base | Change | Change - base |
|---|---|---|---|
| one segment (control) | 128,196 | 128,216 | +20 (0.0%) |
| two segments, `round_robin` | 629,125 | 612,719 | -16,406 (-2.6%) |
| three segments, `round_robin` | 1,212,451 | 1,181,941 | -30,510 (-2.5%) |

The saving is about 16k instructions per anchor (per stream) and nothing without one. Wall time per request is within the spread. `reference-items/2-segments` is bimodal on both builds, with overlapping ranges, and the change median is 2.4% higher. That is below the 5% block, and unsettled: a dedicated machine decides whether a per-item cost exists.

## The single-stream limit

Two velo 0.19 nodes in one process over loopback TCP, default settings, move one stream of small items at about 12 K items/s, or 83 us per item. This is `velo-direct` in `crates/gdl-runtime/benches/remote.rs`, 4 tokio workers. Thirty-two streams at once move 234 K items/s, so the limit is per stream. The ask to velo is in [Development: Velo](../development/velo.md#open-asks-of-velo).

## Instructions per item

What one item costs to cross, in instructions, with the request's cost taken out. This is the per-item claim that wall time and items per second cannot settle, because velo's single-stream limit hides it.

**Setup.** The gb300 bench host: 72 Neoverse-V2 cores, quiet, Linux 6.17.0 (64 KiB pages), rustc 1.93.1, perf 6.17.13. Each run is pinned to cores 16 to 23. Load average 5.3, falling to 1.4 over the run. One process runs both ends, with 4 tokio workers and two velo nodes over loopback TCP. `requests lean-<size> <count>` sends one stream of `<count>` items through the lean graph `S => D` over two segments. `requests direct-<size> <count>` sends the same stream on velo alone, opened by ticket as gdl opens it. Each run is counted at 10,000 and at 20,000 items, and the difference divided by 10,000 is the cost of one item. `perf stat` counts user and kernel instructions apart, across all threads and both ends. Seven rounds alternate the builds, the shapes, and the order of the two counts. The table gives the median and the range across rounds.

The two builds are the `WireItem` frame at `523afc0` and the byte path at `cadd2f3`, whose runtime merged unchanged as `d1fa15a`. Each has the item shapes of `requests` added and nothing else. Both were built in turn in one checkout and one target directory.

| User instructions per item | 16 B | 1 KiB | 16 KiB |
|---|---|---|---|
| Lean, `WireItem` frame (`523afc0`) | 10,420 (9,974 to 10,807) | 12,770 (12,673 to 12,889) | 35,544 (35,283 to 35,918) |
| Lean, byte path (`cadd2f3`) | 9,172 (8,703 to 9,560) | 11,186 (11,027 to 11,352) | 32,010 (31,756 to 32,108) |
| velo direct, at `523afc0` | 8,579 (8,107 to 8,812) | 10,022 (9,909 to 10,105) | 28,809 (28,336 to 29,112) |
| velo direct, at `cadd2f3` | 8,174 (7,552 to 8,791) | 10,048 (9,836 to 10,110) | 28,551 (28,128 to 29,047) |

| Kernel instructions per item | 16 B | 1 KiB | 16 KiB |
|---|---|---|---|
| Lean, `WireItem` frame (`523afc0`) | 6,846 (6,170 to 7,297) | 8,847 (8,372 to 9,326) | 27,607 (26,817 to 28,897) |
| Lean, byte path (`cadd2f3`) | 7,430 (6,377 to 7,857) | 9,178 (8,068 to 9,374) | 28,895 (27,563 to 34,592) |
| velo direct, at `523afc0` | 7,280 (5,806 to 9,652) | 8,644 (8,404 to 9,412) | 30,284 (28,630 to 31,831) |
| velo direct, at `cadd2f3` | 6,261 (5,507 to 7,251) | 8,629 (8,137 to 11,421) | 29,345 (27,940 to 30,868) |

**Reading.**
- **With the `WireItem` frame, a crossing adds 1,841, 2,748, and 6,735 user instructions per item** to what velo spends on the same stream: 21%, 27%, and 23% more.
- **The byte path removes 1,248, 1,584, and 3,534 of them** (12%, 12%, and 10% of gdl's count). The ranges of the two builds do not overlap at any size. What is left over velo direct is 998, 1,138, and 3,459.
- **velo direct is the control.** Its code is the same in both builds. Its medians differ by 405 instructions at 16 B and under 260 at the other sizes, inside the ranges.
- **Kernel instructions settle nothing.** They are as large as the user count at 16 KiB, their ranges overlap for every pair, and they include parking and waking the workers, which grows with wall time rather than with items.
- **A count per item includes idle work.** One stream keeps the workers busy for only part of each item's time, so each item also pays for the park and wake of the threads that wait for it. The two paths pay it alike.

**Main.** A second run of seven rounds, set up the same way (load average 5.9, falling to 1.6), compared the byte path with main at `d1fa15a`. Main also has a change to local hops (`gdl/src/items.rs`, `cx.rs`, and `compose.rs`).

| User instructions per item | 16 B | 1 KiB | 16 KiB |
|---|---|---|---|
| Lean, byte path (`cadd2f3`) | 9,187 (9,006 to 9,560) | 11,145 (11,032 to 11,302) | 31,995 (31,437 to 32,242) |
| Lean, main (`d1fa15a`) | 8,895 (8,842 to 9,526) | 11,158 (11,062 to 11,260) | 31,824 (31,564 to 32,085) |
| velo direct, at `cadd2f3` | 8,462 (8,100 to 9,060) | 9,993 (9,765 to 10,123) | 28,734 (28,396 to 28,915) |
| velo direct, at `d1fa15a` | 8,245 (7,891 to 8,653) | 9,903 (9,798 to 9,987) | 28,421 (28,023 to 28,771) |

- **The local-hop change shows no cost or saving per crossed item.** Main's ranges overlap the byte path's at every size.
- **The byte path's counts repeat.** Its medians are within 0.4% of the first run's.
- **At main, a crossed item costs 650, 1,255, and 3,403 user instructions more than velo carrying it**: 8%, 13%, and 12% more.

**Main after in-place encode.** A third run of seven rounds, set up the same way (load average 7.2, falling to 1.6), compared main at `d1fa15a` with main at `0ff3a6b`, where the edge codec encodes each item in place into velo's frame. `0ff3a6b` also pins a newer velo (`e892f87`).

| User instructions per item | 16 B | 1 KiB | 16 KiB |
|---|---|---|---|
| Lean, main (`d1fa15a`) | 9,004 (8,192 to 9,289) | 11,134 (11,041 to 11,213) | 31,860 (31,567 to 32,240) |
| Lean, in place (`0ff3a6b`) | 8,856 (8,522 to 9,190) | 10,336 (10,250 to 10,435) | 28,942 (28,786 to 29,334) |
| velo direct, at `d1fa15a` | 8,613 (7,278 to 8,889) | 9,908 (9,697 to 10,172) | 28,478 (27,977 to 28,818) |
| velo direct, at `0ff3a6b` | 8,256 (8,083 to 8,561) | 9,983 (9,921 to 10,176) | 28,691 (28,526 to 28,927) |

- **The in-place encode removes 798 user instructions per item at 1 KiB and 2,918 at 16 KiB** (7% and 9%). The ranges do not overlap. At 16 B the ranges overlap, so this run shows no change there.
- **After it, a crossed item costs 600, 353, and 251 user instructions more than velo carrying it**: 7%, 4%, and 1%. At 16 KiB the ranges of the lean graph and velo direct overlap, so the crossing costs no measurable amount more there.
- **The controls hold.** velo direct's ranges overlap between the two builds at every size, although `0ff3a6b` pins a newer velo. Main's lean medians repeat the second run within 1.2%.

**Reproduce.** On Linux with `perf` allowed (`perf_event_paranoid` 1 or lower):

```bash
python3 scripts/bench/items.py count --ref main=<base commit> --ref bytepath=<change commit> --out <new directory> --cpus 16-23 --rounds 7
```

Each ref must contain the item shapes of `crates/gdl-runtime/benches/requests.rs`. The script builds each ref in a temporary worktree under `target/`, prints the table above, and writes each round to `rounds.json`.

## Where an item's cycles go

A sampling profile of the same streams shows where the cycles of an item go, at both ends. One process runs both ends, so one profile covers the producer's send and the consumer's receive.

**Setup.** The gb300 bench host as above, load average 2.0. `scripts/bench/items.py profile` builds `requests` with line tables and runs one stream of 100,000 items of 16 B. `perf record -e cycles -c 250000 --call-graph dwarf,8192 --no-buildid` samples every 250,000 cycles on any thread, in user space or in the kernel. The script expands each frame's inlined functions with `addr2line`, because async functions are inlined into tokio's task poll. Then it names each sample by the first step that a frame matches, from the sampled frame out to the thread's root. A step's callees count as the step's, so an allocation or a copy that a step makes is that step's cost. Every sampled address was expanded. All three profiles ran with the same `items.py`.

| Share of all cycles | Lean, `WireItem` frame | Lean, byte path | Lean, main | Lean, in place | velo direct |
|---|---|---|---|---|---|
| velo transport (mux, batcher, TCP, sockets) | 60.0% | 60.9% | 60.2% | 60.4% | 62.1% |
| tokio scheduler (park, wake, task polls) | 24.7% | 25.8% | 27.3% | 25.2% | 25.4% |
| velo send (frame encode, enqueue) | 4.4% | 4.3% | 4.0% | 5.4% | 6.7% |
| velo receive (frame decode) | 2.2% | 2.1% | 1.9% | 1.8% | 1.9% |
| `WireItem` frame (`dist/envelope.rs`) | 5.1% | 0.0% | 0.0% | 0.0% | 0.6% |
| gdl `dist/hop.rs` (the pump, `InboundItems`) | 1.0% | 0.8% | 1.0% | 0.7% | 0.0% |
| gdl `dist/fabric.rs`, `dist/stages.rs` (`Outbound`, `Inbound`, the codec streams) | 0.7% | 2.7% | 2.7% | 3.1% | 0.0% |
| edge codec (`gdl::codec`) | 0.3% | 0.4% | 0.4% | 0.1% | 0.0% |
| other gdl and velo | 1.1% | 2.2% | 2.1% | 2.4% | 0.7% |
| bench (build and check blobs) | 0.5% | 0.5% | 0.2% | 0.7% | 2.5% |
| other | 0.1% | 0.3% | 0.2% | 0.1% | 0.2% |
| Samples | 7,926 | 7,366 | 7,184 | 7,356 | 7,106 |
| Cycles per item | 19,800 | 18,400 | 18,000 | 18,400 | 17,800 |

The builds are the ones the instruction counts used: main is `d1fa15a`, and in place is `0ff3a6b`. In place, the edge encode runs as `gdl::codec::write_msgpack`, which the script's edge codec step does not match. Its samples count under other gdl and velo, which is 2.4% in all. Each profile is one run, so its shares and cycles carry a run-to-run spread that these runs did not measure. In the transport, 34% to 36% of the cycles are in the kernel (socket calls). In the scheduler, 40% to 43% are (futex waits and wakes). Setup takes about 80 samples, which is about 1% of each profile.

**Reading.**
- **The transport and the scheduler take 85% to 88% of every profile.** They take it on velo direct as well. One stream of small items spends its cycles moving bytes over loopback and waking threads, not on the item.
- **gdl's own steps were 7.8% of the cycles with the `WireItem` frame, 4.4% on the byte path, 4.6% at main, and 4.7% in place.** The `WireItem` frame alone was 5.1%. On the byte path, `dist/fabric.rs` and `dist/stages.rs` take more than before (2.7% against 0.7%). The profile does not show why.
- **The two views rank the builds alike.** Cycles per item and user instructions per item both put the `WireItem` frame first, then the byte path, then main, and velo direct last. At 16 B the instruction ranges of main and in place overlap, so neither view orders those two. Over velo direct, the lean graph is 11% in cycles and 21% in user instructions with the `WireItem` frame, and 4% and 12% on the byte path. The sizes differ: cycles include the kernel's work, whose counts overlap for every pair, and user instructions do not.
- **About one point of a step's share is attribution error.** velo direct never calls gdl's envelope code, yet 0.6% of its samples name `dist/envelope.rs`. The cause is not known. One possible cause is that the compiler merged identical functions, so that a merged function names only one of its sources. A share under about one point does not show a cost.
- **Inclusive shares.** In place (`0ff3a6b`), `pump_watching` (the producer end, `dist/hop.rs`) holds 8.4% of the cycles with its callees, velo's send included. At `d1fa15a` it held 7.0%. `InboundItems::poll_next` (the consumer end) holds 3.9%, against 4.1%. Each is one run. The [hot-paths register](hot-paths.md) lists both.

**Reproduce.**

```bash
python3 scripts/bench/items.py profile --ref main=<commit> --out <new directory> --shape lean-16 --count 100000 --period 250000 --stack 8192 --cpus 16-23
```

The script prints the step table, the leaf functions, and the project functions by inclusive share, and writes them to `steps.txt`.

## Codec cost

Application messages cross as named-field MessagePack (`msgpack`) by default. A fixed layout can choose positional MessagePack (`msgpack_array`), and a message can choose JSON. Named MessagePack keeps omitted fields, including nested ones. JSON has [value restrictions](limitations.md#json-cannot-preserve-every-serde-value).

**Crossing overhead, in process.** Encode and decode of one item as velo carries it directly, against gdl's crossing. The crossing's edge codec writes the item straight into velo's frame, as one velo byte item, and the receiver decodes it in the buffer it arrived in ([Crossings](../concepts/crossings.md#what-crosses-the-wire)). The item is a 16 B, 1 KiB, or 16 KiB blob. Two earlier frames are shown for comparison: the `WireItem` frame wrapped the bytes in an enum inside velo's frame, and the byte path with a copy encoded the item into its own buffer, which `send_bytes` copied into the frame.

| Item size | velo direct | `WireItem` frame | Byte path with a copy | In place | In place minus velo |
|---|---|---|---|---|---|
| 16 B | 68.4 ns | 164.1 ns | 60.3 ns | 47.6 ns | -20.8 ns |
| 1 KiB | 144.1 ns | 284.5 ns | 207.4 ns | 105.9 ns | -38.2 ns |
| 16 KiB | 484.6 ns | 939.7 ns | 691.7 ns | 456.6 ns | -28.0 ns |

The in-place crossing in three steps, timed one by one:

| Item size | Encode in place | Frame decode | Edge decode |
|---|---|---|---|
| 16 B | 18.6 ns | 7.7 ns | 21.6 ns |
| 1 KiB | 65.7 ns | 8.3 ns | 33.6 ns |
| 16 KiB | 229.9 ns | 9.5 ns | 226.8 ns |

The byte path with a copy spent 13.5, 67.1, and 228.2 ns copying the item into the frame, and its edge encode took 15.9, 85.4, and 235.2 ns.

- **The item is written in place.** The edge codec writes each item into velo's frame (velo's `ByteItemBuilder`, sent with `send_frame`), so the copy into the frame is gone. A frame reserves the size of the stream's recent items (below), so a stream of like items needs no buffer growth either.
- **Velo direct writes structs as positional arrays.** gdl's default codec writes named maps, so for a struct message part of the gap is the codec choice, not the frame. The blob here is one byte string, the same in both.
- **Allocations per item, end to end.** `crates/gdl-runtime/tests/item_allocs.rs` runs the lean graph and velo direct over two in-process nodes, 1,000-item requests, and takes each path's fewest allocations and fewest bytes per item over 15 alternating rounds. With the item written in place, the crossing makes 6.27, 6.36, and 11.39 allocations per item at 16 B, 1 KiB, and 16 KiB, against velo's 7.23, 8.32, and 13.44. It allocates 457 B, 5,806 B, and 116,284 B per item, against velo's 439 B, 5,799 B, and 117,146 B. The byte path with a copy made 7.27, 7.36, and 12.38 allocations and 480 B, 6,837 B, and 134,283 B in the same test. A second, paced workload sends each item only once the consumer waits for it, as a token stream does, so that a cost paid on each wait counts on every item: 24.29, 24.28, and 27.40 allocations against velo's 25.19, 26.18, and 29.22. The test fails if the crossing makes more allocations than velo (plus 0.02), allocates more than 128 B over velo at 16 B, or more than 2% over velo at 1 KiB and 16 KiB. Some rounds cost about one allocation per item more on either path, depending on how the two ends are scheduled, so the test compares the fewest, not the median.
- **The frame size hint.** Each stream reserves the smaller of its last two items' sizes for the next frame. A third workload in the same test sends 16 KiB and 16 B items in turn, and fails if the crossing allocates more than velo plus 2% plus 128 B per small item. The crossing allocates 61,163 B per item there, against velo's 61,127 B. A hint of the last item's size fails it at 69,348 B, because each small item reserves 16 KiB. A hint of the last size capped at 1 KiB fails the 1 KiB rows at 7,870 B against 5,799 B, because a 1 KiB blob encodes to 1,027 bytes and its frame grows once.
- **CPU per item, end to end**, 32 streams of 1,000 16 B items at once (the bench's first lines): 1.45 µs for the lean graph against 1.19 µs for velo direct with the `WireItem` frame, and 1.36 µs against 1.23 µs with the byte path. One run each, so this is not a claim.

**Setup.** gb300 bench host (Neoverse-V2, pinned to CPU 9, load average 3.6 to 3.9), rustc 1.93.1. The `WireItem` frame at `523afc0` (velo `cee2834`). The byte path with a copy at `ddc0d4a` (velo `cee2834`), and in place at `d8ac735` (velo `e892f87`). Each row is criterion's median with 1 s warm-up and 3 s measurement. Velo direct is the same function in every run: 68.4 and 69.1 ns at 16 B, 144.1 and 133.2 ns at 1 KiB, 484.6 and 486.7 ns at 16 KiB. The table gives the in-place run's.

```bash
cargo bench -p gdl-runtime --bench remote -- 'codec-' --warm-up-time 1 --measurement-time 3
timeout 120 cargo nextest run -p gdl-runtime --test item_allocs --no-capture
```

**The three encodings.** `crates/gdl-runtime/benches/codecs.rs` calls `Codec<T>` through its production function pointers and measures encode plus owned decode, with no counting allocator. The change adds one reservation of 128 bytes per encoded value in both MessagePack encoders (`crates/gdl/src/codec.rs`), which removes buffer growth for small items. It does not change the bytes. JSON is the control.

**Setup.** Shared Arm host (Cortex-X925, CPU 5, governor `performance`, Linux 7.0.0-1019-nvidia, hardware counters unavailable), rustc 1.93.1, rmp-serde 1.3.1, serde_json 1.0.154, velo `cee2834`. Baseline `9ce6b6d`, change `3cdf19a`, each in its own target directory with the same lockfile. Ten rounds alternating order, 100 ms warm-up, 500 ms measurement, 20 samples, load average 3.69 to 9.62. Medians of round medians, interquartile range in parentheses.

| Fixture | Encoding | Bytes | Baseline ns | Change ns | Difference |
|---|---|---|---|---|---|
| Token | named MessagePack | 40 | 112.80 (112.71 to 112.92) | 72.96 (72.84 to 73.31) | -35.3% |
| Token | positional MessagePack | 14 | 67.44 (67.10 to 67.53) | 51.58 (50.87 to 52.15) | -23.5% |
| Token | JSON | 57 | 164.85 (164.73 to 164.93) | 166.01 (165.13 to 166.32) | +0.7% |
| Nested chunk | named MessagePack | 108 | 274.78 (268.01 to 281.79) | 206.07 (205.70 to 207.22) | -25.0% |
| Nested chunk | positional MessagePack | 44 | 195.55 (194.81 to 196.02) | 144.41 (143.86 to 145.09) | -26.2% |
| Nested chunk | JSON | 150 | 359.67 (357.16 to 361.23) | 360.65 (359.92 to 361.28) | +0.3% |

- **Positional beats named** by about 30% in time and 60% to 65% in bytes (14 against 40; 44 against 108). **JSON is the slowest** and larger than either MessagePack. Name a codec by the message, not for speed alone: positional arrays cannot omit a field.
- **These are codec timings, not throughput.** A two-segment loopback test with one graph per codec showed about 11 K items/s per stream for every codec, because the transport limit above hides the codec. Concurrent runs spread more than the codecs differ. No end-to-end speedup is claimed.
- **The allocation claim is a test.** `crates/gdl-runtime/tests/allocs.rs` requires one output allocation for a small named token under each codec. It failed with four before the reservation. `tests/wire.rs` compares bytes against serde across buffer sizes, including 64 KiB.
- **Decoding still allocates** for each `String` and `Vec` field, because the codec returns an owned message. Reusing that storage needs an ownership path that returns the fields after the consumer releases them. Serde's in-place deserialization is not enabled, and its borrowed model differs from the `DeserializeOwned` boundary. No allocator comparison (for example jemalloc) has run.

**Reproduce.** Build each source tree in its own target directory. In the baseline tree, copy `crates/gdl-runtime/benches/codecs.rs`, replace `Encoding::ALL` in the copy with `[Encoding::Msgpack, Encoding::MsgpackArray, Encoding::Json]`, and add `[[bench]] name = "codecs"` with `harness = false` to its `crates/gdl-runtime/Cargo.toml`. Without `harness = false`, libtest runs and rejects `--noplot`.

```bash
CARGO_TARGET_DIR=/path/to/base-target cargo bench --locked --manifest-path /path/to/base/Cargo.toml -p gdl-runtime --bench codecs --no-run
CARGO_TARGET_DIR=/path/to/change-target cargo bench --locked -p gdl-runtime --bench codecs --no-run
python3 scripts/bench/codecs.py --base /path/to/base-binary --change /path/to/change-binary --out /path/to/new-results --cpu 5 --rounds 10
timeout 60 cargo nextest run -p gdl-runtime --test allocs --test wire
```

The runner alternates the order, records binary hashes and load, and keeps each criterion result.
