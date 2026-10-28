//! What crossing segments costs, in time. All velo nodes run in this
//! process over loopback TCP. No instrumentation runs here: allocation and
//! memory counts come from `benches/footprint.rs`, whose counting allocator
//! would otherwise be measured too.
//!
//! - **Matched:** a lean graph (`S => D`, the sink emitting `n` blobs of
//!   `size` bytes) split over two segments, against the same request and
//!   stream written directly on velo, opened the same way gdl opens it
//!   (velo's ticket path, with no attach round trip). Both sides do the same
//!   work per item: build a blob of `size` bytes and send it; neither logs.
//!   Measured at 16 B, 1 KiB, and 16 KiB, one stream at a time and 32 at
//!   once. velo's plain attach is reported alongside.
//! - **Codec:** one item's encode and decode as each side frames it, in
//!   process, for the copy a crossing adds per item; and the crossing's
//!   four steps on their own (`gdl-crossing/1-edge-encode` and so on).
//! - **Reference graph:** the test suites' graph (with its event log off)
//!   local, in one segment (the distributed stages without a crossing),
//!   over two segments, and over three.
//! - **Metrics:** the reference rows again with the runtime's per-segment
//!   figures recorded (`-metrics`), and 32 reference requests at once, each
//!   a task of its own (`concurrent-32x1-reference`), where a figure every
//!   thread touches would contend (see docs/src/operations/feature-costs.md).
//! - **Heads:** a request whose head is one blob of 16 B, 64 KiB, or 4 MiB,
//!   over two segments, answered with one item (`head-<size>B-2-segments`):
//!   what a crossing costs per byte of request head. And one whose head is
//!   1,000 words of 8 B (`fields-1000-2-segments`): what it costs per field.
//! - **Upload:** a streamed request (`S => D`, `n` blobs of `size` bytes in,
//!   one count back) over two segments, against the same exchange on velo
//!   alone: the server mints the input anchor and answers with its ticket.
//!
//! Every request is checked: no errors, exactly the requested items, and on
//! the direct baseline an explicit `Finalized`. A truncated or failed stream
//! panics instead of being counted as fast.

#![allow(missing_docs)]
// criterion_group! generates an undocumented public fn.

#[path = "../tests/common/mod.rs"]
mod common;
#[path = "remote/setup.rs"]
mod setup;

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use setup::*;

/// Nothing is counted while timing.
#[inline(always)]
fn produced() {}

/// `setup` builds its velo nodes as a segment does.
async fn node() -> velo::Velo {
    common::velo_node().await
}

/// How `rmp_serde` encodes every `velo::StreamFrame::Item`: a one-entry map
/// keyed by the fixstr `"Item"`. velo's own copy is crate-private.
const ITEM_PREFIX: &[u8] = &[0x81, 0xa4, b'I', b't', b'e', b'm'];

/// One item's encode and decode as velo carries it directly, as the frame's
/// payload. For the copy a crossing adds per item, which the end-to-end rate
/// (bounded by velo's single-stream rate) cannot show.
fn codec_direct(blob: &Blob) -> usize {
    let frame = rmp_serde::to_vec(&velo::StreamFrame::Item(blob)).expect("encode");
    match rmp_serde::from_slice::<velo::StreamFrame<Blob>>(&frame).expect("decode") {
        velo::StreamFrame::Item(blob) => blob.0.len(),
        _ => unreachable!(),
    }
}

/// The same item as gdl carries it, in the three steps each row below times
/// on its own: the edge codec encodes it straight into velo's frame
/// (`ByteItemBuilder`, sent with `StreamSender::send_frame`), the receiver
/// takes the payload as a view of the frame (`StreamAnchor::next_bytes`),
/// and the edge codec decodes it. The frame reserves the size of the item
/// before it, as a stream of like items does.
fn codec_crossing(blob: &Blob, hint: usize) -> usize {
    let frame = encode_in_place(blob, hint);
    let item = frame_decode(frame);
    edge_decode(&item).0.len()
}

/// What the runtime's encode does for each crossing item, as
/// `velo::streaming::ByteItemBuilder` does it: the item prefix and a `bin32`
/// header in a buffer with room for `hint` bytes of payload, the edge codec
/// writing after them, then the length. velo keeps the built frame's buffer
/// private, so the bench builds the same bytes itself.
fn encode_in_place(blob: &Blob, hint: usize) -> Vec<u8> {
    let encode = gdl::__runtime::encoder_into::<Blob>(Default::default());
    let mut frame = Vec::with_capacity(HEADER + hint);
    frame.extend_from_slice(ITEM_PREFIX);
    frame.extend_from_slice(&[0xc6, 0, 0, 0, 0]);
    encode(blob, &mut frame).expect("edge encode");
    let len = u32::try_from(frame.len() - HEADER).expect("under 4 GiB");
    frame[HEADER - 4..HEADER].copy_from_slice(&len.to_be_bytes());
    frame
}

/// The bytes before a built item's payload.
const HEADER: usize = ITEM_PREFIX.len() + 5;

fn edge_encode(blob: &Blob) -> Vec<u8> {
    gdl_runtime::wire_codec!(Blob)
        .expect("wire")
        .encode(blob)
        .expect("edge encode")
}

fn edge_decode(bytes: &[u8]) -> Blob {
    gdl_runtime::wire_codec!(Blob)
        .expect("wire")
        .decode(bytes)
        .expect("edge decode")
}

/// What `next_bytes` hands on: the payload as a view of the frame's buffer,
/// found by its header, with no copy.
fn frame_decode(frame: Vec<u8>) -> bytes::Bytes {
    use bytes::Buf;
    let header = match frame[ITEM_PREFIX.len()] {
        0xc4 => 2,
        0xc5 => 3,
        _ => 5,
    };
    let mut item = bytes::Bytes::from(frame);
    item.advance(ITEM_PREFIX.len() + header);
    item
}

/// Process CPU time (user plus system), from `/proc/self/stat`, in
/// microseconds. Linux only: elsewhere it reads zero and the report says so.
fn cpu_micros() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // Fields after the command name, which is in parentheses and may hold
    // spaces: utime and stime are the 12th and 13th of them.
    let after = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
    let fields: Vec<&str> = after.split_whitespace().collect();
    let ticks: u64 = fields
        .get(11..13)
        .map_or(0, |f| f.iter().filter_map(|v| v.parse::<u64>().ok()).sum());
    // The kernel reports in clock ticks of 10 ms on every Linux build.
    ticks * 10_000
}

/// CPU time per item for many streams at once: the measure for per-item
/// work that throughput, bounded by velo's stream rate, cannot show.
fn cpu_per_item(rt: &tokio::runtime::Runtime, setups: &Setups) {
    const STREAMS: u32 = 32;
    const ROUNDS: u32 = 20;
    let ask = Ask {
        items: 1_000,
        size: 16,
    };
    println!("CPU per item, {STREAMS} streams at once of 1,000 items of 16 B:");
    for (label, lean) in [("lean 2 segments", true), ("velo direct", false)] {
        let run = || async {
            if lean {
                futures::future::join_all((0..STREAMS).map(|_| setups.lean(ask))).await;
            } else {
                futures::future::join_all((0..STREAMS).map(|_| setups.direct(ask))).await;
            }
        };
        rt.block_on(run());
        let before = cpu_micros();
        for _ in 0..ROUNDS {
            rt.block_on(run());
        }
        let items = f64::from(STREAMS * ROUNDS * ask.items);
        let per_item = (cpu_micros() - before) as f64 / items;
        println!("{label:<24} {per_item:>6.2} us CPU per item (all threads, both ends)");
    }
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    // Shared, so that concurrent requests can run as tasks of their own.
    let setups = std::sync::Arc::new(rt.block_on(setups()));
    cpu_per_item(&rt, &setups);

    let one = Ask { items: 1, size: 16 };
    let mut latency = c.benchmark_group("request");
    latency.bench_function("reference-local", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.local, black_box(1)))
    });
    latency.bench_function("reference-1-segment", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.one, black_box(1)))
    });
    latency.bench_function("reference-1-segment-metrics", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.one_metrics, black_box(1)))
    });
    latency.bench_function("reference-2-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.two, black_box(1)))
    });
    latency.bench_function("reference-2-segments-metrics", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.two_metrics, black_box(1)))
    });
    latency.bench_function("reference-3-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.three, black_box(1)))
    });
    latency.bench_function("reference-3-segments-metrics", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.three_metrics, black_box(1)))
    });
    latency.bench_function("lean-2-segments", |b| {
        b.to_async(&rt).iter(|| setups.lean(black_box(one)))
    });
    latency.bench_function("velo-direct", |b| {
        b.to_async(&rt).iter(|| setups.direct(black_box(one)))
    });
    latency.bench_function("velo-direct-attach", |b| {
        b.to_async(&rt)
            .iter(|| setups.direct_attach(black_box(one)))
    });
    latency.bench_function("upload-2-segments", |b| {
        b.to_async(&rt).iter(|| setups.upload(black_box(one)))
    });
    latency.bench_function("velo-direct-upload", |b| {
        b.to_async(&rt)
            .iter(|| setups.direct_upload(black_box(one)))
    });
    for size in HEAD_SIZES {
        latency.bench_function(format!("head-{size}B-2-segments"), |b| {
            b.to_async(&rt)
                .iter(|| head(&setups.heads, black_box(size)))
        });
    }
    latency.bench_function(format!("fields-{HEAD_WORDS}-2-segments"), |b| {
        b.to_async(&rt)
            .iter(|| fields(&setups.heads, black_box(HEAD_WORDS)))
    });
    latency.finish();

    const ITEMS: u32 = 1_000;
    for size in SIZES {
        let ask = Ask { items: ITEMS, size };
        let mut items = c.benchmark_group(format!("items-{size}B"));
        items.throughput(Throughput::Elements(u64::from(ITEMS)));
        items.sample_size(20);
        items.measurement_time(Duration::from_secs(8));
        items.bench_function("lean-2-segments", |b| {
            b.to_async(&rt).iter(|| setups.lean(black_box(ask)))
        });
        items.bench_function("velo-direct", |b| {
            b.to_async(&rt).iter(|| setups.direct(black_box(ask)))
        });
        items.bench_function("upload-2-segments", |b| {
            b.to_async(&rt).iter(|| setups.upload(black_box(ask)))
        });
        items.bench_function("velo-direct-upload", |b| {
            b.to_async(&rt)
                .iter(|| setups.direct_upload(black_box(ask)))
        });
        items.finish();
    }

    for size in SIZES {
        let item = Blob(vec![7; size as usize]);
        let mut codec = c.benchmark_group(format!("codec-{size}B"));
        codec.throughput(Throughput::Bytes(u64::from(size)));
        codec.bench_function("velo-direct", |b| b.iter(|| codec_direct(black_box(&item))));
        // A stream of like items reserves each one's encoded size.
        let hint = edge_encode(&item).len();
        codec.bench_function("gdl-crossing", |b| {
            b.iter(|| codec_crossing(black_box(&item), hint))
        });
        // The crossing's steps, so a change that regrows one shows where.
        let frame = encode_in_place(&item, hint);
        // The bench's frame is the one velo builds: the same payload, read
        // by serde as velo's consumers read it.
        let mut built = velo::streaming::ByteItemBuilder::with_capacity(hint);
        gdl::__runtime::encoder_into::<Blob>(Default::default())(&item, built.buffer())
            .expect("encode");
        assert_eq!(&frame[HEADER..], built.finish().expect("finish").payload());
        let serde: velo::StreamFrame<serde_bytes::ByteBuf> =
            rmp_serde::from_slice(&frame).expect("decode");
        assert!(matches!(&serde, velo::StreamFrame::Item(bytes) if bytes[..] == frame[HEADER..]));
        assert_eq!(frame.len(), frame.capacity(), "the hint is not the size");
        codec.bench_function("gdl-crossing/1-encode-in-place", |b| {
            b.iter(|| encode_in_place(black_box(&item), hint))
        });
        codec.bench_function("gdl-crossing/2-frame-decode", |b| {
            b.iter_batched(
                || frame.clone(),
                |frame| frame_decode(black_box(frame)),
                criterion::BatchSize::SmallInput,
            )
        });
        let edge = frame[HEADER..].to_vec();
        codec.bench_function("gdl-crossing/3-edge-decode", |b| {
            b.iter(|| edge_decode(black_box(&edge)))
        });
        codec.finish();
    }

    // Many streams at once: per-item costs that touch shared state (a
    // runtime-wide timer lock, for example) show here and not one at a time.
    const STREAMS: u32 = 32;
    let ask = Ask {
        items: ITEMS,
        size: 16,
    };
    let mut concurrent = c.benchmark_group("concurrent-32x1000-16B");
    concurrent.throughput(Throughput::Elements(u64::from(STREAMS * ITEMS)));
    concurrent.sample_size(10);
    concurrent.measurement_time(Duration::from_secs(10));
    concurrent.bench_function("lean-2-segments", |b| {
        b.to_async(&rt)
            .iter(|| futures::future::join_all((0..STREAMS).map(|_| setups.lean(black_box(ask)))))
    });
    concurrent.bench_function("velo-direct", |b| {
        b.to_async(&rt)
            .iter(|| futures::future::join_all((0..STREAMS).map(|_| setups.direct(black_box(ask)))))
    });
    concurrent.finish();

    // Many requests at once, each a task of its own on the four workers:
    // what a shared counter's contention costs a request, which one
    // request at a time cannot show.
    let mut requests = c.benchmark_group("concurrent-32x1-reference");
    requests.throughput(Throughput::Elements(u64::from(STREAMS)));
    requests.sample_size(20);
    for (name, metrics) in [("1-segment", false), ("1-segment-metrics", true)] {
        requests.bench_function(name, |b| {
            b.to_async(&rt).iter(|| {
                futures::future::join_all((0..STREAMS).map(|_| {
                    let setups = std::sync::Arc::clone(&setups);
                    tokio::spawn(async move {
                        let caller = if metrics {
                            &setups.one_metrics
                        } else {
                            &setups.one
                        };
                        reference(caller, black_box(1)).await
                    })
                }))
            })
        });
    }
    requests.finish();

    let mut reference_items = c.benchmark_group("reference-items");
    reference_items.throughput(Throughput::Elements(u64::from(ITEMS)));
    reference_items.sample_size(20);
    reference_items.measurement_time(Duration::from_secs(8));
    reference_items.bench_function("local", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.local, black_box(ITEMS)))
    });
    reference_items.bench_function("2-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.two, black_box(ITEMS)))
    });
    reference_items.bench_function("2-segments-metrics", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.two_metrics, black_box(ITEMS)))
    });
    reference_items.bench_function("3-segments", |b| {
        b.to_async(&rt)
            .iter(|| reference(&setups.three, black_box(ITEMS)))
    });
    reference_items.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
