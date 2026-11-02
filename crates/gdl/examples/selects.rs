//! `selects <case> <count>` makes `count` selections by `affinity`, keyed
//! on a session's metadata entry, as the runtime makes them, through the
//! erased selector, for callgrind or `perf stat` to count a selection's
//! instructions: run it at two counts and divide the difference by the
//! difference in selections, so that the setup cancels. The key takes
//! turns over 64 sessions. `<case>` is one of:
//!
//! - `five`: 5 instances, nothing declined;
//! - `alone`: 1 instance of 25, one declined, as a walk after an
//!   instance's own decline asks a selector that names one again;
//! - `full`: the 25 instances, one declined, as a walk asks;
//! - `stamped`: `five` with each instance's join stamp, as a pool's row
//!   gives them, and `through` not set;
//! - `cutoff`: `stamped` with `through` set and a session's cutoff that
//!   keeps 4 of the 5;
//! - `uncut`: `stamped` with `through` set and no cutoff;
//! - `full-cut`: `full` with stamps and a cutoff that keeps 20 of the 25.
//!
//! ```text
//! cargo build --release -p gdl --example selects
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/selects five 100000
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/selects five 200000
//! ```

#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use gdl::__runtime::{Erased, Typed};
use gdl::{Affinity, AffinityConfig, Cx, InstanceId, Instances, JoinStamps, JoinedThrough};

fn main() {
    let mut args = std::env::args().skip(1);
    let case = args.next().expect("a case");
    let count: u64 = args
        .next()
        .expect("a count")
        .parse()
        .expect("a count of selections");
    let ids: Vec<InstanceId> = (0..25u64).map(id).collect();
    let joined: Vec<u64> = (1..=25).collect();
    let (instances, declined, stamps, through, cut) = match case.as_str() {
        "five" => (&ids[..5], &[][..], &[][..], false, None),
        "alone" => (&ids[3..4], &ids[..1], &[][..], false, None),
        "full" => (&ids[..], &ids[..1], &[][..], false, None),
        "stamped" => (&ids[..5], &[][..], &joined[..5], false, None),
        "cutoff" => (&ids[..5], &[][..], &joined[..5], true, Some(4)),
        "uncut" => (&ids[..5], &[][..], &joined[..5], true, None),
        "full-cut" => (&ids[..], &ids[..1], &joined[..], true, Some(20)),
        other => {
            panic!("no case `{other}`: five, alone, full, stamped, cutoff, uncut, or full-cut")
        }
    };
    let selector = Typed(Arc::new(Affinity::new(AffinityConfig {
        key: Some("session".into()),
        through,
    })));
    let stamps = JoinStamps::new(instances, stamps.into(), 7);
    let cxs: Vec<Cx> = (0..64)
        .map(|n| {
            let mut cx = Cx::new();
            cx.insert_meta("session", format!("session-{n:04}"));
            if let Some(joined) = cut {
                cx.insert_ext(JoinedThrough { epoch: 7, joined });
            }
            cx
        })
        .collect();
    let mut waker = Context::from_waker(Waker::noop());
    let mut sink = 0usize;
    for turn in 0..count {
        let cx = &cxs[(turn % 64) as usize];
        let mut chosen = selector.select(
            cx,
            black_box(b"head"),
            gdl_model::Encoding::default(),
            black_box(declined),
            Instances::with_joined("worker", black_box(instances), &stamps),
        );
        let Poll::Ready(picked) = chosen.as_mut().poll(&mut waker) else {
            panic!("affinity never waits");
        };
        sink = sink.wrapping_add(picked.expect("a choice").index);
    }
    println!("{}", black_box(sink));
}

/// An instance id spread over 128 bits, the same in every run.
fn id(n: u64) -> InstanceId {
    let mix = |mut x: u64| {
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^ (x >> 31)
    };
    InstanceId::from_u128(u128::from(mix(n + 1)) << 64 | u128::from(mix(!n)))
}
