//! What a pool's entry instances say of their KV cache, as a load summary
//! carries it: five optional numbers, each read alone, and the sequence
//! the pool's hub numbers each summary with. Builds roll one at a time, so
//! a reader of either build reads a summary of the other.

use gdl_pools::{KvLoad, LoadSummary};
use serde::Deserialize;
use serde_json::json;

fn kv() -> KvLoad {
    KvLoad::NONE
        .with_capacity(1_000_000)
        .with_active(120_000)
        .with_committed(640_000)
        .with_reserved(90_000)
        .with_eviction_age_ms(45_000)
}

fn summary() -> LoadSummary {
    LoadSummary {
        admitted: 7,
        limit: 64,
        instances: 2,
        refusals: 1,
        period_ms: 10_000,
        kv: kv(),
        sequence: 12,
    }
}

/// A summary as a reader of the build before the KV numbers and the
/// sequence reads it: every field it knows, and nothing else. Frozen here,
/// so that a change such a reader could not read fails this test.
#[derive(Debug, PartialEq, Eq, Deserialize)]
struct EarlierSummary {
    admitted: u64,
    limit: u64,
    instances: u32,
    refusals: u64,
    period_ms: u64,
}

/// The numbers travel flat, under the names the pools publish, and each
/// reads back alone.
#[test]
fn a_summary_carries_its_kv_numbers_and_sequence_flat() {
    let text = serde_json::to_string(&summary()).expect("json");
    let value: serde_json::Value = serde_json::from_str(&text).expect("json");
    assert_eq!(value["kv_capacity"], 1_000_000, "{text}");
    assert_eq!(value["kv_active"], 120_000, "{text}");
    assert_eq!(value["kv_committed"], 640_000, "{text}");
    assert_eq!(value["kv_reserved"], 90_000, "{text}");
    assert_eq!(value["eviction_age_ms"], 45_000, "{text}");
    assert_eq!(value["sequence"], 12, "{text}");
    assert_eq!(
        serde_json::from_str::<LoadSummary>(&text).expect("a summary"),
        summary()
    );
}

/// A reader of the earlier build ignores the new fields, and reads the
/// summary it knew.
#[test]
fn an_earlier_reader_ignores_the_kv_numbers_and_the_sequence() {
    let text = serde_json::to_string(&summary()).expect("json");
    let read: EarlierSummary = serde_json::from_str(&text).expect("an earlier summary");
    assert_eq!(
        read,
        EarlierSummary {
            admitted: 7,
            limit: 64,
            instances: 2,
            refusals: 1,
            period_ms: 10_000,
        }
    );
}

/// A summary of the earlier build reads with no KV number and sequence 0,
/// and a summary with no KV number writes none of their names.
#[test]
fn an_earlier_summary_reads_with_no_kv_numbers() {
    let earlier = json!({
        "admitted": 3, "limit": 8, "instances": 1, "refusals": 0, "period_ms": 1000,
    });
    let read: LoadSummary = serde_json::from_value(earlier).expect("a summary");
    assert_eq!(read.kv, KvLoad::NONE);
    assert_eq!(read.sequence, 0);
    for field in [
        read.kv.capacity(),
        read.kv.active(),
        read.kv.committed(),
        read.kv.reserved(),
        read.kv.eviction_age_ms(),
    ] {
        assert_eq!(field, None);
    }
    let text = serde_json::to_string(&read).expect("json");
    for name in ["kv_", "eviction_age_ms", "sequence"] {
        assert!(!text.contains(name), "{name} in {text}");
    }
}

/// Each number is read alone: an instance that reports only its capacity
/// has none of the others.
#[test]
fn each_kv_number_is_read_alone() {
    let read: KvLoad = serde_json::from_value(json!({ "kv_capacity": 5 })).expect("kv");
    assert_eq!(read.capacity(), Some(5));
    assert_eq!(read.active(), None);
    assert_eq!(read.eviction_age_ms(), None);
    assert_eq!(read, KvLoad::NONE.with_capacity(5));
}

/// A pool's KV is the sum of its instances': each size sums, saturating,
/// over the instances that report any KV number, and the eviction age is
/// the least of those reported.
#[test]
fn summing_adds_each_size_and_takes_the_least_eviction_age() {
    let a = KvLoad::NONE
        .with_capacity(100)
        .with_active(10)
        .with_eviction_age_ms(9_000);
    let b = KvLoad::NONE
        .with_capacity(u64::MAX - 1)
        .with_active(5)
        .with_eviction_age_ms(4_000);
    let summed = KvLoad::summed([&a, &b]);
    assert_eq!(
        summed.capacity(),
        Some(u64::MAX - 1),
        "saturates below none"
    );
    assert_eq!(summed.active(), Some(15));
    assert_eq!(summed.committed(), None, "neither reports it");
    assert_eq!(summed.eviction_age_ms(), Some(4_000));
    assert_eq!(KvLoad::summed([]), KvLoad::NONE);
    assert_eq!(KvLoad::summed([&a]), a);
}

/// An instance that reports no KV number, as one of an earlier build
/// during a roll, is left out: the sizes are the others' alone, not a
/// guess for every instance.
#[test]
fn an_instance_with_no_kv_number_is_left_out() {
    let a = KvLoad::NONE.with_capacity(100).with_committed(60);
    assert_eq!(KvLoad::summed([&a, &KvLoad::NONE]), a);
    assert_eq!(KvLoad::summed([&KvLoad::NONE, &a]), a);
}

/// Two sizes of one summary cover the same instances, so their ratio
/// means something: a size that one reporting instance leaves out is
/// none for the pool, rather than a sum over fewer instances than the
/// others. The eviction age is the least of those reported, since an
/// instance that evicted nothing leaves it out.
#[test]
fn a_size_one_reporting_instance_leaves_out_is_none() {
    let a = KvLoad::NONE
        .with_capacity(100)
        .with_committed(100)
        .with_eviction_age_ms(5_000);
    let b = KvLoad::NONE.with_capacity(100);
    for summed in [KvLoad::summed([&a, &b]), KvLoad::summed([&b, &a])] {
        assert_eq!(summed.capacity(), Some(200));
        assert_eq!(summed.committed(), None, "b's committed KV is unknown");
        assert_eq!(summed.eviction_age_ms(), Some(5_000));
    }
    let c = KvLoad::NONE.with_capacity(50).with_committed(10);
    assert_eq!(
        KvLoad::summed([&a, &b, &c]).committed(),
        None,
        "a later instance that reports it does not bring it back"
    );
}

/// `u64::MAX` is how a number is held as none, so a node that sets it, or
/// a wire that carries it, says none.
#[test]
fn the_largest_number_reads_as_none() {
    assert_eq!(KvLoad::NONE.with_active(u64::MAX).active(), None);
    let read: KvLoad = serde_json::from_value(json!({ "kv_active": u64::MAX })).expect("kv");
    assert_eq!(read, KvLoad::NONE);
}

/// A summary with the five numbers and its sequence takes no more room in
/// a policy's pool row than it must: the numbers are held as plain words,
/// not as options.
#[test]
fn a_summary_holds_the_kv_numbers_as_plain_words() {
    assert_eq!(std::mem::size_of::<KvLoad>(), 40);
    assert_eq!(std::mem::size_of::<LoadSummary>(), 88);
}
