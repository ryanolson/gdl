//! The keyed record table: least-recently-used records of key to value
//! and version, in shards, so that operations on keys of different shards
//! never wait for each other.

use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use gdl_watch::{Expected, KeyedAnswer, KeyedOp, KeyedRecord};
use lru::LruCache;

/// The fewest records a shard holds when the table has more than one: a
/// table smaller than two of these is one shard, whose eviction is exact.
const MIN_PER_SHARD: usize = 1024;

/// The most shards: enough that the hub's worker threads seldom meet on
/// one shard's lock.
const MAX_SHARDS: usize = 64;

/// A shard's oldest touch while it is empty.
const EMPTY: u64 = u64::MAX;

/// The keyed record table. Each key belongs to one shard, which keeps its
/// records in least-recently-used order under a lock of its own, and
/// evicts its least recently used record once it holds its share of the
/// capacity. So a table of one shard evicts exactly the least recently
/// used record, and one of many evicts the least recently used of the
/// key's shard. Versions come from one counter for the whole table, taken
/// under the shard's lock with the write, so each write that applies to a
/// key has a greater version than the key's last.
pub(crate) struct Table {
    shards: Box<[Shard]>,
    hasher: std::hash::RandomState,
    /// The last version given.
    versions: AtomicU64,
    /// The instant that touches are measured from.
    start: Instant,
}

struct Shard {
    records: Mutex<Records>,
    /// The last touch of the shard's least recently used record, in
    /// nanoseconds since the table's start, or [`EMPTY`]: read without
    /// the lock for the eviction age.
    oldest: AtomicU64,
}

struct Records {
    lru: LruCache<Box<[u8]>, Stored>,
    capacity: usize,
}

struct Stored {
    value: Vec<u8>,
    version: u64,
    /// The last touch, in nanoseconds since the table's start.
    touched: u64,
}

impl Stored {
    fn record(&self) -> KeyedRecord {
        KeyedRecord {
            value: self.value.clone(),
            version: self.version,
        }
    }
}

impl Table {
    /// A table of `capacity` records, at least 1, measuring ages from
    /// `start`. It holds nothing until it is written: it does not take
    /// its capacity's memory at once.
    pub(crate) fn new(capacity: usize, start: Instant) -> Self {
        let capacity = capacity.max(1);
        let count = shard_count(capacity);
        let shards = shares(capacity, count)
            .map(|capacity| Shard {
                records: Mutex::new(Records {
                    lru: LruCache::unbounded(),
                    capacity,
                }),
                oldest: AtomicU64::new(EMPTY),
            })
            .collect();
        Self {
            shards,
            hasher: std::hash::RandomState::new(),
            versions: AtomicU64::new(0),
            start,
        }
    }

    /// Apply `op` at `now`, under its key's shard's lock alone.
    pub(crate) fn apply(&self, op: KeyedOp, now: Instant) -> KeyedAnswer {
        let key = match &op {
            KeyedOp::Get { key } | KeyedOp::Set { key, .. } | KeyedOp::Check { key, .. } => key,
        };
        let shard = &self.shards[self.shard_of(key)];
        let at = self.nanos(now);
        let mut records = shard.records.lock().unwrap_or_else(PoisonError::into_inner);
        let answer = match op {
            KeyedOp::Get { key } => match records.lru.get_mut(key.as_slice()) {
                Some(stored) => {
                    stored.touched = at;
                    KeyedAnswer::Found {
                        record: stored.record(),
                    }
                }
                None => KeyedAnswer::Missing,
            },
            KeyedOp::Check { key, version } => match records.lru.get_mut(key.as_slice()) {
                Some(stored) => {
                    stored.touched = at;
                    if stored.version == version {
                        KeyedAnswer::Unchanged
                    } else {
                        KeyedAnswer::Newer {
                            record: stored.record(),
                        }
                    }
                }
                None => KeyedAnswer::Gone,
            },
            KeyedOp::Set {
                key,
                value,
                expected,
            } => match records.lru.get_mut(key.as_slice()) {
                Some(stored) => {
                    stored.touched = at;
                    let applies = match expected {
                        Expected::Replace => true,
                        Expected::Absent => false,
                        Expected::Version(version) => stored.version == version,
                    };
                    if applies {
                        let version = self.next_version();
                        *stored = Stored {
                            value,
                            version,
                            touched: at,
                        };
                        KeyedAnswer::Applied { version }
                    } else {
                        KeyedAnswer::Refused {
                            current: Some(stored.record()),
                        }
                    }
                }
                None if matches!(expected, Expected::Version(_)) => {
                    KeyedAnswer::Refused { current: None }
                }
                None => {
                    let version = self.next_version();
                    records.lru.push(
                        key.into_boxed_slice(),
                        Stored {
                            value,
                            version,
                            touched: at,
                        },
                    );
                    if records.lru.len() > records.capacity {
                        records.lru.pop_lru();
                    }
                    KeyedAnswer::Applied { version }
                }
            },
        };
        let oldest = records
            .lru
            .peek_lru()
            .map_or(EMPTY, |(_, stored)| stored.touched);
        shard.oldest.store(oldest, Ordering::Relaxed);
        answer
    }

    /// The age at `now` of the table's least recently used record, or
    /// `None` while the table is empty. Reads each shard's oldest touch
    /// without its lock, so it never waits for an operation.
    pub(crate) fn eviction_age(&self, now: Instant) -> Option<Duration> {
        let oldest = self
            .shards
            .iter()
            .map(|shard| shard.oldest.load(Ordering::Relaxed))
            .min()
            .filter(|&oldest| oldest != EMPTY)?;
        Some(Duration::from_nanos(self.nanos(now).saturating_sub(oldest)))
    }

    fn shard_of(&self, key: &[u8]) -> usize {
        // The shard count is a power of two.
        (self.hasher.hash_one(key) as usize) & (self.shards.len() - 1)
    }

    fn next_version(&self) -> u64 {
        self.versions.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn nanos(&self, now: Instant) -> u64 {
        u64::try_from(now.saturating_duration_since(self.start).as_nanos()).unwrap_or(EMPTY - 1)
    }

    #[cfg(test)]
    fn capacities(&self) -> Vec<usize> {
        self.shards
            .iter()
            .map(|shard| shard.records.lock().expect("unpoisoned").capacity)
            .collect()
    }
}

/// How many shards a table of `capacity` records has: a power of two, at
/// most [`MAX_SHARDS`], with at least [`MIN_PER_SHARD`] records in each
/// when there is more than one.
fn shard_count(capacity: usize) -> usize {
    let wanted = (capacity / MIN_PER_SHARD).clamp(1, MAX_SHARDS);
    1 << wanted.ilog2()
}

/// `capacity` shared among `count` shards, as evenly as it divides: the
/// shares sum to `capacity`.
fn shares(capacity: usize, count: usize) -> impl Iterator<Item = usize> {
    let (each, rest) = (capacity / count, capacity % count);
    (0..count).map(move |shard| each + usize::from(shard < rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(key: &[u8]) -> KeyedOp {
        KeyedOp::Get { key: key.to_vec() }
    }

    fn set(key: &[u8], value: &[u8], expected: Expected) -> KeyedOp {
        KeyedOp::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            expected,
        }
    }

    fn check(key: &[u8], version: u64) -> KeyedOp {
        KeyedOp::Check {
            key: key.to_vec(),
            version,
        }
    }

    fn applied(answer: KeyedAnswer) -> u64 {
        match answer {
            KeyedAnswer::Applied { version } => version,
            other => panic!("not applied: {other:?}"),
        }
    }

    fn record(value: &[u8], version: u64) -> KeyedRecord {
        KeyedRecord {
            value: value.to_vec(),
            version,
        }
    }

    /// A small table is one shard; a large one has many, at least 1024
    /// records each and at most 64, and every share of the capacity is
    /// held by some shard.
    #[test]
    fn the_capacity_is_shared_among_shards_without_loss() {
        let start = Instant::now();
        for (capacity, shards) in [
            (1, 1),
            (3, 1),
            (2047, 1),
            (2048, 2),
            (4095, 2),
            (4096, 4),
            (100_000, 64),
            (1_000_000, 64),
            (1_000_003, 64),
        ] {
            let capacities = Table::new(capacity, start).capacities();
            assert_eq!(capacities.len(), shards, "{capacity}");
            assert_eq!(capacities.iter().sum::<usize>(), capacity, "{capacity}");
            if shards > 1 {
                assert!(
                    capacities.iter().all(|&c| c >= MIN_PER_SHARD),
                    "{capacity}: {capacities:?}"
                );
            }
        }
    }

    /// A set with `absent` applies only to a key with no record; the
    /// second such set is refused and gets the first one's record.
    #[test]
    fn the_first_absent_write_wins() {
        let now = Instant::now();
        let table = Table::new(8, now);
        let first = applied(table.apply(set(b"f", b"model-x-01", Expected::Absent), now));
        assert_eq!(
            table.apply(set(b"f", b"model-x-02", Expected::Absent), now),
            KeyedAnswer::Refused {
                current: Some(record(b"model-x-01", first)),
            }
        );
        assert_eq!(
            table.apply(get(b"f"), now),
            KeyedAnswer::Found {
                record: record(b"model-x-01", first),
            }
        );
        assert_eq!(table.apply(get(b"g"), now), KeyedAnswer::Missing);
    }

    /// A set at a version applies only at that version, and is refused
    /// with the record as it is otherwise, or with none when there is no
    /// record; a replace always applies; every write that applies has a
    /// greater version than the last, whatever its key.
    #[test]
    fn a_stale_version_is_refused_and_a_replace_always_applies() {
        let now = Instant::now();
        let table = Table::new(8, now);
        let one = applied(table.apply(set(b"f", b"a", Expected::Absent), now));
        let two = applied(table.apply(set(b"f", b"b", Expected::Version(one)), now));
        assert!(two > one);
        assert_eq!(
            table.apply(set(b"f", b"c", Expected::Version(one)), now),
            KeyedAnswer::Refused {
                current: Some(record(b"b", two)),
            }
        );
        assert_eq!(
            table.apply(set(b"g", b"c", Expected::Version(one)), now),
            KeyedAnswer::Refused { current: None }
        );
        let three = applied(table.apply(set(b"f", b"d", Expected::Replace), now));
        let four = applied(table.apply(set(b"g", b"e", Expected::Replace), now));
        assert!(three > two && four > three, "{two} {three} {four}");
    }

    /// A check answers unchanged at the record's version, the record at
    /// any other, and gone once the table no longer holds it.
    #[test]
    fn a_check_answers_unchanged_newer_or_gone() {
        let now = Instant::now();
        let table = Table::new(1, now);
        let one = applied(table.apply(set(b"f", b"a", Expected::Replace), now));
        assert_eq!(table.apply(check(b"f", one), now), KeyedAnswer::Unchanged);
        let two = applied(table.apply(set(b"f", b"b", Expected::Replace), now));
        assert_eq!(
            table.apply(check(b"f", one), now),
            KeyedAnswer::Newer {
                record: record(b"b", two),
            }
        );
        // A capacity of one: a second key evicts the first.
        applied(table.apply(set(b"g", b"c", Expected::Replace), now));
        assert_eq!(table.apply(check(b"f", two), now), KeyedAnswer::Gone);
    }

    /// At its capacity a one-shard table evicts its least recently used
    /// record, and a get, a check, or a refused set each make a record
    /// the most recently used.
    #[test]
    fn the_least_recently_used_record_is_evicted() {
        let now = Instant::now();
        let table = Table::new(3, now);
        let a = applied(table.apply(set(b"a", b"1", Expected::Absent), now));
        applied(table.apply(set(b"b", b"2", Expected::Absent), now));
        let c = applied(table.apply(set(b"c", b"3", Expected::Absent), now));
        // Order, least recent first: a, b, c. Touch a by a get: b, c, a.
        assert!(matches!(
            table.apply(get(b"a"), now),
            KeyedAnswer::Found { .. }
        ));
        applied(table.apply(set(b"d", b"4", Expected::Absent), now));
        assert_eq!(table.apply(get(b"b"), now), KeyedAnswer::Missing);
        // c, a, d. Touch c by a check, then a by a refused set: d, c, a.
        assert_eq!(table.apply(check(b"c", c), now), KeyedAnswer::Unchanged);
        assert!(matches!(
            table.apply(set(b"a", b"x", Expected::Absent), now),
            KeyedAnswer::Refused { .. }
        ));
        applied(table.apply(set(b"e", b"5", Expected::Absent), now));
        assert_eq!(table.apply(get(b"d"), now), KeyedAnswer::Missing);
        assert_eq!(
            table.apply(get(b"a"), now),
            KeyedAnswer::Found {
                record: record(b"1", a),
            }
        );
        assert!(matches!(
            table.apply(get(b"c"), now),
            KeyedAnswer::Found { .. }
        ));
        assert!(matches!(
            table.apply(get(b"e"), now),
            KeyedAnswer::Found { .. }
        ));
    }

    /// The eviction age is the age of the least recently used record: it
    /// rises as time passes, falls when that record is touched or
    /// evicted, and is none while the table is empty.
    #[test]
    fn the_eviction_age_follows_the_least_recently_used_record() {
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        let table = Table::new(2, start);
        assert_eq!(table.eviction_age(at(5)), None);
        applied(table.apply(set(b"a", b"1", Expected::Absent), at(10)));
        applied(table.apply(set(b"b", b"2", Expected::Absent), at(20)));
        assert_eq!(table.eviction_age(at(30)), Some(Duration::from_millis(20)));
        assert_eq!(table.eviction_age(at(50)), Some(Duration::from_millis(40)));
        // a touched at 60: b, touched at 20, is now the oldest.
        table.apply(get(b"a"), at(60));
        assert_eq!(table.eviction_age(at(60)), Some(Duration::from_millis(40)));
        // c evicts b: a, touched at 60, is the oldest.
        applied(table.apply(set(b"c", b"3", Expected::Absent), at(70)));
        assert_eq!(table.apply(get(b"b"), at(70)), KeyedAnswer::Missing);
        assert_eq!(table.eviction_age(at(70)), Some(Duration::from_millis(10)));
    }

    /// The eviction age of a table of many shards is the oldest of all
    /// its shards' records, whichever shard holds it.
    #[test]
    fn the_eviction_age_spans_every_shard() {
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        let table = Table::new(64 * MIN_PER_SHARD, start);
        assert_eq!(table.shards.len(), 64);
        for (n, key) in (0u32..256).enumerate() {
            let ms = 10 + u64::try_from(n).expect("small");
            applied(table.apply(set(&key.to_be_bytes(), b"v", Expected::Absent), at(ms)));
        }
        // Key 0, written at 10, is the oldest, in whichever shard.
        assert_eq!(
            table.eviction_age(at(1000)),
            Some(Duration::from_millis(990))
        );
        table.apply(get(&0u32.to_be_bytes()), at(1000));
        assert_eq!(
            table.eviction_age(at(1000)),
            Some(Duration::from_millis(989))
        );
    }
}
