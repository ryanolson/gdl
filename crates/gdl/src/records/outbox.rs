//! A table's write queue and the task that sends it: the writes are copied
//! into buffers allocated once, and sent to the hub in one request every
//! [`FLUSH`], or as soon as [`BATCH_BYTES`] are queued.

use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use tokio::sync::watch;

use super::{Expected, RecordAnswer, RecordLink, RecordOp, Table, Written};

/// How many locks the queue is split over. Writes of one key always meet
/// the same one, so they stay in order.
pub(super) const SHARDS: usize = 8;
const _: () = assert!(SHARDS.is_power_of_two());
/// A batch's worth: the queue is sent at once when it holds this much.
pub(super) const BATCH_BYTES: usize = 64 * 1024;
/// How long the first write of a batch waits for others.
const FLUSH: Duration = Duration::from_millis(5);
/// How long a batch waits for the hub's answer; after that, each of its
/// writes is answered [`Written::Unanswered`], and the next batch still
/// waits for the request to end.
const ANSWER_WAIT: Duration = Duration::from_secs(1);
/// The queue's bound: two batches, so writes keep queueing while one
/// batch is on its way.
const QUEUE_BYTES: usize = 2 * BATCH_BYTES;
const SHARD_BYTES: usize = QUEUE_BYTES / SHARDS;
/// What each write costs of the bound besides its bytes, so that the
/// bound also caps how many writes a shard holds, and a write of no bytes
/// costs something.
const WRITE_COST: usize = 32;
const SHARD_WRITES: usize = SHARD_BYTES / WRITE_COST;

/// What a write of `bytes` (prefix, key, and value) costs of the bound.
pub(super) fn cost(bytes: usize) -> usize {
    bytes.saturating_add(WRITE_COST)
}

/// The shard of `key`: FNV-1a, which needs no state and no allocation.
pub(super) fn shard(key: &[u8]) -> usize {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    // FNV's bits barely move for keys that differ in their last bytes:
    // a Fibonacci multiply spreads them, and its top bits pick the shard.
    (hash.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> (64 - SHARDS.trailing_zeros())) as usize
}

/// One shard's queued writes: their bytes end to end, and where each one
/// lies. Both buffers are allocated once, at their bound, so a write
/// within the bound never allocates.
pub(super) struct Batch {
    bytes: Vec<u8>,
    writes: Vec<Entry>,
    /// What the writes cost of the bound.
    cost: usize,
}

/// One queued write: its key (with the table's prefix) at `start`, its
/// value right after.
#[derive(Clone, Copy)]
struct Entry {
    start: u32,
    key: u32,
    value: u32,
    expected: Expected,
}

impl Batch {
    pub(super) fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(SHARD_BYTES),
            writes: Vec::with_capacity(SHARD_WRITES),
            cost: 0,
        }
    }

    /// Queue one write that costs `cost`, unless the shard has no room
    /// for it: whether it was queued. Within the bound, the bytes are at
    /// most `SHARD_BYTES` and the writes at most `SHARD_WRITES`, the
    /// buffers' capacities, so neither grows.
    pub(super) fn push(
        &mut self,
        prefix: &[u8],
        key: &[u8],
        value: &[u8],
        expected: Expected,
        cost: usize,
    ) -> bool {
        if cost > SHARD_BYTES - self.cost {
            return false;
        }
        // Every length is within SHARD_BYTES, so fits in a u32.
        let start = self.bytes.len() as u32;
        self.bytes.extend_from_slice(prefix);
        self.bytes.extend_from_slice(key);
        self.bytes.extend_from_slice(value);
        self.writes.push(Entry {
            start,
            key: (prefix.len() + key.len()) as u32,
            value: value.len() as u32,
            expected,
        });
        self.cost += cost;
        true
    }

    fn ops(&self) -> impl Iterator<Item = RecordOp<'_>> {
        self.writes.iter().map(|entry| {
            let (key, value) = self.parts(entry);
            RecordOp::Set {
                key,
                value,
                expected: entry.expected,
            }
        })
    }

    /// The keys of the writes, without the table's prefix of `prefix`
    /// bytes, in order.
    fn keys(&self, prefix: usize) -> impl Iterator<Item = &[u8]> {
        self.writes
            .iter()
            .map(move |entry| &self.parts(entry).0[prefix..])
    }

    fn parts(&self, entry: &Entry) -> (&[u8], &[u8]) {
        let start = entry.start as usize;
        let value = start + entry.key as usize;
        (
            &self.bytes[start..value],
            &self.bytes[value..value + entry.value as usize],
        )
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.writes.clear();
        self.cost = 0;
    }
}

/// Send `table`'s writes once the hub is linked, until every handle on it
/// is dropped and its queue is sent.
pub(super) async fn drain(
    table: Arc<Table>,
    mut link: watch::Receiver<Option<Arc<dyn RecordLink>>>,
) {
    let mut spares: Vec<Batch> = (0..SHARDS).map(|_| Batch::new()).collect();
    let link = loop {
        if let Some(link) = link.borrow_and_update().clone() {
            break link;
        }
        if table.closed.load(Ordering::Acquire) {
            // No hub to send them to.
            take(&table, &mut spares);
            unanswered(&table, &spares);
            return;
        }
        tokio::select! {
            changed = link.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = table.pending.notified() => {}
        }
    };
    loop {
        let queued = table.queued.load(Ordering::Acquire);
        let closed = table.closed.load(Ordering::Acquire);
        if queued == 0 {
            if closed {
                return;
            }
            // A write into the empty queue, or the last handle's drop,
            // signals; a signal sent before this wait is kept for it.
            table.pending.notified().await;
            continue;
        }
        if queued < BATCH_BYTES && !closed {
            // From the batch's first write, which may have waited behind
            // the last request already. A signal of a full queue left
            // from an earlier batch ends no wait: the queue is checked.
            let first = Duration::from_nanos(table.first_write.load(Ordering::Acquire));
            let deadline = table.born + first + FLUSH;
            while table.queued.load(Ordering::Acquire) < BATCH_BYTES {
                if tokio::time::timeout_at(deadline, table.full.notified())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
        take(&table, &mut spares);
        send(&table, &*link, &spares).await;
        for spare in &mut spares {
            spare.clear();
        }
    }
}

/// Swap each shard's batch with an empty one of `spares`, so that writers
/// go on queueing while the taken batches are sent.
fn take(table: &Table, spares: &mut [Batch]) {
    let mut taken = 0;
    for (shard, spare) in table.shards.iter().zip(spares.iter_mut()) {
        let mut batch = shard.lock().unwrap_or_else(PoisonError::into_inner);
        taken += batch.cost;
        std::mem::swap(&mut *batch, spare);
    }
    // Each write added its cost under its shard's lock, so what was taken
    // was added before.
    table.queued.fetch_sub(taken, Ordering::AcqRel);
}

/// Send `batches` in one request, and give each write its answer.
async fn send(table: &Table, link: &dyn RecordLink, batches: &[Batch]) {
    if batches.iter().all(|batch| batch.writes.is_empty()) {
        return;
    }
    let mut asked = link.ask(&mut batches.iter().flat_map(Batch::ops));
    let reply = match tokio::time::timeout(ANSWER_WAIT, &mut asked).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(_)) => return unanswered(table, batches),
        Err(_) => {
            unanswered(table, batches);
            // The request can still reach the hub, which applies requests
            // as they arrive: a later batch sent now could arrive first and
            // be overwritten by it. So the next batch waits until this one
            // ends; the link's request ends by a timeout of its own.
            if let Ok(reply) = asked.await {
                table.saw(reply.eviction_age);
            }
            return;
        }
    };
    table.saw(reply.eviction_age);
    let mut answers = reply.answers.iter();
    for key in batches
        .iter()
        .flat_map(|batch| batch.keys(table.prefix.len()))
    {
        let written = match answers.next() {
            Some(RecordAnswer::Applied(version)) => Written::Applied { version: *version },
            Some(RecordAnswer::Refused(current)) => Written::Refused {
                current: current.as_ref(),
            },
            // A reply with too few answers, or with an answer of another
            // operation, says nothing of this write.
            _ => Written::Unanswered,
        };
        table.answered(key, written);
    }
}

fn unanswered(table: &Table, batches: &[Batch]) {
    for key in batches
        .iter()
        .flat_map(|batch| batch.keys(table.prefix.len()))
    {
        table.answered(key, Written::Unanswered);
    }
}
