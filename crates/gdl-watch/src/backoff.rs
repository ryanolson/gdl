//! How long a reader waits before it opens a watch again, after one that
//! ended, failed, or could not begin.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

#[cfg(test)]
mod tests;

/// How long a watch must run after its snapshot completed for its end to
/// return the wait to the base: long enough that a hub which takes a
/// watch and drops it soon after, or a peer that keeps failing part way,
/// still backs off, and short beside how long a healthy watch lives.
/// Measured from the snapshot rather than from the connection, because a
/// watch that only sends keep-alives has shown its reader nothing.
pub(crate) const HEALTHY: Duration = Duration::from_secs(30);

/// The waits between one reader's watches: a base, doubled after each
/// watch that fails, or ends before it has run for 30 s after its
/// snapshot completed, up to a cap, and back to the base once one has run
/// that long. A watch that ran that long and then failed returns the step
/// to the base and doubles it, so that a hub that keeps failing its
/// watchers late is still asked less often; only one that closed is not
/// counted as a failure. Each wait is drawn uniformly
/// between half its step and the whole step, so that the readers of one
/// hub that fail at the same instant, as all of them do when it restarts,
/// spread their next attempts over that window rather than arriving
/// together; half the step is kept so that a reader never asks again at
/// once. The first wait is within the base, so a reader whose watch was
/// healthy reconnects as quickly as the base allows.
///
/// One per reader, held across its watches. The generator is seeded apart
/// for each, from the process's random hasher keys, so that two readers,
/// in one process or in two, do not draw the same waits.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    cap: Duration,
    /// What the next failure draws its wait from.
    step: Duration,
    /// The generator's state.
    state: u64,
}

impl Backoff {
    /// Waits from `base`, doubled up to `cap`; a cap below the base is
    /// taken as the base, so that a configured base is never shortened.
    pub fn new(base: Duration, cap: Duration) -> Self {
        static READERS: AtomicU64 = AtomicU64::new(0);
        let seed = RandomState::new().hash_one(READERS.fetch_add(1, Ordering::Relaxed));
        Self::seeded(base, cap, seed)
    }

    fn seeded(base: Duration, cap: Duration, seed: u64) -> Self {
        Self {
            base,
            cap: cap.max(base),
            step: base,
            state: seed,
        }
    }

    /// The watch that completed its snapshot at `synced`, if it did, ended
    /// as `end` says at `now`: how long to wait before the next one is
    /// opened. If it ran for 30 s after its snapshot, the step returns to
    /// the base; it then advances unless the watch ran that long and
    /// closed. A watch that could not begin failed, with no snapshot.
    pub fn wait(&mut self, synced: Option<Instant>, end: WatchEnd, now: Instant) -> Duration {
        let healthy = self.healthy(synced, now);
        let wait = self.draw(self.step);
        if !healthy || end == WatchEnd::Failed {
            self.step = self.step.saturating_mul(2).min(self.cap);
        }
        wait
    }

    /// The reader let go of the watch whose snapshot completed at `synced`,
    /// at `now`, to open the next at once: no wait is drawn, and the step
    /// returns to the base if the watch ran for 30 s after its snapshot.
    /// The next watch is a new one: its own end is told from its own
    /// snapshot, so the time spent opening it never counts as this one's.
    pub fn released(&mut self, synced: Instant, now: Instant) {
        self.healthy(Some(synced), now);
    }

    /// Whether the watch whose snapshot completed at `synced` ran for the
    /// healthy span by `now`, returning the step to the base if it did.
    fn healthy(&mut self, synced: Option<Instant>, now: Instant) -> bool {
        let healthy = synced.is_some_and(|at| now.saturating_duration_since(at) >= HEALTHY);
        if healthy {
            self.step = self.base;
        }
        healthy
    }

    /// A wait drawn uniformly from half of `step` to the whole of it.
    fn draw(&mut self, step: Duration) -> Duration {
        let half = step / 2;
        let span = u64::try_from((step - half).as_nanos()).unwrap_or(u64::MAX);
        half + Duration::from_nanos(self.next() % span.saturating_add(1))
    }

    /// splitmix64: cheap, and enough to spread waits; nothing depends on
    /// its draws being unpredictable.
    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// How a watch ended, for [`Backoff::wait`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEnd {
    /// It ended without a failure: its hub closed it, or its reader let go
    /// of it.
    Closed,
    /// It failed, was refused, or could not begin: a connection refused or
    /// lost, a `503`, a [`crate::WatchFailure`], or what its reader takes
    /// for a broken protocol.
    Failed,
}
