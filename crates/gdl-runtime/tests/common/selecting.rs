//! A selector that counts its leases, and placements that route every
//! crossing through it.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use gdl::{Choice, Encoded, Instances, Request, Selector};

use gdl_runtime::{Layout, Placement, RequestRouter, RuntimeConfig};

use super::{WAIT, ms};

/// Leases by the count: how many were taken, and how many are held. Its
/// clones share the counts, so every segment of a deployment can register
/// one.
#[derive(Clone, Default)]
pub struct Counting {
    taken: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
    /// Which lease, by the order taken from 0, takes how long to give back.
    slow: Option<(usize, Duration)>,
}

struct Held {
    live: Arc<AtomicUsize>,
    release: Duration,
}

impl Drop for Held {
    fn drop(&mut self) {
        if !self.release.is_zero() {
            std::thread::sleep(self.release);
        }
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Counting {
    /// The name the placements below route through.
    pub const NAME: &str = "counting";

    /// A selector whose `nth` lease, counted from 0, blocks the thread
    /// that drops it for `release` before it counts as given back.
    pub fn slow(nth: usize, release: Duration) -> Self {
        Self {
            slow: Some((nth, release)),
            ..Self::default()
        }
    }

    pub fn taken(&self) -> usize {
        self.taken.load(Ordering::SeqCst)
    }

    pub fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    /// Wait until every lease is given back, for at most [`WAIT`]. A
    /// segment can settle before a lease it took is given back: a relay
    /// gives back its admission share and egress permit, then the lease.
    pub async fn released(&self) -> bool {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            if self.live() == 0 {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(ms(5)).await;
        }
    }

    /// A runtime configuration that registers this selector.
    pub fn config(&self) -> RuntimeConfig {
        let mut config = RuntimeConfig::default();
        config
            .selectors
            .add(Self::NAME, self.clone())
            .expect("a selector");
        config
    }
}

impl Selector for Counting {
    type Head = Encoded;

    async fn select<'a>(
        &'a self,
        _: Request<'a, Encoded>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        let nth = self.taken.fetch_add(1, Ordering::SeqCst);
        self.live.fetch_add(1, Ordering::SeqCst);
        let release = match self.slow {
            Some((slow, release)) if slow == nth => release,
            _ => Duration::ZERO,
        };
        let chosen = instances.get(0).expect("never empty");
        Ok(Choice::new(chosen).with_lease(Held {
            live: Arc::clone(&self.live),
            release,
        }))
    }
}

/// `placement` with every request boundary of `layout` routed through
/// [`Counting`].
pub fn counted(placement: Placement, layout: &Layout) -> Placement {
    let mut placement = placement;
    for hop in layout.hops() {
        for run in hop.forward_runs.iter().skip(1) {
            let node = run.stages.first().expect("a run has a stage");
            placement =
                placement.route(node.clone(), RequestRouter::Selector(Counting::NAME.into()));
        }
    }
    placement
}
