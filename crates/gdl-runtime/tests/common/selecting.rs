//! A selector that counts its leases, and placements that route every
//! crossing through it.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use gdl::{Choice, Encoded, Instances, Request, Selector};

use gdl_runtime::{Layout, Placement, RequestRouter, RuntimeConfig};

/// Leases by the count: how many were taken, and how many are held. Its
/// clones share the counts, so every segment of a deployment can register
/// one.
#[derive(Clone, Default)]
pub struct Counting {
    taken: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
}

struct Held(Arc<AtomicUsize>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Counting {
    /// The name the placements below route through.
    pub const NAME: &str = "counting";

    pub fn taken(&self) -> usize {
        self.taken.load(Ordering::SeqCst)
    }

    pub fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
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
        self.taken.fetch_add(1, Ordering::SeqCst);
        self.live.fetch_add(1, Ordering::SeqCst);
        let chosen = instances.get(0).expect("never empty");
        Ok(Choice::new(chosen).with_lease(Held(Arc::clone(&self.live))))
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
