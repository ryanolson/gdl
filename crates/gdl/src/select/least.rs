//! `least_loaded`: the instance this sender has the fewest requests at.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;

use super::{Choice, Encoded, InstanceId, Instances, Request, Selector};
use crate::error::Result;
use crate::role::Build;

/// Sends each request to the instance where this sending instance holds
/// the fewest leases: requests it sent there that the sending segment
/// still holds (until the response it returns ends, a relay has sent its
/// input, or the hand-off is acknowledged). Equal counts take turns.
///
/// It sees only its own sender's requests, not the instance's whole load.
/// Where the lease ends at the acknowledgement (a forward run that hands
/// on, with nothing of the response passing it), the counts stay near
/// zero and it takes turns as round robin does.
#[derive(Debug, Default)]
pub struct LeastLoaded {
    /// One count per instance ever seen, sorted by id. Replaced whole, and
    /// only when an instance not seen before appears: a pick reads it
    /// without a lock. A count is never dropped or replaced: one selector
    /// serves every crossing that names it, into any segment, so a view of
    /// one segment cannot tell that another's idle count is unused, and a
    /// pick that loaded a count may be about to add to it. An entry is a
    /// few dozen bytes per instance this sender has ever routed to.
    counts: ArcSwap<Vec<(InstanceId, Arc<AtomicUsize>)>>,
    /// Where the next scan starts, so that equal counts take turns.
    turn: AtomicUsize,
}

impl LeastLoaded {
    /// A selector with no counts yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The counts, with one for every instance in `instances`: an
    /// instance not seen before adds one. Counts are only ever added, so
    /// the table loaded after the update holds every instance asked for.
    fn counts(&self, instances: &Instances<'_>) -> Arc<Vec<(InstanceId, Arc<AtomicUsize>)>> {
        let counts = self.counts.load_full();
        if instances.iter().all(|i| find(&counts, i.id()).is_some()) {
            return counts;
        }
        self.counts.rcu(|counts| {
            let mut next = Vec::clone(counts);
            for instance in instances.iter() {
                if let Err(at) = next.binary_search_by_key(&instance.id(), |(id, _)| *id) {
                    next.insert(at, (instance.id(), Arc::new(AtomicUsize::new(0))));
                }
            }
            next
        });
        self.counts.load_full()
    }

    #[cfg(test)]
    fn held(&self, id: InstanceId) -> usize {
        find(&self.counts.load(), id).map_or(0, |count| count.load(Ordering::Relaxed))
    }
}

fn find(counts: &[(InstanceId, Arc<AtomicUsize>)], id: InstanceId) -> Option<&Arc<AtomicUsize>> {
    counts
        .binary_search_by_key(&id, |(known, _)| *known)
        .ok()
        .map(|at| &counts[at].1)
}

impl Build for LeastLoaded {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self::new())
    }
}

impl Selector for LeastLoaded {
    type Head = Encoded;

    async fn select<'a>(
        &'a self,
        _request: Request<'a, Encoded>,
        instances: Instances<'a>,
    ) -> Result<Choice<'a>> {
        let counts = self.counts(&instances);
        let n = instances.len();
        let start = self.turn.fetch_add(1, Ordering::Relaxed) % n;
        let mut best: Option<(usize, usize, &Arc<AtomicUsize>)> = None;
        for step in 0..n {
            let index = (start + step) % n;
            let Some(instance) = instances.get(index) else {
                continue;
            };
            let Some(count) = find(&counts, instance.id()) else {
                continue;
            };
            let held = count.load(Ordering::Relaxed);
            if best.is_none_or(|(_, least, _)| held < least) {
                best = Some((index, held, count));
            }
        }
        let (index, _, count) = best.expect("every instance has a count");
        count.fetch_add(1, Ordering::Relaxed);
        let chosen = instances.get(index).expect("an index from the scan");
        Ok(Choice::new(chosen).with_lease(Release(Arc::clone(count))))
    }
}

/// Gives back one count when the sending segment lets the request go.
struct Release(Arc<AtomicUsize>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cx::Cx;
    use crate::select::{Erased, Picked, Typed};

    fn ids(n: usize) -> Vec<InstanceId> {
        (0..n).map(|_| crate::select::random_id()).collect()
    }

    async fn pick(selector: &Typed<LeastLoaded>, ids: &[InstanceId]) -> (usize, Picked) {
        let picked = selector
            .select(&Cx::new(), &[], Instances::new("worker", ids))
            .await
            .expect("a choice");
        (picked.index, picked)
    }

    fn id(ids: &[InstanceId], at: usize) -> InstanceId {
        ids[at]
    }

    #[tokio::test]
    async fn the_instance_with_fewest_leases_wins_and_a_dropped_lease_counts_again() {
        let selector = Typed(Arc::new(LeastLoaded::new()));
        let ids = ids(3);
        // Three picks with every lease held: each instance once.
        let mut held = Vec::new();
        let mut chosen = std::collections::BTreeSet::new();
        for _ in 0..3 {
            let (index, picked) = pick(&selector, &ids).await;
            chosen.insert(index);
            held.push((index, picked));
        }
        assert_eq!(chosen.len(), 3, "equal counts did not take turns");
        // Free the lease on one instance: it is now the least loaded.
        let (freed, lease) = held.remove(1);
        drop(lease);
        assert_eq!(selector.0.held(id(&ids, freed)), 0);
        for _ in 0..1 {
            let (index, picked) = pick(&selector, &ids).await;
            assert_eq!(index, freed);
            held.push((index, picked));
        }
        for at in 0..3 {
            assert_eq!(selector.0.held(id(&ids, at)), 1);
        }
        drop(held);
        for at in 0..3 {
            assert_eq!(selector.0.held(id(&ids, at)), 0);
        }
    }

    #[tokio::test]
    async fn equal_counts_take_turns() {
        let selector = Typed(Arc::new(LeastLoaded::new()));
        let ids = ids(4);
        let mut counts = [0; 4];
        for _ in 0..40 {
            // Each lease dropped at once, as at an acknowledgement.
            let (index, _) = pick(&selector, &ids).await;
            counts[index] += 1;
        }
        assert_eq!(counts, [10; 4]);
    }

    /// One selector routes every crossing that names it, into any segment:
    /// a view of one segment must never drop or replace the count of an
    /// instance of another, even an idle one, since a pick that loaded it
    /// may be about to add to it.
    #[tokio::test]
    async fn a_view_of_one_segment_keeps_every_count_of_another() {
        let selector = Typed(Arc::new(LeastLoaded::new()));
        let (one, other) = (ids(1), ids(1));
        let (_, idle) = pick(&selector, &one).await;
        drop(idle);
        let counted = |id: InstanceId| {
            selector
                .0
                .counts
                .load()
                .iter()
                .find(|(known, _)| known.as_u128() == id.as_u128())
                .map(|(_, count)| Arc::as_ptr(count))
        };
        let before = counted(one[0]).expect("a count");
        let _ = pick(&selector, &other).await;
        assert_eq!(counted(one[0]), Some(before), "an idle count was replaced");
    }

    /// An instance already counted, seen with a new one that sorts before
    /// it, keeps its one count.
    #[tokio::test]
    async fn a_new_instance_never_doubles_the_count_of_a_known_one() {
        let selector = Typed(Arc::new(LeastLoaded::new()));
        let mut ids = ids(2);
        ids.sort_by_key(|id| id.as_u128());
        let (low, high) = (ids[0], ids[1]);
        let (_, held) = pick(&selector, &[high]).await;
        let _ = pick(&selector, &[low, high]).await;
        let entries = selector.0.counts.load();
        let highs = entries
            .iter()
            .filter(|(id, _)| id.as_u128() == high.as_u128())
            .count();
        assert_eq!(highs, 1, "{} counts for one instance", highs);
        drop(held);
    }
}
