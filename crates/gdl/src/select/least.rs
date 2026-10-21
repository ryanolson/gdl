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
    /// One count per instance seen, sorted by id. Replaced whole, and only
    /// when an instance not seen before appears: a pick reads it without a
    /// lock.
    counts: ArcSwap<Vec<(InstanceId, Arc<AtomicUsize>)>>,
    /// Where the next scan starts, so that equal counts take turns.
    turn: AtomicUsize,
}

impl LeastLoaded {
    /// A selector with no counts yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The counts, with one for every instance in `instances`. An instance
    /// not seen before adds one; counts of instances that are gone and
    /// hold no lease are dropped then.
    fn counts(&self, instances: &Instances<'_>) -> Arc<Vec<(InstanceId, Arc<AtomicUsize>)>> {
        let counts = self.counts.load_full();
        if instances.iter().all(|i| find(&counts, i.id()).is_some()) {
            return counts;
        }
        self.counts.rcu(|counts| {
            let mut next: Vec<(InstanceId, Arc<AtomicUsize>)> = counts
                .iter()
                .filter(|(id, count)| {
                    count.load(Ordering::Relaxed) > 0 || instances.iter().any(|i| i.id() == *id)
                })
                .cloned()
                .collect();
            for instance in instances.iter() {
                if find(&next, instance.id()).is_none() {
                    next.push((instance.id(), Arc::new(AtomicUsize::new(0))));
                }
            }
            next.sort_by_key(|(id, _)| *id);
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

    fn ids(n: usize) -> Vec<velo::InstanceId> {
        (0..n).map(|_| velo::InstanceId::new_v4()).collect()
    }

    async fn pick(selector: &Typed<LeastLoaded>, ids: &[velo::InstanceId]) -> (usize, Picked) {
        let picked = selector
            .select(&Cx::new(), &[], Instances::new("worker", ids))
            .await
            .expect("a choice");
        (picked.index, picked)
    }

    fn id(ids: &[velo::InstanceId], at: usize) -> InstanceId {
        InstanceId(ids[at].as_u128())
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

    #[tokio::test]
    async fn a_joiner_gets_a_count_and_an_instance_gone_is_forgotten_once_free() {
        let selector = Typed(Arc::new(LeastLoaded::new()));
        let mut ids = ids(2);
        let (gone, lease) = pick(&selector, &ids).await;
        let gone = ids.remove(gone);
        let joiner = velo::InstanceId::new_v4();
        ids.push(joiner);
        let (_, _held) = pick(&selector, &ids).await;
        assert_eq!(
            selector.0.held(InstanceId(gone.as_u128())),
            1,
            "a held count was dropped"
        );
        drop(lease);
        ids.push(velo::InstanceId::new_v4());
        let _ = pick(&selector, &ids).await;
        assert!(
            find(&selector.0.counts.load(), InstanceId(gone.as_u128())).is_none(),
            "a free count of an instance gone was kept"
        );
    }
}
