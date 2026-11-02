//! `affinity`: the same key goes to the same instance, from every sender.

use serde::Deserialize;

use super::{Choice, Encoded, InstanceId, Instances, JoinedThrough, Request, Selector};
use crate::error::Result;
use crate::role::Build;

/// Sends requests with the same key to the same instance, from every
/// sending instance: a session's requests, or one prompt's retries, reach
/// the instance that has its state.
///
/// The key is the value of the context's metadata entry named by
/// [`AffinityConfig::key`] when the request carries it, and the request's
/// encoded head otherwise. The instance is chosen by rendezvous hashing:
/// each instance's score is a hash of the key and the instance's id, and
/// the highest score wins. A join or a retirement therefore moves only the
/// keys of the instance that came or went, and every sender, whatever order
/// it lists the instances in, chooses the same one.
///
/// After instances decline a request ([`Request::declined`]), the highest
/// score among those that have not wins, so that each failover moves to
/// the key's next instance, the same from every sender; when every one
/// has declined, the highest score overall.
///
/// A join still moves the keys the joiner scores highest on. A session
/// that must stay on its instance while instances join carries a cutoff:
/// with [`AffinityConfig::through`] set, and a [`JoinedThrough`] in the
/// request's context of the same hub epoch as the instances
/// ([`Instances::epoch`]), only the instances that joined at or before it
/// ([`Instance::joined`](super::Instance::joined)) are ranked. A later
/// instance outscores none of them, so the session stays, and moves only
/// when its own instance leaves, to the next score among those that
/// remain. Declines apply inside that set first; once each instance in it
/// has declined, the order above applies to every instance. With no
/// cutoff, one of another epoch, or no instance at or below it, every
/// instance is ranked, as without one. The cutoff costs at most one more
/// pass over the instances, and no allocation.
#[derive(Debug, Default)]
pub struct Affinity {
    key: Option<String>,
    through: bool,
}

/// `[selector.affinity]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffinityConfig {
    /// The metadata entry whose value is the key, such as a session id.
    /// Unset, or absent from a request, the key is the request's head.
    #[serde(default)]
    pub key: Option<String>,
    /// Rank only the instances that joined the pool at or before the
    /// request's cutoff ([`JoinedThrough`]), when it carries one of the
    /// pool hub's epoch.
    #[serde(default)]
    pub through: bool,
}

impl Affinity {
    /// An affinity selector with `config`.
    pub fn new(config: AffinityConfig) -> Self {
        Self {
            key: config.key,
            through: config.through,
        }
    }

    /// The hash of the key `request` carries.
    fn key(&self, request: &Request<'_, Encoded>) -> u64 {
        let from_meta = self
            .key
            .as_deref()
            .and_then(|key| request.cx().meta().get(key));
        match from_meta {
            Some(value) => fnv(value.as_bytes()),
            None => fnv(request.bytes()),
        }
    }
}

impl Build for Affinity {
    type Config = AffinityConfig;
    type Metrics = ();

    async fn build(config: AffinityConfig, (): ()) -> Result<Self> {
        Ok(Self::new(config))
    }
}

impl Selector for Affinity {
    type Head = Encoded;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Encoded>,
        instances: Instances<'a>,
    ) -> Result<Choice<'a>> {
        let key = self.key(&request);
        let ranked = |instance: &super::Instance<'_>| (score(key, instance.id()), instance.id());
        let declined = request.declined();
        let open = |instance: &super::Instance<'_>| !declined.contains(&instance.id());
        // The cutoff is read only when the selector is set to, so that
        // the default costs one test of a flag. An empty set of instances
        // at or below it chooses as if there were none.
        let through = self
            .through
            .then(|| request.cx().ext::<JoinedThrough>())
            .flatten()
            .filter(|cut| cut.epoch == instances.epoch() && cut.epoch > 0)
            .map(|cut| cut.joined);
        let kept = through.and_then(|through| {
            instances
                .iter()
                .filter(|instance| instance.joined().is_some_and(|joined| joined <= through))
                .filter(open)
                .max_by_key(ranked)
        });
        // The first choice, with nothing declined, filters nothing: an
        // empty slice's check is its length.
        let chosen = kept
            .or_else(|| instances.iter().filter(open).max_by_key(ranked))
            .or_else(|| instances.iter().max_by_key(ranked))
            .expect("a selector is never asked to choose from nothing");
        Ok(Choice::new(chosen))
    }
}

/// FNV-1a, 64 bits: the same in every process and build, as the choice
/// must be.
fn fnv(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The rendezvous score of `instance` for `key`: SplitMix64's finalizer
/// over the key mixed with both halves of the id, so that nearby ids and
/// nearby keys score unlike each other.
fn score(key: u64, instance: InstanceId) -> u64 {
    let id = instance.as_u128();
    let mut x = key ^ (id as u64) ^ ((id >> 64) as u64).rotate_left(29);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cx::Cx;
    use crate::select::Erased;
    use crate::select::{JoinStamps, Typed};
    use std::sync::Arc;

    fn ids(n: usize) -> Vec<InstanceId> {
        (0..n).map(|_| crate::select::random_id()).collect()
    }

    async fn choose(
        selector: &Typed<Affinity>,
        cx: &Cx,
        head: &[u8],
        ids: &[InstanceId],
    ) -> InstanceId {
        choose_after(selector, cx, head, ids, &[]).await
    }

    /// [`choose`], for a request that the instances in `declined` declined.
    async fn choose_after(
        selector: &Typed<Affinity>,
        cx: &Cx,
        head: &[u8],
        ids: &[InstanceId],
        declined: &[InstanceId],
    ) -> InstanceId {
        let picked = selector
            .select(
                cx,
                head,
                Default::default(),
                declined,
                Instances::new("worker", ids),
            )
            .await
            .expect("a choice");
        ids[picked.index]
    }

    #[tokio::test]
    async fn one_key_goes_to_one_instance_from_every_sender_in_any_order() {
        let first = Typed(Arc::new(Affinity::default()));
        let second = Typed(Arc::new(Affinity::default()));
        let ids = ids(5);
        let mut reversed = ids.clone();
        reversed.reverse();
        let cx = Cx::new();
        let mut seen = std::collections::BTreeSet::new();
        for key in 0..64u32 {
            let head = key.to_be_bytes();
            let a = choose(&first, &cx, &head, &ids).await;
            assert_eq!(a, choose(&first, &cx, &head, &ids).await);
            assert_eq!(a, choose(&second, &cx, &head, &reversed).await);
            seen.insert(a.as_u128());
        }
        assert_eq!(
            seen.len(),
            5,
            "64 keys used only {} of 5 instances",
            seen.len()
        );
    }

    #[tokio::test]
    async fn the_metadata_key_wins_over_the_head() {
        let selector = Typed(Arc::new(Affinity::new(AffinityConfig {
            key: Some("session".into()),
            ..Default::default()
        })));
        let ids = ids(8);
        let mut cx = Cx::new();
        cx.insert_meta("session", "s-1");
        let chosen = choose(&selector, &cx, b"one", &ids).await;
        for head in [&b"two"[..], b"three", b"four", b"five"] {
            assert_eq!(choose(&selector, &cx, head, &ids).await, chosen);
        }
        // Without the entry, the head decides.
        let bare = Cx::new();
        let by_head: std::collections::BTreeSet<_> =
            futures::future::join_all((0..32u32).map(|k| {
                let ids = &ids;
                let bare = &bare;
                let selector = &selector;
                async move {
                    choose(selector, bare, &k.to_be_bytes(), ids)
                        .await
                        .as_u128()
                }
            }))
            .await
            .into_iter()
            .collect();
        assert!(by_head.len() > 1);
    }

    #[tokio::test]
    async fn a_join_moves_only_the_keys_the_joiner_takes() {
        let selector = Typed(Arc::new(Affinity::default()));
        let mut ids = ids(4);
        let cx = Cx::new();
        let mut before = Vec::new();
        for key in 0..200u32 {
            before.push(choose(&selector, &cx, &key.to_be_bytes(), &ids).await);
        }
        let joiner = crate::select::random_id();
        ids.insert(2, joiner);
        let mut moved = 0;
        for (key, was) in before.iter().enumerate() {
            let now = choose(&selector, &cx, &(key as u32).to_be_bytes(), &ids).await;
            if now != *was {
                assert_eq!(now, joiner, "key {key} moved to an instance that was there");
                moved += 1;
            }
        }
        assert!(moved > 0 && moved < 100, "{moved} of 200 keys moved");
    }

    /// [`choose_after`] over instances with the join stamps `joined` of
    /// the pool's hub of `epoch`.
    async fn choose_cut(
        selector: &Typed<Affinity>,
        cx: &Cx,
        head: &[u8],
        (ids, joined, epoch): (&[InstanceId], &[u64], u64),
        declined: &[InstanceId],
    ) -> InstanceId {
        let stamps = JoinStamps::new(ids, joined.into(), epoch);
        let picked = selector
            .select(
                cx,
                head,
                Default::default(),
                declined,
                Instances::with_joined("worker", ids, &stamps),
            )
            .await
            .expect("a choice");
        ids[picked.index]
    }

    fn through() -> Typed<Affinity> {
        Typed(Arc::new(Affinity::new(AffinityConfig {
            key: Some("session".into()),
            through: true,
        })))
    }

    /// A session's context with the cutoff `joined` of the hub of `epoch`.
    fn session(key: u32, cut: Option<(u64, u64)>) -> Cx {
        let mut cx = Cx::new();
        cx.insert_meta("session", format!("s-{key}"));
        if let Some((epoch, joined)) = cut {
            cx.insert_ext(JoinedThrough { epoch, joined });
        }
        cx
    }

    /// With a cutoff, an instance that joined after it is never chosen,
    /// whatever its score, and the instances at or below it are ranked as
    /// they were before it joined: a join moves no session that has one.
    #[tokio::test]
    async fn an_instance_past_the_cutoff_is_never_chosen() {
        let selector = through();
        let ids = ids(8);
        let joined: Vec<u64> = (1..=8).collect();
        let mut later = 0;
        for key in 0..400u32 {
            let cx = session(key, Some((7, 5)));
            let chosen = choose_cut(&selector, &cx, b"h", (&ids, &joined, 7), &[]).await;
            let at = ids
                .iter()
                .position(|id| *id == chosen)
                .expect("one of them");
            assert!(
                joined[at] <= 5,
                "key {key} went to an instance joined at {}",
                joined[at]
            );
            let before = choose_cut(&selector, &cx, b"h", (&ids[..5], &joined[..5], 7), &[]).await;
            assert_eq!(chosen, before, "a join moved key {key}");
            let bare = session(key, None);
            let free = choose_cut(&selector, &bare, b"h", (&ids, &joined, 7), &[]).await;
            let at = ids.iter().position(|id| *id == free).expect("one of them");
            later += usize::from(joined[at] > 5);
        }
        assert!(
            later > 0,
            "without a cutoff, no key went to a later instance"
        );
    }

    /// Declines apply inside the cutoff's instances first: the next score
    /// among those, before any instance past it; once each of them has
    /// declined, the next score among those that have not; once every
    /// instance has, the highest score overall.
    #[tokio::test]
    async fn declines_move_to_the_next_score_inside_the_cutoff_first() {
        let selector = through();
        let ids = ids(6);
        let joined = [1, 2, 3, 4, 5, 6];
        for key in 0..64u32 {
            let cx = session(key, Some((3, 3)));
            let hashed = fnv(format!("s-{key}").as_bytes());
            let mut inside = ids[..3].to_vec();
            inside.sort_by_key(|id| std::cmp::Reverse((score(hashed, *id), *id)));
            let mut outside = ids[3..].to_vec();
            outside.sort_by_key(|id| std::cmp::Reverse((score(hashed, *id), *id)));
            let mut overall = ids.clone();
            overall.sort_by_key(|id| std::cmp::Reverse((score(hashed, *id), *id)));
            let chosen = |declined: Vec<InstanceId>| {
                let (selector, cx, ids) = (&selector, &cx, &ids);
                async move { choose_cut(selector, cx, b"h", (ids, &joined, 3), &declined).await }
            };
            assert_eq!(chosen(vec![]).await, inside[0], "key {key}");
            assert_eq!(chosen(vec![inside[0]]).await, inside[1], "key {key}");
            assert_eq!(
                chosen(vec![inside[0], inside[1]]).await,
                inside[2],
                "key {key}"
            );
            assert_eq!(chosen(inside.clone()).await, outside[0], "key {key}");
            assert_eq!(chosen(ids.clone()).await, overall[0], "key {key}");
        }
    }

    /// No instance at or below the cutoff, a cutoff of another hub's
    /// epoch, instances with no stamp, a stamp of 0, no cutoff, or a
    /// selector not set to read one: every instance is ranked, as without
    /// a cutoff.
    #[tokio::test]
    async fn without_a_usable_cutoff_every_instance_is_ranked() {
        let selector = through();
        let plain = Typed(Arc::new(Affinity::new(AffinityConfig {
            key: Some("session".into()),
            through: false,
        })));
        let ids = ids(6);
        let joined = [10, 11, 12, 13, 14, 15];
        let zeroed = [0, 11, 0, 13, 14, 15];
        for key in 0..64u32 {
            let bare = session(key, None);
            let all = choose(&plain, &bare, b"h", &ids).await;
            // The selector, the cutoff's epoch and stamp, the instances'
            // stamps, and their epoch.
            type Case<'c> = (&'c Typed<Affinity>, Option<(u64, u64)>, &'c [u64], u64);
            let cases: [Case<'_>; 6] = [
                // Every instance joined after the cutoff.
                (&selector, Some((4, 9)), &joined, 4),
                // Another hub's epoch.
                (&selector, Some((3, 12)), &joined, 4),
                // No stamps: an earlier hub.
                (&selector, Some((4, 12)), &[], 0),
                // No cutoff.
                (&selector, None, &joined, 4),
                // Not set to read one.
                (&plain, Some((4, 12)), &joined, 4),
                // Only stamps of 0 at or below it.
                (&selector, Some((4, 10)), &zeroed, 4),
            ];
            for (n, (selector, cut, stamps, epoch)) in cases.into_iter().enumerate() {
                let cx = session(key, cut);
                let chosen = if stamps.is_empty() {
                    choose_after(selector, &cx, b"h", &ids, &[]).await
                } else {
                    choose_cut(selector, &cx, b"h", (&ids, stamps, epoch), &[]).await
                };
                assert_eq!(chosen, all, "case {n}, key {key}");
            }
        }
    }

    /// An instance's join stamp, as the selector reads it: `None` from an
    /// earlier hub, and for a stamp of 0.
    #[test]
    fn an_instance_reads_its_join_stamp() {
        let ids = ids(3);
        let stamps = JoinStamps::new(&ids, Box::new([0, 4, 9]), 2);
        let instances = Instances::with_joined("worker", &ids, &stamps);
        let stamps: Vec<_> = instances.iter().map(|i| i.joined()).collect();
        assert_eq!(stamps, [None, Some(4), Some(9)]);
        assert_eq!(instances.epoch(), 2);
        let bare = Instances::new("worker", &ids);
        assert!(bare.iter().all(|i| i.joined().is_none()));
        assert_eq!(bare.epoch(), 0);
    }

    /// After a decline, the next offer goes to the instance with the next
    /// highest score among those that have not declined, the same one
    /// from every sender, whatever order it lists the instances in; when
    /// every instance has declined, the highest score is chosen again.
    #[tokio::test]
    async fn a_declined_instance_gives_way_to_the_next_score() {
        let first = Typed(Arc::new(Affinity::default()));
        let second = Typed(Arc::new(Affinity::default()));
        let ids = ids(5);
        let mut reversed = ids.clone();
        reversed.reverse();
        let cx = Cx::new();
        for key in 0..32u32 {
            let head = key.to_be_bytes();
            let mut ranked = ids.clone();
            let hashed = fnv(&head);
            ranked.sort_by_key(|id| std::cmp::Reverse((score(hashed, *id), *id)));
            let top = choose(&first, &cx, &head, &ids).await;
            assert_eq!(top, ranked[0]);
            let next = choose_after(&first, &cx, &head, &ids, &[top]).await;
            assert_eq!(next, ranked[1], "key {key}");
            assert_eq!(
                choose_after(&second, &cx, &head, &reversed, &[top]).await,
                next,
                "the senders disagree on key {key}"
            );
            let third = choose_after(&first, &cx, &head, &ids, &[top, next, top]).await;
            assert_eq!(third, ranked[2], "key {key}");
            assert_eq!(
                choose_after(&second, &cx, &head, &reversed, &ranked).await,
                top,
                "every instance declined key {key}"
            );
        }
    }
}
