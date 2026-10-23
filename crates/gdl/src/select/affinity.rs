//! `affinity`: the same key goes to the same instance, from every sender.

use serde::Deserialize;

use super::{Choice, Encoded, InstanceId, Instances, Request, Selector};
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
#[derive(Debug, Default)]
pub struct Affinity {
    key: Option<String>,
}

/// `[selector.affinity]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffinityConfig {
    /// The metadata entry whose value is the key, such as a session id.
    /// Unset, or absent from a request, the key is the request's head.
    #[serde(default)]
    pub key: Option<String>,
}

impl Affinity {
    /// An affinity selector with `config`.
    pub fn new(config: AffinityConfig) -> Self {
        Self { key: config.key }
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
        let chosen = instances
            .iter()
            .max_by_key(|instance| (score(key, instance.id()), instance.id()))
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
    use crate::select::Typed;
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
        let picked = selector
            .select(cx, head, Default::default(), Instances::new("worker", ids))
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
}
