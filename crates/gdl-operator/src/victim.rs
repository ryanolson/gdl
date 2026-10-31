//! Victim policies: how a `GdlScaler` chooses the instance to remove. A
//! policy ranks each candidate; the scaler removes the lowest rank, and a
//! tie goes to the newest pod, the one with the least work behind it. A
//! policy is registered by name, as pool policies are, so that a fleet can
//! add its own.

use std::collections::BTreeMap;
use std::sync::Arc;

/// The default policy: the instance with the fewest admitted requests,
/// whose drain is the shortest.
pub const FEWEST_ADMITTED: &str = "fewestAdmitted";

/// The newest pod.
pub const NEWEST: &str = "newest";

/// A pod the scaler may remove, with what it read of its instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// Its pod's name.
    pub pod: String,
    /// Its pod's uid.
    pub uid: String,
    /// When its pod was created, in milliseconds since the Unix epoch.
    pub created_ms: i64,
    /// Its load: its admitted requests, or its requests in flight where
    /// those are more ([`crate::Held::load`]). `None` when neither its
    /// `/metrics` nor its hub answered.
    pub load: Option<u64>,
}

/// Ranks candidates: the lowest rank is removed first.
pub trait VictimPolicy: Send + Sync {
    /// `candidate`'s rank.
    fn rank(&self, candidate: &Candidate) -> u64;
}

/// The fewest admitted requests first. A candidate whose load could not
/// be read ranks last, so that one whose load is known to be low goes
/// first.
#[derive(Debug, Clone, Copy, Default)]
pub struct FewestAdmitted;

impl VictimPolicy for FewestAdmitted {
    fn rank(&self, candidate: &Candidate) -> u64 {
        candidate.load.unwrap_or(u64::MAX)
    }
}

/// Every candidate ranks the same, so the newest pod goes.
#[derive(Debug, Clone, Copy, Default)]
pub struct Newest;

impl VictimPolicy for Newest {
    fn rank(&self, _: &Candidate) -> u64 {
        0
    }
}

/// The registered victim policies, by name: [`FEWEST_ADMITTED`] and
/// [`NEWEST`], and any a fleet adds.
#[derive(Clone)]
pub struct Victims {
    policies: BTreeMap<String, Arc<dyn VictimPolicy>>,
}

impl Default for Victims {
    fn default() -> Self {
        let mut policies: BTreeMap<String, Arc<dyn VictimPolicy>> = BTreeMap::new();
        policies.insert(FEWEST_ADMITTED.into(), Arc::new(FewestAdmitted));
        policies.insert(NEWEST.into(), Arc::new(Newest));
        Self { policies }
    }
}

impl std::fmt::Debug for Victims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.policies.keys()).finish()
    }
}

impl Victims {
    /// Register `policy` as `name`: a letter, then letters and digits, at
    /// most 64 characters. Refused for a name already registered, the
    /// built-in ones included.
    pub fn add(&mut self, name: &str, policy: impl VictimPolicy + 'static) -> Result<(), String> {
        let shaped = name.len() <= 64
            && name.starts_with(|c: char| c.is_ascii_alphabetic())
            && name.chars().all(|c| c.is_ascii_alphanumeric());
        if !shaped {
            return Err(format!(
                "victim policy `{name}`: a name is a letter, then letters and digits, at most 64 characters"
            ));
        }
        if self.policies.contains_key(name) {
            return Err(format!("victim policy `{name}` is already registered"));
        }
        self.policies.insert(name.to_string(), Arc::new(policy));
        Ok(())
    }

    /// The policy registered as `name`.
    pub fn get(&self, name: &str) -> Option<&dyn VictimPolicy> {
        self.policies.get(name).map(Arc::as_ref)
    }

    /// The `count` candidates `name` removes first, in order: the lowest
    /// rank, and of equal ranks the newest pod, and of pods created in the
    /// same millisecond the greater uid, so that every reader agrees.
    /// `None` when no policy has the name.
    pub fn choose<'c>(
        &self,
        name: &str,
        candidates: &'c [Candidate],
        count: usize,
    ) -> Option<Vec<&'c Candidate>> {
        let policy = self.get(name)?;
        let mut ranked: Vec<(u64, &Candidate)> = candidates
            .iter()
            .map(|candidate| (policy.rank(candidate), candidate))
            .collect();
        ranked.sort_by(|(a_rank, a), (b_rank, b)| {
            a_rank
                .cmp(b_rank)
                .then(b.created_ms.cmp(&a.created_ms))
                .then(b.uid.cmp(&a.uid))
        });
        Some(ranked.into_iter().take(count).map(|(_, c)| c).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(uid: &str, created_ms: i64, load: Option<u64>) -> Candidate {
        Candidate {
            pod: format!("pod-{uid}"),
            uid: uid.into(),
            created_ms,
            load,
        }
    }

    fn chosen(
        victims: &Victims,
        name: &str,
        candidates: &[Candidate],
        count: usize,
    ) -> Vec<String> {
        victims
            .choose(name, candidates, count)
            .expect("a policy")
            .into_iter()
            .map(|c| c.uid.clone())
            .collect()
    }

    #[test]
    fn the_fewest_admitted_goes_first() {
        let victims = Victims::default();
        let candidates = [
            candidate("a", 1, Some(5)),
            candidate("b", 2, Some(1)),
            candidate("c", 3, Some(3)),
        ];
        assert_eq!(chosen(&victims, FEWEST_ADMITTED, &candidates, 1), ["b"]);
        assert_eq!(
            chosen(&victims, FEWEST_ADMITTED, &candidates, 2),
            ["b", "c"]
        );
    }

    #[test]
    fn a_tie_goes_to_the_newest_pod() {
        let victims = Victims::default();
        let candidates = [
            candidate("a", 1, Some(2)),
            candidate("b", 3, Some(2)),
            candidate("c", 2, Some(2)),
        ];
        assert_eq!(chosen(&victims, FEWEST_ADMITTED, &candidates, 1), ["b"]);
        let same_time = [candidate("a", 1, Some(0)), candidate("b", 1, Some(0))];
        assert_eq!(chosen(&victims, FEWEST_ADMITTED, &same_time, 1), ["b"]);
    }

    #[test]
    fn an_unknown_load_ranks_last() {
        let victims = Victims::default();
        let candidates = [candidate("a", 9, None), candidate("b", 1, Some(40))];
        assert_eq!(chosen(&victims, FEWEST_ADMITTED, &candidates, 1), ["b"]);
        let unknown = [candidate("a", 1, None), candidate("b", 2, None)];
        assert_eq!(chosen(&victims, FEWEST_ADMITTED, &unknown, 1), ["b"]);
    }

    #[test]
    fn newest_ignores_load() {
        let victims = Victims::default();
        let candidates = [candidate("a", 1, Some(0)), candidate("b", 2, Some(100))];
        assert_eq!(chosen(&victims, NEWEST, &candidates, 1), ["b"]);
    }

    #[test]
    fn a_policy_is_registered_once_by_a_shaped_name() {
        struct Oldest;
        impl VictimPolicy for Oldest {
            fn rank(&self, candidate: &Candidate) -> u64 {
                u64::try_from(candidate.created_ms).unwrap_or(0)
            }
        }
        let mut victims = Victims::default();
        victims.add("oldest", Oldest).expect("registers");
        let candidates = [candidate("a", 1, Some(9)), candidate("b", 2, Some(0))];
        assert_eq!(chosen(&victims, "oldest", &candidates, 1), ["a"]);
        assert!(victims.add("oldest", Oldest).is_err());
        assert!(victims.add(FEWEST_ADMITTED, Oldest).is_err());
        for refused in ["", "1st", "by_load", "by-load", &"a".repeat(65)] {
            assert!(victims.add(refused, Oldest).is_err(), "{refused}");
        }
        assert!(victims.choose("unknown", &candidates, 1).is_none());
    }
}
