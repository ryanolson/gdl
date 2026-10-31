//! A pool policy as a crate outside gdl writes and tests it: it reads a
//! routing hint through a typed projection of the request, and it builds
//! the inputs to its choice itself, with no hub, no velo node, and no
//! runtime.

use std::collections::BTreeSet;
use std::time::Duration;

use gdl::{
    Cx, Damped, HomeFirst, Load, LoadSummary, NoPool, Pool, PoolChoice, PoolCondition,
    PoolEvidence, PoolPolicies, PoolPolicy, PoolRequest, PoolState, Projected, Projection,
    RoundRobin,
};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// What a frontend sends into a shard's class: the request it serves,
/// and the hint a router reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Routed {
    request: Internal,
    hint: Hint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Internal {
    messages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Hint {
    shard: String,
    fingerprint: [u8; 16],
    first_turn: bool,
}

/// The part of [`Routed`] the policy reads: only its hint.
#[derive(Debug, Deserialize, Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: Hint,
}

/// Sends a request to a pool whose name starts with its shard, and,
/// among those, as `home_first` would. It names a shard that no pool
/// serves.
struct ByShard;

impl PoolPolicy for ByShard {
    type Head = Projected<Hinted>;

    fn choose(
        &self,
        request: PoolRequest<'_, Projected<Hinted>>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let shard = request.head().hint.shard.as_str();
        let serves = |pool: &Pool| pool.name().starts_with(shard);
        if !pools.iter().any(serves) {
            return Err(NoPool::because(format!("no pool serves shard `{shard}`")));
        }
        HomeFirst
            .choose_where(request, pools, serves)
            .map(PoolChoice::new)
            .ok_or_else(NoPool::new)
    }

    /// As `home_first` checks it, since it chooses as `home_first` does.
    fn eligible(&self, pool: &Pool, region: &str, now: Instant) -> bool {
        HomeFirst.eligible(pool, region, now)
    }
}

const HOME: &str = "us-east-1";
const AWAY: &str = "eu-west-1";

/// An idle summary, arrived at `now`, of a pool with one entry instance.
fn idle(now: Instant) -> Load {
    Load::new(
        LoadSummary {
            admitted: 0,
            limit: 10,
            instances: 1,
            refusals: 0,
            period_ms: 10_000,
        },
        now,
    )
}

fn pool(name: &str, region: &str, now: Instant) -> Pool {
    Pool::new("model", name, region, PoolState::Ready)
        .with_entries(1)
        .with_load(idle(now))
}

fn hinted(shard: &str) -> Projected<Hinted> {
    Projected(Hinted {
        hint: Hint {
            shard: shard.into(),
            fingerprint: [7; 16],
            first_turn: true,
        },
    })
}

/// Two requests that differ only in their hint's shard reach different
/// pools of one class.
#[test]
fn the_hint_chooses_the_pool() {
    let now = Instant::now();
    let pools = [pool("llama-1", HOME, now), pool("qwen-1", HOME, now)];
    let cx = Cx::new();
    let chosen = |shard: &str| {
        let head = hinted(shard);
        ByShard
            .choose(PoolRequest::new(&cx, &head, HOME, now), &pools)
            .map(|choice| choice.at())
    };
    assert_eq!(chosen("llama"), Ok(0));
    assert_eq!(chosen("qwen"), Ok(1));
    assert_eq!(
        chosen("mistral"),
        Err(NoPool::because("no pool serves shard `mistral`"))
    );
}

/// A policy reads the request's excluded pools and the pools' state as
/// the runtime gives them.
#[test]
fn a_policy_sees_what_the_runtime_gives_it() {
    let now = Instant::now();
    let pools = [
        pool("llama-1", HOME, now),
        pool("llama-2", AWAY, now),
        Pool::new("model", "llama-3", HOME, PoolState::Draining).with_entries(1),
    ];
    let cx = Cx::new();
    let head = hinted("llama");
    let excluded = [pools[0].id()];
    let request = PoolRequest::new(&cx, &head, HOME, now).with_excluded(&excluded);
    assert_eq!(request.region(), HOME);
    assert_eq!(request.now(), now);
    assert_eq!(ByShard.choose(request, &pools).map(|c| c.at()), Ok(1));
    let excluded = [pools[0].id(), pools[1].id()];
    let request = PoolRequest::new(&cx, &head, HOME, now).with_excluded(&excluded);
    assert_eq!(
        ByShard.choose(request, &pools).map(|c| c.at()),
        Err(NoPool::new()),
        "a draining pool, with no reason"
    );
    // A pool is named the same in every table: its class and name.
    assert_eq!(
        pools[0].id(),
        Pool::new("model", "llama-1", AWAY, PoolState::Degraded).id()
    );
    assert_ne!(
        pools[0].id(),
        Pool::new("other", "llama-1", HOME, PoolState::Ready).id()
    );
}

/// Moves a request off a pool that its hub lists as draining, as a set
/// router moves a pin: it reads the drain from its request, which holds
/// the same table as the pools it chooses from.
struct Undrained;

impl PoolPolicy for Undrained {
    type Head = Projected<Hinted>;

    fn choose(
        &self,
        request: PoolRequest<'_, Projected<Hinted>>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        HomeFirst
            .choose_where(request, pools, |pool| !request.is_draining(pool.name()))
            .map(PoolChoice::new)
            .ok_or_else(|| NoPool::because("every pool drains"))
    }
}

/// A policy reads which pools drain from its request, by name, for a
/// followed pool and for one whose follow was dropped alike. A request
/// built without them lists no pool as draining.
#[test]
fn a_policy_reads_which_pools_drain_from_its_request() {
    let now = Instant::now();
    let pools = [pool("llama-1", HOME, now), pool("llama-2", AWAY, now)];
    let cx = Cx::new();
    let head = hinted("llama");
    // `llama-0`'s follow was dropped: it is not among the pools.
    let draining = PoolEvidence::new(3, PoolCondition::Live).with_draining(true);
    let evidence: [(Box<str>, PoolEvidence); 3] = [
        ("llama-0".into(), draining),
        ("llama-1".into(), draining),
        ("llama-2".into(), PoolEvidence::new(3, PoolCondition::Live)),
    ];
    let request = PoolRequest::new(&cx, &head, HOME, now).with_evidence(&evidence);
    assert!(request.is_draining("llama-0"), "a pool no longer followed");
    assert!(request.is_draining("llama-1"), "a followed pool");
    assert!(!request.is_draining("llama-2"));
    assert_eq!(
        Undrained.choose(request, &pools).map(|c| c.at()),
        Ok(1),
        "the home pool drains, so the request goes away"
    );
    let undrained = PoolRequest::new(&cx, &head, HOME, now);
    assert!(!undrained.is_draining("llama-1"));
    assert_eq!(Undrained.choose(undrained, &pools).map(|c| c.at()), Ok(0));
    let every: [(Box<str>, PoolEvidence); 2] =
        [("llama-1".into(), draining), ("llama-2".into(), draining)];
    assert_eq!(
        Undrained
            .choose(
                PoolRequest::new(&cx, &head, HOME, now).with_evidence(&every),
                &pools
            )
            .map(|c| c.at()),
        Err(NoPool::because("every pool drains"))
    );
}

/// A policy that filters the pools chooses among the ones it keeps as
/// `home_first` or `round_robin` would, and gets an index into all of
/// them: the shard's pool elsewhere while its home pool is saturated,
/// never another shard's pool, and the shard's pools in turn.
#[test]
fn a_policy_chooses_as_gdls_own_among_the_pools_it_keeps() {
    let now = Instant::now();
    let full = Load::new(
        LoadSummary {
            admitted: 10,
            ..*idle(now).summary()
        },
        now,
    );
    let pools = [
        pool("llama-1", HOME, now),
        Pool::new("model", "qwen-1", HOME, PoolState::Ready)
            .with_entries(1)
            .with_load(full),
        pool("llama-2", AWAY, now),
        pool("qwen-2", AWAY, now),
    ];
    let cx = Cx::new();
    let chosen = |shard: &str| -> BTreeSet<usize> {
        let head = hinted(shard);
        (0..64)
            .filter_map(|turn| {
                let request = PoolRequest::new(&cx, &head, HOME, now).with_turn(turn);
                ByShard
                    .choose(request, &pools)
                    .ok()
                    .map(|choice| choice.at())
            })
            .collect()
    };
    assert_eq!(chosen("qwen"), BTreeSet::from([3]));
    assert_eq!(chosen("llama"), BTreeSet::from([0]));
    assert!(chosen("mistral").is_empty());
    let head = hinted("qwen");
    let turns: Vec<Option<usize>> = (0..4)
        .map(|turn| {
            let request = PoolRequest::new(&cx, &head, HOME, now).with_turn(turn);
            RoundRobin.choose_where(request, &pools, |pool| pool.name().starts_with("qwen"))
        })
        .collect();
    assert_eq!(turns, [Some(1), Some(3), Some(1), Some(3)]);
}

/// The guide's fixture (`docs/src/guides/write-a-pool-policy.md`, step 3)
/// works with both versions of its policy: the first match, and the one
/// that chooses as `home_first` does, which needs a fresh summary.
#[test]
fn the_guide_fixture_works_with_home_first() {
    let now = Instant::now();
    let idle = Load::new(
        LoadSummary {
            limit: 10,
            period_ms: 10_000,
            ..LoadSummary::default()
        },
        now,
    );
    let pools = [
        Pool::new("model", "llama-1", "us-east-1", PoolState::Ready)
            .with_entries(1)
            .with_load(idle),
        Pool::new("model", "qwen-1", "us-east-1", PoolState::Ready)
            .with_entries(1)
            .with_load(idle),
    ];
    let cx = Cx::new();
    let head = hinted("qwen");
    let request = PoolRequest::new(&cx, &head, "us-east-1", now);

    assert_eq!(
        pools.iter().position(|pool| {
            pool.name().starts_with(request.head().hint.shard.as_str())
                && pool.takes_work()
                && !request.is_excluded(pool)
        }),
        Some(1),
        "the guide's first policy works with this fixture"
    );
    assert_eq!(ByShard.choose(request, &pools).map(|c| c.at()), Ok(1));
}

/// gdl's own policies take the same inputs that a policy crate builds:
/// `home_first` takes home pools before pools elsewhere, and passes a
/// damped pool over while its window lasts.
#[test]
fn home_first_runs_on_inputs_a_policy_crate_builds() {
    let now = Instant::now();
    let pools = [
        pool("a", HOME, now),
        pool("b", HOME, now),
        pool("away", AWAY, now),
    ];
    let cx = Cx::new();
    let chosen = |damped: &Damped| -> BTreeSet<&str> {
        (0..64)
            .filter_map(|turn| {
                let request = PoolRequest::new(&cx, &gdl::Encoded, HOME, now)
                    .with_damped(damped)
                    .with_turn(turn);
                HomeFirst
                    .choose(request, &pools)
                    .ok()
                    .map(|choice| choice.at())
            })
            .map(|at| pools[at].name())
            .collect()
    };
    assert_eq!(chosen(&Damped::default()), BTreeSet::from(["a", "b"]));
    let damped = Damped::new([(pools[0].id(), now + Duration::from_secs(1))]);
    assert!(damped.lasts(now) && damped.is_damped(pools[0].id()));
    assert_eq!(damped.left(pools[0].id(), now), 1000);
    assert_eq!(chosen(&damped), BTreeSet::from(["b"]));
    // A summary older than three periods is stale: such a pool is not
    // chosen by `home_first`.
    let stale = [Pool::new("model", "old", HOME, PoolState::Ready)
        .with_entries(1)
        .with_load(idle(now - Duration::from_secs(31)))];
    let request = PoolRequest::new(&cx, &gdl::Encoded, HOME, now);
    assert_eq!(
        HomeFirst.choose(request, &stale).map(|c| c.at()),
        Err(NoPool::new())
    );
    assert!(!HomeFirst.eligible(&stale[0], HOME, now));
    assert!(HomeFirst.eligible(&pools[0], HOME, now));
}

/// A policy is registered by a name a placement can use, once, and not
/// under the name of one of gdl's own.
#[test]
fn a_policy_is_registered_by_name() {
    let mut policies = PoolPolicies::new();
    policies.add("by_shard", ByShard).expect("registered");
    assert!(policies.contains("by_shard"));
    let twice = policies
        .add("by_shard", ByShard)
        .expect_err("registered twice");
    assert!(twice.to_string().contains("by_shard"), "{twice}");
    for name in ["home_first", "round_robin"] {
        let error = policies.add(name, ByShard).expect_err("a built-in name");
        assert!(error.to_string().contains(name), "{error}");
    }
    for name in ["ByShard", "by__shard", "by_shard_", ""] {
        assert!(policies.add(name, ByShard).is_err(), "{name:?} was taken");
    }
}

/// The edge type as it names its hint on the wire: `routing_hint`.
#[derive(Debug, Deserialize)]
struct Renamed {
    #[allow(dead_code, reason = "only its names are read")]
    request: Internal,
    #[serde(rename = "routing_hint")]
    #[allow(dead_code, reason = "only its names are read")]
    hint: Hint,
}

/// Reads `hint` by its Rust name, which is not the name it crosses under.
#[derive(Debug, Deserialize, Projection)]
#[gdl(of = Renamed)]
struct Unrenamed {
    hint: Hint,
}

struct ByUnrenamed;

impl PoolPolicy for ByUnrenamed {
    type Head = Projected<Unrenamed>;

    fn choose(
        &self,
        request: PoolRequest<'_, Projected<Unrenamed>>,
        _: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        (!request.head().hint.shard.is_empty())
            .then(|| PoolChoice::new(0))
            .ok_or_else(NoPool::new)
    }
}

impl gdl::Selector for ByUnrenamed {
    type Head = Projected<Unrenamed>;

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, Self::Head>,
        instances: gdl::Instances<'a>,
    ) -> gdl::Result<gdl::Choice<'a>> {
        Ok(gdl::Choice::new(instances.get(0).expect("one instance")))
    }
}

/// A projection whose field crosses under another name than the edge
/// type's would fail every request's read, so it is refused when it is
/// registered, as a policy and as a selector, naming the field.
#[test]
fn a_projection_of_a_field_the_wire_names_otherwise_is_refused() {
    let error = PoolPolicies::new()
        .add("by_unrenamed", ByUnrenamed)
        .expect_err("the edge carries no `hint`");
    assert_eq!(error.kind(), gdl::ErrorKind::Placement, "{error}");
    let message = error.to_string();
    assert!(
        message.contains("by_unrenamed") && message.contains("`hint`"),
        "{message}"
    );
    assert!(message.contains("routing_hint"), "{message}");
    let error = gdl::Selectors::new()
        .add("by_unrenamed", ByUnrenamed)
        .expect_err("the edge carries no `hint`");
    assert_eq!(error.kind(), gdl::ErrorKind::Placement, "{error}");
    assert!(error.to_string().contains("`hint`"), "{error}");
}

/// The test property type.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Test {
    name: String,
}

/// Chooses a pool that lists the test property `a` ready, from the
/// class's property index of its kind, which it registers under `kind`.
struct ByProperty {
    kind: gdl::PropertyKind<Test>,
}

impl PoolPolicy for ByProperty {
    type Head = gdl::Encoded;
    const NEEDS: &'static [&'static str] = &["test"];

    fn choose(
        &self,
        request: PoolRequest<'_, gdl::Encoded>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let index = request
            .properties()
            .get(&self.kind)
            .ok_or_else(NoPool::new)?;
        let listed = index.get(&Test { name: "a".into() });
        pools
            .iter()
            .position(|pool| {
                listed.iter().any(|(_, listing)| {
                    listing.state() == gdl::PropertyState::Ready && listing.pool() == pool.name()
                })
            })
            .map(PoolChoice::new)
            .ok_or_else(|| NoPool::because("no pool lists `a` ready"))
    }

    fn properties(&self, kinds: &gdl::PropertyKinds) -> gdl::Result<()> {
        kinds.register(&self.kind)
    }
}

/// A policy registers the property kinds it reads, and chooses by the
/// index of its kind in the request; a second policy that registers the
/// kind with another type is refused, naming the policy.
#[test]
fn a_policy_registers_the_kinds_it_reads_and_chooses_by_them() {
    const TEST: gdl::PropertyKind<Test> = gdl::PropertyKind::new("test");
    let mut policies = PoolPolicies::new();
    policies
        .add("by_property", ByProperty { kind: TEST })
        .expect("registered");
    let kinds = gdl::PropertyKinds::new();
    policies.register_properties(&kinds).expect("one type");
    let listing =
        |pool: &str, state| gdl::PropertyPool::new(pool, "r", state, gdl::PoolCondition::Live);
    let indexes = gdl::PropertyIndexes::new(true).with(
        &TEST,
        gdl::PropertyIndex::new([
            (
                Test { name: "a".into() },
                listing("p1", gdl::PropertyState::Starting),
            ),
            (
                Test { name: "a".into() },
                listing("p2", gdl::PropertyState::Ready),
            ),
        ]),
    );
    let pools = [
        Pool::new("c", "p1", "r", PoolState::Ready),
        Pool::new("c", "p2", "r", PoolState::Ready),
    ];
    let cx = Cx::new();
    let request =
        PoolRequest::new(&cx, &gdl::Encoded, "r", Instant::now()).with_properties(&indexes);
    let policy = ByProperty { kind: TEST };
    assert_eq!(policy.choose(request, &pools).map(|c| c.at()), Ok(1));

    struct Other;
    impl PoolPolicy for Other {
        type Head = gdl::Encoded;
        const NEEDS: &'static [&'static str] = &["test"];
        fn choose(
            &self,
            _: PoolRequest<'_, gdl::Encoded>,
            _: &[Pool],
        ) -> Result<PoolChoice, NoPool> {
            Err(NoPool::new())
        }
        fn properties(&self, kinds: &gdl::PropertyKinds) -> gdl::Result<()> {
            kinds.register(&gdl::PropertyKind::<String>::new("test"))
        }
    }
    policies.add("other", Other).expect("registered");
    let error = policies
        .register_properties(&kinds)
        .expect_err("another type under `test`");
    assert!(error.to_string().contains("`other`"), "{error}");
}

/// A policy reads only the kinds its `NEEDS` names, so that what the
/// generator's supply check reads of its source is what it reads: one that
/// registers another kind fails the segment's start, naming the policy and
/// the kind.
#[test]
fn a_policy_registers_only_the_kinds_its_needs_names() {
    struct Unsaid;
    impl PoolPolicy for Unsaid {
        type Head = gdl::Encoded;
        const NEEDS: &'static [&'static str] = &["region"];
        fn choose(
            &self,
            _: PoolRequest<'_, gdl::Encoded>,
            _: &[Pool],
        ) -> Result<PoolChoice, NoPool> {
            Err(NoPool::new())
        }
        fn properties(&self, kinds: &gdl::PropertyKinds) -> gdl::Result<()> {
            kinds.register(&gdl::PropertyKind::<Test>::new("test"))
        }
    }
    let mut policies = PoolPolicies::new();
    policies.add("unsaid", Unsaid).expect("registered");
    let kinds = gdl::PropertyKinds::new();
    let error = policies
        .register_properties(&kinds)
        .expect_err("a kind its NEEDS does not name");
    let error = error.to_string();
    assert!(error.contains("pool policy `unsaid`"), "{error}");
    assert!(
        error.contains("the policy reads property kind `test`, which its `NEEDS` does not name"),
        "{error}"
    );
    assert!(
        format!("{kinds:?}") == "[]",
        "nothing registered: {kinds:?}"
    );
}

/// Keeps a request with its pin, accepted under the pin's hub epoch, and
/// moves it only on evidence at least as new as the pin, as the endpoints'
/// set router does: here, a withdrawal of the pinned pool at or past the
/// pin's epoch. A withdrawal of an earlier hub of the pool says nothing of
/// the hub the pin was accepted under.
struct Pinned {
    pool: &'static str,
    epoch: u64,
}

impl PoolPolicy for Pinned {
    type Head = Projected<Hinted>;

    fn choose(
        &self,
        request: PoolRequest<'_, Projected<Hinted>>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let gone = request.is_withdrawn(self.pool)
            && request.epoch(self.pool).is_some_and(|at| at >= self.epoch);
        if !gone {
            return pools
                .iter()
                .position(|pool| pool.name() == self.pool)
                .map(PoolChoice::new)
                .ok_or_else(|| NoPool::because("the pin has no row"));
        }
        HomeFirst
            .choose_where(request, pools, |pool| pool.name() != self.pool)
            .map(PoolChoice::new)
            .ok_or_else(|| NoPool::because("the pin withdrew, and no other pool takes work"))
    }
}

/// A policy reads each pool's hub epoch and whether it withdrew from its
/// request, by name, followed or not. A withdrawn pool reads the epoch of
/// the hub that withdrew it, so a policy can hold a withdrawal against the
/// epoch it pinned a request under. A request built without them reads no
/// epoch and no withdrawal.
#[test]
fn a_policy_reads_epochs_and_withdrawals_from_its_request() {
    let now = Instant::now();
    let pools = [pool("llama-1", HOME, now), pool("llama-2", AWAY, now)];
    let cx = Cx::new();
    let head = hinted("llama");
    // `llama-0` withdrew at epoch 7; `llama-3` is listed, with no row.
    let live = |epoch| PoolEvidence::new(epoch, PoolCondition::Live);
    let evidence: [(Box<str>, PoolEvidence); 4] = [
        ("llama-0".into(), live(7).with_withdrawn(true)),
        ("llama-1".into(), live(5)),
        ("llama-2".into(), live(0)),
        (
            "llama-3".into(),
            PoolEvidence::new(9, PoolCondition::Missing),
        ),
    ];
    let request = PoolRequest::new(&cx, &head, HOME, now).with_evidence(&evidence);
    assert_eq!(
        request.evidence("llama-3"),
        Some(PoolEvidence::new(9, PoolCondition::Missing)),
        "one value: the condition with the epoch it is of"
    );
    assert_eq!(request.epoch("llama-0"), Some(7), "the withdrawing hub's");
    assert_eq!(request.epoch("llama-1"), Some(5));
    assert_eq!(
        request.epoch("llama-2"),
        Some(0),
        "a hub of an earlier build"
    );
    assert_eq!(request.epoch("llama-3"), Some(9), "listed, with no row");
    assert_eq!(request.epoch("llama-4"), None, "no word of it");
    assert!(request.is_withdrawn("llama-0"));
    assert!(!request.is_withdrawn("llama-1"));
    assert!(!request.is_withdrawn("llama-3"));

    let bare = PoolRequest::new(&cx, &head, HOME, now);
    assert_eq!(bare.epoch("llama-1"), None);
    assert!(!bare.is_withdrawn("llama-0"));

    // A pin on `llama-0` at its withdrawal's epoch, or an earlier one,
    // moves; a pin from a later hub of it holds.
    let choose = |epoch| {
        Pinned {
            pool: "llama-0",
            epoch,
        }
        .choose(request, &pools)
        .map(|c| c.at())
    };
    assert_eq!(choose(7), Ok(0), "withdrawn at the pin's epoch");
    assert_eq!(choose(6), Ok(0), "withdrawn past it");
    assert_eq!(
        choose(8),
        Err(NoPool::because("the pin has no row")),
        "a withdrawal older than the pin says nothing of it"
    );
}

/// Evidence of an earlier hub is dropped whole, and evidence of a later
/// hub replaces it whole: a condition never pairs with another hub's
/// epoch. Of one hub, the later word wins, but a withdrawal lasts, and a
/// drain lasts until the pool withdraws.
#[test]
fn evidence_of_an_earlier_hub_is_dropped_whole() {
    let at = |epoch, condition| PoolEvidence::new(epoch, condition);
    let held = at(6, PoolCondition::Live).with_draining(true);
    for earlier in [
        at(5, PoolCondition::Missing),
        at(5, PoolCondition::ScaledToZero),
        at(5, PoolCondition::Live).with_withdrawn(true),
    ] {
        assert_eq!(held.merged(earlier), held, "{earlier:?}");
    }
    let later = at(7, PoolCondition::Missing);
    assert_eq!(held.merged(later), later, "a later hub's, drain and all");
    assert_eq!(
        held.merged(at(6, PoolCondition::Missing)),
        at(6, PoolCondition::Missing).with_draining(true),
        "of one hub, the later word, and the drain holds"
    );
    let withdrawn = at(6, PoolCondition::Live).with_withdrawn(true);
    assert_eq!(
        held.merged(withdrawn),
        withdrawn,
        "a withdrawal ends the drain"
    );
    assert_eq!(
        withdrawn.merged(at(6, PoolCondition::Live)),
        withdrawn,
        "of one hub, a withdrawal lasts"
    );
    assert_eq!(
        withdrawn.merged(at(7, PoolCondition::Live)),
        at(7, PoolCondition::Live),
        "a later hub of the pool ends it"
    );
}

/// Of one hub, every field from either side: the condition is the later
/// word's, a withdrawal from either side lasts, and a drain from either
/// side lasts unless the pool withdrew. Which side is the later word
/// matters only for the condition, so only a matrix over it tells the
/// held value from the incoming one.
#[test]
fn of_one_hub_each_field_merges_from_either_side() {
    let values = || {
        [
            PoolCondition::Live,
            PoolCondition::Missing,
            PoolCondition::ScaledToZero,
        ]
        .into_iter()
        .flat_map(|condition| {
            [(false, false), (true, false), (false, true), (true, true)].map(
                move |(draining, withdrawn)| {
                    PoolEvidence::new(6, condition)
                        .with_draining(draining)
                        .with_withdrawn(withdrawn)
                },
            )
        })
    };
    let mut cases = 0;
    for held in values() {
        for incoming in values() {
            let withdrawn = held.is_withdrawn() || incoming.is_withdrawn();
            let expected = PoolEvidence::new(6, incoming.condition())
                .with_withdrawn(withdrawn)
                .with_draining(!withdrawn && (held.is_draining() || incoming.is_draining()));
            assert_eq!(
                held.merged(incoming),
                expected,
                "held {held:?}, incoming {incoming:?}"
            );
            cases += 1;
        }
    }
    assert_eq!(cases, 144);
}

/// A pool that declined an offer as gone in a follow of one hub is not
/// gone in the follow of a later hub of it, though that hub numbers its
/// follows from 1 again and the two share a generation and a count of
/// changes; nor does a decline in the earlier hub's follow replace a mark
/// made in the later one.
#[test]
fn a_gone_mark_is_of_one_hubs_follow() {
    let pool = |epoch| {
        Pool::new("model", "llama-1", HOME, PoolState::Draining)
            .with_follow(1, 0)
            .with_epoch(epoch)
    };
    let gone = Damped::EMPTY.with_gone(&pool(5));
    assert!(gone.is_gone(&pool(5)));
    assert!(!gone.is_gone(&pool(6)), "a later hub's follow");
    let later = Damped::EMPTY.with_gone(&pool(6));
    assert!(
        later.holds_gone_since(&pool(5)),
        "the earlier hub's is older"
    );
    assert!(!gone.holds_gone_since(&pool(6)));
}
