//! A pool policy as a crate outside gdl writes and tests it: it reads a
//! routing hint through a typed projection of the request, and it builds
//! the inputs to its choice itself, with no hub, no velo node, and no
//! runtime.

use std::collections::BTreeSet;
use std::time::Duration;

use gdl::{
    Cx, Damped, HomeFirst, Load, LoadSummary, NoPool, Pool, PoolChoice, PoolPolicies, PoolPolicy,
    PoolRequest, PoolState, Projected, Projection, RoundRobin,
};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// What a frontend sends into a model's class: the request it serves,
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
    model: String,
    fingerprint: [u8; 16],
    first_turn: bool,
}

/// The part of [`Routed`] the policy reads: only its hint.
#[derive(Debug, Deserialize, Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: Hint,
}

/// Sends a request to a pool whose name starts with its model, and,
/// among those, as `home_first` would. It names a model that no pool
/// serves.
struct ByModel;

impl PoolPolicy for ByModel {
    type Head = Projected<Hinted>;

    fn choose(
        &self,
        request: PoolRequest<'_, Projected<Hinted>>,
        pools: &[Pool],
    ) -> Result<PoolChoice, NoPool> {
        let model = request.head().hint.model.as_str();
        let serves = |pool: &Pool| pool.name().starts_with(model);
        if !pools.iter().any(serves) {
            return Err(NoPool::because(format!("no pool serves model `{model}`")));
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

fn hinted(model: &str) -> Projected<Hinted> {
    Projected(Hinted {
        hint: Hint {
            model: model.into(),
            fingerprint: [7; 16],
            first_turn: true,
        },
    })
}

/// Two requests that differ only in their hint's model reach different
/// pools of one class.
#[test]
fn the_hint_chooses_the_pool() {
    let now = Instant::now();
    let pools = [pool("llama-1", HOME, now), pool("qwen-1", HOME, now)];
    let cx = Cx::new();
    let chosen = |model: &str| {
        let head = hinted(model);
        ByModel
            .choose(PoolRequest::new(&cx, &head, HOME, now), &pools)
            .map(|choice| choice.at())
    };
    assert_eq!(chosen("llama"), Ok(0));
    assert_eq!(chosen("qwen"), Ok(1));
    assert_eq!(
        chosen("mistral"),
        Err(NoPool::because("no pool serves model `mistral`"))
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
    assert_eq!(ByModel.choose(request, &pools).map(|c| c.at()), Ok(1));
    let excluded = [pools[0].id(), pools[1].id()];
    let request = PoolRequest::new(&cx, &head, HOME, now).with_excluded(&excluded);
    assert_eq!(
        ByModel.choose(request, &pools).map(|c| c.at()),
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

/// A policy that filters the pools chooses among the ones it keeps as
/// `home_first` or `round_robin` would, and gets an index into all of
/// them: the model's pool elsewhere while its home pool is saturated,
/// never another model's pool, and the model's pools in turn.
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
    let chosen = |model: &str| -> BTreeSet<usize> {
        let head = hinted(model);
        (0..64)
            .filter_map(|turn| {
                let request = PoolRequest::new(&cx, &head, HOME, now).with_turn(turn);
                ByModel
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
            pool.name().starts_with(request.head().hint.model.as_str())
                && pool.takes_work()
                && !request.is_excluded(pool)
        }),
        Some(1),
        "the guide's first policy works with this fixture"
    );
    assert_eq!(ByModel.choose(request, &pools).map(|c| c.at()), Ok(1));
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
    policies.add("by_model", ByModel).expect("registered");
    assert!(policies.contains("by_model"));
    let twice = policies
        .add("by_model", ByModel)
        .expect_err("registered twice");
    assert!(twice.to_string().contains("by_model"), "{twice}");
    for name in ["home_first", "round_robin"] {
        let error = policies.add(name, ByModel).expect_err("a built-in name");
        assert!(error.to_string().contains(name), "{error}");
    }
    for name in ["ByModel", "by__model", "by_model_", ""] {
        assert!(policies.add(name, ByModel).is_err(), "{name:?} was taken");
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
        (!request.head().hint.model.is_empty())
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
