# Write a pool policy

A pool policy chooses the pool of a class that a hand-off goes to, when a hop crosses into that class. This guide writes one that reads a routing hint from the request, tests it without a runtime, binds it in the spec or registers it, and names it in a placement. [Following and choosing pools](../concepts/pool-routing.md#choosing-a-pool) explains when a policy runs and what gdl's own policies do. Try `round_robin` and `home_first` first: they need no code.

## 1. Read only what you route by

A policy's `Head` is what it reads of the request's head, as for a [selector](write-a-selector.md):

- the edge's own type, decoded whole;
- `gdl::Encoded`, the bytes only, with no decode;
- `gdl::Projected<P>`, a part of the head: a type `P` that declares some of the edge type's fields, under the same names and types.

A request that crosses into a model's class often carries a large body and a small hint. A projection reads the hint and skips the body:

```rust
use gdl::Projection;

/// What crosses into the model's class.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Routed {
    pub request: Internal,
    pub hint: Hint,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Hint {
    pub model: String,
    pub fingerprint: [u8; 16],
    pub first_turn: bool,
}

/// The part of `Routed` the policy reads.
#[derive(serde::Deserialize, Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: Hint,
}
```

`#[gdl(of = ...)]` names the edge type. A projection is always derived, and the derive checks that the edge type has each field, under its name and of its exact type. A field that the edge type lacks does not build, and the error names it. The derive reads each field through the edge type, so the edge type's fields must be visible where the projection is: `pub` when the edge type is in another crate.

The derive refuses `#[serde(deny_unknown_fields)]` on a projection, which skips the edge type's other fields, and `#[serde(alias)]` on its fields. The policy's registration compares the field names that each type's `Deserialize` reads, which serde's `rename` and `rename_all` change. It refuses a projection field whose name the edge type does not read, and names the field. [Known limitations](../operations/limitations.md#a-projection-is-not-checked-against-what-its-edge-type-writes) lists what passes this check. The segment refuses to start when the edge carries another type than `Of`, or when the edge is encoded as `msgpack_array`, which has no field names. `cargo gdl check` refuses a [bound](#4-bind-it-in-the-spec) policy in both cases. [Runtime feature costs](../operations/feature-costs.md#pool-policies) measures what a projection saves.

## 2. Implement `gdl::PoolPolicy`

```rust
use gdl::{Pool, PoolChoice, PoolPolicy, PoolRequest, NoPool, Projected};

/// The first pool that serves the request's model.
struct ByModel;

impl PoolPolicy for ByModel {
    type Head = Projected<Hinted>;

    fn choose(&self, request: PoolRequest<'_, Projected<Hinted>>, pools: &[Pool]) -> Result<PoolChoice, NoPool> {
        let model = request.head().hint.model.as_str();
        let serves = |pool: &Pool| pool.name().starts_with(model);
        if !pools.iter().any(serves) {
            return Err(NoPool::because(format!("no pool serves model `{model}`")));
        }
        let at = pools
            .iter()
            .position(|pool| serves(pool) && pool.takes_work() && !request.is_excluded(pool))
            .ok_or_else(NoPool::new)?;
        Ok(PoolChoice::new(at))
    }
}
```

- Return a `PoolChoice` of the index in `pools` of the pool to offer this attempt to, or a `NoPool` when no pool may take it. A `NoPool` fails the request with `Routing`.
- Give a `NoPool` a short reason with `NoPool::because` when the error should say why, such as the model that no pool serves. The error then reads ``pool policy `by_model` of class `model` chose no pool for segment `d`: no pool serves model `mistral` ``. `NoPool::new()` gives no reason, and the error says that no ready pool has an instance of the segment.
- A reason is a `Cow<'static, str>`. A fixed `&'static str` allocates nothing. A reason that you `format!` allocates only when no pool is chosen. The reason reaches the error only on the first attempt. After a decline, the request fails with the last pool's refusal, unless the chooser holds every pool that declined it gone and the policy chooses no other pool: then it fails with `Routing` and the reason.
- Skip a pool that `request.is_excluded` names: it declined this request, or it declined an offer as gone and its follow has not changed since (`request.damped().is_gone(pool)`).
- `request` also gives the chooser's region, the clock, the damped pools (`request.damped()`), the attempt's turn, and a random draw (`request.draw()`).
- `choose` is not `async`, and it runs once per attempt. It runs once more when every pool that declined the request is gone, to learn whether it chooses another. That choice is dropped and offered nothing. Take no lock in it, and allocate nothing when it chooses a pool.
- Override `eligible` when a pool can stop fitting while a selector waits, as `home_first` does for a stale summary. By default, a pool is eligible while it takes work.

### Choose as gdl's own policies do, among the pools you keep

A policy often filters the pools first, then wants to choose among the rest by load and region. `HomeFirst::choose_where` and `RoundRobin::choose_where` run gdl's own policies over only the pools a filter keeps. They return an index into all of `pools`, and allocate nothing. `ByModel` written this way chooses as `home_first` does among its model's pools:

```rust
use gdl::HomeFirst;
use tokio::time::Instant;

impl PoolPolicy for ByModel {
    type Head = Projected<Hinted>;

    fn choose(&self, request: PoolRequest<'_, Projected<Hinted>>, pools: &[Pool]) -> Result<PoolChoice, NoPool> {
        let model = request.head().hint.model.as_str();
        let at = HomeFirst
            .choose_where(request, pools, |pool| pool.name().starts_with(model))
            .ok_or_else(NoPool::new)?;
        Ok(PoolChoice::new(at))
    }

    fn eligible(&self, pool: &Pool, region: &str, now: Instant) -> bool {
        HomeFirst.eligible(pool, region, now)
    }
}
```

- The choice is the one the policy would make over a copy of the kept pools. The tiers, the turns, and the damping cap count only the kept pools, so a lone kept pool is never damped out.
- The filter must answer for a pool from that pool alone: the same each time, whatever other pools it was called for, and in any order. It is called more than once for a pool within one choice, and `home_first` calls it for every pool, eligible or not.
- A policy that chooses as `home_first` does should also override `eligible` as above. The default does not check that a pool's summary is fresh.
- Both take a request of any `Head`, since neither reads the head.
- `HomeFirst` chooses at the default thresholds, not at the chooser's `[runtime] home_first_*` settings. `HomeFirstAt { saturated_percent, spill_percent }` chooses alike at thresholds of its own. Whether a summary is fresh (`Load::is_fresh`) follows the chooser's `pool_fresh_periods` either way.

### Learn which pool took the request

A choice is only a proposal: the pool may decline the offer, and then the runtime chooses again. A policy that must know which pool took the request, to record it for the next request of a session for example, gives its choice a lease:

```rust
use gdl::ChoiceLease;

/// Records the pool that took a session's request.
struct Record {
    store: Arc<Store>,
    fingerprint: [u8; 16],
    pool: String,
}

impl ChoiceLease for Record {
    fn accepted(&mut self) {
        self.store.insert(self.fingerprint, &self.pool);
    }
}

// In `choose`:
Ok(PoolChoice::new(at).with_lease(Record { store, fingerprint, pool: pools[at].name().into() }))
```

- The runtime calls `accepted` once if the chosen pool accepts the offer. Then it holds the lease while the sending segment holds the request, as it holds a selector's lease, and drops it.
- It drops the lease without calling `accepted` when the pool declines the offer, when the offer never left, and when the outcome is unknown. An unknown outcome means no answer came, so the pool may have run the request, but it did not say that it took it. A request that no pool accepts tells none of its leases.
- Each attempt has its own lease. After a decline, the first attempt's lease is dropped before the next choice is made.
- A choice without a lease allocates nothing. A lease allocates twice, so give one only to the choices that need it. A router that records a session's pool needs one on a first turn, on a turn that finds no record, and on a failover (`request.excluded()` is not empty). It does not need one on a turn that goes to its recorded pool.

## 3. Test it without a runtime

Every input is a plain value that a test can build:

```rust
use gdl::{PoolState, Cx, Load, LoadSummary};
use tokio::time::Instant;

let now = Instant::now();
// A summary with a period, so that `home_first` takes it as fresh.
let idle = Load::new(LoadSummary { limit: 10, period_ms: 10_000, ..LoadSummary::default() }, now);
let pools = [
    Pool::new("model", "llama-1", "us-east-1", PoolState::Ready).with_entries(1).with_load(idle),
    Pool::new("model", "qwen-1", "us-east-1", PoolState::Ready).with_entries(1).with_load(idle),
];
let cx = Cx::new();
let head = Projected(Hinted { hint: Hint { model: "qwen".into(), fingerprint: [0; 16], first_turn: true } });
let request = PoolRequest::new(&cx, &head, "us-east-1", now);
assert_eq!(ByModel.choose(request, &pools).map(|choice| choice.at()), Ok(1));
```

Both versions of `ByModel` choose `qwen-1` here. The `home_first` version takes no pool whose summary is missing, or has no period (`LoadSummary::default()` has none). `with_excluded`, `with_damped` (a `gdl::Damped`), and `with_turn` set the rest. `crates/gdl/tests/policies.rs` has more.

## 4. Bind it in the spec

For a generated binary, implement `gdl::Build` too: the segment builds the policy from its settings, as it builds a node.

```rust
use gdl::Build;

#[derive(serde::Deserialize)]
struct ByModelSettings {}

impl Build for ByModel {
    type Config = ByModelSettings;
    type Metrics = ();

    async fn build(_: ByModelSettings, (): ()) -> gdl::Result<Self> {
        Ok(ByModel)
    }
}
```

Then name its crate and type in the spec, under the name the placement uses:

```toml
[pool_policy.by_model]
crate = "router"
path = "router"
type = "router::ByModel"
```

- `cargo gdl check` reads the crate and refuses a type that the crate does not offer as a pool policy. Write the type from the crate's root, as `router::ByModel`, and make it reachable there.
- `cargo gdl check` compares the policy's `Head` with the request that crosses into each class that names it, as the chooser does when it starts. A message must be the edge's message, and a projection's `Of` must be too: the same type from the same package, not one of the same name. `()` fits only a request of `()`, and `Encoded` fits any. A projection on an edge encoded as `msgpack_array` is refused. A spec with a `[contract]` crate is not checked this way.
- Each segment that chooses pools of a class named `by_model` in `[pools.<class>] policy` depends on the crate. No other segment does.
- That segment builds the policy from the `[pool_policy.by_model]` table of its settings file, or `GDL_POOL_POLICY__BY_MODEL__<KEY>` variables. A table for a policy that the segment does not register stops it at start.
- Under a hub, the instance registers first, then waits for the policy's `ready`, with its nodes' and selectors', before it announces itself as loaded.
- The spec can name the crate by `version`, or by `git` with `rev` (and `lib` when the library has another name), in place of `path`. `cargo gdl compose` needs `--local` only for a crate named by `path`, and `cargo gdl export` writes an archive that builds anywhere only when no crate is named by `path`.

## 5. Or register it in code

In an in-process deployment, add it to the `RuntimeConfig` with `config.pool_policies.add("by_model", ByModel)?`. A chooser launched through a `ProcessConfig` checks the placement's names against that config's `runtime.pool_policies`, so register it there too. In a hand-written `SegmentBinary`, call `builder.pool_policy("by_model", ByModel)?` in `build`, or `builder.build_pool_policy::<ByModel>("by_model", table).await?` to build it from a settings table as a generated segment does, with its `ready` waited for after the instance registers.

A name is lowercase letters and digits in words joined by single `_`s, starts with a letter, and has at most 64 characters. It cannot be `home_first` or `round_robin`, and it can be registered once.

## 6. Name it in the placement

```toml
[pools.model]
policy = "by_model"
```

The placement hash covers the name. A chooser refuses to launch when nothing registers the name and gdl does not have it, and names the policy and the class.
