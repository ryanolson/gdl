//! A pool's load reaches the choosers that may offer to it: each entry
//! instance of a model pool reports its admission to its own hub once a
//! summary period, the hub sums the reports into the pool's summary,
//! which its record and its pool watch carry, the global hub relays it,
//! and a chooser's table holds it with its age. Every hub and instance
//! runs in this process on OS-assigned ports, sharing one directory, with
//! the shortest summary period, 1 s; every wait is for something
//! observed.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::boundary::{
    Model, ModelPool, RUNTIME, Running, SUMMARY, ask, model_proxied, model_summarized, placement,
    run_graph, spec_of,
};
use common::pools::{Resolving, followed, global_hub, global_pool, pool_hub_of, settings, until};
use common::proxy::{Mode, Proxy};
use common::{Chunk, Events, Plan, Prompt, WAIT, prompt, reference_graph};
use futures::StreamExt;
use gdl::{Cx, ErrorKind, Next};
use gdl_hub::PoolWatch;
use gdl_pools::{LoadSummary, PoolDirectory, PoolEvent};
use gdl_runtime::{DeploymentSpec, PoolState, Pools, Register, RuntimeConfig};
use gdl_watch::{FollowEvent, PoolWatchEvent};
use velo_hub::Hub;

/// The test property type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct Shard {
    name: String,
}

const SHARD: gdl::PropertyKind<Shard> = gdl::PropertyKind::new("test");

fn shard(name: &str) -> Shard {
    Shard { name: name.into() }
}

const M1: &str = "model-x-01-us-east-1";

/// Each entry instance admits this many roots at once.
const LIMIT: usize = 4;

fn limited(runtime: RuntimeConfig) -> RuntimeConfig {
    RuntimeConfig {
        admission_limit: LIMIT,
        ..runtime
    }
}

fn summary(admitted: u64, entries: u32, refusals: u64) -> Option<LoadSummary> {
    Some(LoadSummary {
        admitted,
        limit: LIMIT as u64 * u64::from(entries),
        instances: entries,
        refusals,
        period_ms: u64::try_from(SUMMARY.as_millis()).expect("a short period"),
    })
}

/// The global pool's hub and a chooser in it.
async fn chooser(
    spec: &DeploymentSpec,
    directory: &Resolving,
) -> (
    Hub,
    Arc<gdl_hub::PoolReadiness<Resolving>>,
    Running<impl Next<Prompt, Chunk> + Register + use<>>,
) {
    let (hub, readiness) = pool_hub_of(spec, global_hub(), directory).await;
    let public = hub.public_addr().port();
    let chooser = chooser_of(spec, &hub, public).await;
    (hub, readiness, chooser)
}

/// [`chooser`], which reaches its hub's public listener through a proxy,
/// also returned.
async fn chooser_proxied(
    spec: &DeploymentSpec,
    directory: &Resolving,
) -> (
    Hub,
    Arc<gdl_hub::PoolReadiness<Resolving>>,
    Running<impl Next<Prompt, Chunk> + Register + use<>>,
    Proxy,
) {
    let (hub, readiness) = pool_hub_of(spec, global_hub(), directory).await;
    let public = Proxy::new(hub.public_addr()).await;
    let chooser = chooser_of(spec, &hub, public.port).await;
    (hub, readiness, chooser, public)
}

/// A chooser in the global pool of `hub`, which it reaches at the port
/// `public` for the hub's public listener.
async fn chooser_of(
    spec: &DeploymentSpec,
    hub: &Hub,
    public: u16,
) -> Running<impl Next<Prompt, Chunk> + Register + use<>> {
    let extra = format!("{}{RUNTIME}", global_pool());
    let config = settings(public, hub.control_addr().port(), 5_000, &extra);
    let quiet = Events::quiet();
    run_graph(&config, "a", spec, config.runtime.clone(), &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves")
}

/// The model pool `M1`, ready with `entries` entry instances, each
/// admitting [`LIMIT`] roots, with `runtime` as its instances' table.
async fn model(
    spec: &DeploymentSpec,
    directory: &Resolving,
    entries: usize,
    runtime: &str,
) -> ModelPool<impl Next<Prompt, Chunk> + Register + use<>> {
    let pool = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: u32::try_from(entries).expect("a few"),
        entries,
    };
    let quiet = Events::quiet();
    model_summarized(spec, directory, pool, runtime, &limited, &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
}

fn tables<H>(chooser: &Running<H>) -> Pools {
    chooser.pools.clone().expect("a global instance chooses")
}

/// A request through the chooser whose sink holds its stream open after
/// one item: it holds its entry's admission until the stream is dropped.
async fn hold<H: Next<Prompt, Chunk>>(chooser: &Running<H>, text: &str) -> gdl::Items<Chunk> {
    let plan = Plan {
        tokens: 1,
        hold_at_end: true,
        ..Plan::default()
    };
    let mut out = tokio::time::timeout(WAIT, chooser.segment.call(Cx::new(), prompt(text, plan)))
        .await
        .expect("handed on in time")
        .expect("handed on");
    let first = tokio::time::timeout(WAIT, out.next())
        .await
        .expect("an item in time");
    assert!(matches!(first, Some(Ok(_))), "{first:?}");
    out
}

/// A watch of the pool, past its snapshot.
async fn pool_watch<H>(pool: &ModelPool<H>) -> PoolWatch {
    let mut watch = tokio::time::timeout(WAIT, pool.readiness.watch())
        .await
        .expect("the watch begins in time")
        .expect("the hub runs");
    tokio::time::timeout(WAIT, async {
        while watch.next().await.expect("the watch goes on") != PoolWatchEvent::Synced {}
    })
    .await
    .expect("the snapshot in time");
    watch
}

/// Read `watch` until it sends the load `load`.
async fn load(watch: &mut PoolWatch, load: Option<LoadSummary>) {
    let mut seen = Vec::new();
    let waited = tokio::time::timeout(WAIT, async {
        loop {
            if let PoolWatchEvent::Load { load: sent } =
                watch.next().await.expect("the watch goes on")
            {
                if sent == load {
                    return;
                }
                seen.push(sent);
            }
        }
    })
    .await;
    assert!(waited.is_ok(), "no load of {load:?} in time: {seen:?}");
}

/// Wait until the chooser's table of `M1` holds `load`.
async fn table_holds(pools: &Pools, load: Option<LoadSummary>) {
    until(pools, "the summary in the chooser's table", |pools| {
        followed(pools, "f", M1).is_some_and(|m| m.load == load)
    })
    .await;
    let age = followed(pools, "f", M1).expect("followed").load_age;
    assert!(
        age.is_some_and(|age| age < WAIT),
        "the summary's age: {age:?}"
    );
}

async fn shut(hubs: impl IntoIterator<Item = Hub>) {
    for hub in hubs {
        hub.shutdown().await.expect("valid test fixture");
    }
}

/// Two entry instances each hold a request: the pool's summary sums their
/// admitted roots and their limits, and counts them, on the pool watch,
/// on the pool's record, on the global hub's relay, and in the chooser's
/// table. As each request finishes, the summary falls, everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_entry_instances_load_reaches_the_choosers_table() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (global_hub, global, chooser) = chooser(&spec, &directory).await;
    let mut relay = global.follows();
    let m1 = model(&spec, &directory, 2, RUNTIME).await;
    let pools = tables(&chooser);
    until(&pools, "M1 followed, with a summary", |pools| {
        followed(pools, "f", M1).is_some_and(|m| {
            m.state == PoolState::Ready && m.eligible.len() == 2 && m.load.is_some()
        })
    })
    .await;
    let mut watch = pool_watch(&m1).await;
    let mut records = directory.watch("model_x", None);

    let held = [hold(&chooser, "one").await, hold(&chooser, "two").await];
    let admitted: Vec<usize> = m1.entries.iter().map(|e| e.segment.admitted()).collect();
    assert_eq!(admitted, [1, 1], "one request held at each entry");
    let busy = summary(2, 2, 0);
    load(&mut watch, busy).await;
    let recorded = tokio::time::timeout(WAIT, async {
        loop {
            match records.next().await {
                Some(Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. }))
                    if record.name == M1 && record.load == busy =>
                {
                    return;
                }
                Some(Ok(_)) => {}
                other => panic!("the directory's watch ended: {other:?}"),
            }
        }
    })
    .await;
    assert!(recorded.is_ok(), "no record carried {busy:?} in time");
    let relayed = tokio::time::timeout(WAIT, async {
        loop {
            match relay.next().await.expect("the relay goes on") {
                FollowEvent::Follow { pool, load, .. } | FollowEvent::Load { pool, load, .. }
                    if pool == M1 && load == busy =>
                {
                    return;
                }
                _ => {}
            }
        }
    })
    .await;
    assert!(
        relayed.is_ok(),
        "the global hub did not relay {busy:?} in time"
    );
    table_holds(&pools, busy).await;

    let [first, second] = held;
    drop(first);
    let one = summary(1, 2, 0);
    load(&mut watch, one).await;
    table_holds(&pools, one).await;
    drop(second);
    let idle = summary(0, 2, 0);
    load(&mut watch, idle).await;
    table_holds(&pools, idle).await;
    drop(chooser);
    shut([global_hub, m1.hub]).await;
}

/// An entry instance that is stopped retires, and stops counting at once,
/// the request it still holds included, while it drains; the other goes
/// on counting. Its drain is long enough to see it hold the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retiring_entry_instance_stops_counting_at_once() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (global_hub, _, chooser) = chooser(&spec, &directory).await;
    let draining = "[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 10000\n";
    let mut m1 = model(&spec, &directory, 2, draining).await;
    let pools = tables(&chooser);
    until(&pools, "M1 followed, with a summary", |pools| {
        followed(pools, "f", M1).is_some_and(|m| m.eligible.len() == 2 && m.load.is_some())
    })
    .await;
    let mut watch = pool_watch(&m1).await;
    let held = [hold(&chooser, "one").await, hold(&chooser, "two").await];
    load(&mut watch, summary(2, 2, 0)).await;

    let retiring = m1.entries.remove(0);
    retiring.process.stop();
    load(&mut watch, summary(1, 1, 0)).await;
    assert_eq!(
        retiring.segment.admitted(),
        1,
        "the retiring instance's request finished before the summary left it out"
    );
    drop(held);
    retiring.stop().await;
    drop(chooser);
    shut([global_hub, m1.hub]).await;
}

/// An entry instance with no room declines an offer, and reports it: the
/// summary counts the refusal in the period it falls in, and not in the
/// next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_instances_refusals_are_in_the_summary() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (global_hub, _, chooser) = chooser(&spec, &directory).await;
    let m1 = model(&spec, &directory, 1, RUNTIME).await;
    let pools = tables(&chooser);
    until(&pools, "M1 followed, with a summary", |pools| {
        followed(pools, "f", M1).is_some_and(|m| m.eligible.len() == 1 && m.load.is_some())
    })
    .await;
    let mut watch = pool_watch(&m1).await;
    let mut held = Vec::new();
    for at in 0..LIMIT {
        held.push(hold(&chooser, &format!("held {at}")).await);
    }
    let refused = ask(&chooser.segment).await.expect_err("no room");
    assert_eq!(refused.kind(), ErrorKind::Overloaded, "{refused}");
    load(&mut watch, summary(LIMIT as u64, 1, 1)).await;
    load(&mut watch, summary(LIMIT as u64, 1, 0)).await;
    drop(held);
    drop(chooser);
    shut([global_hub, m1.hub]).await;
}

/// A summary keeps its age when the chooser watches the relay again. The
/// global hub's link to M1 falls silent without closing, so the relay
/// holds M1's last summary, in the same generation, and no new one comes.
/// Four seconds later the summary, of a 1 s period, is stale in the
/// chooser's table; the chooser's relay watch is cut, and the snapshot of
/// its next watch shows the same summary of the same generation, which is
/// still as old, and stale. The four seconds are the claim: a summary is
/// stale after three of its periods.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_summary_keeps_its_age_when_the_chooser_watches_the_relay_again() {
    const SILENT: Duration = Duration::from_secs(4);
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let (global_hub, global, chooser, public) = chooser_proxied(&spec, &directory).await;
    let pool = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries: 1,
    };
    let quiet = Events::quiet();
    let (m1, link) = model_proxied(&spec, &directory, pool, RUNTIME, &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await;
    let pools = tables(&chooser);
    until(&pools, "M1 followed, with a summary", |pools| {
        followed(pools, "f", M1).is_some_and(|m| m.eligible.len() == 1 && m.load.is_some())
    })
    .await;
    let mut relay_watchers = global.follows_watchers();
    assert_eq!(*relay_watchers.borrow_and_update(), 1);

    link.set(Mode::Silent);
    let generation = followed(&pools, "f", M1).expect("followed").generation;
    tokio::time::sleep(SILENT).await;
    let before = followed(&pools, "f", M1).expect("followed");
    assert_eq!(before.generation, generation, "the follow goes on");
    let stale = |age: Option<Duration>| age.is_some_and(|age| age >= SILENT);
    assert!(
        stale(before.load_age),
        "stale before the watch is cut: {:?}",
        before.load_age
    );

    let mut changes = pools.changes();
    changes.borrow_and_update();
    public.set(Mode::Refuse);
    tokio::time::timeout(WAIT, relay_watchers.wait_for(|watchers| *watchers == 0))
        .await
        .expect("the chooser's relay watch is cut in time")
        .expect("the hub lives");
    public.set(Mode::Forward);
    tokio::time::timeout(WAIT, relay_watchers.wait_for(|watchers| *watchers == 1))
        .await
        .expect("the chooser watches the relay again in time")
        .expect("the hub lives");
    tokio::time::timeout(WAIT, changes.changed())
        .await
        .expect("the new watch's snapshot replaces the tables in time")
        .expect("the tables live");
    let after = followed(&pools, "f", M1).expect("followed");
    assert_eq!(after.generation, generation, "the same follow, shown again");
    assert_eq!(after.load, before.load, "no new summary");
    assert!(
        stale(after.load_age),
        "stale after the chooser watched the relay again: {:?}",
        after.load_age
    );
    link.set(Mode::Forward);
    drop(chooser);
    shut([global_hub, m1.hub]).await;
}

/// A kind a node registers in its build reaches the chooser of its
/// segment: the node reads an entry instance's property from the class's
/// index, decoded to the kind's type, in the same read as the pool, once
/// the property is `ready` in the pool's record. A kind registered with
/// two types stops the chooser from starting, naming the kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kind_a_node_registers_reaches_the_choosers_index() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let properties = gdl::InstanceProperties::new();
    properties
        .set(&SHARD, [(shard("a"), gdl::EntryState::Ready)])
        .expect("a list");
    let quiet = Events::quiet();
    let given = properties.clone();
    let reporting = move |runtime: RuntimeConfig| RuntimeConfig {
        properties: given.clone(),
        ..limited(runtime)
    };
    let pool = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries: 1,
    };
    let m1 = model_summarized(
        &spec,
        &directory,
        pool,
        RUNTIME,
        &reporting,
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let (hub, _readiness) = pool_hub_of(&spec, global_hub(), &directory).await;
    let extra = format!("{}{RUNTIME}", global_pool());
    let mut config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    let followed = gdl::FollowedPools::new();
    followed
        .scope(async { gdl::FollowedPools::current().register(&SHARD) })
        .await
        .expect("registered");
    config.runtime.followed = followed.clone();
    let chooser = run_graph(&config, "a", &spec, config.runtime.clone(), &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    .expect("the chooser serves");
    let read = || {
        followed
            .read("model_x", |pools| {
                let index = pools.properties().get(&SHARD)?;
                let listed = index.get(&shard("a")).first()?;
                Some((
                    listed.1.pool().to_string(),
                    listed.1.state(),
                    pools.pool(listed.1.pool()).is_some(),
                ))
            })
            .flatten()
    };
    until(
        &tables(&chooser),
        "the property in the chooser's index",
        |_| read().is_some_and(|(_, state, _)| state == gdl::PropertyState::Ready),
    )
    .await;
    assert_eq!(
        read(),
        Some((M1.to_string(), gdl::PropertyState::Ready, true)),
        "listed by the followed pool"
    );

    let conflicting = gdl::FollowedPools::new();
    conflicting.register(&SHARD).expect("registered");
    let mut config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &extra,
    );
    config.runtime.followed = conflicting;
    struct Other;
    impl gdl::PoolPolicy for Other {
        type Head = gdl::Encoded;
        fn choose(
            &self,
            _: gdl::PoolRequest<'_, gdl::Encoded>,
            _: &[gdl::Pool],
        ) -> Result<gdl::PoolChoice, gdl::NoPool> {
            Err(gdl::NoPool::new())
        }
        fn properties(&self, kinds: &gdl::PropertyKinds) -> gdl::Result<()> {
            kinds.register(&gdl::PropertyKind::<String>::new("test"))
        }
    }
    config
        .runtime
        .pool_policies
        .add("other", Other)
        .expect("registered");
    let refused = match run_graph(&config, "a", &spec, config.runtime.clone(), &|deployment| {
        reference_graph(deployment, &quiet)
    })
    .await
    {
        Ok(_) => panic!("a chooser whose kind has two types started"),
        Err(refused) => refused.to_string(),
    };
    assert!(
        refused.contains("property kind `test`") && refused.contains("`other`"),
        "{refused}"
    );
    drop(chooser);
    shut([hub, m1.hub]).await;
}

/// What an entry instance's nodes say of its properties reaches its pool's
/// record with its load reports: a property it can serve is `ready` there,
/// and one it says it is removing is `draining` as soon as it says so,
/// without waiting for the next period.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_entry_instances_properties_reach_the_pools_record() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let properties = gdl::InstanceProperties::new();
    let said = |state| (shard("llama-70b"), state);
    properties
        .set(&SHARD, [said(gdl::EntryState::Ready)])
        .expect("a list");
    let mut records = directory.watch("model_x", None);
    let pool = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries: 1,
    };
    let quiet = Events::quiet();
    let given = properties.clone();
    let reporting = move |runtime: RuntimeConfig| RuntimeConfig {
        properties: given.clone(),
        ..limited(runtime)
    };
    let m1 = model_summarized(
        &spec,
        &directory,
        pool,
        RUNTIME,
        &reporting,
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let mut until_state = async |state: gdl_pools::PropertyState| {
        let recorded = tokio::time::timeout(WAIT, async {
            loop {
                match records.next().await {
                    Some(Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. }))
                        if record.name == M1
                            && record.properties.iter().map(|m| m.state).eq([state]) =>
                    {
                        return;
                    }
                    Some(Ok(_)) => {}
                    other => panic!("the directory's watch ended: {other:?}"),
                }
            }
        })
        .await;
        assert!(
            recorded.is_ok(),
            "no record had the model {state:?} in time"
        );
    };
    until_state(gdl_pools::PropertyState::Ready).await;
    properties
        .set(&SHARD, [said(gdl::EntryState::Draining)])
        .expect("a list");
    let asked = tokio::time::Instant::now();
    until_state(gdl_pools::PropertyState::Draining).await;
    assert!(
        asked.elapsed() < SUMMARY,
        "the change waited for the next period"
    );
    shut([m1.hub]).await;
}

/// A node that drains a property and drops it before its instance reports
/// does not lose the drain: the instance reports the property `draining`,
/// then drops it, and the record shows it `draining`, then `drained`, not
/// `unavailable`.
#[tokio::test]
async fn a_property_removed_without_yielding_finishes_its_drain() {
    let spec = spec_of(placement());
    let directory = Resolving::new();
    let properties = gdl::InstanceProperties::new();
    let said = |state| (shard("m"), state);
    properties
        .set(&SHARD, [said(gdl::EntryState::Ready)])
        .expect("a list");
    let mut records = directory.watch("model_x", None);
    let pool = Model {
        name: M1,
        hub: "m1-hub:1337",
        quorum: 1,
        entries: 1,
    };
    let quiet = Events::quiet();
    let given = properties.clone();
    let reporting = move |runtime: RuntimeConfig| RuntimeConfig {
        properties: given.clone(),
        ..limited(runtime)
    };
    let m1 = model_summarized(
        &spec,
        &directory,
        pool,
        RUNTIME,
        &reporting,
        &|deployment| reference_graph(deployment, &quiet),
    )
    .await;
    let mut next_state = async |after: Option<gdl_pools::PropertyState>| {
        tokio::time::timeout(WAIT, async {
            loop {
                match records.next().await {
                    Some(Ok(PoolEvent::Current(record) | PoolEvent::Put { record, .. }))
                        if record.name == M1 && record.properties.len() == 1 =>
                    {
                        let state = record.properties[0].state;
                        if after != Some(state) {
                            return state;
                        }
                    }
                    Some(Ok(_)) => {}
                    other => panic!("the directory's watch ended: {other:?}"),
                }
            }
        })
        .await
        .expect("a changed model state in time")
    };
    assert_eq!(next_state(None).await, gdl_pools::PropertyState::Ready);

    // An idle property can finish unloading without yielding to the reporter.
    properties
        .set(&SHARD, [said(gdl::EntryState::Draining)])
        .expect("a list");
    properties.set(&SHARD, []).expect("an empty list");
    let draining = next_state(Some(gdl_pools::PropertyState::Ready)).await;
    let after = next_state(Some(draining)).await;
    shut([m1.hub]).await;
    assert_eq!(
        (draining, after),
        (
            gdl_pools::PropertyState::Draining,
            gdl_pools::PropertyState::Drained
        )
    );
}
