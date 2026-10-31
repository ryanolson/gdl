//! What a chooser holds of each pool it follows, read from its tables when
//! the registry is scraped: the pool's load summary and its age, its
//! state, and whether it takes work. An offer does nothing for these; the
//! scrape loads each entry segment's table, as a choice does, and takes no
//! lock the follower or a request takes.
//!
//! A pool's series show what the tables hold now, so they go with its
//! follow, unlike the counts of its offers, which stay for the life of
//! the process: a pool the chooser no longer follows has no state to show.

use std::collections::BTreeMap;

use gdl::{Error, ErrorKind, Result};
use gdl_pools::{LoadSummary, PoolState};
use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};
use tokio::time::Instant;

use super::super::Pools;
use super::{REGION, label};

/// A family of the load summary: its name, its help, and how it reads the
/// summary.
type LoadFamily = (&'static str, &'static str, fn(&LoadSummary) -> f64);

/// The families of the load summary the chooser holds for a pool.
const LOADS: [LoadFamily; 4] = [
    (
        "pool_load_admitted",
        "Requests the pool's entry instances held admitted, in the last load summary this chooser has of the pool.",
        |load| load.admitted as f64,
    ),
    (
        "pool_load_limit",
        "The admission limit over the same entry instances, in the last load summary this chooser has of the pool.",
        |load| load.limit as f64,
    ),
    (
        "pool_load_refusals",
        "Hand-offs the pool's entry instances declined in the summary's period, in the last load summary this chooser has of the pool.",
        |load| load.refusals as f64,
    ),
    (
        "pool_load_instances",
        "The pool's usable entry instances, in the last load summary this chooser has of the pool.",
        |load| f64::from(load.instances),
    ),
];
/// The family of the age of the summary, and its help.
pub(crate) const LOAD_AGE: &str = "pool_load_age_seconds";
const LOAD_AGE_HELP: &str =
    "Seconds since the pool's hub made the last load summary this chooser has of it.";
/// The family of the pool's state, one series per state, and its help.
pub(crate) const STATE: &str = "pool_state";
const STATE_HELP: &str =
    "1 for the pool's state as its hub last sent it to this chooser, 0 for the others.";
/// The family of whether the pool takes work, and its help.
pub(crate) const TAKES_WORK: &str = "pool_takes_work";
const TAKES_WORK_HELP: &str = "1 while a pool policy of this chooser sees the pool take work: ready or degraded, with an entry instance to route to.";

/// Every state, with its `state` label: the names its hub sends.
const STATES: [(PoolState, &str); 4] = [
    (PoolState::Ready, "ready"),
    (PoolState::Degraded, "degraded"),
    (PoolState::Unavailable, "unavailable"),
    (PoolState::Draining, "draining"),
];

/// The names of every family here.
#[cfg(test)]
pub(crate) fn names() -> Vec<&'static str> {
    let mut names: Vec<_> = LOADS.iter().map(|(name, ..)| *name).collect();
    names.extend([LOAD_AGE, STATE, TAKES_WORK]);
    names
}

/// The chooser's gauges of the pools it follows, read from its tables.
pub(crate) struct PoolGauges {
    pools: Pools,
    descs: Vec<Desc>,
}

impl PoolGauges {
    /// Register the gauges of the pools `pools` holds in `registry`.
    pub(crate) fn register(registry: &Registry, pools: Pools) -> Result<()> {
        let pool_labels = || ["class", "pool", REGION].map(str::to_string).to_vec();
        let desc = |name: &str, help: &str, labels: Vec<String>| {
            Desc::new(
                name.to_string(),
                help.to_string(),
                labels,
                Default::default(),
            )
            .map_err(failed)
        };
        let mut descs = Vec::with_capacity(LOADS.len() + 3);
        for (name, help, _) in LOADS {
            descs.push(desc(name, help, pool_labels())?);
        }
        descs.push(desc(LOAD_AGE, LOAD_AGE_HELP, pool_labels())?);
        let mut labels = pool_labels();
        labels.push("state".to_string());
        descs.push(desc(STATE, STATE_HELP, labels)?);
        descs.push(desc(TAKES_WORK, TAKES_WORK_HELP, pool_labels())?);
        registry
            .register(Box::new(Self { pools, descs }))
            .map_err(failed)
    }

    /// What each followed pool shows, by class and pool, over every entry
    /// segment's table. The tables of a pool's entry segments share its
    /// follow, so its region, state and load are the same in each; it
    /// takes work if a pool policy sees it take work in any of them.
    fn held(&self) -> BTreeMap<(String, String), Held> {
        let mut held: BTreeMap<(String, String), Held> = BTreeMap::new();
        for slot in self.pools.shared.segments.values() {
            let table = slot.load();
            for (pool, view) in table.pools.iter().zip(&table.views) {
                let shown = held
                    .entry((pool.class.clone(), pool.pool.clone()))
                    .or_insert_with(|| Held {
                        region: pool.region.clone(),
                        state: pool.state,
                        load: pool.load.map(|load| (*load.summary(), load.arrived())),
                        takes_work: false,
                    });
                shown.takes_work |= view.takes_work();
            }
        }
        held
    }
}

/// One followed pool as its tables show it.
struct Held {
    region: String,
    state: PoolState,
    /// The load summary and when it was made, on the chooser's clock.
    load: Option<(LoadSummary, Instant)>,
    takes_work: bool,
}

fn failed(error: prometheus::Error) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("registering the chooser's pool gauges: {error}"),
    )
}

fn gauge(labels: Vec<proto::LabelPair>, value: f64) -> proto::Metric {
    let mut metric = proto::Metric::from_label(labels);
    let mut gauge = proto::Gauge::default();
    gauge.set_value(value);
    metric.set_gauge(gauge);
    metric
}

impl Collector for PoolGauges {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let held = self.held();
        let now = Instant::now();
        let labels = |(class, pool): &(String, String), shown: &Held| {
            vec![
                label("class", class),
                label("pool", pool),
                label(REGION, &shown.region),
            ]
        };
        let mut loads: Vec<Vec<proto::Metric>> = vec![Vec::new(); LOADS.len()];
        let mut ages = Vec::new();
        let mut states = Vec::with_capacity(held.len() * STATES.len());
        let mut takes_work = Vec::with_capacity(held.len());
        for (key, shown) in &held {
            // A pool with no summary yet has no load to show: zeros would
            // read as an idle pool.
            if let Some((load, made)) = &shown.load {
                for ((_, _, read), series) in LOADS.iter().zip(&mut loads) {
                    series.push(gauge(labels(key, shown), read(load)));
                }
                let age = now.saturating_duration_since(*made).as_secs_f64();
                ages.push(gauge(labels(key, shown), age));
            }
            for (state, name) in STATES {
                let mut labels = labels(key, shown);
                labels.push(label("state", name));
                states.push(gauge(labels, f64::from(u8::from(shown.state == state))));
            }
            takes_work.push(gauge(
                labels(key, shown),
                f64::from(u8::from(shown.takes_work)),
            ));
        }
        let family = |name: &str, help: &str, metrics: Vec<proto::Metric>| {
            super::family(name, help, MetricType::GAUGE, metrics)
        };
        let mut families: Vec<MetricFamily> = LOADS
            .iter()
            .zip(loads)
            .map(|((name, help, _), series)| family(name, help, series))
            .collect();
        families.push(family(LOAD_AGE, LOAD_AGE_HELP, ages));
        families.push(family(STATE, STATE_HELP, states));
        families.push(family(TAKES_WORK, TAKES_WORK_HELP, takes_work));
        families
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use velo::InstanceId;

    use super::super::super::damping::Damping;
    use super::super::super::lease::Leases;
    use super::super::super::{EntryTable, PoolTable, pool_id};
    use super::*;

    /// `pool` of `model_x` in `region`, in `state`, with `load`, routing to
    /// `eligible`.
    fn pool(
        pool: &str,
        region: &str,
        state: PoolState,
        load: Option<gdl::Load>,
        eligible: &[InstanceId],
    ) -> Arc<PoolTable> {
        Arc::new(PoolTable {
            class: "model_x".into(),
            pool: pool.into(),
            id: pool_id("model_x", pool),
            generation: 1,
            epoch: 0,
            region: region.into(),
            state,
            load,
            eligible: eligible.into(),
            offered: eligible
                .iter()
                .map(|id| crate::dist::roster::selector_id(*id))
                .collect(),
            retired: Box::new([]),
            unread: false,
            turn: Arc::new(AtomicUsize::new(0)),
            offering: true,
            revision: 1,
            changes: 0,
            leases: Arc::new(Leases::counted(None)),
            properties_version: 0,
        })
    }

    /// The value of `family`'s series for `pool`, with `labels` too.
    fn value(
        registry: &Registry,
        family: &str,
        pool: &str,
        labels: &[(&str, &str)],
    ) -> Option<f64> {
        let mut all = vec![("class", "model_x"), ("pool", pool)];
        all.extend_from_slice(labels);
        registry
            .gather()
            .into_iter()
            .filter(|gathered| gathered.name() == family)
            .flat_map(|gathered| gathered.get_metric().to_vec())
            .find(|metric| {
                all.iter().all(|(name, value)| {
                    metric
                        .get_label()
                        .iter()
                        .any(|pair| pair.name() == *name && pair.value() == *value)
                })
            })
            .map(|metric| metric.get_gauge().value())
    }

    /// Each pool shows its region, state, and the load summary it holds
    /// with its age; a pool with no summary shows no load. A pool in the
    /// tables of two entry segments shows once, and takes work when it
    /// does in either. The series follow the tables: a pool that leaves
    /// them leaves the scrape.
    #[test]
    fn each_pool_shows_what_its_tables_hold() {
        let pools = Pools::new(
            "home",
            [("model_x", "d"), ("model_x", "e")],
            Damping::default(),
            gdl::PropertyKinds::new(),
        );
        let registry = Registry::new();
        PoolGauges::register(&registry, pools.clone()).expect("registered");
        let load = gdl::Load::new(
            LoadSummary {
                admitted: 3,
                limit: 8,
                instances: 2,
                refusals: 5,
                period_ms: 1_000,
            },
            Instant::now() - Duration::from_secs(2),
        );
        let x = InstanceId::new_v4();
        pools.store(
            "d",
            EntryTable::new(Box::new([
                pool("m1", "east", PoolState::Ready, Some(load), &[]),
                pool("m2", "west", PoolState::Draining, None, &[]),
            ])),
        );
        pools.store(
            "e",
            EntryTable::new(Box::new([
                pool("m1", "east", PoolState::Ready, Some(load), &[x]),
                pool("m2", "west", PoolState::Draining, None, &[]),
            ])),
        );

        let east = [(REGION, "east")];
        assert_eq!(
            value(&registry, "pool_load_admitted", "m1", &east),
            Some(3.0)
        );
        assert_eq!(value(&registry, "pool_load_limit", "m1", &east), Some(8.0));
        assert_eq!(
            value(&registry, "pool_load_instances", "m1", &east),
            Some(2.0)
        );
        assert_eq!(
            value(&registry, "pool_load_refusals", "m1", &east),
            Some(5.0)
        );
        let age = value(&registry, LOAD_AGE, "m1", &east).expect("an age");
        assert!((2.0..3.0).contains(&age), "{age}");
        assert_eq!(value(&registry, TAKES_WORK, "m1", &east), Some(1.0));
        let west = [(REGION, "west")];
        for (name, ..) in LOADS {
            assert_eq!(value(&registry, name, "m2", &[]), None, "{name}");
        }
        assert_eq!(value(&registry, LOAD_AGE, "m2", &[]), None);
        assert_eq!(value(&registry, TAKES_WORK, "m2", &west), Some(0.0));
        for (pool, region, state) in [("m1", "east", "ready"), ("m2", "west", "draining")] {
            for (_, other) in STATES {
                let labels = [(REGION, region), ("state", other)];
                let shown = value(&registry, STATE, pool, &labels);
                assert_eq!(
                    shown,
                    Some(f64::from(u8::from(other == state))),
                    "{pool} {other}"
                );
            }
        }
        // One series per pool, though two tables hold each.
        let states = registry
            .gather()
            .into_iter()
            .find(|gathered| gathered.name() == STATE)
            .expect("the family");
        assert_eq!(states.get_metric().len(), 2 * STATES.len());

        pools.store("d", EntryTable::new(Box::new([])));
        pools.store("e", EntryTable::new(Box::new([])));
        assert_eq!(value(&registry, STATE, "m1", &[]), None);
    }
}
