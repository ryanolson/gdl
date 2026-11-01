//! What a chooser records of its offers to pools: for each pool of each
//! class it sends to, how its offers ended, how long each took from its
//! send to its answer, how many of them were failovers, and how many
//! requests it re-queued for the pool. The counts are kept by the pool's
//! name, not by its follow, so that a new generation of the pool, and an
//! older one that still answers offers made under it, count into the same
//! series.
//!
//! Each pool's counts are sharded by thread, as the runtime's figures are
//! (`gdl::__runtime::Meters`), and summed when the registry is scraped. An
//! offer adds to its thread's shard with relaxed adds, and takes no lock
//! and allocates nothing: the counts are reached through the lease on the
//! pool's follow that the offer already holds, or, for a re-queue, through
//! the pool's current table.
//!
//! What the chooser holds of each pool now, its load summary and state, is
//! read from its tables at the scrape instead ([`gauges`]): an offer does
//! nothing for it.
//!
//! A pool's series stay once it is first followed, for the life of the
//! process, also after its follow is dropped: an answer to an offer made
//! before the drop still counts, and no scrape misses the last counts.
//! The pools a chooser's hub ever followed bound them.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use gdl::__runtime::Spans;
use gdl::{Error, ErrorKind, Result};
use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};

use gdl::Decline;

use crate::dist::offers::{Answer, Missed};

mod gauges;

pub(crate) use gauges::PoolGauges;

/// The most shards a pool's counts are split into.
const SHARD_LIMIT: usize = 64;

/// The family of the offers' outcomes, and its help.
pub(crate) const OFFERS: &str = "pool_offers_total";
const OFFERS_HELP: &str = "Offers this chooser made to the pool, by how each ended.";
/// The family of the offers made after another pool missed the request.
pub(crate) const FAILOVERS: &str = "pool_failovers_total";
const FAILOVERS_HELP: &str =
    "Offers this chooser made to the pool after another offer of the request missed.";
/// The family of the offers' round trips, and its help.
pub(crate) const ROUND_TRIPS: &str = "pool_offer_round_trip_seconds";
const ROUND_TRIPS_HELP: &str =
    "Time from the send of this chooser's offer to the pool to its answer, by how it ended.";
/// The family of the requests re-queued for the pool, and its help.
pub(crate) const REQUEUES: &str = "pool_requeues_total";
const REQUEUES_HELP: &str =
    "Requests this chooser re-queued for the pool after it declined them as full.";
/// The family of the re-queued requests that ended at their deadline.
pub(crate) const REQUEUE_DEADLINES: &str = "pool_requeue_deadlines_total";
const REQUEUE_DEADLINES_HELP: &str =
    "Requests this chooser re-queued for the pool that ended at their deadline while re-queued.";

/// How an offer to a pool ended, as the chooser read its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The pool's entry took the request.
    Accepted,
    /// The entry declined it, saying its pool is full.
    DeclinedFull,
    /// The entry declined it, saying its pool is gone.
    DeclinedGone,
    /// The entry declined it, saying that it cannot serve it and another
    /// entry instance of its pool may.
    DeclinedInstance,
    /// The entry declined it, saying that it does not have the property
    /// the request wants.
    DeclinedNotPresent,
    /// The entry declined it against its pool's budget for the view the
    /// offer carried.
    DeclinedBudget,
    /// velo refused to send it: no entry said anything.
    Unsent,
    /// The send failed after velo took it, or no answer came in time: the
    /// entry may have taken it.
    Unknown,
    /// The entry answered with the request's own error, or with an answer
    /// that could not be read.
    Failed,
}

impl Outcome {
    /// Every outcome, in the order of their counts in a shard.
    const ALL: [Outcome; 9] = [
        Outcome::Accepted,
        Outcome::DeclinedFull,
        Outcome::DeclinedGone,
        Outcome::DeclinedInstance,
        Outcome::DeclinedNotPresent,
        Outcome::DeclinedBudget,
        Outcome::Unsent,
        Outcome::Unknown,
        Outcome::Failed,
    ];

    /// The outcome of the answer `answer`.
    pub(crate) fn of(answer: &Answer) -> Self {
        match answer {
            Answer::Accepted(_) => Outcome::Accepted,
            Answer::Missed(Missed::Declined(_, Decline::Full)) => Outcome::DeclinedFull,
            Answer::Missed(Missed::Declined(_, Decline::Gone)) => Outcome::DeclinedGone,
            Answer::Missed(Missed::Declined(_, Decline::Instance)) => Outcome::DeclinedInstance,
            Answer::Missed(Missed::Declined(_, Decline::NotPresent)) => Outcome::DeclinedNotPresent,
            Answer::Missed(Missed::Declined(_, Decline::Budget)) => Outcome::DeclinedBudget,
            Answer::Missed(Missed::Unsent(_)) => Outcome::Unsent,
            Answer::Failed(error) if error.kind() == ErrorKind::OutcomeUnknown => Outcome::Unknown,
            Answer::Failed(_) => Outcome::Failed,
        }
    }

    /// The `outcome` label.
    fn label(self) -> &'static str {
        match self {
            Outcome::Accepted => "accepted",
            Outcome::DeclinedFull => "declined_full",
            Outcome::DeclinedGone => "declined_gone",
            Outcome::DeclinedInstance => "declined_instance",
            Outcome::DeclinedNotPresent => "declined_not_present",
            Outcome::DeclinedBudget => "declined_budget",
            Outcome::Unsent => "unsent",
            Outcome::Unknown => "outcome_unknown",
            Outcome::Failed => "failed",
        }
    }
}

/// The pools' counts by class and pool, shared by the follower, which adds
/// a pool's as it first follows it, and the scrape. Cheap to clone.
#[derive(Clone)]
pub(crate) struct PoolMeters {
    pools: ByPool,
    shards: usize,
}

/// Each pool's counts by class and pool.
type ByPool = Arc<Mutex<BTreeMap<(String, String), &'static PoolFigures>>>;

/// The `region` label of a pool's series.
const REGION: &str = "region";

impl PoolMeters {
    /// Register the pools' families in `registry`, with no pool yet.
    pub(crate) fn register(registry: &Registry) -> Result<Self> {
        let meters = Self::unregistered();
        let scrape = Scrape::new(Arc::clone(&meters.pools))?;
        registry.register(Box::new(scrape)).map_err(failed)?;
        Ok(meters)
    }

    /// Counts that no registry reads, for a test.
    pub(crate) fn unregistered() -> Self {
        let shards = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .next_power_of_two()
            .min(SHARD_LIMIT);
        Self {
            pools: ByPool::default(),
            shards,
        }
    }

    /// The counts of the pool `pool` of `class`, the same for every follow
    /// of it; made, and its series shown at zero, the first time, labelled
    /// with `region`, the pool's region in that follow. Taken off a
    /// request's path, as a follow is made.
    ///
    /// The counts are leaked: a scrape reads them for as long as the
    /// process serves, and a `&'static` reference costs an offer nothing,
    /// where an `Arc` would be one more atomic that every thread touches.
    pub(crate) fn figures(&self, class: &str, pool: &str, region: &str) -> &'static PoolFigures {
        let mut pools = self.pools.lock().unwrap_or_else(PoisonError::into_inner);
        pools
            .entry((class.to_string(), pool.to_string()))
            .or_insert_with(|| {
                Box::leak(Box::new(PoolFigures {
                    region: region.into(),
                    shards: (0..self.shards).map(|_| Shard::default()).collect(),
                }))
            })
    }
}

fn failed(error: prometheus::Error) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("registering the chooser's pool metrics: {error}"),
    )
}

/// One pool's counts, one shard per thread up to `SHARD_LIMIT`.
pub(crate) struct PoolFigures {
    /// The pool's region when its counts were made: a pool's region is
    /// set where it is deployed, and its series keep the first.
    region: Box<str>,
    shards: Box<[Shard]>,
}

impl PoolFigures {
    /// Count one offer that ended with `outcome`, and its round trip, the
    /// time from its send to its answer, if it was timed: one relaxed add,
    /// and two more for the round trip.
    #[inline]
    pub(crate) fn offered(&self, outcome: Outcome, round_trip: Option<Duration>) {
        let shard = self.shard();
        shard.offers[outcome as usize].fetch_add(1, Relaxed);
        if let Some(round_trip) = round_trip {
            shard.round_trips[outcome as usize].observe(round_trip);
        }
    }

    /// Count one offer made after another pool missed the request.
    pub(crate) fn failover(&self) {
        self.shard().failovers.fetch_add(1, Relaxed);
    }

    /// Count one request re-queued for the pool.
    pub(crate) fn requeued(&self) {
        self.shard().requeues.fetch_add(1, Relaxed);
    }

    /// Count one re-queued request that ended at its deadline.
    pub(crate) fn requeue_deadline(&self) {
        self.shard().requeue_deadlines.fetch_add(1, Relaxed);
    }

    /// The calling thread's shard.
    #[inline]
    fn shard(&self) -> &Shard {
        thread_local! {
            static INDEX: usize = {
                static NEXT: AtomicUsize = AtomicUsize::new(0);
                NEXT.fetch_add(1, Relaxed)
            };
        }
        let index = INDEX.with(|index| *index);
        &self.shards[index & (self.shards.len() - 1)]
    }

    /// The offers that ended with `outcome`, over every shard.
    fn offers(&self, outcome: Outcome) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.offers[outcome as usize].load(Relaxed))
            .sum()
    }

    /// The failovers, over every shard.
    fn failovers(&self) -> u64 {
        self.sum(|shard| &shard.failovers)
    }

    /// The count `read` reads, over every shard.
    fn sum(&self, read: impl Fn(&Shard) -> &AtomicU64) -> u64 {
        self.shards
            .iter()
            .map(|shard| read(shard).load(Relaxed))
            .sum()
    }

    /// The round trips of the offers that ended with `outcome`, over every
    /// shard.
    fn round_trips(&self, outcome: Outcome) -> proto::Histogram {
        gdl::__runtime::histogram(
            self.shards
                .iter()
                .map(move |shard| &shard.round_trips[outcome as usize]),
        )
    }
}

impl std::fmt::Debug for PoolFigures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolFigures").finish_non_exhaustive()
    }
}

/// One thread's share of a pool's counts, on cache lines of its own.
#[derive(Default)]
#[repr(align(128))]
struct Shard {
    offers: [AtomicU64; Outcome::ALL.len()],
    failovers: AtomicU64,
    round_trips: [Spans; Outcome::ALL.len()],
    requeues: AtomicU64,
    requeue_deadlines: AtomicU64,
}

/// The families, summed over each pool's shards when the registry is
/// scraped.
struct Scrape {
    pools: ByPool,
    descs: [Desc; 5],
}

impl Scrape {
    fn new(pools: ByPool) -> Result<Self> {
        let desc = |name: &str, help: &str, labels: &[&str]| {
            Desc::new(
                name.to_string(),
                help.to_string(),
                labels.iter().map(|label| label.to_string()).collect(),
                Default::default(),
            )
            .map_err(failed)
        };
        Ok(Self {
            pools,
            descs: [
                desc(OFFERS, OFFERS_HELP, &["class", "pool", "outcome"])?,
                desc(FAILOVERS, FAILOVERS_HELP, &["class", "pool"])?,
                desc(
                    ROUND_TRIPS,
                    ROUND_TRIPS_HELP,
                    &["class", "pool", REGION, "outcome"],
                )?,
                desc(REQUEUES, REQUEUES_HELP, &["class", "pool", REGION])?,
                desc(
                    REQUEUE_DEADLINES,
                    REQUEUE_DEADLINES_HELP,
                    &["class", "pool", REGION],
                )?,
            ],
        })
    }
}

fn label(name: &str, value: &str) -> proto::LabelPair {
    let mut pair = proto::LabelPair::default();
    pair.set_name(name.to_string());
    pair.set_value(value.to_string());
    pair
}

fn counter(labels: Vec<proto::LabelPair>, value: u64) -> proto::Metric {
    let mut metric = proto::Metric::from_label(labels);
    let mut counter = proto::Counter::default();
    counter.set_value(value as f64);
    metric.set_counter(counter);
    metric
}

fn family(name: &str, help: &str, kind: MetricType, metrics: Vec<proto::Metric>) -> MetricFamily {
    let mut family = MetricFamily::default();
    family.set_name(name.to_string());
    family.set_help(help.to_string());
    family.set_field_type(kind);
    family.set_metric(metrics);
    family
}

impl Collector for Scrape {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let pools = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut offers = Vec::with_capacity(pools.len() * Outcome::ALL.len());
        let mut failovers = Vec::with_capacity(pools.len());
        let mut round_trips = Vec::with_capacity(pools.len() * Outcome::ALL.len());
        let mut requeues = Vec::with_capacity(pools.len());
        let mut deadlines = Vec::with_capacity(pools.len());
        for ((class, pool), figures) in &pools {
            let region = label(REGION, &figures.region);
            for outcome in Outcome::ALL {
                let labels = vec![
                    label("class", class),
                    label("pool", pool),
                    label("outcome", outcome.label()),
                ];
                offers.push(counter(labels, figures.offers(outcome)));
                let labels = vec![
                    label("class", class),
                    label("pool", pool),
                    region.clone(),
                    label("outcome", outcome.label()),
                ];
                let mut metric = proto::Metric::from_label(labels);
                metric.set_histogram(figures.round_trips(outcome));
                round_trips.push(metric);
            }
            let labels = vec![label("class", class), label("pool", pool)];
            failovers.push(counter(labels, figures.failovers()));
            let labels = vec![label("class", class), label("pool", pool), region];
            requeues.push(counter(
                labels.clone(),
                figures.sum(|shard| &shard.requeues),
            ));
            deadlines.push(counter(
                labels,
                figures.sum(|shard| &shard.requeue_deadlines),
            ));
        }
        vec![
            family(OFFERS, OFFERS_HELP, MetricType::COUNTER, offers),
            family(FAILOVERS, FAILOVERS_HELP, MetricType::COUNTER, failovers),
            family(
                ROUND_TRIPS,
                ROUND_TRIPS_HELP,
                MetricType::HISTOGRAM,
                round_trips,
            ),
            family(REQUEUES, REQUEUES_HELP, MetricType::COUNTER, requeues),
            family(
                REQUEUE_DEADLINES,
                REQUEUE_DEADLINES_HELP,
                MetricType::COUNTER,
                deadlines,
            ),
        ]
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::dist::envelope::Accepted;

    /// The value of `family`'s series whose labels are `labels`, in a
    /// scrape of `registry`, read from the parsed families; `None` when it
    /// has no such series.
    pub(crate) fn scraped(
        registry: &Registry,
        family: &str,
        labels: &[(&str, &str)],
    ) -> Option<f64> {
        registry
            .gather()
            .into_iter()
            .filter(|gathered| gathered.name() == family)
            .flat_map(|gathered| gathered.get_metric().to_vec())
            .find(|metric| {
                labels.iter().all(|(name, value)| {
                    metric
                        .get_label()
                        .iter()
                        .any(|pair| pair.name() == *name && pair.value() == *value)
                })
            })
            .map(|metric| metric.get_counter().value())
    }

    /// The offers to `pool` of `model_x` that ended with `outcome`.
    pub(crate) fn offers(registry: &Registry, pool: &str, outcome: &str) -> Option<f64> {
        scraped(
            registry,
            OFFERS,
            &[("class", "model_x"), ("pool", pool), ("outcome", outcome)],
        )
    }

    /// Each answer counts under its own outcome: a decline by the reason
    /// its entry gave, and a failure apart from an unknown outcome.
    #[test]
    fn each_answer_counts_under_its_outcome() {
        let error = |kind| Error::new(kind, "x");
        let cases = [
            (Answer::Accepted(Accepted::default()), Outcome::Accepted),
            (
                Answer::Missed(Missed::Declined(
                    error(ErrorKind::Overloaded),
                    Decline::Full,
                )),
                Outcome::DeclinedFull,
            ),
            (
                Answer::Missed(Missed::Declined(
                    error(ErrorKind::Overloaded),
                    Decline::Gone,
                )),
                Outcome::DeclinedGone,
            ),
            (
                Answer::Missed(Missed::Declined(
                    error(ErrorKind::Overloaded),
                    Decline::Instance,
                )),
                Outcome::DeclinedInstance,
            ),
            (
                Answer::Missed(Missed::Declined(
                    error(ErrorKind::Overloaded),
                    Decline::NotPresent,
                )),
                Outcome::DeclinedNotPresent,
            ),
            (
                Answer::Missed(Missed::Unsent(error(ErrorKind::Transport))),
                Outcome::Unsent,
            ),
            (
                Answer::Failed(error(ErrorKind::OutcomeUnknown)),
                Outcome::Unknown,
            ),
            (
                Answer::Failed(error(ErrorKind::DeadlineExceeded)),
                Outcome::Failed,
            ),
        ];
        for (answer, outcome) in cases {
            assert_eq!(Outcome::of(&answer), outcome);
        }
        let labels: std::collections::BTreeSet<_> =
            Outcome::ALL.iter().map(|outcome| outcome.label()).collect();
        assert_eq!(
            labels.len(),
            Outcome::ALL.len(),
            "two outcomes share a label"
        );
    }

    /// A pool's series appear at zero once it is first followed, its
    /// counts are summed over the threads that added to them, and a second
    /// call for the same pool gives the same counts.
    #[test]
    fn a_pools_counts_are_summed_over_threads() {
        let registry = Registry::new();
        let meters = PoolMeters::register(&registry).expect("registered");
        assert_eq!(offers(&registry, "m1", "accepted"), None);
        let figures = meters.figures("model_x", "m1", "home");
        assert!(std::ptr::eq(
            figures,
            meters.figures("model_x", "m1", "home")
        ));
        for outcome in Outcome::ALL {
            assert_eq!(offers(&registry, "m1", outcome.label()), Some(0.0));
        }
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        figures.offered(Outcome::Accepted, None);
                    }
                    figures.offered(Outcome::DeclinedGone, None);
                    figures.failover();
                });
            }
        });
        assert_eq!(offers(&registry, "m1", "accepted"), Some(4000.0));
        assert_eq!(offers(&registry, "m1", "declined_gone"), Some(4.0));
        assert_eq!(offers(&registry, "m1", "declined_full"), Some(0.0));
        assert_eq!(
            scraped(
                &registry,
                FAILOVERS,
                &[("class", "model_x"), ("pool", "m1")]
            ),
            Some(4.0)
        );
    }

    /// The histogram of `pool`'s round trips that ended with `outcome`.
    pub(crate) fn round_trips(
        registry: &Registry,
        pool: &str,
        outcome: &str,
    ) -> Option<proto::Histogram> {
        registry
            .gather()
            .into_iter()
            .filter(|gathered| gathered.name() == ROUND_TRIPS)
            .flat_map(|gathered| gathered.get_metric().to_vec())
            .find(|metric| {
                let has = |name: &str, value: &str| {
                    metric
                        .get_label()
                        .iter()
                        .any(|pair| pair.name() == name && pair.value() == value)
                };
                has("pool", pool) && has("outcome", outcome) && has(REGION, "home")
            })
            .map(|metric| proto::Histogram::clone(metric.get_histogram()))
    }

    /// Each offer's round trip, when it was timed, counts in its outcome's
    /// histogram, labelled with the region of the pool's first follow,
    /// summed over the threads; one that was not timed counts as an offer
    /// only. The re-queues and their deadlines count under the pool.
    #[test]
    fn round_trips_and_re_queues_are_summed_over_threads() {
        let registry = Registry::new();
        let meters = PoolMeters::register(&registry).expect("registered");
        let figures = meters.figures("model_x", "m1", "home");
        // A later follow in another region counts into the same series.
        assert!(std::ptr::eq(
            figures,
            meters.figures("model_x", "m1", "away")
        ));
        let empty = round_trips(&registry, "m1", "accepted").expect("at zero");
        assert_eq!(empty.get_sample_count(), 0);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..10 {
                        figures.offered(Outcome::Accepted, Some(Duration::from_micros(30)));
                    }
                    figures.offered(Outcome::Accepted, None);
                    figures.offered(Outcome::DeclinedFull, Some(Duration::from_millis(2)));
                    figures.requeued();
                });
            }
        });
        figures.requeue_deadline();
        let accepted = round_trips(&registry, "m1", "accepted").expect("a histogram");
        assert_eq!(accepted.get_sample_count(), 40);
        assert!((accepted.get_sample_sum() - 40.0 * 30e-6).abs() < 1e-9);
        // 30 us is at most the first bound, 100 us.
        assert_eq!(accepted.get_bucket()[0].cumulative_count(), 40);
        assert_eq!(offers(&registry, "m1", "accepted"), Some(44.0));
        let full = round_trips(&registry, "m1", "declined_full").expect("a histogram");
        assert_eq!(full.get_sample_count(), 4);
        // 2 ms is above 1 ms and at most 2.5 ms: the fifth bound.
        assert_eq!(full.get_bucket()[3].cumulative_count(), 0);
        assert_eq!(full.get_bucket()[4].cumulative_count(), 4);
        let pool = [("class", "model_x"), ("pool", "m1"), (REGION, "home")];
        assert_eq!(scraped(&registry, REQUEUES, &pool), Some(4.0));
        assert_eq!(scraped(&registry, REQUEUE_DEADLINES, &pool), Some(1.0));
    }

    /// A node's own metric cannot take the families' names.
    #[test]
    fn every_family_is_reserved() {
        let mut names = vec![OFFERS, FAILOVERS, ROUND_TRIPS, REQUEUES, REQUEUE_DEADLINES];
        names.extend(gauges::names());
        for name in names {
            assert!(gdl_model::RUNTIME_METRICS.contains(&name), "{name}");
        }
    }
}
