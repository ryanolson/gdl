//! What a chooser records of its offers to cells: for each cell of each
//! class it sends to, how its offers ended, and how many of them were
//! failovers. The counts are kept by the cell's name, not by its follow,
//! so that a new generation of the cell, and an older one that still
//! answers offers made under it, count into the same series.
//!
//! Each cell's counts are sharded by thread, as the runtime's figures are
//! (`gdl::__runtime::Meters`), and summed when the registry is scraped. A
//! request's path adds to its thread's shard once, with a relaxed add,
//! and takes no lock and allocates nothing: the counts are reached through
//! the lease on the cell's follow that the offer already holds.
//!
//! A cell's series stay once it is first followed, for the life of the
//! process, also after its follow is dropped: an answer to an offer made
//! before the drop still counts, and no scrape misses the last counts.
//! The cells a chooser's hub ever followed bound them.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};

use gdl::{Error, ErrorKind, Result};
use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};

use crate::dist::offers::{Answer, Decline, Missed};

/// The most shards a cell's counts are split into.
const SHARD_LIMIT: usize = 64;

/// The family of the offers' outcomes, and its help.
pub(crate) const OFFERS: &str = "cell_offers_total";
const OFFERS_HELP: &str = "Offers this chooser made to the cell, by how each ended.";
/// The family of the offers made after another cell missed the request.
pub(crate) const FAILOVERS: &str = "cell_failovers_total";
const FAILOVERS_HELP: &str =
    "Offers this chooser made to the cell after another cell did not take the request.";

/// How an offer to a cell ended, as the chooser read its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The cell's entry took the request.
    Accepted,
    /// The entry declined it, saying its cell is full.
    DeclinedFull,
    /// The entry declined it, saying its cell is gone.
    DeclinedGone,
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
    const ALL: [Outcome; 6] = [
        Outcome::Accepted,
        Outcome::DeclinedFull,
        Outcome::DeclinedGone,
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
            Outcome::Unsent => "unsent",
            Outcome::Unknown => "outcome_unknown",
            Outcome::Failed => "failed",
        }
    }
}

/// The cells' counts by class and cell, shared by the follower, which adds
/// a cell's as it first follows it, and the scrape. Cheap to clone.
#[derive(Clone)]
pub(crate) struct CellMeters {
    cells: ByCell,
    shards: usize,
}

/// Each cell's counts by class and cell.
type ByCell = Arc<Mutex<BTreeMap<(String, String), &'static CellFigures>>>;

impl CellMeters {
    /// Register the cells' families in `registry`, with no cell yet.
    pub(crate) fn register(registry: &Registry) -> Result<Self> {
        let meters = Self::unregistered();
        let scrape = Scrape::new(Arc::clone(&meters.cells))?;
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
            cells: ByCell::default(),
            shards,
        }
    }

    /// The counts of the cell `cell` of `class`, the same for every follow
    /// of it; made, and its series shown at zero, the first time. Taken off
    /// a request's path, as a follow is made.
    ///
    /// The counts are leaked: a scrape reads them for as long as the
    /// process serves, and a `&'static` reference costs an offer nothing,
    /// where an `Arc` would be one more atomic that every thread touches.
    pub(crate) fn figures(&self, class: &str, cell: &str) -> &'static CellFigures {
        let mut cells = self.cells.lock().unwrap_or_else(PoisonError::into_inner);
        cells
            .entry((class.to_string(), cell.to_string()))
            .or_insert_with(|| {
                Box::leak(Box::new(CellFigures {
                    shards: (0..self.shards).map(|_| Shard::default()).collect(),
                }))
            })
    }
}

fn failed(error: prometheus::Error) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("registering the chooser's cell metrics: {error}"),
    )
}

/// One cell's counts, one shard per thread up to `SHARD_LIMIT`.
pub(crate) struct CellFigures {
    shards: Box<[Shard]>,
}

impl CellFigures {
    /// Count one offer that ended with `outcome`: one relaxed add.
    #[inline]
    pub(crate) fn offered(&self, outcome: Outcome) {
        self.shard().offers[outcome as usize].fetch_add(1, Relaxed);
    }

    /// Count one offer made after another cell missed the request.
    pub(crate) fn failover(&self) {
        self.shard().failovers.fetch_add(1, Relaxed);
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
        self.shards
            .iter()
            .map(|shard| shard.failovers.load(Relaxed))
            .sum()
    }
}

impl std::fmt::Debug for CellFigures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellFigures").finish_non_exhaustive()
    }
}

/// One thread's share of a cell's counts, on cache lines of its own.
#[derive(Default)]
#[repr(align(128))]
struct Shard {
    offers: [AtomicU64; Outcome::ALL.len()],
    failovers: AtomicU64,
}

/// The two families, summed over each cell's shards when the registry is
/// scraped.
struct Scrape {
    cells: ByCell,
    descs: [Desc; 2],
}

impl Scrape {
    fn new(cells: ByCell) -> Result<Self> {
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
            cells,
            descs: [
                desc(OFFERS, OFFERS_HELP, &["class", "cell", "outcome"])?,
                desc(FAILOVERS, FAILOVERS_HELP, &["class", "cell"])?,
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

fn family(name: &str, help: &str, metrics: Vec<proto::Metric>) -> MetricFamily {
    let mut family = MetricFamily::default();
    family.set_name(name.to_string());
    family.set_help(help.to_string());
    family.set_field_type(MetricType::COUNTER);
    family.set_metric(metrics);
    family
}

impl Collector for Scrape {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let cells = self
            .cells
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut offers = Vec::with_capacity(cells.len() * Outcome::ALL.len());
        let mut failovers = Vec::with_capacity(cells.len());
        for ((class, cell), figures) in &cells {
            for outcome in Outcome::ALL {
                let labels = vec![
                    label("class", class),
                    label("cell", cell),
                    label("outcome", outcome.label()),
                ];
                offers.push(counter(labels, figures.offers(outcome)));
            }
            let labels = vec![label("class", class), label("cell", cell)];
            failovers.push(counter(labels, figures.failovers()));
        }
        vec![
            family(OFFERS, OFFERS_HELP, offers),
            family(FAILOVERS, FAILOVERS_HELP, failovers),
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

    /// The offers to `cell` of `model_x` that ended with `outcome`.
    pub(crate) fn offers(registry: &Registry, cell: &str, outcome: &str) -> Option<f64> {
        scraped(
            registry,
            OFFERS,
            &[("class", "model_x"), ("cell", cell), ("outcome", outcome)],
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

    /// A cell's series appear at zero once it is first followed, its
    /// counts are summed over the threads that added to them, and a second
    /// call for the same cell gives the same counts.
    #[test]
    fn a_cells_counts_are_summed_over_threads() {
        let registry = Registry::new();
        let meters = CellMeters::register(&registry).expect("registered");
        assert_eq!(offers(&registry, "m1", "accepted"), None);
        let figures = meters.figures("model_x", "m1");
        assert!(std::ptr::eq(figures, meters.figures("model_x", "m1")));
        for outcome in Outcome::ALL {
            assert_eq!(offers(&registry, "m1", outcome.label()), Some(0.0));
        }
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        figures.offered(Outcome::Accepted);
                    }
                    figures.offered(Outcome::DeclinedGone);
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
                &[("class", "model_x"), ("cell", "m1")]
            ),
            Some(4.0)
        );
    }

    /// A node's own metric cannot take the families' names.
    #[test]
    fn every_family_is_reserved() {
        for name in [OFFERS, FAILOVERS] {
            assert!(gdl_model::RUNTIME_METRICS.contains(&name), "{name}");
        }
    }
}
