//! What a segment records of the calls its nodes make to unary services,
//! through the clients they take in their builds: the same figures whether
//! the copy called runs in the caller's segment or in another
//! (docs/src/operations/observability.md#calls-to-unary-services).
//!
//! Calls and errors are counted always, in the calling thread's shard, as
//! a visit's are. A call is timed only when the segment records
//! durations: its two clock reads cost more than three times its count
//! (docs/src/operations/feature-costs.md#counting-a-call). A scrape sums
//! the shards.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};

use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};
use std::time::Instant;

use super::{ERROR_KINDS, Spans, error_slot, failed, histogram, label, shard_count, shard_index};
use crate::Cx;
use crate::cx::Deadline;
use crate::error::{Error, ErrorKind, Result};

/// Where the copy a call reaches runs: the `placement` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    /// In the caller's segment.
    Local,
    /// In another segment, through the runtime's link.
    Remote,
}

impl Placement {
    fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

/// The figures of one caller's calls of one service.
pub struct CallFigures {
    caller: String,
    service: String,
    placement: Placement,
    /// The segment that hosts the copy called.
    segment: String,
    /// Whether each call is timed.
    timed: bool,
    shards: Box<[CallShard]>,
}

/// One thread's share of a caller's figures, on cache lines of its own.
#[derive(Default)]
#[repr(align(128))]
struct CallShard {
    calls: AtomicU64,
    cancelled: AtomicU64,
    errors: [AtomicU64; ERROR_KINDS.len()],
    duration: Spans,
}

impl CallFigures {
    #[inline]
    fn shard(&self) -> &CallShard {
        &self.shards[shard_index() & (self.shards.len() - 1)]
    }

    /// A call began: count it, and the time it began if calls are timed.
    // hot path: local-unary-call
    #[inline]
    pub(crate) fn begin(&self) -> Option<Instant> {
        self.shard().calls.fetch_add(1, Relaxed);
        if self.timed {
            Some(Instant::now())
        } else {
            None
        }
    }

    /// A call that began at `started` ended, with `error` if it failed. Not
    /// called for a call that answered and is not timed: it has nothing to
    /// count.
    #[inline(never)]
    pub(crate) fn end(&self, error: Option<&Error>, started: Option<Instant>) {
        if let Some(error) = error {
            self.failed(error.kind());
        }
        if let Some(started) = started {
            self.timed(started);
        }
    }

    /// A call under `cx` was dropped before it ended: a cancellation, or
    /// `deadline_exceeded` once its deadline has passed.
    #[cold]
    #[inline(never)]
    pub(crate) fn dropped(&self, cx: &Cx, started: Option<Instant>) {
        let expired = cx.deadline().is_some_and(Deadline::is_expired);
        let kind = if expired {
            ErrorKind::DeadlineExceeded
        } else {
            ErrorKind::Cancelled
        };
        self.failed(kind);
        if let Some(started) = started {
            self.timed(started);
        }
    }

    #[cold]
    fn failed(&self, kind: ErrorKind) {
        let shard = self.shard();
        match error_slot(kind) {
            Some(slot) => shard.errors[slot].fetch_add(1, Relaxed),
            None => shard.cancelled.fetch_add(1, Relaxed),
        };
    }

    #[inline(never)]
    fn timed(&self, started: Instant) {
        self.shard().duration.observe(started.elapsed());
    }

    fn labels(&self) -> Vec<proto::LabelPair> {
        vec![
            label("caller", &self.caller),
            label("service", &self.service),
            label("placement", self.placement.label()),
            label("segment", &self.segment),
        ]
    }
}

/// Each caller's figures of each service it calls, by caller and service.
type Callers = Arc<Mutex<BTreeMap<(String, String), Arc<CallFigures>>>>;

/// The call figures of one segment instance: one [`CallFigures`] per
/// caller and service, made when the caller takes its client.
pub struct CallMeter {
    callers: Callers,
    shards: usize,
    timed: AtomicBool,
}

impl Default for CallMeter {
    fn default() -> Self {
        Self {
            callers: Callers::default(),
            shards: shard_count(),
            timed: AtomicBool::new(false),
        }
    }
}

impl CallMeter {
    /// Serve the figures in `registry`, and, when `durations`, time every
    /// call of a client taken from now on.
    pub fn register(&self, registry: &Registry, durations: bool) -> Result<()> {
        self.timed.store(durations, Relaxed);
        let scrape = Scrape::new(Arc::clone(&self.callers))?;
        registry.register(Box::new(scrape)).map_err(failed)
    }

    /// The figures of `caller`'s calls of `service`, whose copy called is
    /// in `segment`, `placement` from the caller. One caller's clients of
    /// one service share them.
    pub fn figures(
        &self,
        caller: &str,
        service: &str,
        placement: Placement,
        segment: &str,
    ) -> Arc<CallFigures> {
        let mut callers = self.callers.lock().unwrap_or_else(PoisonError::into_inner);
        let figures = callers
            .entry((caller.to_string(), service.to_string()))
            .or_insert_with(|| {
                Arc::new(CallFigures {
                    caller: caller.to_string(),
                    service: service.to_string(),
                    placement,
                    segment: segment.to_string(),
                    timed: self.timed.load(Relaxed),
                    shards: (0..self.shards).map(|_| CallShard::default()).collect(),
                })
            });
        Arc::clone(figures)
    }
}

/// A family of call figures: its name, its help, and how a shard is read.
type Family = (&'static str, &'static str, Read);

#[derive(Clone, Copy)]
enum Read {
    Calls,
    Errors,
    Cancelled,
    Duration,
}

const FAMILIES: [Family; 4] = [
    (
        "calls_total",
        "Calls this segment's nodes made to a unary service, by caller, service, where the copy called runs (placement), and its segment.",
        Read::Calls,
    ),
    (
        "call_errors_total",
        "Calls that ended with an error, by its kind: the service's own, or the call's, such as a refusal (overloaded).",
        Read::Errors,
    ),
    (
        "call_cancellation_total",
        "Calls dropped by their caller before they ended, with their deadline not yet passed.",
        Read::Cancelled,
    ),
    (
        "call_duration_seconds",
        "Time from a call's start to its end, recorded only while the segment records durations.",
        Read::Duration,
    ),
];

/// The families, summed over each caller's shards at a scrape.
struct Scrape {
    callers: Callers,
    descs: Vec<Desc>,
}

impl Scrape {
    fn new(callers: Callers) -> Result<Self> {
        let descs = FAMILIES
            .iter()
            .map(|(name, help, read)| {
                let mut labels: Vec<String> = ["caller", "service", "placement", "segment"]
                    .map(str::to_string)
                    .into();
                if matches!(read, Read::Errors) {
                    labels.push("error_type".to_string());
                }
                Desc::new(
                    name.to_string(),
                    help.to_string(),
                    labels,
                    Default::default(),
                )
                .map_err(failed)
            })
            .collect::<Result<_>>()?;
        Ok(Self { callers, descs })
    }
}

fn counter(labels: Vec<proto::LabelPair>, value: u64) -> proto::Metric {
    let mut metric = proto::Metric::from_label(labels);
    let mut counter = proto::Counter::default();
    counter.set_value(value as f64);
    metric.set_counter(counter);
    metric
}

impl Collector for Scrape {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let callers: Vec<Arc<CallFigures>> = self
            .callers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        FAMILIES
            .iter()
            .filter_map(|(name, help, read)| {
                let mut metrics = Vec::new();
                for figures in &callers {
                    let shards = &figures.shards;
                    let sum = |read: &dyn Fn(&CallShard) -> u64| shards.iter().map(read).sum();
                    match read {
                        Read::Calls => {
                            metrics.push(counter(figures.labels(), sum(&|s| s.calls.load(Relaxed))))
                        }
                        Read::Cancelled => metrics.push(counter(
                            figures.labels(),
                            sum(&|s| s.cancelled.load(Relaxed)),
                        )),
                        Read::Errors => {
                            for (slot, kind) in ERROR_KINDS.iter().enumerate() {
                                let mut labels = figures.labels();
                                labels.push(label("error_type", kind));
                                metrics
                                    .push(counter(labels, sum(&|s| s.errors[slot].load(Relaxed))));
                            }
                        }
                        Read::Duration if figures.timed => {
                            let mut metric = proto::Metric::from_label(figures.labels());
                            metric.set_histogram(histogram(shards.iter().map(|s| &s.duration)));
                            metrics.push(metric);
                        }
                        Read::Duration => {}
                    }
                }
                // A family with no series is left out, as an untimed
                // segment's durations are.
                if metrics.is_empty() {
                    return None;
                }
                let mut family = MetricFamily::default();
                family.set_name(name.to_string());
                family.set_help(help.to_string());
                family.set_field_type(match read {
                    Read::Duration => MetricType::HISTOGRAM,
                    _ => MetricType::COUNTER,
                });
                family.set_metric(metrics);
                Some(family)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The families' names are the ones the derive refuses for a node's
    /// own metrics.
    #[test]
    fn every_family_is_reserved() {
        for (name, ..) in FAMILIES {
            assert!(gdl_model::RUNTIME_METRICS.contains(&name), "{name}");
        }
    }
}
