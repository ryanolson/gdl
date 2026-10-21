//! What the runtime records for every node a segment hosts, where it calls
//! the node: requests, errors by kind, cancellations, requests in flight,
//! and how long each took. dynamo's work handler records the same five for
//! each endpoint, under the same names.
//!
//! Each node's figures are sharded by thread and summed when the registry
//! is scraped. A prometheus metric is one atomic that every thread
//! touches: measured on the laptop, 32 requests in flight on four workers
//! took 27% longer with one per figure, against 4% for one request at a
//! time (`agent-docs/perf/m21-metrics.md`). Nothing is recorded per item.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering::Relaxed};

use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};
use tokio::time::Instant;

use crate::cx::Cx;
use crate::error::{Error, ErrorKind, Result};
use gdl_model::ENDPOINT_LABEL;

/// dynamo's buckets for a request's duration, 5 ms to 10 minutes: an
/// inference request can last minutes.
const DURATION_BUCKETS: [f64; 17] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0,
    600.0,
];

/// The `error_type` label of each slot of `errors_total`, as
/// [`error_slot`] numbers them.
const ERROR_KINDS: [&str; 8] = [
    "node",
    "deadline_exceeded",
    "truncated",
    "not_hosted",
    "overloaded",
    "transport",
    "codec",
    "placement",
];

/// The slot of `errors_total` that counts `kind`, or `None` for a
/// cancellation, which `cancellation_total` counts. Exhaustive, so that a
/// new kind cannot go uncounted.
fn error_slot(kind: ErrorKind) -> Option<usize> {
    match kind {
        ErrorKind::Cancelled => None,
        ErrorKind::Node => Some(0),
        ErrorKind::DeadlineExceeded => Some(1),
        ErrorKind::Truncated => Some(2),
        ErrorKind::NotHosted => Some(3),
        ErrorKind::Overloaded => Some(4),
        ErrorKind::Transport => Some(5),
        ErrorKind::Codec => Some(6),
        ErrorKind::Placement => Some(7),
    }
}

/// The most shards a node's figures are split into.
const SHARD_LIMIT: usize = 64;

/// The runtime's figures for each node a segment hosts, by node name.
pub(crate) struct Meters(BTreeMap<String, &'static Meter>);

impl Meters {
    /// Make figures for each of `nodes` and register them in `registry`,
    /// where a scrape sums their shards.
    ///
    /// The figures are leaked: the registry, which holds them for its
    /// scrapes, lives as long as the process that serves it, and a
    /// `&'static` reference costs a node's request nothing, where an `Arc`
    /// would be one more atomic every thread touches.
    pub(crate) fn register<'a>(
        registry: &Registry,
        nodes: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self> {
        let shards = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .next_power_of_two()
            .min(SHARD_LIMIT);
        let meters: BTreeMap<String, &'static Meter> = nodes
            .into_iter()
            .map(|node| {
                let meter: &'static Meter = Box::leak(Box::new(Meter {
                    shards: (0..shards).map(|_| Shard::default()).collect(),
                }));
                (node.to_string(), meter)
            })
            .collect();
        let collector = Scrape::new(meters.clone())?;
        registry.register(Box::new(collector)).map_err(failed)?;
        Ok(Self(meters))
    }

    /// The figures of node `node`, if this segment hosts it.
    pub(crate) fn get(&self, node: &str) -> Option<&'static Meter> {
        self.0.get(node).copied()
    }
}

fn failed(error: prometheus::Error) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("registering the runtime's metrics: {error}"),
    )
}

/// One node's figures, one shard per thread up to [`SHARD_LIMIT`].
#[doc(hidden)]
pub struct Meter {
    shards: Box<[Shard]>,
}

/// One thread's share of a node's figures, on cache lines of its own, so
/// that threads recording at once do not contend.
#[derive(Default)]
#[repr(align(128))]
struct Shard {
    requests: AtomicU64,
    cancelled: AtomicU64,
    /// Started here less ended here: a request may end on another thread,
    /// so only the sum over shards is the count in flight.
    inflight: AtomicI64,
    errors: [AtomicU64; ERROR_KINDS.len()],
    /// Requests by the first bucket whose bound holds their duration; the
    /// last counts those longer than every bound.
    buckets: [AtomicU64; DURATION_BUCKETS.len() + 1],
    nanos: AtomicU64,
}

impl Meter {
    /// The calling thread's shard.
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
}

/// Start one request of the node `meter` measures, under `cx`, if the
/// segment records at all.
pub(crate) fn start(meter: Option<&'static Meter>, cx: &Cx) -> Option<Reading> {
    let meter = meter?;
    let shard = meter.shard();
    shard.requests.fetch_add(1, Relaxed);
    shard.inflight.fetch_add(1, Relaxed);
    Some(Reading {
        meter,
        started: Instant::now(),
        deadline: cx.deadline().map(|deadline| deadline.instant()),
        ended: false,
    })
}

/// End the request `reading` holds, if it has not ended, with `error` if
/// it failed: what a node boundary calls as it ends its stream, and what a
/// call that returns no stream calls with its result.
pub(crate) fn ended(reading: &mut Option<Reading>, error: Option<&Error>) {
    if let Some(reading) = reading.take() {
        reading.end(error);
    }
}

/// One request of a node whose call is a function with many exits, each of
/// which notes its outcome here; dropped without one, the request was
/// given up.
pub(crate) struct Outcome(Option<Reading>);

impl Outcome {
    /// Start one request of the node `meter` measures, under `cx`.
    pub(crate) fn start(meter: Option<&'static Meter>, cx: &Cx) -> Self {
        Self(start(meter, cx))
    }

    /// End the request with `result`'s error, if it is one, and pass the
    /// result on.
    pub(crate) fn note<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result
            && let Some(reading) = self.0.take()
        {
            reading.end(Some(error));
        }
        result
    }

    /// Take `cx`'s deadline as the request's: the one the runtime gives a
    /// request that crosses segments without one of its own, so that a
    /// drop after it reads as the deadline, as at every node it reached.
    pub(crate) fn deadline(&mut self, cx: &Cx) {
        if let Some(reading) = &mut self.0 {
            reading.deadline = cx.deadline().map(|deadline| deadline.instant());
        }
    }

    /// The request, for the boundary of the stream that ends it.
    pub(crate) fn reading(self) -> Option<Reading> {
        self.0
    }
}

/// One request of one node, from its call until its end.
pub(crate) struct Reading {
    meter: &'static Meter,
    started: Instant,
    /// The request's deadline: a request dropped after it ended by it, not
    /// by a cancel.
    deadline: Option<Instant>,
    ended: bool,
}

impl Reading {
    /// The request ended, with `error` if it failed.
    fn end(mut self, error: Option<&Error>) {
        self.record(Instant::now(), error.map(Error::kind));
    }

    fn record(&mut self, now: Instant, kind: Option<ErrorKind>) {
        self.ended = true;
        let elapsed = now.saturating_duration_since(self.started);
        let shard = self.meter.shard();
        shard.inflight.fetch_sub(1, Relaxed);
        let seconds = elapsed.as_secs_f64();
        let bucket = DURATION_BUCKETS
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(DURATION_BUCKETS.len());
        shard.buckets[bucket].fetch_add(1, Relaxed);
        shard.nanos.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Relaxed,
        );
        match kind.map(error_slot) {
            None => {}
            Some(None) => {
                shard.cancelled.fetch_add(1, Relaxed);
            }
            Some(Some(slot)) => {
                shard.errors[slot].fetch_add(1, Relaxed);
            }
        }
    }
}

impl Drop for Reading {
    /// Dropped before it ended: the request was given up, by a cancel, or
    /// at its deadline.
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        let now = Instant::now();
        let expired = self.deadline.is_some_and(|deadline| now >= deadline);
        self.record(
            now,
            Some(if expired {
                ErrorKind::DeadlineExceeded
            } else {
                ErrorKind::Cancelled
            }),
        );
    }
}

/// The five families' names and help.
const FAMILIES: [(&str, &str, MetricType); 5] = [
    (
        "requests_total",
        "Requests the node took.",
        MetricType::COUNTER,
    ),
    (
        "errors_total",
        "Requests the node ended with an error, by its kind.",
        MetricType::COUNTER,
    ),
    (
        "cancellation_total",
        "Requests cancelled before the node finished them.",
        MetricType::COUNTER,
    ),
    (
        "inflight_requests",
        "Requests the node holds now.",
        MetricType::GAUGE,
    ),
    (
        "request_duration_seconds",
        "Time from the node's call to the end of its response.",
        MetricType::HISTOGRAM,
    ),
];

/// The five families, summed over each node's shards when the registry is
/// scraped. Shards are read one at a time, not as one snapshot: a scrape
/// may see a request's end before its start, so a negative count in
/// flight reads as zero.
struct Scrape {
    nodes: BTreeMap<String, &'static Meter>,
    descs: Vec<Desc>,
}

impl Scrape {
    fn new(nodes: BTreeMap<String, &'static Meter>) -> Result<Self> {
        let descs = FAMILIES
            .iter()
            .map(|(name, help, _)| {
                let mut labels = vec![ENDPOINT_LABEL.to_string()];
                if *name == "errors_total" {
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
        Ok(Self { nodes, descs })
    }
}

fn label(name: &str, value: &str) -> proto::LabelPair {
    let mut pair = proto::LabelPair::default();
    pair.set_name(name.to_string());
    pair.set_value(value.to_string());
    pair
}

fn sum(shards: &[Shard], read: impl Fn(&Shard) -> u64) -> u64 {
    shards.iter().map(read).sum()
}

fn counter(node: &str, kind: Option<&str>, value: u64) -> proto::Metric {
    let mut labels = vec![label(ENDPOINT_LABEL, node)];
    labels.extend(kind.map(|kind| label("error_type", kind)));
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
        let mut families: [Vec<proto::Metric>; 5] = Default::default();
        for (node, meter) in &self.nodes {
            let shards = &meter.shards;
            families[0].push(counter(
                node,
                None,
                sum(shards, |s| s.requests.load(Relaxed)),
            ));
            for (index, kind) in ERROR_KINDS.iter().enumerate() {
                let errors = sum(shards, |s| s.errors[index].load(Relaxed));
                families[1].push(counter(node, Some(kind), errors));
            }
            families[2].push(counter(
                node,
                None,
                sum(shards, |s| s.cancelled.load(Relaxed)),
            ));

            let inflight: i64 = shards.iter().map(|s| s.inflight.load(Relaxed)).sum();
            let mut metric = proto::Metric::from_label(vec![label(ENDPOINT_LABEL, node)]);
            let mut gauge = proto::Gauge::default();
            gauge.set_value(inflight.max(0) as f64);
            metric.set_gauge(gauge);
            families[3].push(metric);

            // The count is the sum of the buckets read, so that `_count`
            // and the `+Inf` bucket agree whatever ends during the scrape.
            let mut histogram = proto::Histogram::default();
            let mut cumulative = 0;
            let mut buckets = Vec::with_capacity(DURATION_BUCKETS.len());
            for (index, bound) in DURATION_BUCKETS.iter().enumerate() {
                cumulative += sum(shards, |s| s.buckets[index].load(Relaxed));
                let mut bucket = proto::Bucket::default();
                bucket.set_cumulative_count(cumulative);
                bucket.set_upper_bound(*bound);
                buckets.push(bucket);
            }
            cumulative += sum(shards, |s| s.buckets[DURATION_BUCKETS.len()].load(Relaxed));
            histogram.set_bucket(buckets);
            histogram.set_sample_count(cumulative);
            histogram.set_sample_sum(sum(shards, |s| s.nanos.load(Relaxed)) as f64 / 1e9);
            let mut metric = proto::Metric::from_label(vec![label(ENDPOINT_LABEL, node)]);
            metric.set_histogram(histogram);
            families[4].push(metric);
        }
        families
            .into_iter()
            .zip(FAMILIES)
            .map(|(metrics, (name, help, kind))| {
                let mut family = MetricFamily::default();
                family.set_name(name.to_string());
                family.set_help(help.to_string());
                family.set_field_type(kind);
                family.set_metric(metrics);
                family
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each kind's slot carries the kind's own label.
    #[test]
    fn each_error_kind_counts_under_its_name() {
        for (kind, label) in [
            (ErrorKind::Node, "node"),
            (ErrorKind::DeadlineExceeded, "deadline_exceeded"),
            (ErrorKind::Truncated, "truncated"),
            (ErrorKind::NotHosted, "not_hosted"),
            (ErrorKind::Overloaded, "overloaded"),
            (ErrorKind::Transport, "transport"),
            (ErrorKind::Codec, "codec"),
            (ErrorKind::Placement, "placement"),
        ] {
            assert_eq!(error_slot(kind).map(|slot| ERROR_KINDS[slot]), Some(label));
        }
        assert_eq!(error_slot(ErrorKind::Cancelled), None);
    }
}
