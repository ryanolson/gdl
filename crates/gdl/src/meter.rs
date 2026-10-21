//! What the runtime records for each segment a request passes through.
//!
//! A *visit* is one request's pass through one segment. It begins at an
//! entry: the root call in the source's segment, an admitted `gdl.fwd`, or
//! a backward run. It has a request part, from the request's arrival until
//! the segment hands it on or an inner end here starts its stream, and a
//! response part, for the stream the segment then sends on; the source's
//! visit is also the request end to end. A visit is in flight from its
//! beginning until the segment has nothing left to do for it, as dynamo
//! counts a request: queued, running, or responding, whichever part it is
//! in. Recorded per segment, the parts
//! add: each segment's request time and the time it adds to the first
//! item sum to the time to the first item end to end, less the wire.
//! Recorded per node, they nest, since a node's request lasts until its
//! response stream ends.
//!
//! Each entry's figures are sharded by thread and summed when the registry
//! is scraped. A prometheus metric is one atomic that every thread
//! touches: measured on the laptop, 32 requests in flight on four workers
//! took 27% longer with one per figure, against 4% for one request at a
//! time (`agent-docs/perf/m21-metrics.md`). Nothing is counted per item.

use std::collections::BTreeMap;
use std::sync::atomic::{
    AtomicI64, AtomicU8, AtomicU64, AtomicUsize,
    Ordering::{Acquire, Relaxed, Release},
};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};
use tokio::time::Instant;

use crate::cx::Deadline;
use crate::error::{Error, ErrorKind, Result};
use gdl_model::ENDPOINT_LABEL;

/// The buckets of every duration: dynamo's, 5 ms to 10 minutes (an
/// inference request can last minutes), with five more below them, since
/// a segment's request path often takes less than a millisecond.
const BUCKETS: [f64; 22] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];

/// The `error_type` label of each slot of an errors counter, as
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

/// The slot of an errors counter that counts `kind`, or `None` for a
/// cancellation, which a cancellation counter counts. Exhaustive, so that a
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

/// The most shards an entry's figures are split into.
const SHARD_LIMIT: usize = 64;

/// The parts an entry's visits have, as bits: which families its series
/// appear in.
pub(crate) const REQUEST: u8 = 1;
pub(crate) const RESPONSE: u8 = 2;
pub(crate) const END_TO_END: u8 = 4;

/// Entries by their node, shared by a segment's hops, which add them, and
/// the scrape, which reads them.
type Entries = Arc<Mutex<BTreeMap<String, &'static Figures>>>;

/// The figures of each entry a segment hosts, by the entry's node.
pub(crate) struct Meters {
    entries: Entries,
    shards: usize,
}

impl Meters {
    /// Register the runtime's figures in `registry`, where a scrape sums
    /// each entry's shards. Entries are added as the hops that own them
    /// are assembled, before the segment serves.
    pub(crate) fn register(registry: &Registry) -> Result<Self> {
        let shards = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .next_power_of_two()
            .min(SHARD_LIMIT);
        let entries = Entries::default();
        let collector = Scrape::new(Arc::clone(&entries))?;
        registry.register(Box::new(collector)).map_err(failed)?;
        Ok(Self { entries, shards })
    }

    /// The figures of the entry at node `entry`, whose visits have `parts`.
    /// Its series appear from now on, at zero.
    ///
    /// The figures are leaked: the registry, which holds them for its
    /// scrapes, lives as long as the process that serves it, and a
    /// `&'static` reference costs a visit nothing, where an `Arc` would be
    /// one more atomic every thread touches.
    pub(crate) fn entry(&self, entry: &str, parts: u8) -> &'static Figures {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let figures = *entries.entry(entry.to_string()).or_insert_with(|| {
            Box::leak(Box::new(Figures {
                shards: (0..self.shards).map(|_| Shard::default()).collect(),
                parts: AtomicU8::new(0),
            }))
        });
        figures.parts.fetch_or(parts, Relaxed);
        figures
    }
}

fn failed(error: prometheus::Error) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("registering the runtime's metrics: {error}"),
    )
}

/// One entry's figures, one shard per thread up to [`SHARD_LIMIT`].
pub(crate) struct Figures {
    shards: Box<[Shard]>,
    /// The parts its visits have ([`REQUEST`], [`RESPONSE`],
    /// [`END_TO_END`]); set before the segment serves.
    parts: AtomicU8,
}

impl Figures {
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

/// One thread's share of an entry's figures, on cache lines of its own, so
/// that threads recording at once do not contend.
#[derive(Default)]
#[repr(align(128))]
struct Shard {
    /// Visits begun here less visits ended here: a visit may end on another
    /// thread, so only the sum over shards is the count in flight.
    inflight: AtomicI64,
    request: Tally,
    request_path: Spans,
    response: Tally,
    first_item: Spans,
    after_first_item: Spans,
    end_to_end: Tally,
    end_to_end_first_item: Spans,
    end_to_end_duration: Spans,
}

/// How one part of a visit ended, counted by kind.
#[derive(Default)]
struct Tally {
    started: AtomicU64,
    cancelled: AtomicU64,
    errors: [AtomicU64; ERROR_KINDS.len()],
}

impl Tally {
    fn start(&self) {
        self.started.fetch_add(1, Relaxed);
    }

    /// Ended: with an error in slot `Some(Some(slot))`, a cancellation
    /// (`Some(None)`), or neither.
    fn end(&self, counted: Option<Option<usize>>) {
        match counted {
            None => {}
            Some(None) => {
                self.cancelled.fetch_add(1, Relaxed);
            }
            Some(Some(slot)) => {
                self.errors[slot].fetch_add(1, Relaxed);
            }
        }
    }
}

/// Durations, by the first bucket whose bound holds each; the last counts
/// those longer than every bound.
#[derive(Default)]
struct Spans {
    buckets: [AtomicU64; BUCKETS.len() + 1],
    nanos: AtomicU64,
}

impl Spans {
    fn observe(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        let bucket = BUCKETS
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(BUCKETS.len());
        self.buckets[bucket].fetch_add(1, Relaxed);
        self.nanos.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Relaxed,
        );
    }
}

/// How a visit's part ended.
#[derive(Clone, Copy)]
pub(crate) enum Ending<'a> {
    /// Its stream ended, or the request was handed on, without an error.
    Finished,
    /// With this error, counted where it was raised, not where it was
    /// relayed.
    Failed(&'a Error),
    /// Its consumer cancelled or dropped it: a cancellation, unless the
    /// deadline had passed.
    Gone,
}

/// The visit is in its request part.
const REQUESTING: u8 = 0;
/// The visit is in its response part.
const RESPONDING: u8 = 1;
/// The visit has ended.
const DONE: u8 = 2;

/// One request's pass through this segment: its request part, its
/// response part, or both, and, for the source's, the request end to end.
///
/// Shared by the contexts of the work it covers, so each transition is a
/// compare-exchange on its state: a part is recorded once, whichever
/// thread ends it. Instants are kept as nanoseconds after `started`, plus
/// one, so that zero means "not yet".
pub(crate) struct Visit {
    figures: &'static Figures,
    started: Instant,
    /// A visit given up after its deadline ended by it, not by a cancel.
    deadline: Option<Instant>,
    end_to_end: bool,
    /// The visit whose request this one's work still carries in: an inner
    /// end run the outer end spawns, whose start ends its parent's request
    /// part.
    parent: Option<Arc<Visit>>,
    state: AtomicU8,
    /// When the response began.
    begun: AtomicU64,
    /// When an inner end under the visit returned its stream, or its
    /// backward run activated.
    producing: AtomicU64,
    /// When the first item reached the visit from another segment.
    first_in: AtomicU64,
    /// When the first item left.
    first_out: AtomicU64,
}

impl Visit {
    /// A visit that begins with its request part, which arrived at
    /// `started`; `end_to_end` for the source's.
    pub(crate) fn request(
        figures: &'static Figures,
        started: Instant,
        deadline: Option<Deadline>,
        end_to_end: bool,
    ) -> Owner {
        let shard = figures.shard();
        shard.inflight.fetch_add(1, Relaxed);
        shard.request.start();
        if end_to_end {
            shard.end_to_end.started.fetch_add(1, Relaxed);
        }
        let visit = Self::new(figures, started, deadline, end_to_end, None, REQUESTING);
        Owner(Arc::new(visit))
    }

    /// A visit that is a response only: a backward run, or an inner end
    /// run the outer end spawns for the request `parent` carries.
    pub(crate) fn response(
        figures: &'static Figures,
        deadline: Option<Deadline>,
        parent: Option<Arc<Visit>>,
    ) -> Owner {
        let shard = figures.shard();
        shard.inflight.fetch_add(1, Relaxed);
        shard.response.start();
        let visit = Self::new(figures, Instant::now(), deadline, false, parent, RESPONDING);
        visit.begun.store(1, Relaxed);
        Owner(Arc::new(visit))
    }

    fn new(
        figures: &'static Figures,
        started: Instant,
        deadline: Option<Deadline>,
        end_to_end: bool,
        parent: Option<Arc<Visit>>,
        state: u8,
    ) -> Self {
        Self {
            figures,
            started,
            deadline: deadline.map(Deadline::instant),
            end_to_end,
            parent,
            state: AtomicU8::new(state),
            begun: AtomicU64::new(0),
            producing: AtomicU64::new(0),
            first_in: AtomicU64::new(0),
            first_out: AtomicU64::new(0),
        }
    }

    /// The request was handed on at `sent`, and accepted: its request
    /// part ends there, since the receiver's begins at its arrival. When
    /// `responds`, its response comes back through this segment, and the
    /// response part begins.
    pub(crate) fn handed_on(&self, responds: bool, sent: Instant) {
        let next = if responds { RESPONDING } else { DONE };
        // Stamped before the state changes: a first item out that sees the
        // response must also see when it began.
        if responds {
            let _ = stamp(&self.begun, self.since(sent));
        }
        if self
            .state
            .compare_exchange(REQUESTING, next, Release, Relaxed)
            .is_ok()
        {
            self.end_request(sent, Ending::Finished);
            if responds {
                self.begin();
            } else {
                self.leave(sent, Ending::Finished);
            }
        }
        if let Some(parent) = &self.parent {
            parent.handed_on(true, sent);
        }
    }

    /// A sink under the visit returned its stream, or its backward run
    /// activated: the request part ends, and the response part begins, if
    /// they have not already.
    pub(crate) fn producing(&self) {
        let now = Instant::now();
        let at = self.since(now);
        let _ = stamp(&self.producing, at);
        // Stamped before the state changes, as in `handed_on`.
        let _ = stamp(&self.begun, at);
        if self
            .state
            .compare_exchange(REQUESTING, RESPONDING, Release, Relaxed)
            .is_ok()
        {
            self.end_request(now, Ending::Finished);
            self.begin();
        }
        if let Some(parent) = &self.parent {
            parent.handed_on(true, now);
        }
    }

    /// The first item reached the visit from another segment.
    pub(crate) fn first_in(&self) {
        let _ = stamp(&self.first_in, self.since(Instant::now()));
    }

    /// The visit's first item left the segment.
    pub(crate) fn first_out(&self) {
        if self.state.load(Relaxed) == REQUESTING {
            self.producing();
        }
        let now = Instant::now();
        let at = self.since(now);
        if !stamp(&self.first_out, at) {
            return;
        }
        // Acquire: the transition to the response published its stamps.
        if self.state.load(Acquire) == RESPONDING {
            // The clock starts at the first item in, if one came: the time
            // before it was spent in other segments. Otherwise at the
            // stream's start here.
            let from = [&self.first_in, &self.producing, &self.begun]
                .into_iter()
                .map(|stamp| stamp.load(Relaxed))
                .find(|&stamp| stamp != 0)
                .unwrap_or(at);
            let shard = self.figures.shard();
            shard.first_item.observe(nanos(at.saturating_sub(from)));
            if self.end_to_end {
                shard.end_to_end_first_item.observe(nanos(at - 1));
            }
        }
    }

    /// The visit ended, however it ends: its request part if it is still
    /// in it, otherwise its response part.
    pub(crate) fn end(&self, ending: Ending<'_>) {
        let mut state = self.state.load(Relaxed);
        loop {
            if state == DONE {
                return;
            }
            match self.state.compare_exchange(state, DONE, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(now) => state = now,
            }
        }
        let now = Instant::now();
        if state == REQUESTING {
            self.end_request(now, ending);
        } else {
            let shard = self.figures.shard();
            shard.response.end(self.counted(ending, now));
            let first_out = self.first_out.load(Relaxed);
            if first_out != 0 {
                let at = self.since(now);
                shard
                    .after_first_item
                    .observe(nanos(at.saturating_sub(first_out)));
            }
        }
        self.leave(now, ending);
    }

    fn end_request(&self, now: Instant, ending: Ending<'_>) {
        let shard = self.figures.shard();
        shard.request.end(self.counted(ending, now));
        shard
            .request_path
            .observe(now.saturating_duration_since(self.started));
    }

    fn begin(&self) {
        self.figures.shard().response.start();
    }

    /// The visit has ended: the segment has nothing left to do for it.
    /// For the source's, the request end to end ends too, and every error
    /// the caller sees counts, relayed or not.
    fn leave(&self, now: Instant, ending: Ending<'_>) {
        let shard = self.figures.shard();
        shard.inflight.fetch_sub(1, Relaxed);
        if !self.end_to_end {
            return;
        }
        let counted = match ending {
            Ending::Failed(error) => Some(error_slot(error.kind())),
            _ => self.counted(ending, now),
        };
        shard.end_to_end.end(counted);
        shard
            .end_to_end_duration
            .observe(now.saturating_duration_since(self.started));
    }

    /// What a part's ending counts: an error raised here by its kind, a
    /// cancellation, or nothing for a clean end or a relayed error.
    fn counted(&self, ending: Ending<'_>, now: Instant) -> Option<Option<usize>> {
        match ending {
            Ending::Finished => None,
            Ending::Failed(error) if error.is_relayed() => None,
            Ending::Failed(error) => Some(error_slot(error.kind())),
            Ending::Gone => {
                let expired = self.deadline.is_some_and(|deadline| now >= deadline);
                Some(error_slot(if expired {
                    ErrorKind::DeadlineExceeded
                } else {
                    ErrorKind::Cancelled
                }))
            }
        }
    }

    /// `now` as a stamp: nanoseconds after the visit started, plus one.
    fn since(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.started);
        u64::try_from(elapsed.as_nanos())
            .unwrap_or(u64::MAX - 1)
            .saturating_add(1)
    }
}

/// Set a stamp that is not yet set; whether this call set it.
fn stamp(slot: &AtomicU64, at: u64) -> bool {
    slot.compare_exchange(0, at, Relaxed, Relaxed).is_ok()
}

fn nanos(stamp_difference: u64) -> Duration {
    Duration::from_nanos(stamp_difference)
}

/// The one owner of a visit: held by the work the visit covers (the root
/// hop's call, the task that accepted a `gdl.fwd`, an inner end run, a
/// backward run), and moved into what sends its response on (a pump, or
/// the root's attempt boundary). Dropped before the visit ended, the visit
/// was given up. Contexts hold the visit too, but only to report on it: a
/// node may keep a clone of its context for as long as it likes.
pub(crate) struct Owner(Arc<Visit>);

impl Owner {
    /// The visit, for the contexts of the work it covers.
    pub(crate) fn visit(&self) -> &Arc<Visit> {
        &self.0
    }

    pub(crate) fn end(&self, ending: Ending<'_>) {
        self.0.end(ending);
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.0.end(Ending::Gone);
    }
}

/// End the visit `owner` holds, if the segment records it.
pub(crate) fn ended(owner: Option<&Owner>, ending: Ending<'_>) {
    if let Some(owner) = owner {
        owner.end(ending);
    }
}

/// End the visit `owner` holds, if the segment records it, with `result`'s
/// error if it is one, and pass the result on.
pub(crate) fn note<T>(owner: Option<&Owner>, result: Result<T>) -> Result<T> {
    if let Err(error) = &result {
        ended(owner, Ending::Failed(error));
    }
    result
}

/// Where a family's value is in a shard.
enum Read {
    Counter(fn(&Shard) -> &AtomicU64),
    Errors(fn(&Shard) -> &Tally),
    Gauge(fn(&Shard) -> &AtomicI64),
    Histogram(fn(&Shard) -> &Spans),
}

/// One family: its name, help, the parts whose entries have it, and where
/// it is read.
struct Family {
    name: &'static str,
    help: &'static str,
    part: u8,
    read: Read,
}

/// The fifteen families. The request path keeps dynamo's work-handler
/// names; its duration does not, since dynamo's `request_duration_seconds`
/// spans the whole handler, its stream included. `inflight_requests` is
/// dynamo's too, and counts as dynamo does: a request until its segment has
/// nothing left to do for it, so every entry has it.
fn families() -> [Family; 15] {
    use Read::*;
    let family = |name, help, part, read| Family {
        name,
        help,
        part,
        read,
    };
    [
        family(
            "requests_total",
            "Requests that entered the segment here.",
            REQUEST,
            Counter(|s| &s.request.started),
        ),
        family(
            "errors_total",
            "Requests that failed in the segment before it handed them on or started their response, by the kind of the error raised here.",
            REQUEST,
            Errors(|s| &s.request),
        ),
        family(
            "cancellation_total",
            "Requests cancelled before the segment handed them on or started their response.",
            REQUEST,
            Counter(|s| &s.request.cancelled),
        ),
        family(
            "inflight_requests",
            "Requests received here that the segment still has work for: queued, running, or responding. A request leaves when the segment hands it on, when its response ends, or when it is cancelled or fails.",
            REQUEST | RESPONSE,
            Gauge(|s| &s.inflight),
        ),
        family(
            "request_path_duration_seconds",
            "Time from a request's arrival until the segment handed it on or an inner end here started its stream.",
            REQUEST,
            Histogram(|s| &s.request_path),
        ),
        family(
            "responses_total",
            "Response streams the segment sent on.",
            RESPONSE,
            Counter(|s| &s.response.started),
        ),
        family(
            "response_errors_total",
            "Response streams that ended with an error raised in the segment, by its kind; an error relayed from another segment is not counted.",
            RESPONSE,
            Errors(|s| &s.response),
        ),
        family(
            "response_cancellation_total",
            "Response streams cancelled or dropped by their consumer.",
            RESPONSE,
            Counter(|s| &s.response.cancelled),
        ),
        family(
            "response_time_to_first_item_seconds",
            "Time from a response's start in the segment (its first item from another segment, or its inner end starting) to its first item out.",
            RESPONSE,
            Histogram(|s| &s.first_item),
        ),
        family(
            "response_time_after_first_item_seconds",
            "Time from a response's first item out to its end.",
            RESPONSE,
            Histogram(|s| &s.after_first_item),
        ),
        family(
            "end_to_end_requests_total",
            "Requests into the graph.",
            END_TO_END,
            Counter(|s| &s.end_to_end.started),
        ),
        family(
            "end_to_end_errors_total",
            "Requests into the graph that ended with an error, by its kind.",
            END_TO_END,
            Errors(|s| &s.end_to_end),
        ),
        family(
            "end_to_end_cancellation_total",
            "Requests into the graph that their caller cancelled or dropped.",
            END_TO_END,
            Counter(|s| &s.end_to_end.cancelled),
        ),
        family(
            "end_to_end_time_to_first_item_seconds",
            "Time from a request into the graph to its first item.",
            END_TO_END,
            Histogram(|s| &s.end_to_end_first_item),
        ),
        family(
            "end_to_end_duration_seconds",
            "Time from a request into the graph to the end of its response.",
            END_TO_END,
            Histogram(|s| &s.end_to_end_duration),
        ),
    ]
}

/// The families, summed over each entry's shards when the registry is
/// scraped. Shards are read one at a time, not as one snapshot: a scrape
/// may see a visit's end before its start, so a negative count in flight
/// reads as zero.
struct Scrape {
    entries: Entries,
    families: [Family; 15],
    descs: Vec<Desc>,
}

impl Scrape {
    fn new(entries: Entries) -> Result<Self> {
        let families = families();
        let descs = families
            .iter()
            .map(|family| {
                let mut labels = vec![ENDPOINT_LABEL.to_string()];
                if matches!(family.read, Read::Errors(_)) {
                    labels.push("error_type".to_string());
                }
                Desc::new(
                    family.name.to_string(),
                    family.help.to_string(),
                    labels,
                    Default::default(),
                )
                .map_err(failed)
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            entries,
            families,
            descs,
        })
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

fn counter(entry: &str, kind: Option<&str>, value: u64) -> proto::Metric {
    let mut labels = vec![label(ENDPOINT_LABEL, entry)];
    labels.extend(kind.map(|kind| label("error_type", kind)));
    let mut metric = proto::Metric::from_label(labels);
    let mut counter = proto::Counter::default();
    counter.set_value(value as f64);
    metric.set_counter(counter);
    metric
}

/// The series of one family for one entry.
fn series(entry: &str, shards: &[Shard], read: &Read, out: &mut Vec<proto::Metric>) {
    match read {
        Read::Counter(read) => {
            out.push(counter(entry, None, sum(shards, |s| read(s).load(Relaxed))));
        }
        Read::Errors(read) => {
            for (index, kind) in ERROR_KINDS.iter().enumerate() {
                let errors = sum(shards, |s| read(s).errors[index].load(Relaxed));
                out.push(counter(entry, Some(kind), errors));
            }
        }
        Read::Gauge(read) => {
            let inflight: i64 = shards.iter().map(|s| read(s).load(Relaxed)).sum();
            let mut metric = proto::Metric::from_label(vec![label(ENDPOINT_LABEL, entry)]);
            let mut gauge = proto::Gauge::default();
            gauge.set_value(inflight.max(0) as f64);
            metric.set_gauge(gauge);
            out.push(metric);
        }
        Read::Histogram(read) => {
            // The count is the sum of the buckets read, so that `_count`
            // and the `+Inf` bucket agree whatever ends during the scrape.
            let mut histogram = proto::Histogram::default();
            let mut cumulative = 0;
            let mut buckets = Vec::with_capacity(BUCKETS.len());
            for (index, bound) in BUCKETS.iter().enumerate() {
                cumulative += sum(shards, |s| read(s).buckets[index].load(Relaxed));
                let mut bucket = proto::Bucket::default();
                bucket.set_cumulative_count(cumulative);
                bucket.set_upper_bound(*bound);
                buckets.push(bucket);
            }
            cumulative += sum(shards, |s| read(s).buckets[BUCKETS.len()].load(Relaxed));
            histogram.set_bucket(buckets);
            histogram.set_sample_count(cumulative);
            histogram.set_sample_sum(sum(shards, |s| read(s).nanos.load(Relaxed)) as f64 / 1e9);
            let mut metric = proto::Metric::from_label(vec![label(ENDPOINT_LABEL, entry)]);
            metric.set_histogram(histogram);
            out.push(metric);
        }
    }
}

impl Collector for Scrape {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        self.families
            .iter()
            .map(|family| {
                let mut metrics = Vec::new();
                for (entry, figures) in &entries {
                    if figures.parts.load(Relaxed) & family.part != 0 {
                        series(entry, &figures.shards, &family.read, &mut metrics);
                    }
                }
                let mut out = MetricFamily::default();
                out.set_name(family.name.to_string());
                out.set_help(family.help.to_string());
                out.set_field_type(match family.read {
                    Read::Counter(_) | Read::Errors(_) => MetricType::COUNTER,
                    Read::Gauge(_) => MetricType::GAUGE,
                    Read::Histogram(_) => MetricType::HISTOGRAM,
                });
                out.set_metric(metrics);
                out
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

    /// The families' names are the ones the derive refuses for a node's
    /// own metrics.
    #[test]
    fn every_family_is_reserved() {
        for family in families() {
            assert!(
                gdl_model::RUNTIME_METRICS.contains(&family.name),
                "{}",
                family.name
            );
        }
    }

    fn figures() -> &'static Figures {
        Box::leak(Box::new(Figures {
            shards: (0..1).map(|_| Shard::default()).collect(),
            parts: AtomicU8::new(REQUEST | RESPONSE | END_TO_END),
        }))
    }

    fn count(spans: &Spans) -> u64 {
        spans.buckets.iter().map(|b| b.load(Relaxed)).sum()
    }

    fn seconds(spans: &Spans) -> f64 {
        spans.nanos.load(Relaxed) as f64 / 1e9
    }

    /// A hand-off ends the request part; the response's clock starts at
    /// the first item in, not at the hand-off.
    #[tokio::test(start_paused = true)]
    async fn a_response_fed_from_another_segment_starts_at_its_first_item_in() {
        let figures = figures();
        let owner = Visit::request(figures, Instant::now(), None, true);
        let visit = Arc::clone(owner.visit());
        tokio::time::advance(Duration::from_millis(10)).await;
        visit.handed_on(true, Instant::now());
        tokio::time::advance(Duration::from_millis(100)).await;
        visit.first_in();
        tokio::time::advance(Duration::from_millis(5)).await;
        visit.first_out();
        visit.first_out();
        tokio::time::advance(Duration::from_millis(20)).await;
        owner.end(Ending::Finished);
        drop(owner);
        let shard = &figures.shards[0];
        assert_eq!(count(&shard.request_path), 1);
        assert!((seconds(&shard.request_path) - 0.010).abs() < 1e-6);
        assert!((seconds(&shard.first_item) - 0.005).abs() < 1e-6);
        assert!((seconds(&shard.after_first_item) - 0.020).abs() < 1e-6);
        assert!((seconds(&shard.end_to_end_first_item) - 0.115).abs() < 1e-6);
        assert!((seconds(&shard.end_to_end_duration) - 0.135).abs() < 1e-6);
        assert_eq!(count(&shard.first_item), 1, "one first item");
        assert_eq!(shard.inflight.load(Relaxed), 0);
        assert_eq!(shard.response.started.load(Relaxed), 1);
    }

    /// Without an item in, the clock starts where the sink started its
    /// stream; a forward run that hands on has no response; and the owner,
    /// dropped, ends the visit though a context still holds it.
    #[tokio::test(start_paused = true)]
    async fn a_response_without_an_item_in_starts_at_its_stream() {
        let figures = figures();
        let owner = Visit::request(figures, Instant::now(), None, false);
        let held = Arc::clone(owner.visit());
        tokio::time::advance(Duration::from_millis(10)).await;
        held.producing();
        let shard = &figures.shards[0];
        assert_eq!(shard.inflight.load(Relaxed), 1, "responding");
        tokio::time::advance(Duration::from_millis(30)).await;
        held.first_out();
        drop(owner);
        assert!((seconds(&shard.request_path) - 0.010).abs() < 1e-6);
        assert!((seconds(&shard.first_item) - 0.030).abs() < 1e-6);
        assert_eq!(shard.response.cancelled.load(Relaxed), 1, "dropped");
        assert_eq!(shard.inflight.load(Relaxed), 0);
        held.end(Ending::Finished);
        assert_eq!(shard.inflight.load(Relaxed), 0, "ended once");

        let handed = Visit::request(figures, Instant::now(), None, false);
        handed.visit().handed_on(false, Instant::now());
        assert_eq!(shard.inflight.load(Relaxed), 0, "left at the hand-off");
        drop(handed);
        assert_eq!(
            shard.response.started.load(Relaxed),
            1,
            "no second response"
        );
        assert_eq!(shard.request.started.load(Relaxed), 2);
        assert_eq!(shard.inflight.load(Relaxed), 0);
        assert_eq!(shard.request.cancelled.load(Relaxed), 0);
    }

    /// A response begun by a hand-off, whose first item goes out before
    /// any comes in (an operator's own), measures from the hand-off: the
    /// stamp is set before the state that makes it visible.
    #[tokio::test(start_paused = true)]
    async fn a_response_begun_by_a_hand_off_measures_from_it() {
        let figures = figures();
        let owner = Visit::request(figures, Instant::now(), None, false);
        tokio::time::advance(Duration::from_millis(10)).await;
        owner.visit().handed_on(true, Instant::now());
        tokio::time::advance(Duration::from_millis(20)).await;
        owner.visit().first_out();
        let shard = &figures.shards[0];
        assert!((seconds(&shard.first_item) - 0.020).abs() < 1e-6);
    }

    /// An inner end run the outer end spawns ends its parent's request
    /// part when its sink starts; both visits stay in flight, since both
    /// still respond.
    #[tokio::test(start_paused = true)]
    async fn a_spawned_inner_run_ends_its_parents_request() {
        let figures = figures();
        let parent = Visit::request(figures, Instant::now(), None, true);
        tokio::time::advance(Duration::from_millis(10)).await;
        let child = Visit::response(figures, None, Some(Arc::clone(parent.visit())));
        tokio::time::advance(Duration::from_millis(5)).await;
        child.visit().producing();
        let shard = &figures.shards[0];
        assert_eq!(count(&shard.request_path), 1);
        assert!((seconds(&shard.request_path) - 0.015).abs() < 1e-6);
        assert_eq!(shard.response.started.load(Relaxed), 2);
        assert_eq!(shard.inflight.load(Relaxed), 2);
        drop(child);
        drop(parent);
        assert_eq!(shard.inflight.load(Relaxed), 0);
    }

    /// A relayed error is counted end to end only; a raised one where it
    /// was raised too; a drop after the deadline is the deadline.
    #[tokio::test(start_paused = true)]
    async fn an_error_counts_where_it_was_raised() {
        let figures = figures();
        let shard = &figures.shards[0];
        let relayed = Error::node("x").relayed();
        let owner = Visit::request(figures, Instant::now(), None, true);
        owner.visit().handed_on(true, Instant::now());
        owner.end(Ending::Failed(&relayed));
        assert_eq!(shard.response.errors[0].load(Relaxed), 0);
        assert_eq!(shard.end_to_end.errors[0].load(Relaxed), 1);

        let owner = Visit::response(figures, None, None);
        owner.end(Ending::Failed(&Error::node("y")));
        assert_eq!(shard.response.errors[0].load(Relaxed), 1);

        let deadline = Deadline::after(Duration::from_millis(5));
        let owner = Visit::request(figures, Instant::now(), Some(deadline), true);
        let _held = Arc::clone(owner.visit());
        tokio::time::advance(Duration::from_millis(10)).await;
        drop(owner);
        assert_eq!(shard.request.errors[1].load(Relaxed), 1);
        assert_eq!(shard.end_to_end.errors[1].load(Relaxed), 1);
        assert_eq!(shard.request.cancelled.load(Relaxed), 0);
    }
}
