//! A gauge vector whose values a node computes when the registry is
//! scraped, from a closure over the node's state.

use std::sync::{Arc, Mutex, PoisonError};

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{GaugeVec, Opts};

use gdl_model::MetricField;

use super::{Construct, labels, opts};

/// A gauge vector that a node fills when the registry is scraped, from a
/// closure over state it already holds, such as each model's status.
///
/// A field of a `derive(gdl::Metrics)` struct. It needs
/// `#[gdl(labels = [...])]`, and the catalogue lists it as a gauge. The
/// segment registers it before `build`, so the node gives it its closure
/// there, with [`fill_with`](Self::fill_with). Each scrape runs the closure
/// once, on an empty vector. A request never runs it.
///
/// The closure must return promptly: it runs inside the registry's
/// gather, and holds the registry's lock while it runs. The system
/// listener runs it on a blocking thread, up to 4 scrapes at once, and
/// answers with 503 a scrape that takes longer than 5 s or finds 4
/// running. A scrape answered so still runs until the closure returns.
/// Scrapes at once run the closure at once, each on its own vector, so
/// their values do not mix, and the closure must be safe to run
/// concurrently. It must not await, and may take a `blocking_read` of a
/// tokio lock.
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicU32, Ordering};
///
/// #[derive(gdl::Metrics)]
/// struct QueueMetrics {
///     /// Sequences waiting, by queue.
///     #[gdl(labels = ["queue"])]
///     queued_sequences: gdl::ScrapeGaugeVec,
/// }
///
/// let registry = gdl::prometheus::Registry::new();
/// let metrics = <QueueMetrics as gdl::Metrics>::register(&registry, "Queue")?;
/// let waiting = Arc::new(AtomicU32::new(3));
/// let state = waiting.clone();
/// metrics.queued_sequences.fill_with(move |gauges| {
///     let _ = gauges.set(&["prefill"], f64::from(state.load(Ordering::Relaxed)));
/// });
/// let mut text = String::new();
/// gdl::prometheus::TextEncoder::new().encode_utf8(&registry.gather(), &mut text)?;
/// assert!(text.contains(r#"queued_sequences{dynamo_endpoint="Queue",queue="prefill"} 3"#));
/// # Ok::<(), gdl::prometheus::Error>(())
/// ```
#[derive(Clone)]
pub struct ScrapeGaugeVec {
    shared: Arc<Shared>,
}

impl ScrapeGaugeVec {
    /// Run `fill` at each scrape to set this gauge's values. Until the
    /// first call, a scrape has no series of it. A later call replaces the
    /// closure.
    pub fn fill_with(&self, fill: impl Fn(&mut GaugeReadings<'_>) + Send + Sync + 'static) {
        *self.shared.lock() = Some(Arc::new(fill));
    }
}

/// The values a [`ScrapeGaugeVec`]'s closure sets for one scrape.
pub struct GaugeReadings<'a> {
    vec: &'a GaugeVec,
}

impl GaugeReadings<'_> {
    /// Set the series of `labels`, given in the order the field's
    /// `#[gdl(labels = [...])]` names them, to `value`.
    ///
    /// # Errors
    ///
    /// When `labels` does not have one value for each label. The scrape
    /// goes on without that series.
    pub fn set(&mut self, labels: &[&str], value: f64) -> prometheus::Result<()> {
        self.vec.get_metric_with_label_values(labels)?.set(value);
        Ok(())
    }
}

type Fill = dyn Fn(&mut GaugeReadings<'_>) + Send + Sync;

/// What the node's copy and the registry's share.
struct Shared {
    opts: Opts,
    labels: Vec<String>,
    /// Never set: it gives the registry the gauge's descriptor, built from
    /// the same options as each scrape's vector.
    template: GaugeVec,
    fill: Mutex<Option<Arc<Fill>>>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Arc<Fill>>> {
        self.fill.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Construct for ScrapeGaugeVec {
    fn construct(definition: &MetricField, node: &str) -> prometheus::Result<Self> {
        let opts = opts(definition, node);
        let template = GaugeVec::new(opts.clone(), &labels(definition))?;
        Ok(Self {
            shared: Arc::new(Shared {
                opts,
                labels: definition.labels.clone(),
                template,
                fill: Mutex::new(None),
            }),
        })
    }
}

impl Collector for ScrapeGaugeVec {
    fn desc(&self) -> Vec<&Desc> {
        self.shared.template.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // The lock is held only to take the closure, so a closure that
        // blocks or panics holds up no `fill_with`.
        let Some(fill) = self.shared.lock().clone() else {
            return Vec::new();
        };
        let labels: Vec<&str> = self.shared.labels.iter().map(String::as_str).collect();
        let Ok(vec) = GaugeVec::new(self.shared.opts.clone(), &labels) else {
            return Vec::new();
        };
        fill(&mut GaugeReadings { vec: &vec });
        vec.collect()
    }
}
