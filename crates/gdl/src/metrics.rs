//! A node's own metrics: the [`Metrics`] trait that `derive(gdl::Metrics)`
//! implements, and what its expansion calls to build and register each
//! field.

use prometheus::core::Collector;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

use gdl_model::{ENDPOINT_LABEL, MetricField};

/// A node's metrics: a struct of prometheus metrics, registered by the
/// runtime and handed to the node's [`Build::build`](crate::Build::build).
///
/// Implemented only by `derive(gdl::Metrics)`, and by `()` for a node
/// without metrics. The source reader takes a metrics type's definitions
/// from the derive's attributes, without building the crate, so a
/// hand-written impl could register metrics the catalogue never shows.
pub trait Metrics: MetricsDerived + Sized + Send + Sync + 'static {
    /// Each metric, in field order, as the catalogue shows it.
    fn definitions() -> Vec<MetricField>;

    /// Build every metric with node `node`'s `dynamo_endpoint` label, and
    /// register each in `registry`.
    fn register(registry: &Registry, node: &str) -> prometheus::Result<Self>;
}

/// The seal on [`Metrics`], which only `derive(gdl::Metrics)` implements.
#[diagnostic::on_unimplemented(
    message = "`{Self}` implements `Metrics` by hand",
    note = "derive it with `#[derive(gdl::Metrics)]`: the source reader reads only the derive"
)]
pub trait MetricsDerived {}

impl MetricsDerived for () {}

impl Metrics for () {
    fn definitions() -> Vec<MetricField> {
        Vec::new()
    }

    fn register(_registry: &Registry, _node: &str) -> prometheus::Result<Self> {
        Ok(())
    }
}

/// Build the metric `definition` describes for node `node`, and register
/// it in `registry`. What the derive's expansion calls for each field; the
/// field's type picks the [`Construct`] impl.
#[doc(hidden)]
pub fn metric<M>(registry: &Registry, node: &str, definition: &MetricField) -> prometheus::Result<M>
where
    M: Construct + Collector + Clone + 'static,
{
    let metric = M::construct(definition, node)?;
    registry.register(Box::new(metric.clone()))?;
    Ok(metric)
}

/// A prometheus metric type a [`Metrics`] field may have.
#[doc(hidden)]
pub trait Construct: Sized {
    /// The metric `definition` describes, labelled with node `node`.
    fn construct(definition: &MetricField, node: &str) -> prometheus::Result<Self>;
}

fn opts(definition: &MetricField, node: &str) -> Opts {
    Opts::new(&definition.name, &definition.help).const_label(ENDPOINT_LABEL, node)
}

fn histogram_opts(definition: &MetricField, node: &str) -> HistogramOpts {
    HistogramOpts::new(&definition.name, &definition.help)
        .const_label(ENDPOINT_LABEL, node)
        .buckets(definition.buckets.clone())
}

fn labels(definition: &MetricField) -> Vec<&str> {
    definition.labels.iter().map(String::as_str).collect()
}

macro_rules! single {
    ($($ty:ty => $opts:ident),* $(,)?) => {$(
        impl Construct for $ty {
            fn construct(definition: &MetricField, node: &str) -> prometheus::Result<Self> {
                <$ty>::with_opts($opts(definition, node))
            }
        }
    )*};
}

macro_rules! vector {
    ($($ty:ty => $opts:ident),* $(,)?) => {$(
        impl Construct for $ty {
            fn construct(definition: &MetricField, node: &str) -> prometheus::Result<Self> {
                <$ty>::new($opts(definition, node), &labels(definition))
            }
        }
    )*};
}

single!(
    IntCounter => opts,
    Counter => opts,
    IntGauge => opts,
    Gauge => opts,
    Histogram => histogram_opts,
);

vector!(
    IntCounterVec => opts,
    CounterVec => opts,
    IntGaugeVec => opts,
    GaugeVec => opts,
    HistogramVec => histogram_opts,
);
