//! A node's own metrics: the [`Metrics`] trait that `derive(gdl::Metrics)`
//! implements, and what its expansion calls to build and register each
//! field.

use std::collections::HashMap;

use prometheus::core::Collector;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

use gdl_model::{ENDPOINT_LABEL, MetricField};

mod scrape;

pub use scrape::{GaugeReadings, ScrapeGaugeVec};

/// A node's metrics: a struct of prometheus metrics and [`ScrapeGaugeVec`]s,
/// registered by the runtime and handed to the node's
/// [`Build::build`](crate::Build::build).
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

    /// Build every metric with `labels` as its const labels, and register
    /// each in `registry`. [`register`](Self::register) is this with
    /// `dynamo_endpoint` set to the node.
    ///
    /// A host whose registry adds the identity labels itself, as
    /// `Registry::new_custom` does, passes `&[]`. A registry does not show
    /// the labels it adds, so a name in both shows twice in each series.
    ///
    /// # Errors
    ///
    /// When `labels` names a label twice, or a label of a field: nothing is
    /// then registered. Otherwise as [`register`](Self::register). A type
    /// that keeps this default applies `dynamo_endpoint` through `register`,
    /// and refuses any other `labels`.
    fn register_with(registry: &Registry, labels: &[(&str, &str)]) -> prometheus::Result<Self> {
        match labels {
            [(ENDPOINT_LABEL, node)] => Self::register(registry, node),
            _ => Err(prometheus::Error::Msg(format!(
                "`{}` applies only the `{ENDPOINT_LABEL}` label: derive it with `#[derive(gdl::Metrics)]`",
                std::any::type_name::<Self>(),
            ))),
        }
    }
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

    fn register_with(_registry: &Registry, _labels: &[(&str, &str)]) -> prometheus::Result<Self> {
        Ok(())
    }
}

/// Refuse `labels` that a series could not carry as given, before anything
/// is registered. Prometheus takes const labels as a map, where a later
/// value would replace an earlier, and keeps them apart from a field's own
/// labels, so a shared name would show twice in each series. What the
/// derive's expansion calls first.
#[doc(hidden)]
pub fn check_labels(
    definitions: &[MetricField],
    labels: &[(&str, &str)],
) -> prometheus::Result<()> {
    for (index, (name, _)) in labels.iter().enumerate() {
        if labels[..index].iter().any(|(earlier, _)| earlier == name) {
            return Err(prometheus::Error::Msg(format!(
                "the label `{name}` is given twice"
            )));
        }
        if let Some(field) = definitions
            .iter()
            .find(|field| field.labels.iter().any(|label| label == name))
        {
            return Err(prometheus::Error::Msg(format!(
                "the label `{name}` is one of the labels of `{}`",
                field.name
            )));
        }
    }
    Ok(())
}

/// Build the metric `definition` describes with `labels`, and register it
/// in `registry`. What the derive's expansion calls for each field; the
/// field's type picks the [`Construct`] impl.
#[doc(hidden)]
pub fn metric<M>(
    registry: &Registry,
    labels: &[(&str, &str)],
    definition: &MetricField,
) -> prometheus::Result<M>
where
    M: Construct + Collector + Clone + 'static,
{
    let metric = M::construct(definition, labels)?;
    registry.register(Box::new(metric.clone()))?;
    Ok(metric)
}

/// A metric type a [`Metrics`] field may have: a prometheus metric, or a
/// [`ScrapeGaugeVec`].
#[doc(hidden)]
pub trait Construct: Sized {
    /// The metric `definition` describes, with `labels` as const labels.
    fn construct(definition: &MetricField, labels: &[(&str, &str)]) -> prometheus::Result<Self>;
}

fn opts(definition: &MetricField, labels: &[(&str, &str)]) -> Opts {
    Opts::new(&definition.name, &definition.help).const_labels(const_labels(labels))
}

fn histogram_opts(definition: &MetricField, labels: &[(&str, &str)]) -> HistogramOpts {
    HistogramOpts::new(&definition.name, &definition.help)
        .const_labels(const_labels(labels))
        .buckets(definition.buckets.clone())
}

fn const_labels(labels: &[(&str, &str)]) -> HashMap<String, String> {
    labels
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

fn label_names(definition: &MetricField) -> Vec<&str> {
    definition.labels.iter().map(String::as_str).collect()
}

macro_rules! single {
    ($($ty:ty => $opts:ident),* $(,)?) => {$(
        impl Construct for $ty {
            fn construct(
                definition: &MetricField,
                labels: &[(&str, &str)],
            ) -> prometheus::Result<Self> {
                <$ty>::with_opts($opts(definition, labels))
            }
        }
    )*};
}

macro_rules! vector {
    ($($ty:ty => $opts:ident),* $(,)?) => {$(
        impl Construct for $ty {
            fn construct(
                definition: &MetricField,
                labels: &[(&str, &str)],
            ) -> prometheus::Result<Self> {
                <$ty>::new($opts(definition, labels), &label_names(definition))
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
