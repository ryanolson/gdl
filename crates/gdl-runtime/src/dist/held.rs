//! What one segment instance holds, read when its registry is scraped: the
//! roots its admission ledger holds and the ledger's limit, the egress
//! permits its streams hold, and the anchors its velo node keeps open. An
//! operator that drains the instance before it deletes it reads these to
//! see it empty. `inflight_requests` cannot say so: a request leaves that
//! gauge when its response ends, while its root and its permit stay until
//! velo has handed the response on. Each is read from counts the instance
//! keeps anyway, so a request does nothing for them.

use std::sync::Weak;

use gdl::{Error, ErrorKind, Result};
use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};

use super::runtime::Runtime;

/// A family of what the instance holds: its name, its help, and how it
/// reads the runtime.
type Family = (&'static str, &'static str, fn(&Runtime) -> f64);

/// The families, each one series with the registry's labels only.
const FAMILIES: [Family; 4] = [
    (
        "admission_admitted",
        "Root requests this instance's admission ledger holds: each until its last stream here is retired by velo.",
        // The sharded count a load report reads, not the ledger's map,
        // whose length would lock every shard a request writes.
        |rt| rt.admission.load().0 as f64,
    ),
    (
        "admission_limit",
        "The most root requests this instance's admission ledger holds at once.",
        |rt| rt.admission.load().1 as f64,
    ),
    (
        "egress_in_use",
        "Egress permits this instance holds: one for each stream it produces, until velo has handed its output on.",
        |rt| rt.egress.in_use() as f64,
    ),
    (
        "active_anchors",
        "Anchors this instance's velo node holds open: streams it is to receive.",
        |rt| rt.fabric.active_anchors() as f64,
    ),
];

/// The names of every family here.
#[cfg(test)]
pub(crate) fn names() -> Vec<&'static str> {
    FAMILIES.iter().map(|(name, ..)| *name).collect()
}

/// What one instance holds, read from its runtime at each scrape. It holds
/// the runtime weakly, so that the registry, which lives as long as the
/// process, does not keep the velo node alive; once the runtime is gone it
/// has no series.
pub(crate) struct Held {
    rt: Weak<Runtime>,
    descs: Vec<Desc>,
}

impl Held {
    /// Register what the instance of `rt` holds in `registry`.
    pub(crate) fn register(registry: &Registry, rt: Weak<Runtime>) -> Result<()> {
        let descs = FAMILIES
            .iter()
            .map(|(name, help, _)| {
                Desc::new(
                    (*name).to_string(),
                    (*help).to_string(),
                    Vec::new(),
                    Default::default(),
                )
            })
            .collect::<std::result::Result<_, _>>()
            .map_err(failed)?;
        registry
            .register(Box::new(Self { rt, descs }))
            .map_err(failed)
    }
}

impl Collector for Held {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let Some(rt) = self.rt.upgrade() else {
            return Vec::new();
        };
        FAMILIES
            .iter()
            .map(|(name, help, read)| {
                let mut gauge = proto::Gauge::default();
                gauge.set_value(read(&rt));
                let mut metric = proto::Metric::default();
                metric.set_gauge(gauge);
                let mut family = MetricFamily::default();
                family.set_name((*name).to_string());
                family.set_help((*help).to_string());
                family.set_field_type(MetricType::GAUGE);
                family.set_metric(vec![metric]);
                family
            })
            .collect()
    }
}

fn failed(error: prometheus::Error) -> Error {
    Error::new(
        ErrorKind::Placement,
        format!("registering what the instance holds: {error}"),
    )
}

#[cfg(test)]
mod tests {
    /// A node's metric cannot take the name of one of these.
    #[test]
    fn the_names_are_reserved() {
        for name in super::names() {
            assert!(gdl_model::RUNTIME_METRICS.contains(&name), "{name}");
        }
    }
}
