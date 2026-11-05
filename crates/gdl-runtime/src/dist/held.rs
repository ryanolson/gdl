//! What one segment instance holds, read when its registry is scraped: the
//! roots its admission ledger holds and the ledger's limit, the egress
//! permits its streams hold, the call permits its accepted calls hold, and
//! the anchors its velo node keeps open. An
//! operator that drains the instance reads these to see the drain's
//! progress; only the process's exit completes it. A request that a source
//! calls in its own segment passes through neither admission nor egress,
//! so these do not count it (docs/src/operations/observability.md).
//! `inflight_requests` falls before them for a request that crosses: it
//! leaves that gauge when its response ends, while its root and its permit
//! stay until velo has handed the response on. Each is read from counts the instance
//! keeps anyway, so a request does nothing for them.

use std::sync::Weak;

use gdl::{Error, ErrorKind, Result};
use prometheus::Registry;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};

use super::runtime::{DECLINES, Runtime};

/// A family of what the instance holds: its name, its help, and how it
/// reads the runtime.
type Family = (&'static str, &'static str, fn(&Runtime) -> f64);

/// The families, each one series with the registry's labels only.
const FAMILIES: [Family; 5] = [
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
        "call_permits_held",
        "Call permits this instance holds: one for each call from another segment it accepted, until velo has sent its reply, or its bound and call_reply_grace have passed. A drain waits for them.",
        |rt| rt.call_limit.held() as f64,
    ),
    (
        "active_anchors",
        "Anchors this instance's velo node holds open: streams it is to receive.",
        |rt| rt.fabric.active_anchors() as f64,
    ),
];

/// The family of the offers the instance declined, by reason, and its
/// help.
const DECLINED: &str = "offers_declined_total";
const DECLINED_HELP: &str = "Offers from another pool this instance declined, by reason: full (no room, or retired), gone, instance, not_present, and budget (its pool's gate, over its budget). A load report counts only full and gone as refusals.";

/// The names of every family here.
#[cfg(test)]
pub(crate) fn names() -> Vec<&'static str> {
    let mut names: Vec<_> = FAMILIES.iter().map(|(name, ..)| *name).collect();
    names.push(DECLINED);
    names
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
        let mut descs: Vec<Desc> = FAMILIES
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
        descs.push(
            Desc::new(
                DECLINED.to_string(),
                DECLINED_HELP.to_string(),
                vec!["reason".to_string()],
                Default::default(),
            )
            .map_err(failed)?,
        );
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
        let mut families: Vec<MetricFamily> = FAMILIES
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
            .collect();
        let declined = DECLINES
            .iter()
            .map(|(decline, reason)| {
                let mut counter = proto::Counter::default();
                counter.set_value(rt.served.declined_as(*decline) as f64);
                let mut label = proto::LabelPair::default();
                label.set_name("reason".to_string());
                label.set_value((*reason).to_string());
                let mut metric = proto::Metric::from_label(vec![label]);
                metric.set_counter(counter);
                metric
            })
            .collect();
        let mut family = MetricFamily::default();
        family.set_name(DECLINED.to_string());
        family.set_help(DECLINED_HELP.to_string());
        family.set_field_type(MetricType::COUNTER);
        family.set_metric(declined);
        families.push(family);
        families
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
