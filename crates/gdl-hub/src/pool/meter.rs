//! The hub's metrics, served on its public listener at
//! `/v1/ext/gdl-pool/metrics` in Prometheus' text format: the load summary
//! the hub last made of its pool, as its watch sends it, and, for each pool
//! it follows, whether the follow's link is up. Each is read when the route
//! is asked, from the state the hub already keeps, so nothing is recorded
//! on a watch's or a report's path.
//!
//! Beside them, the hub's process's own figures (`process_*`, and
//! `process_cgroup_*` where its cgroup v2 has them), which `gdl-cgroup`
//! reads from /proc and the cgroup's files on the same scrape.
//!
//! The names carry no prefix and the series no identity labels: a hub has
//! no `[system]` settings, unlike a segment, and its pool's class, name and
//! region label every series of the pool's.

use std::sync::Arc;

use gdl_pools::{LoadSummary, PoolRecord};
use prometheus::proto::{self, MetricFamily, MetricType};
use prometheus::{Encoder, TextEncoder};
use velo_hub::axum::Router;
use velo_hub::axum::http::{StatusCode, header};
use velo_hub::axum::response::IntoResponse;
use velo_hub::axum::routing::get;

use super::feed::Feed;
use crate::follow::Relay;

/// The route, under the extension's.
const ROUTE: &str = "/metrics";

/// The families of the pool's own load summary: name, help, and how each
/// reads the summary.
type LoadFamily = (&'static str, &'static str, fn(&LoadSummary) -> f64);

const LOADS: [LoadFamily; 4] = [
    (
        "hub_pool_load_admitted",
        "Requests the pool's entry instances held admitted, in the load summary this hub last sent of its pool.",
        |load| load.admitted as f64,
    ),
    (
        "hub_pool_load_limit",
        "The admission limit over the same entry instances, in the load summary this hub last sent of its pool.",
        |load| load.limit as f64,
    ),
    (
        "hub_pool_load_refusals",
        "Hand-offs the pool's entry instances declined in the summary's period, in the load summary this hub last sent of its pool.",
        |load| load.refusals as f64,
    ),
    (
        "hub_pool_load_instances",
        "The pool's usable entry instances, in the load summary this hub last sent of its pool.",
        |load| f64::from(load.instances),
    ),
];
const FOLLOW_UP: &str = "hub_follow_up";
const FOLLOW_UP_HELP: &str = "1 while this hub's follow of the pool has a link up: its watch's snapshot installed, and the link not ended.";

/// The route that serves the hub's metrics, for the pool of `record`.
pub(super) fn routes(record: &PoolRecord, feed: Arc<Feed>, relay: Arc<Relay>) -> Router {
    let own = [
        record.class.clone(),
        record.name.clone(),
        record.region.clone(),
    ];
    // The pool's families are made by hand; this registry holds only the
    // process's figures. A scrape reads them from a few small kernel
    // files, so the handler reads them in place, as it reads the summary.
    let process = prometheus::Registry::new();
    if let Err(error) = gdl_cgroup::register(&process) {
        tracing::warn!(%error, "the hub's metrics leave out its process's figures");
    }
    Router::new().route(
        ROUTE,
        get(move || {
            let mut families = families(&own, feed.summary(), &relay);
            families.extend(process.gather());
            async move {
                let mut text = Vec::new();
                match TextEncoder::new().encode(&families, &mut text) {
                    Ok(()) => {
                        ([(header::CONTENT_TYPE, prometheus::TEXT_FORMAT)], text).into_response()
                    }
                    Err(error) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                    }
                }
            }
        }),
    )
}

/// The families: the pool's own load summary, `load`, when it has one, and
/// a link gauge for each pool `relay` follows.
fn families(own: &[String; 3], load: Option<LoadSummary>, relay: &Relay) -> Vec<MetricFamily> {
    let mut families: Vec<MetricFamily> = match load {
        Some(load) => LOADS
            .iter()
            .map(|(name, help, read)| family(name, help, vec![gauge(own, read(&load))]))
            .collect(),
        None => Vec::new(),
    };
    let links: Vec<_> = relay
        .links()
        .into_iter()
        .map(|link| {
            let labels = [link.class, link.pool, link.region];
            gauge(&labels, f64::from(u8::from(link.up)))
        })
        .collect();
    if !links.is_empty() {
        families.push(family(FOLLOW_UP, FOLLOW_UP_HELP, links));
    }
    families
}

/// A gauge of `value` for the pool whose class, name and region are
/// `labels`.
fn gauge(labels: &[String; 3], value: f64) -> proto::Metric {
    let labels = ["class", "pool", "region"]
        .iter()
        .zip(labels)
        .map(|(name, value)| {
            let mut pair = proto::LabelPair::default();
            pair.set_name(name.to_string());
            pair.set_value(value.clone());
            pair
        })
        .collect();
    let mut metric = proto::Metric::from_label(labels);
    let mut gauge = proto::Gauge::default();
    gauge.set_value(value);
    metric.set_gauge(gauge);
    metric
}

fn family(name: &str, help: &str, metrics: Vec<proto::Metric>) -> MetricFamily {
    let mut family = MetricFamily::default();
    family.set_name(name.to_string());
    family.set_help(help.to_string());
    family.set_field_type(MetricType::GAUGE);
    family.set_metric(metrics);
    family
}
