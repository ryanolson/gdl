//! `pool_record_writes_total`: each table's writes by how they ended,
//! read from its counts when the registry is scraped.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};

use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};

use super::Binding;

const WRITES: &str = "pool_record_writes_total";
const WRITES_HELP: &str = "Writes to a table of the pool hub's records, by how each ended.";

/// The outcomes, by their label.
const OUTCOMES: [&str; 4] = ["applied", "refused", "dropped", "unanswered"];

pub(super) struct Scrape {
    binding: Arc<Binding>,
    desc: Desc,
}

impl Scrape {
    pub(super) fn new(binding: Arc<Binding>) -> prometheus::Result<Self> {
        let desc = Desc::new(
            WRITES.to_string(),
            WRITES_HELP.to_string(),
            vec!["table".to_string(), "outcome".to_string()],
            Default::default(),
        )?;
        Ok(Self { binding, desc })
    }
}

impl Collector for Scrape {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // Two handles on one table are one series.
        let mut tables: BTreeMap<Box<str>, [u64; OUTCOMES.len()]> = BTreeMap::new();
        for (table, counts) in self
            .binding
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            let sums = tables.entry(table.clone()).or_default();
            for (sum, count) in sums.iter_mut().zip([
                &counts.applied,
                &counts.refused,
                &counts.dropped,
                &counts.unanswered,
            ]) {
                *sum += count.load(Ordering::Relaxed);
            }
        }
        let mut metrics = Vec::with_capacity(tables.len() * OUTCOMES.len());
        for (table, sums) in &tables {
            for (outcome, value) in OUTCOMES.iter().zip(sums) {
                let mut metric = proto::Metric::from_label(vec![
                    label("table", table),
                    label("outcome", outcome),
                ]);
                let mut counter = proto::Counter::default();
                counter.set_value(*value as f64);
                metric.set_counter(counter);
                metrics.push(metric);
            }
        }
        let mut family = MetricFamily::default();
        family.set_name(WRITES.to_string());
        family.set_help(WRITES_HELP.to_string());
        family.set_field_type(MetricType::COUNTER);
        family.set_metric(metrics);
        vec![family]
    }
}

fn label(name: &str, value: &str) -> proto::LabelPair {
    let mut pair = proto::LabelPair::default();
    pair.set_name(name.to_string());
    pair.set_value(value.to_string());
    pair
}
