//! The metric names a segment's registry refuses under its metrics prefix.
//!
//! The process's own figures (`gdl-cgroup`'s `process_*`) are served
//! unprefixed beside the registry. A metric whose prefixed name is one of
//! them would put that family in one scrape twice, which breaks the
//! scrape: under `metrics_prefix = "process"`, a node's
//! `cpu_seconds_total` is served as `process_cpu_seconds_total`. One rule
//! answers both places that refuse it: [`Reserved::refusal`], which the
//! segment binary's build asks first, to name the prefix and the figure,
//! and the collector itself, which the registry holds so that it refuses
//! every other registration of such a name.

use std::collections::BTreeMap;
use std::sync::Arc;

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;

/// What [`Reserved`]'s descriptors say, if a scrape ever showed them.
const HELP: &str = "Reserved: under the metrics prefix, this name is one of the process's own figures, served beside the registry.";

/// The names a metric registered under `prefix` cannot take, each with the
/// process figure it would be served as. As a [`Collector`], it describes
/// those names and collects nothing.
#[derive(Clone)]
pub(crate) struct Reserved {
    prefix: Option<String>,
    /// Each refused name, with the figure it would be served as.
    names: Arc<BTreeMap<String, String>>,
    descs: Vec<Desc>,
}

impl Reserved {
    /// The names refused under `prefix`: each figure's name with `prefix`
    /// and `_` taken off, or whole with no prefix.
    pub(crate) fn new(prefix: Option<&str>) -> Self {
        let names: BTreeMap<String, String> = gdl_cgroup::families()
            .into_iter()
            .filter_map(|family| {
                let name = match prefix {
                    Some(prefix) => family.strip_prefix(prefix)?.strip_prefix('_')?,
                    None => &family,
                };
                Some((name.to_string(), family.clone()))
            })
            .collect();
        let descs = names
            .keys()
            .filter_map(|name| {
                Desc::new(name.clone(), HELP.into(), Vec::new(), Default::default()).ok()
            })
            .collect();
        Self {
            prefix: prefix.map(str::to_string),
            names: Arc::new(names),
            descs,
        }
    }

    /// Whether no name is refused, as under the default prefix.
    pub(crate) fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Why a metric named `name` cannot register, if it cannot.
    pub(crate) fn refusal(&self, name: &str) -> Option<String> {
        let family = self.names.get(name)?;
        let under = match &self.prefix {
            Some(prefix) => format!("under `metrics_prefix` {prefix:?}"),
            None => "with no `metrics_prefix`".to_string(),
        };
        Some(format!(
            "its metric `{name}` is served as `{family}` {under}, the name of one of the process's own figures: name the metric otherwise, or choose another prefix"
        ))
    }
}

impl Collector for Reserved {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Reserved;

    /// velo's families are served unprefixed beside the registry, under
    /// names that start with `velo_`: a node metric served under such a
    /// name is refused, under any prefix, and one that is not registers.
    #[test]
    #[ignore = "velo metrics: a name served as velo's is not refused"]
    fn a_name_served_as_velos_is_refused() {
        let refused = |prefix: Option<&str>, name: &str| Reserved::new(prefix).refusal(name);
        let empty = refused(None, "velo_streaming_active_anchors").expect("refused");
        assert!(empty.contains("velo"), "{empty}");
        assert!(refused(Some("velo"), "tokens_total").is_some());
        assert!(refused(Some("velo_x"), "tokens_total").is_some());
        assert!(refused(Some("dynamo_component"), "velo_frames_total").is_none());
        assert!(refused(Some("velocity"), "tokens_total").is_none());
        assert!(refused(None, "tokens_total").is_none());
    }
}
