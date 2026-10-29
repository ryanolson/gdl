//! The gdl graph model without the runtime: a graph's [`Manifest`], a
//! [`Placement`] of its nodes in segments, a spec's [`Placements`] (one
//! for each of its layouts), the [`Layout`] that checks a placement against
//! the graph, diagrams of both, and the pools that deploy a placement's
//! classes: the [`Fleet`] file and a pool's own [`PoolSettings`].
//!
//! The generator and the runtime check placements with these rules. The
//! crate builds for `wasm32-unknown-unknown` so that the same rules can
//! run in a browser; today the composer checks a draft on its server.
//! `gdl` re-exports the configuration and metric metadata, and
//! `gdl-runtime` the placement types it reads; the rest is here.

mod config;
mod deployment;
mod diagram;
mod encoding;
mod fleet;
mod hash;
mod manifest;
mod metrics;
mod placement;
mod pool;
mod problem;
mod properties;

use std::fmt;

use serde::{Deserialize, Serialize};

pub use config::{ConfigField, ConfigSchema, ConfigValue, GraphConfig, resolve_config};
pub use deployment::{
    DEFAULT_DEADLINE, DeploymentSpec, HUB_CONTROL_PORT, HUB_PUBLIC_PORT, SEGMENT_NAME_LIMIT,
    SegmentPhase, SegmentRegistration, segment_name_problem, segment_service,
};
pub use encoding::Encoding;
pub use fleet::{Fleet, FleetError, FleetErrors, FleetTable};
pub use manifest::{HopInfo, ItemInfo, Manifest, NodeInfo, TypeInfo};
pub use metrics::{
    COMPONENT_LABEL, ENDPOINT_LABEL, MetricField, MetricKind, NAMESPACE_LABEL, RUNTIME_LABELS,
    RUNTIME_METRICS, WORKER_LABEL,
};
pub use placement::{
    ClassBoundary, CrossingReport, HopLayout, Layout, Leg, Placement, PlacementError,
    PlacementErrors, Placements, PoolPolicy, RequestRouter, Run, SELECTORS, SelectorInfo, WireEdge,
    WireReason, check_class_name, check_layout_name, check_pool_policy_name, check_selector_name,
    layout,
};
pub use pool::{
    DirectoryBackend, HubTiming, PoolSettings, WatchCaps, check_hub_address, check_hub_timing,
    check_pool_name, check_region_name, check_relay_lease, check_summary, missing_parent_hub,
    missing_state_dir, pool_variable,
};
pub use problem::{Origin, Problem, Problems, read_toml};
pub use properties::{
    EntryState, PropertyEntry, PropertyKey, PropertyLists, check_property_kind, read_each,
};

/// Support for `derive(gdl::Config)`. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::config::ConfigDerived;
}

/// The role a node plays in a graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// The entry point, optionally hosted by a frontend implementation.
    Source,
    /// Sees both the request and the response.
    Operator,
    /// Transforms the request only.
    Forward,
    /// The innermost node.
    Sink,
    /// Transforms the response stream only.
    Backward,
}

impl Role {
    /// The role's name, as graph files and catalogues spell it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Operator => "operator",
            Self::Forward => "forward",
            Self::Sink => "sink",
            Self::Backward => "backward",
        }
    }

    /// The role trait's ports, its associated types, in the order the trait
    /// declares them. A frontend implements the source role.
    pub fn ports(self) -> &'static [&'static str] {
        match self {
            Self::Source => &["Req", "Item"],
            Self::Operator => &["UpReq", "UpItem", "DownReq", "DownItem"],
            Self::Forward | Self::Backward => &["In", "Out"],
            Self::Sink => &["Req", "Item"],
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A model that cannot be read or drawn: a manifest or placement file that
/// does not parse, or a diagram of a layout checked against another
/// contract. `gdl` turns it into an error of kind `Placement`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}
