//! The gdl graph model without the runtime: a graph's [`Manifest`], a
//! [`Placement`] of its nodes in segments, the [`Layout`] that checks one
//! against the other, and diagrams of both.
//!
//! The generator and the runtime check placements with these rules, and
//! the composer runs the same rules in a browser while a user draws, so
//! this crate builds for `wasm32-unknown-unknown`. `gdl` re-exports
//! everything here under the same names.

mod config;
mod deployment;
mod diagram;
mod hash;
mod manifest;
mod metrics;
mod placement;

use std::fmt;

use serde::{Deserialize, Serialize};

pub use config::{ConfigField, ConfigSchema, ConfigValue, GraphConfig, resolve_config};
pub use deployment::{
    DEFAULT_DEADLINE, DeploymentSpec, HUB_CONTROL_PORT, HUB_PUBLIC_PORT, SEGMENT_NAME_LIMIT,
    SegmentRegistration, segment_name_problem,
};
pub use manifest::{HopInfo, ItemInfo, Manifest, NodeInfo, TypeInfo};
pub use metrics::{
    COMPONENT_LABEL, ENDPOINT_LABEL, MetricField, MetricKind, NAMESPACE_LABEL, RUNTIME_LABELS,
    RUNTIME_METRICS, WORKER_LABEL,
};
pub use placement::{
    CrossingReport, HopLayout, Layout, Placement, PlacementError, PlacementErrors, RequestRouter,
    Run, WireReason, layout,
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
