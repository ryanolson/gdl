//! The gdl graph model without the runtime: a graph's [`Manifest`], a
//! [`Placement`] of its nodes in segments, the [`Layout`] that checks one
//! against the other, and diagrams of both.
//!
//! The generator and the runtime check placements with these rules, and
//! the composer runs the same rules in a browser while a user draws, so
//! this crate builds for `wasm32-unknown-unknown`. `gdl` re-exports
//! everything here under the same names.

mod diagram;
mod hash;
mod manifest;
mod placement;

use std::fmt;

use serde::{Deserialize, Serialize};

pub use manifest::{HopInfo, ItemInfo, Manifest, NodeInfo, TypeInfo};
pub use placement::{
    CrossingReport, HopLayout, Layout, Placement, PlacementError, PlacementErrors, Run, WireReason,
    layout,
};

/// The role a node plays in a graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// The entry point. It has no implementation.
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
