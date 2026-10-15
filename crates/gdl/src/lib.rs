//! A graph definition language and runtime for distributed request/response
//! streaming pipelines.
//!
//! A graph is a source and a nested chain of hops. Each hop has a forward
//! chain that transforms the request, an inner end (an [`Operator`] or a
//! [`Sink`]), and a backward chain that transforms the response stream. An
//! operator sees both directions and calls the rest of the graph through
//! [`Next`]; forward and backward nodes see one direction each.
//!
//! The design and its rulings are in `agent-docs/plan.md`.

mod compose;
mod cx;
mod decl;
mod error;
mod hash;
mod items;
mod manifest;
mod placement;
mod role;
mod wire;

pub use compose::{BackwardChain, ForwardChain, Hop, Inner, OperatorStage, SinkStage, Source};
pub use cx::{Cx, Deadline, RequestId};
pub use decl::{
    Absent, BackwardDecl, ForwardDecl, NodeDecl, OperatorDecl, Role, SinkDecl, SourceDecl,
};
pub use error::{Error, ErrorKind, Result};
pub use items::{Items, items};
pub use manifest::{HopInfo, Manifest, NodeInfo, TypeInfo};
pub use placement::{
    CrossingReport, HopLayout, Layout, Placement, PlacementError, PlacementErrors, Run, WireReason,
    layout,
};
pub use role::{Backward, Build, Forward, Next, Operator, Sink};
pub use wire::Codec;

/// Support for macro expansions. Not a stable interface.
#[doc(hidden)]
pub mod __private {
    pub use crate::wire::probe::{Probe, ViaLocal, ViaWire};
}
