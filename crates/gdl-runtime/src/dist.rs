//! The distributed runtime: a graph's segments as velo nodes.

mod admission;
mod envelope;
mod fabric;
mod hop;
mod launch;
mod roster;
mod runtime;
mod segment;
mod stages;

#[cfg(feature = "hub")]
pub use fabric::HubSession;
pub use fabric::bind_node;
#[cfg(feature = "hub")]
pub(crate) use fabric::bind_under_hub;
pub(crate) use fabric::shut_down;
pub use hop::{DHop, Register};
pub(crate) use launch::{announce, announced, follow as follow_directory};
pub use roster::Roster;
pub use runtime::{Entries, RuntimeConfig};
#[cfg(feature = "fault-injection")]
pub use runtime::{Faults, Gate};
pub use segment::{Deployment, Segment, Served};
pub use stages::{BwdEnd, BwdStep, DBackward, DForward, DInner, DOperator, DSink, FwdEnd, FwdStep};
