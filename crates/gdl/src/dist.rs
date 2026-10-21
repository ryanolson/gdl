//! The distributed runtime: a graph's segments as velo nodes.

mod admission;
mod envelope;
mod fabric;
mod hop;
mod launch;
mod meter;
mod runtime;
mod segment;
mod stages;

pub use fabric::bind_node;
pub(crate) use fabric::shut_down;
#[cfg(feature = "hub")]
pub use fabric::{HubSession, bind_container_node};
pub use hop::{DHop, Register};
pub(crate) use launch::{announce, announced};
pub(crate) use meter::{Reading, ended};
pub use runtime::{Entries, Roster, RuntimeConfig};
#[cfg(feature = "fault-injection")]
pub use runtime::{Faults, Gate};
pub use segment::{Deployment, Segment, Served};
pub use stages::{BwdEnd, BwdStep, DBackward, DForward, DInner, DOperator, DSink, FwdEnd, FwdStep};
