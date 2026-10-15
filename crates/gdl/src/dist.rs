//! The distributed runtime: a graph's segments as velo nodes.

mod admission;
mod envelope;
mod fabric;
mod hop;
mod launch;
mod runtime;
mod segment;
mod stages;

pub use hop::{DHop, Register};
pub use launch::bind_node;
#[cfg(feature = "fault-injection")]
pub use runtime::Faults;
pub use runtime::{Entries, Roster, RuntimeConfig};
pub use segment::{Deployment, Segment};
pub use stages::{BwdEnd, BwdStep, DBackward, DForward, DInner, DOperator, DSink, FwdEnd, FwdStep};
