//! The distributed runtime: a graph's segments as velo nodes.

mod admission;
#[cfg(test)]
mod counting;
mod envelope;
mod fabric;
mod hop;
mod launch;
mod offers;
#[cfg(feature = "hub")]
mod pools;
mod roster;
mod runtime;
mod segment;
mod stages;

#[doc(hidden)]
pub use envelope::Head;
#[doc(hidden)]
pub use fabric::EncodedItem;
#[cfg(feature = "hub")]
pub use fabric::HubSession;
pub use fabric::bind_node;
#[cfg(feature = "hub")]
pub(crate) use fabric::bind_under_hub;
pub(crate) use fabric::shut_down;
#[cfg(feature = "hub")]
pub(crate) use fabric::{Parent, PoolDrain};
pub use hop::{DHop, Register};
pub(crate) use launch::{announce, announced, follow as follow_directory};
pub use offers::Decline;
#[cfg(feature = "hub")]
pub use pools::{FollowedPool, Pools};
#[cfg(feature = "hub")]
pub(crate) use pools::{check_policies as check_pool_policies, follow as follow_pools};
pub use roster::Roster;
pub use runtime::{Entries, RuntimeConfig};
#[cfg(feature = "fault-injection")]
pub use runtime::{Faults, Gate};
pub use segment::{Deployment, Segment, SegmentLoad, Served};
pub use stages::{BwdEnd, BwdStep, DBackward, DForward, DInner, DOperator, DSink, FwdEnd, FwdStep};
