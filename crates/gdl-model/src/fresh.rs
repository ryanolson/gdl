//! What a parent pool's hub answers an operator's command for a child
//! whose record is still fresh: shared by the hub that writes it and the
//! deploy tools that read it, none of which links the transport.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The body of the `409 Conflict` that refuses a command for a child whose
/// record is fresh: `POST /children/{class}/{pool}/withdraw`, or
/// `scale-to-zero`, on the parent hub's control listener. Any other `409`
/// from those routes is plain text, so a caller that cannot read this
/// body treats the answer as a refusal, not as a reason to ask again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshChild {
    /// One line that names the pool and says when its lease passes.
    pub error: String,
    /// How long until the record's lease passes on the parent hub's clock,
    /// in milliseconds, unless the hub hears it again.
    pub retry_after_ms: u64,
    /// The record's lease, in milliseconds.
    pub lease_ms: u64,
}

impl FreshChild {
    /// How long a caller waits before it asks once more: until the lease
    /// passes, and never more than one lease.
    pub fn wait(&self) -> Duration {
        Duration::from_millis(self.retry_after_ms.min(self.lease_ms))
    }
}
