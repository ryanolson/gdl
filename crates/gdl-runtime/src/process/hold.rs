//! The hold: an instance its pool's hub asked to retire, once it has
//! drained and its leave hooks have ended, stays a member of its hub with
//! its velo node up and takes no work, until something ends the hold. The
//! controller that asked reads `drained` from the hub, and removes the
//! instance when it is ready to. See docs/src/concepts/lifecycle.md.

use std::time::Duration;

use gdl::Result;

use super::leave::Stopping;
use super::{DRAIN_POLL, Drains, Launch};

/// How long past the drain's bound a hold lasts by default, from the
/// stop. A controller removes the instance at the latest at a bound that
/// matches the drain's, counted from before the stop, so this only has to
/// cover what comes after that bound: a reconcile, a ReplicaSet, and a
/// kubelet. The value is the margin the render adds to a pod's grace. A
/// hold that ends by itself exits the process, and its pod starts it
/// again, so the default must outlast the controller.
pub(super) const MARGIN: Duration = Duration::from_secs(30);

impl Launch {
    /// Hold this instance, whose stop a retire request began, until a stop
    /// signal, its pool's drain, a lost lease, or the hold's bound. A
    /// signal that came during the drain ends it at once. Meanwhile, once
    /// it holds no request, it says that it drained, retried until the
    /// hold ends: a drain cut at its bound said nothing, a hub that did
    /// not answer has not heard, and the controller reads only this. The
    /// lease's error is its own; every other end is a clean one.
    pub(super) async fn hold(&mut self, segment: &dyn Drains, stopping: &Stopping) -> Result<()> {
        let process = self.process.clone();
        let name = self.segment;
        let until = stopping.began() + self.hold;
        let draining = self.pool_draining();
        let lost = self.lease_lost();
        let saying = async {
            while segment.admitted() != 0 {
                tokio::time::sleep(DRAIN_POLL).await;
            }
            stopping.drained();
            self.announce_drained(until).await;
            std::future::pending::<()>().await;
        };
        tokio::select! {
            biased;
            () = process.stops(1) => Ok(()),
            error = lost => Err(error),
            () = draining => Ok(()),
            () = tokio::time::sleep_until(until) => {
                eprintln!("{name}: the hold's bound passed");
                Ok(())
            }
            () = saying => Ok(()),
        }
    }
}
