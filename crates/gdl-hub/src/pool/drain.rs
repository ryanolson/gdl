//! The cell's drain, as its hub keeps it: once started, the cell is
//! `draining` whatever its quorum, for the life of the hub process, and
//! it ends once no instance of the cell's class is a member of the hub. A
//! drain cannot be cancelled: the instances that learned of it are already
//! on their way out, and nothing would bring them back.
//!
//! The members are counted here, from the registration hooks, rather than
//! read from the hub's admission: the hub tells its extensions of a
//! removal in reverse start order, so this extension hears of one before
//! the admission it requires has dropped the member.
//!
//! Once the drain has ended, every registration is refused: the cell's
//! record is withdrawn and its watch, from which an instance learns of the
//! drain, is closed, so an instance that joined would serve a cell nothing
//! publishes and never learn why. A registration the hub has prepared but
//! not committed counts as a member until it is committed or aborted, so
//! that the drain never ends between the two.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::watch;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::InstanceId;

pub(super) struct Drain {
    started: watch::Sender<bool>,
    /// The segments of the cell's class.
    segments: HashSet<String>,
    members: Mutex<Members>,
    /// How many members and registrations under way there are.
    present: watch::Sender<usize>,
}

/// The members of the class's segments, and the registrations under way.
#[derive(Default)]
struct Members {
    /// Each member, with its committed incarnation.
    committed: HashMap<InstanceId, Incarnation>,
    /// Registrations prepared and not yet committed or aborted.
    joining: usize,
    /// Whether the drain has ended: no registration is taken from then.
    ended: bool,
}

impl Members {
    fn present(&self) -> usize {
        self.committed.len() + self.joining
    }
}

/// A registration under way, from its preparation until it is committed
/// or aborted, when it is dropped. Public only as the extension's staged
/// registration; nothing outside the crate can name or make one.
pub struct Joining(Arc<Drain>);

impl Drop for Joining {
    fn drop(&mut self) {
        let mut members = self.0.lock();
        members.joining -= 1;
        self.0.present.send_replace(members.present());
    }
}

impl Drain {
    /// The drain of a cell whose class has `segments`.
    pub(super) fn new(segments: impl IntoIterator<Item = String>) -> Self {
        Self {
            started: watch::Sender::new(false),
            segments: segments.into_iter().collect(),
            members: Mutex::default(),
            present: watch::Sender::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Members> {
        self.members.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Start the drain; a drain already started is left as it is.
    pub(super) fn start(&self) {
        self.started
            .send_if_modified(|started| !std::mem::replace(started, true));
    }

    /// Whether the drain has started, as it changes.
    pub(super) fn started(&self) -> watch::Receiver<bool> {
        self.started.subscribe()
    }

    /// The hub prepares a registration: held until it is committed or
    /// aborted, or refused once the drain has ended.
    pub(super) fn join(self: &Arc<Self>) -> Result<Joining, &'static str> {
        let mut members = self.lock();
        if members.ended {
            return Err(
                "the cell has drained and its record is withdrawn: its hub serves it no more",
            );
        }
        members.joining += 1;
        self.present.send_replace(members.present());
        Ok(Joining(Arc::clone(self)))
    }

    /// The hub committed a registration of `id` in `segment`, as
    /// `incarnation`.
    pub(super) fn joined(&self, segment: &str, id: InstanceId, incarnation: Incarnation) {
        if !self.segments.contains(segment) {
            return;
        }
        let mut members = self.lock();
        members.committed.insert(id, incarnation);
        self.present.send_replace(members.present());
    }

    /// The hub removed an incarnation of an instance: it is no longer a
    /// member, unless a later registration of it has been committed.
    pub(super) fn removed(&self, removal: &Removal) {
        let mut members = self.lock();
        if members
            .committed
            .get(&removal.id)
            .is_some_and(|incarnation| *incarnation <= removal.incarnation)
        {
            members.committed.remove(&removal.id);
            self.present.send_replace(members.present());
        }
    }

    /// Resolves once the drain has started and no instance of the cell's
    /// class is a member of the hub or joining it; from then every
    /// registration is refused.
    pub(super) async fn ended(&self) {
        let (mut started, mut present) = (self.started(), self.present.subscribe());
        loop {
            if *started.borrow_and_update() && *present.borrow_and_update() == 0 {
                let mut members = self.lock();
                if members.present() == 0 {
                    members.ended = true;
                    return;
                }
            }
            // Both senders live as long as the drain, which outlives this.
            tokio::select! {
                _ = started.changed() => {}
                _ = present.changed() => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::Drain;
    use futures::FutureExt;
    use velo_hub::store::{Incarnation, Removal};
    use velo_hub::velo::InstanceId;

    fn removal(id: InstanceId, incarnation: Incarnation) -> Removal {
        Removal { id, incarnation }
    }

    /// The drain ends once it has started and the last member of the
    /// class's segments has gone; a member of another segment, and the
    /// removal of an earlier incarnation of a member that registered
    /// again, change nothing.
    #[test]
    fn a_drain_ends_with_its_last_member() {
        let drain = Arc::new(Drain::new(["f".to_string(), "d".to_string()]));
        let (entry, interior, other) = (
            InstanceId::new_v4(),
            InstanceId::new_v4(),
            InstanceId::new_v4(),
        );
        let (first, second) = (Incarnation::from_u64(1), Incarnation::from_u64(2));
        drain.joined("f", entry, first);
        drain.joined("d", interior, first);
        drain.joined("a", other, first);
        assert!(
            drain.ended().now_or_never().is_none(),
            "ended before it started"
        );
        drain.start();
        drain.removed(&removal(entry, first));
        drain.joined("d", interior, second);
        drain.removed(&removal(interior, first));
        assert!(
            drain.ended().now_or_never().is_none(),
            "ended with a member that registered again"
        );
        drain.removed(&removal(interior, second));
        assert!(drain.ended().now_or_never().is_some(), "not ended");
    }

    /// A drain of a hub with no member ends as it starts.
    #[test]
    fn a_drain_with_no_member_ends_as_it_starts() {
        let drain = Drain::new(["f".to_string()]);
        drain.start();
        drain.start();
        assert!(drain.ended().now_or_never().is_some());
    }

    /// A registration prepared before the drain could end keeps it from
    /// ending until it is committed or aborted; once it has ended, a
    /// registration is refused.
    #[test]
    fn a_registration_under_way_holds_the_drain_and_one_after_it_is_refused() {
        let drain = Arc::new(Drain::new(["f".to_string()]));
        drain.start();
        let joining = drain.join().expect("a registration before the end");
        assert!(
            drain.ended().now_or_never().is_none(),
            "ended with a registration under way"
        );
        let id = InstanceId::new_v4();
        drain.joined("f", id, Incarnation::from_u64(1));
        drop(joining);
        assert!(
            drain.ended().now_or_never().is_none(),
            "ended with a member"
        );
        drain.removed(&removal(id, Incarnation::from_u64(1)));
        let aborted = drain.join().expect("a registration before the end");
        drop(aborted);
        assert!(drain.ended().now_or_never().is_some(), "not ended");
        assert!(drain.join().is_err(), "a registration after the end");
    }
}
