//! The follower's acknowledgements to its hub that it holds no offer to a
//! follow the hub marked not offered: when it looks for one to send, which
//! are due, and what the hub's answer changes.

use std::time::Duration;

use gdl_watch::Unoffered;
use tokio::time::Instant;

use super::{Followed, Follows};
use crate::dist::fabric::Registry;

/// How often the follower looks for a follow not offered whose last lease
/// has ended, while it has one to acknowledge.
pub(in crate::dist::cells) const ACK_POLL: Duration = Duration::from_millis(50);

/// How long after its hub did not take an acknowledgement the follower
/// sends it again.
pub(in crate::dist::cells) const ACK_RETRY: Duration = Duration::from_millis(500);

impl<R: Registry> Follows<R> {
    /// When the follower looks again for a follow to acknowledge: `None`
    /// while it has none. It is set where a follow may come to need one,
    /// as a change makes it not offered or a snapshot shows it so, and by
    /// each look that leaves one waiting, so that an event never passes
    /// over the follows to find out.
    pub(in crate::dist::cells) fn next_poll(&self) -> Option<Instant> {
        self.poll
    }

    /// The acknowledgements due at `now`, each of a follow, current or
    /// earlier, that is not offered, that this instance holds no lease on,
    /// and whose revision it has yet to acknowledge: each is marked as
    /// being sent, until [`Follows::acknowledged`] says whether its hub
    /// took it. The follower looks again [`ACK_POLL`] later while a follow
    /// still waits for one.
    pub(in crate::dist::cells) fn acknowledgements(&mut self, now: Instant) -> Vec<Unoffered> {
        let due = self.followed.due(now);
        self.poll = self.followed.awaiting().then(|| now + ACK_POLL);
        due
    }

    /// The hub took `unoffered`, or did not, when it is sent again after
    /// [`ACK_RETRY`]. One of a revision the follow has since left behind
    /// changes nothing.
    pub(in crate::dist::cells) fn acknowledged(
        &mut self,
        unoffered: &Unoffered,
        taken: bool,
        now: Instant,
    ) {
        let key = (unoffered.class.clone(), unoffered.cell.clone());
        let Some(follow) = self.followed.find_mut(&key, unoffered.generation) else {
            return;
        };
        if follow.ack != Ack::Sending(unoffered.revision) {
            return;
        }
        if taken {
            follow.ack = Ack::Sent(unoffered.revision);
            return;
        }
        let again = now + ACK_RETRY;
        follow.ack = Ack::Failed(again);
        self.poll = Some(self.poll.map_or(again, |poll| poll.min(again)));
    }
}

impl Followed {
    /// Whether a follow not offered has yet to be acknowledged, or to be
    /// sent again.
    fn awaiting(&self) -> bool {
        self.follows
            .values()
            .chain(self.closing.values())
            .inspect(|_| scanned())
            .any(|follow| !follow.offered && matches!(follow.ack, Ack::None | Ack::Failed(_)))
    }

    /// The acknowledgements due at `now`, marked as being sent.
    fn due(&mut self, now: Instant) -> Vec<Unoffered> {
        let mut due = Vec::new();
        let follows = self.follows.iter_mut().chain(
            self.closing
                .iter_mut()
                .map(|((key, _), follow)| (key, follow)),
        );
        for (key, follow) in follows {
            scanned();
            let ready = match follow.ack {
                Ack::None => true,
                Ack::Failed(at) => at <= now,
                Ack::Sending(_) | Ack::Sent(_) => false,
            };
            if follow.offered || !ready || !follow.leases.idle() {
                continue;
            }
            follow.ack = Ack::Sending(follow.revision);
            due.push(Unoffered {
                class: key.0.clone(),
                cell: key.1.clone(),
                generation: follow.generation,
                revision: follow.revision,
            });
        }
        due
    }
}

/// Where a follow's acknowledgement of its current revision is, while it
/// is not offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Ack {
    /// Not sent: the follow is offered, or a lease on it is held.
    None,
    /// Sent at this revision, and not yet answered.
    Sending(u64),
    /// The hub took it at this revision.
    Sent(u64),
    /// The hub did not take it: sent again from this instant.
    Failed(Instant),
}

#[cfg(test)]
thread_local! {
    /// The follows the acknowledgements' bookkeeping on this thread has
    /// looked at.
    pub(in crate::dist::cells) static SCANNED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Count one follow looked at for an acknowledgement, in a test.
fn scanned() {
    #[cfg(test)]
    SCANNED.with(|scanned| scanned.set(scanned.get() + 1));
}
