//! The follower's acknowledgements to its hub that it avoids a followed
//! pool's retired entry instance: once its tables hold the instance
//! retired, this chooser never picks it again, and its hub tells the
//! pool's hub once every chooser has said so, so that the retiree's stop
//! need not wait out its bound.
//!
//! An acknowledgement is queued as its retirement is published, never
//! found by a pass over the follows: a retirement the relay sends after
//! its snapshot, each retired instance of a snapshot that replaces what
//! the follower held, whether or not it acknowledged it before, since its
//! hub may have restarted, and each of a pool read from a watch of its
//! own. A choice made from a table loaded before the retirement is checked
//! again when it returns ([`super::super::Crossing::choose`]), so it does
//! not pick the instance either. It promises nothing about the hand-offs
//! already sent: the retiree's setup timeout covers those. One its hub did
//! not take is sent again [`super::ack::ACK_RETRY`] later, for as long as
//! the follow holds the instance retired.

use std::collections::VecDeque;

use gdl_watch::Avoids;
use tokio::time::Instant;
use velo::InstanceId;

use super::ack::ACK_RETRY;
use super::{Follows, Key};
use crate::dist::fabric::Registry;

/// The acknowledgements to send: those due since they were queued, and
/// those to send again, each in the order it falls due.
#[derive(Default)]
pub(super) struct Avoiding {
    /// Due from the instant each was queued.
    fresh: VecDeque<(Instant, Avoids)>,
    /// Due again from the instant with each, all [`ACK_RETRY`] after the
    /// hub did not take them, so in order as the queue grows.
    again: VecDeque<(Instant, Avoids)>,
}

impl Avoiding {
    /// When the next acknowledgement falls due, if one does.
    pub(super) fn next(&self) -> Option<Instant> {
        let fresh = self.fresh.front().map(|(at, _)| *at);
        let again = self.again.front().map(|(at, _)| *at);
        fresh.into_iter().chain(again).min()
    }
}

impl<R: Registry> Follows<R> {
    /// When the next acknowledgement of a retirement falls due: `None`
    /// while none is queued. Kept apart from the look for a follow not
    /// offered ([`Follows::next_poll`]), which passes over every follow:
    /// a retirement's acknowledgement is found in its queue alone.
    pub(in crate::dist::pools) fn next_avoid(&self) -> Option<Instant> {
        self.avoiding.next()
    }

    /// Acknowledge, from `now`, that this instance avoids `instance`, which
    /// `key`'s follow of `generation` holds retired in the tables just
    /// published.
    pub(super) fn avoid(&mut self, key: &Key, generation: u64, instance: InstanceId, now: Instant) {
        let avoids = Avoids {
            class: key.0.clone(),
            pool: key.1.clone(),
            generation,
            instance,
        };
        self.avoiding.fresh.push_back((now, avoids));
    }

    /// Acknowledge, from `now`, every retired instance that the follows of
    /// `key`, or of every pool when `key` is `None`, hold, current and
    /// closing: the tables just published hold them all.
    pub(super) fn avoid_retired(&mut self, key: Option<&Key>, now: Instant) {
        let mut due = Vec::new();
        let current = self
            .followed
            .follows
            .iter()
            .filter(|(of, _)| key.is_none_or(|key| key == *of));
        let closing = self
            .followed
            .closing
            .iter()
            .filter(|((of, _), _)| key.is_none_or(|key| key == of))
            .map(|((of, _), follow)| (of, follow));
        for (of, follow) in current.chain(closing) {
            for instance in follow.retired.keys() {
                due.push((of.clone(), follow.generation, *instance));
            }
        }
        for (of, generation, instance) in due {
            self.avoid(&of, generation, instance, now);
        }
    }

    /// The acknowledgements of a retirement due at `now`, each of a follow
    /// that still holds its instance retired: one whose instance left, or
    /// whose follow was dropped, needs none.
    pub(in crate::dist::pools) fn avoidances(&mut self, now: Instant) -> Vec<Avoids> {
        let mut due = Vec::new();
        while let Some((_, avoids)) = self.avoiding.fresh.pop_front() {
            due.push(avoids);
        }
        while let Some((at, _)) = self.avoiding.again.front()
            && *at <= now
        {
            let (_, avoids) = self.avoiding.again.pop_front().expect("just seen");
            due.push(avoids);
        }
        due.retain(|avoids| self.holds_retired(avoids));
        due
    }

    /// The hub took `avoids`, or did not, when it is sent again after
    /// [`ACK_RETRY`] while the follow still holds the instance retired.
    pub(in crate::dist::pools) fn avoided(&mut self, avoids: Avoids, taken: bool, now: Instant) {
        if !taken && self.holds_retired(&avoids) {
            self.avoiding.again.push_back((now + ACK_RETRY, avoids));
        }
    }

    fn holds_retired(&self, avoids: &Avoids) -> bool {
        let key = (avoids.class.clone(), avoids.pool.clone());
        self.followed
            .find(&key, avoids.generation)
            .is_some_and(|follow| follow.retired.contains_key(&avoids.instance))
    }
}
