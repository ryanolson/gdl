//! Batch held reply reservations without a timer per request.
use dashmap::DashMap;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, Ordering},
};
use tokio_util::sync::CancellationToken;
use velo::{Velo, streaming::AnchorReservation};

pub(super) struct Renewals {
    held: DashMap<u64, AnchorReservation>,
    next: AtomicU64,
    cancel: CancellationToken,
}
impl Renewals {
    pub(super) fn new(velo: &Velo, interval: std::time::Duration) -> Arc<Self> {
        let renewals = Arc::new(Self {
            held: DashMap::new(),
            next: AtomicU64::new(0),
            cancel: CancellationToken::new(),
        });
        let weak = Arc::downgrade(&renewals);
        // A weak handle: a strong one would keep the node's streaming alive
        // for as long as this task runs.
        let node = velo.downgrade();
        let cancel = renewals.cancel.clone();
        tokio::spawn(async move {
            let mut ticks =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    _ = ticks.tick() => {}
                }
                let Some(held) = weak.upgrade() else {
                    break;
                };
                let batch: Vec<_> = held.held.iter().map(|entry| *entry.value()).collect();
                drop(held);
                if batch.is_empty() {
                    continue;
                }
                let Some(node) = node.upgrade() else {
                    break;
                };
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    result = tokio::time::timeout(interval, node.anchor_manager().renew_reservations(&batch)) => {
                        match result {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => tracing::warn!(%error, "reply reservation renewal failed"),
                            Err(_) => tracing::warn!("reply reservation renewal timed out"),
                        }
                    }
                }
            }
        });
        renewals
    }
    pub(super) fn hold(self: &Arc<Self>, reservation: AnchorReservation) -> HeldReply {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.held.insert(id, reservation);
        HeldReply {
            owner: Arc::downgrade(self),
            id,
        }
    }
}
impl Drop for Renewals {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub(crate) struct HeldReply {
    owner: Weak<Renewals>,
    id: u64,
}
impl Drop for HeldReply {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.held.remove(&self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guards_release_holds_and_do_not_retain_the_runtime() {
        let owner = Arc::new(Renewals {
            held: DashMap::new(),
            next: AtomicU64::new(0),
            cancel: CancellationToken::new(),
        });
        let reservation =
            AnchorReservation::new(velo::streaming::StreamAnchorHandle::from_u128(1), None);
        let first = owner.hold(reservation);
        let second = owner.hold(reservation);
        assert_eq!(owner.held.len(), 2);
        drop(first);
        assert_eq!(owner.held.len(), 1);
        let cancel = owner.cancel.clone();
        let weak = Arc::downgrade(&owner);
        drop(owner);
        assert!(weak.upgrade().is_none());
        assert!(cancel.is_cancelled());
        drop(second);
    }
}
