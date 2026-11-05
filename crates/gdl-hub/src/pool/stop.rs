//! The pool's stop, as its hub runs it when it is told to stop by
//! draining: a pool that a hop enters drains, by the drain request's own
//! code, and the stop ends once the drained pool's record is withdrawn. A
//! pool whose class no hop enters cannot drain, and its instances follow
//! no pool drain, so the stop ends once they have left the hub. It does
//! not end when they announce that they have drained: a source's instance
//! says so as soon as it is stopped, while it still serves what it
//! admitted, and a hub that stopped then would take its lease mid-request.
//! Either stop is bounded by the wait it is given.

use std::sync::Arc;
use std::time::Duration;

use gdl_pools::PoolState;
use tokio::sync::watch;

use super::drain::Drain;
use super::{PoolError, PoolHealth, SETTLE, publish};

/// The pool's stop, apart from the type of its directory, so that a
/// program that cannot name the pool's extension can run it: the hub
/// binary finds it through the hub's admission
/// ([`crate::GraphAdmission::pool_stop`]). A clone runs the same stop.
#[derive(Clone)]
pub struct PoolStop {
    drain: Arc<Drain>,
    health: watch::Sender<PoolHealth>,
    stored: watch::Sender<Option<publish::Stored>>,
    /// Whether a hop enters the pool's class, which only then can drain.
    drains: bool,
}

/// How a pool's stop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stopped {
    /// The pool drained and its record is withdrawn.
    Withdrawn,
    /// The instances of a pool whose class no hop enters have left. Its
    /// record is left as it is, to lapse on its readers' clocks.
    Left,
    /// The wait passed first, with `members` instances of the pool's class
    /// still members of the hub, or registering with it.
    Waited {
        /// The wait the stop was given.
        wait: Duration,
        /// How many of the pool's instances were left.
        members: usize,
    },
}

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stopped::Withdrawn => f.write_str("the pool drained and its record is withdrawn"),
            Stopped::Left => f.write_str("the pool's instances have left"),
            Stopped::Waited { wait, members } => write!(
                f,
                "its wait of {wait:?} passed first, with {members} of the pool's instances still members"
            ),
        }
    }
}

impl PoolStop {
    pub(super) fn new(
        drain: Arc<Drain>,
        health: watch::Sender<PoolHealth>,
        stored: watch::Sender<Option<publish::Stored>>,
        drains: bool,
    ) -> Self {
        Self {
            drain,
            health,
            stored,
            drains,
        }
    }

    /// Start the pool's drain, and return the pool's health once it shows
    /// the pool `draining` and the directory has stored a record that says
    /// so, or after [`SETTLE`]; what [`super::PoolReadiness::drain`] and
    /// `POST drain` run. Refused for a pool whose class no hop enters.
    pub(super) async fn drain(&self) -> Result<PoolHealth, PoolError> {
        if !self.drains {
            return Err(PoolError::new(
                "a pool whose class no hop enters cannot be drained",
            ));
        }
        self.drain.start();
        let (mut shown, mut stored) = (self.health.subscribe(), self.stored.subscribe());
        // The judgment takes the drain at once, and the writer writes it
        // next; the bound is for a hub whose judgment has ended, or a
        // directory that does not answer, and the drain is answered with
        // what the hub shows then.
        let _ = tokio::time::timeout(SETTLE, async {
            let _ = shown
                .wait_for(|health| health.state == PoolState::Draining)
                .await;
            let _ = stored
                .wait_for(|stored| {
                    stored
                        .as_ref()
                        .is_some_and(|stored| stored.state == PoolState::Draining)
                })
                .await;
        })
        .await;
        let health = shown.borrow().clone();
        Ok(health)
    }

    /// Stop the pool, and return once it has stopped, or once `wait` has
    /// passed. A pool that a hop enters drains, as a drain request drains
    /// it, and has stopped once its record is withdrawn. A pool whose class
    /// no hop enters has stopped once no instance of its class is a member
    /// of the hub or registering with it. The hub serves throughout, and
    /// nothing here shuts it down. A stop after a drain, or a second stop,
    /// waits for the same end.
    pub async fn run(&self, wait: Duration) -> Stopped {
        let stopped = tokio::time::timeout(wait, async {
            if self.drains {
                // The drain starts before the first await, so a wait cut
                // short still leaves the pool draining.
                let _ = self.drain().await;
                let mut health = self.health.subscribe();
                // The stop holds a sender, so the channel never closes.
                let _ = health.wait_for(|health| health.withdrawn).await;
                Stopped::Withdrawn
            } else {
                self.drain.left().await;
                Stopped::Left
            }
        })
        .await;
        stopped.unwrap_or(Stopped::Waited {
            wait,
            members: self.drain.present(),
        })
    }
}
