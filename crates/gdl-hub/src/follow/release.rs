//! What a follower tells a followed pool's hub. Its releases: once the
//! relay closes a follow, every chooser having acknowledged that it holds
//! no offer to the pool in it, the follower tells the pool's hub, so that
//! the pool no longer counts that follow's watch among its followers. The
//! pool's hub cannot tell a watch that closed because its follower let go
//! from one whose connection was lost while its follower still held
//! offers, so only this says the former. And its acknowledgements of a
//! retirement: once every chooser avoids one of the pool's retired entry
//! instances, the follower says that its whole pool does, so that the
//! retiree need not wait out its bound.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use gdl_watch::{Avoided, Follower, PoolWatcher};
use velo_hub::velo::InstanceId;

/// How long after a release the hub did not take it is sent again.
const RETRY: Duration = Duration::from_millis(500);

/// What to tell a followed pool's hub of one follow: the pool's hub, the
/// follow, by the pool's class and name and the follow's generation, and
/// what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Release {
    pub(crate) hub: String,
    pub(crate) class: String,
    pub(crate) pool: String,
    pub(crate) generation: u64,
    pub(crate) told: Told,
}

/// What a follower tells a followed pool's hub of a follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Told {
    /// The relay closed the follow.
    Released,
    /// Every chooser avoids this retired entry instance of the pool.
    Avoided(InstanceId),
}

/// Send what `releases` gives, as `me` with the follow's generation, until
/// `cancel`. What the pool's hub does not take is sent again every
/// [`RETRY`] for up to `patience`, this hub's watch lease, checked between
/// attempts, so an attempt in flight can outlast it. When both hubs share
/// one `PoolTiming`, the pool's hub no longer counts the watch by then; a
/// hub that cannot be reached is no longer waited for.
pub(crate) async fn run(
    mut releases: mpsc::UnboundedReceiver<Release>,
    watcher: PoolWatcher,
    me: Follower,
    patience: Duration,
    cancel: CancellationToken,
) {
    let mut sending = JoinSet::new();
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            release = releases.recv() => {
                let Some(release) = release else {
                    return;
                };
                let follower = Follower {
                    generation: release.generation,
                    ..me.clone()
                };
                sending.spawn(send(watcher.clone(), release.hub, follower, release.told, patience));
            }
            Some(_) = sending.join_next() => {}
        }
    }
}

async fn send(
    watcher: PoolWatcher,
    hub: String,
    follower: Follower,
    told: Told,
    patience: Duration,
) {
    let until = tokio::time::Instant::now() + patience;
    loop {
        let sent = match told {
            Told::Released => watcher.release(&hub, &follower).await,
            Told::Avoided(instance) => {
                let avoided = Avoided {
                    follower: follower.clone(),
                    instance,
                };
                watcher.avoided(&hub, &avoided).await
            }
        };
        match sent {
            Ok(()) => return,
            Err(error) if tokio::time::Instant::now() + RETRY > until => {
                tracing::info!(hub, %error, ?told, "a followed pool's hub did not take what it was told");
                return;
            }
            Err(_) => tokio::time::sleep(RETRY).await,
        }
    }
}
