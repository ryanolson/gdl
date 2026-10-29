//! A chooser's watches of its hub's relay, read by the watch client within
//! a short bound set for the test, from a server that sends a snapshot's
//! first events and then only keep-alives: the relay's watch, and a pool's
//! reading of its own, end at the bound, the tables never hold the partial
//! snapshot, and each is watched again as any watch that ends is.

use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use gdl_pools::PoolState;
use gdl_watch::{FollowEvent, PoolWatcher, WatchLimits};

use super::super::served::Server;
use super::super::task::{REWATCH, Relaying, Source, relayed};
use super::{M1, exposed, follow, follower, peer, run, table};

const WITHIN: Duration = Duration::from_millis(300);

/// The relay at `address`, read by the watch client within [`WITHIN`],
/// whose acknowledgements are all taken.
pub(super) struct Served {
    watcher: PoolWatcher,
    address: String,
}

impl Served {
    fn new(server: &Server) -> Self {
        Self::at(&server.address)
    }

    /// The relay at `address`.
    pub(super) fn at(address: &str) -> Self {
        let watcher = PoolWatcher::with_limits(WatchLimits {
            synced_within: WITHIN,
            ..WatchLimits::default()
        })
        .expect("a client");
        Self {
            watcher,
            address: address.to_string(),
        }
    }
}

impl Source for Served {
    async fn watch(&self) -> Result<BoxStream<'static, Relaying>, String> {
        self.watcher
            .follows(&self.address, None)
            .await
            .map(relayed)
            .map_err(|e| e.to_string())
    }

    async fn watch_of(
        &self,
        class: &str,
        pool: &str,
    ) -> Result<BoxStream<'static, Relaying>, String> {
        self.watcher
            .follows_of(&self.address, class, pool)
            .await
            .map(relayed)
            .map_err(|e| e.to_string())
    }

    async fn declare(&self) -> Result<(), String> {
        Ok(())
    }

    fn acknowledge(&self, _: gdl_watch::Unoffered) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn avoid(&self, _: gdl_watch::Avoids) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

/// The relay's watch sends a follow and its instance, then only
/// keep-alives: at the bound it ends, with nothing of it in the tables or
/// registered, and the relay is watched again; a complete snapshot then
/// fills the tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_relay_snapshot_that_never_completes_is_not_installed() {
    let (follows, pools, recorded, _) = follower();
    let mut server = Server::new().await;
    // Before the watch is asked for, so that its bound has run by then.
    let began = tokio::time::Instant::now();
    let task = tokio::spawn(run(Served::new(&server), follows));
    let (x, y) = (peer(), peer());

    let mut first = server.next().await;
    assert_eq!(first.path, "/v1/ext/gdl-pool/follows");
    first.answer().await;
    first.send(&follow(M1, 1, PoolState::Ready)).await;
    first.send(&exposed(M1, 1, &x)).await;
    first.keep_alive();
    let mut second = server.next().await;
    assert_eq!(second.path, "/v1/ext/gdl-pool/follows");
    assert!(
        second.at - began >= WITHIN + REWATCH / 2,
        "watched again after {:?}",
        second.at - began
    );
    assert_eq!(pools.followed("f"), [], "the partial snapshot is not held");
    assert_eq!(recorded.take(), [], "nothing of it is registered");

    second.answer().await;
    second.send(&follow(M1, 1, PoolState::Ready)).await;
    second.send(&exposed(M1, 1, &y)).await;
    second.send(&FollowEvent::Synced { listed: false }).await;
    let mut changes = pools.changes();
    tokio::time::timeout(
        Duration::from_secs(5),
        changes.wait_for(|_| table(&pools, M1).is_some()),
    )
    .await
    .expect("the snapshot in time")
    .expect("the task runs");
    assert_eq!(
        table(&pools, M1).expect("a table").eligible,
        [y.instance_id()]
    );
    task.abort();
}

/// A pool the relay sent an event of a kind this build does not know for
/// is read from a watch of its own, which sends the follow and another
/// instance, then only keep-alives: at the bound the reading ends, the
/// pool stays unread with nothing of the reading in its table, and it is
/// read again from a new watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pools_reading_that_never_completes_leaves_it_unread() {
    let (follows, pools, recorded, _) = follower();
    let mut server = Server::new().await;
    let task = tokio::spawn(run(Served::new(&server), follows));
    let (x, y) = (peer(), peer());

    let mut relay = server.next().await;
    relay.answer().await;
    relay.send(&follow(M1, 1, PoolState::Ready)).await;
    relay.send(&exposed(M1, 1, &x)).await;
    relay.send(&FollowEvent::Synced { listed: false }).await;
    // Before the reading is asked for, so that its bound has run by then.
    let began = tokio::time::Instant::now();
    relay
        .raw(&format!(
            r#"{{"event":"paused","class":"model_x","pool":"{M1}","generation":1}}"#
        ))
        .await;
    let own_path = format!("/v1/ext/gdl-pool/follows/model_x/{M1}");
    let mut reading = server.next().await;
    assert_eq!(reading.path, own_path);
    reading.answer().await;
    reading.send(&follow(M1, 1, PoolState::Ready)).await;
    reading.send(&exposed(M1, 1, &y)).await;
    reading.keep_alive();
    let again = server.next().await;
    assert_eq!(again.path, own_path, "the pool is read again");
    assert!(
        again.at - began >= WITHIN + REWATCH / 2,
        "read again after {:?}",
        again.at - began
    );
    let table = table(&pools, M1).expect("a table");
    assert!(table.unread, "the pool stays unread");
    assert_eq!(table.eligible, []);
    assert!(!pools.is_registered(y.instance_id()));
    assert!(
        !recorded
            .take()
            .iter()
            .any(|call| *call == super::Call::Register(y.instance_id())),
        "nothing of the reading is registered"
    );
    drop(relay);
    task.abort();
}
