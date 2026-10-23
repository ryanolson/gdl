//! The chooser's follower as a task: one watch of its own hub's relay, for
//! the life of the instance, read into [`Follows`], and the
//! acknowledgements it sends its hub of the follows it holds no offer to.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use gdl::{Error, ErrorKind, Result};
use gdl_model::Layout;
use gdl_watch::{CellWatcher, FollowEvent, Unoffered};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use velo::Velo;

use super::Cells;
use super::follow::Follows;
use crate::dist::fabric::{Acknowledger, Registry};
use crate::dist::roster::Live;

/// How long to wait before watching the relay again after a watch ended
/// or could not begin, as the membership follower waits.
const REWATCH: Duration = Duration::from_millis(100);

/// Start following the cells of the classes `segment` sends to, for an
/// instance in a cell of `region` at the hub `host`, whose public
/// listener is on `public_port`: one task, which reads the hub's relay
/// into the tables it returns and registers their instances with `node`,
/// until the handle is aborted. `None`, and nothing started, for a
/// segment that holds no hop into another class. Only an instance in a
/// cell may call it: one without a cell follows nothing. `live` is the
/// instance's own membership, `drain` how long an instance in no table
/// stays registered. `acknowledger` tells the hub, as this instance, of
/// each follow it holds no offer to once the hub marked it not offered.
#[expect(
    clippy::too_many_arguments,
    reason = "the chooser's settings, its hub, and its membership, as the launch holds them"
)]
pub(crate) fn follow(
    layout: &Layout,
    segment: &str,
    region: &str,
    host: &str,
    public_port: u16,
    node: &Velo,
    live: &Arc<Live>,
    drain: Duration,
    acknowledger: Acknowledger,
) -> Result<Option<(Cells, JoinHandle<()>)>> {
    let wanted = super::entries(layout, segment);
    if wanted.is_empty() {
        return Ok(None);
    }
    let watcher =
        CellWatcher::new().map_err(|e| Error::new(ErrorKind::Transport, e.to_string()))?;
    let address = hub_address(host, public_port);
    let cells = Cells::new(region, wanted.values().flatten().map(String::as_str));
    let follows = Follows::new(cells.clone(), node.clone(), Arc::clone(live), drain, wanted);
    let task = tokio::spawn(run(
        Hub {
            watcher,
            address,
            acknowledger,
        },
        follows,
    ));
    Ok(Some((cells, task)))
}

/// The address of the hub at `host` whose public listener is on `port`,
/// `host:port` as a cell's watches take it, with an IPv6 host bracketed.
pub(crate) fn hub_address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Where the follower reads the relay from, and sends its
/// acknowledgements to.
pub(super) trait Source: Send + Sync + 'static {
    /// One watch of the relay: its snapshot, then each change, until it
    /// ends.
    fn watch(
        &self,
    ) -> impl Future<Output = std::result::Result<BoxStream<'static, FollowEvent>, String>> + Send;

    /// Tell the hub `unoffered`: whether it took it, or why not.
    fn acknowledge(
        &self,
        unoffered: Unoffered,
    ) -> BoxFuture<'static, std::result::Result<(), String>>;
}

/// The instance's own hub.
struct Hub {
    watcher: CellWatcher,
    address: String,
    acknowledger: Acknowledger,
}

impl Source for Hub {
    async fn watch(&self) -> std::result::Result<BoxStream<'static, FollowEvent>, String> {
        self.watcher
            .follows(&self.address)
            .await
            .map_err(|e| e.to_string())
    }

    fn acknowledge(
        &self,
        unoffered: Unoffered,
    ) -> BoxFuture<'static, std::result::Result<(), String>> {
        let acknowledger = self.acknowledger.clone();
        async move { acknowledger.acknowledge(&unoffered).await }.boxed()
    }
}

/// Follow `source` into `follows` until aborted: watch, read the snapshot
/// and then each change, and when the watch ends or cannot begin, watch
/// again after [`REWATCH`]. What the tables hold stays until a new
/// snapshot is complete. A watch refused again for the same reason is
/// not logged again, since a hub without the cell's extension refuses
/// every one. Unregistrations and acknowledgements go on throughout, a
/// watch's absence included.
pub(super) async fn run<S: Source, R: Registry>(source: S, mut follows: Follows<R>) {
    let mut chores = Chores {
        acks: JoinSet::new(),
        refused: None,
    };
    let mut refused: Option<String> = None;
    loop {
        // A hub can take the connection and not answer: the deadlines are
        // kept while the watch is asked for.
        let opening = source.watch();
        tokio::pin!(opening);
        let opened = loop {
            let (due, poll) = (follows.next_due(), follows.next_poll());
            tokio::select! {
                opened = &mut opening => break opened,
                chore = next_chore(&mut chores.acks, due, poll) => chores.run(chore, &mut follows, &source),
            }
        };
        match opened {
            Ok(events) => {
                if refused.take().is_some() {
                    tracing::info!("the hub's relay of the cells it follows is watched again");
                }
                read(&mut follows, events, &mut chores, &source).await;
            }
            Err(error) => {
                if refused.as_deref() != Some(error.as_str()) {
                    tracing::warn!(%error, "the hub's relay of the cells it follows could not be watched");
                    refused = Some(error);
                }
            }
        }
        let pause = tokio::time::sleep(REWATCH);
        tokio::pin!(pause);
        loop {
            let (due, poll) = (follows.next_due(), follows.next_poll());
            tokio::select! {
                () = &mut pause => break,
                chore = next_chore(&mut chores.acks, due, poll) => chores.run(chore, &mut follows, &source),
            }
        }
    }
}

/// Read one watch of the relay into `follows` until it ends: its snapshot
/// apart, applied whole at its `synced`, then each change. A second
/// `synced` in one watch is not the protocol, and ends it.
async fn read<S: Source, R: Registry>(
    follows: &mut Follows<R>,
    mut events: BoxStream<'static, FollowEvent>,
    chores: &mut Chores,
    source: &S,
) {
    let mut snapshot = Some(follows.snapshot());
    loop {
        let (due, poll) = (follows.next_due(), follows.next_poll());
        tokio::select! {
            event = events.next() => {
                let Some(event) = event else {
                    return;
                };
                let synced = matches!(event, FollowEvent::Synced);
                match snapshot.take() {
                    Some(complete) if synced => follows.resnapshot(complete, Instant::now()),
                    Some(mut filling) => {
                        filling.apply(event, Instant::now());
                        snapshot = Some(filling);
                    }
                    None if synced => return,
                    None => follows.apply(event, Instant::now()),
                }
            }
            chore = next_chore(&mut chores.acks, due, poll) => chores.run(chore, follows, source),
        }
    }
}

/// What the follower does besides reading the relay: unregister what fell
/// due, and acknowledge the follows it holds no offer to, each in a task
/// of its own so that a hub slow to answer never holds up the relay.
struct Chores {
    /// The acknowledgements under way. Dropping the set ends them.
    acks: Acks,
    /// The last reason an acknowledgement was not taken, logged once until
    /// one is taken again.
    refused: Option<String>,
}

/// One thing for [`Chores::run`] to do.
enum Chore {
    /// An unregistration fell due.
    Forget,
    /// Look for the acknowledgements due.
    Poll,
    /// The hub answered an acknowledgement.
    Answered(Unoffered, std::result::Result<(), String>),
}

/// The acknowledgements under way, each answering with whether the hub
/// took it.
type Acks = JoinSet<(Unoffered, std::result::Result<(), String>)>;

/// The next chore: an unregistration due at `due`, a look for
/// acknowledgements at `poll`, or an answer.
async fn next_chore(acks: &mut Acks, due: Option<Instant>, poll: Option<Instant>) -> Chore {
    tokio::select! {
        () = until(due) => Chore::Forget,
        () = until(poll) => Chore::Poll,
        Some(Ok((unoffered, answer))) = acks.join_next() => Chore::Answered(unoffered, answer),
    }
}

impl Chores {
    fn run<S: Source, R: Registry>(&mut self, chore: Chore, follows: &mut Follows<R>, source: &S) {
        let now = Instant::now();
        match chore {
            Chore::Forget => follows.forget(now),
            Chore::Poll => {
                for unoffered in follows.acknowledgements(now) {
                    let sent = source.acknowledge(unoffered.clone());
                    self.acks.spawn(async move { (unoffered, sent.await) });
                }
            }
            Chore::Answered(unoffered, answer) => {
                match &answer {
                    Ok(()) => {
                        if self.refused.take().is_some() {
                            tracing::info!("the hub takes this instance's acknowledgements again");
                        }
                    }
                    Err(error) => {
                        if self.refused.as_deref() != Some(error.as_str()) {
                            tracing::warn!(%error, "the hub did not take an acknowledgement that a cell is no longer offered to");
                            self.refused = Some(error.clone());
                        }
                    }
                }
                follows.acknowledged(&unoffered, answer.is_ok(), now);
            }
        }
    }
}

/// Resolves at `at`, or never.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
