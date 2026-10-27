//! The chooser's follower as a task: one watch of its own hub's relay, for
//! the life of the instance, read into [`Follows`], beside it a reading
//! of each cell left unread by an event of a kind this build does not
//! know, and the acknowledgements it sends its hub of the follows it holds
//! no offer to and of the retired entry instances it avoids.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use gdl::{Error, ErrorKind, Result};
use gdl_model::Layout;
use gdl_watch::{
    Avoids, Backoff, CellWatcher, FollowEvent, UnknownFollowEvent, Unoffered, WatchEnd,
    WatchStream, Watched,
};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use velo::Velo;

use super::damping::Damping;
use super::follow::{Asked, Follows, Key};
use super::{CellMeters, Cells};
use crate::RuntimeConfig;
use crate::dist::fabric::{ChooserHub, Registry};
use crate::dist::roster::Live;

/// The first wait before an instance watches its own hub again, after a
/// watch ended or could not begin: the relay, a cell read alone, and the
/// cell's watch for its drain. Kept as short as it always was, so that a
/// hub that restarts once is watched again as quickly.
pub(crate) const REWATCH: Duration = Duration::from_millis(100);

/// The longest an instance waits before it watches its own hub again,
/// however often its watches failed. Short, because what those watches
/// carry bounds how late the instance learns that its cell drains, that
/// its retirement is acknowledged, and what its tables route to; long
/// enough that a hub that is down is asked about once a second by each of
/// its instances, rather than ten times.
pub(crate) const REWATCH_CAP: Duration = Duration::from_secs(1);

/// Start following the cells of the classes `segment` sends to, for an
/// instance in a cell of `region` at the hub `host`, whose public
/// listener is on `public_port`: one task, which reads the hub's relay
/// into the tables it returns and registers their instances with `node`,
/// until the handle is aborted. `None`, and nothing started, for a
/// segment that holds no hop into another class. Only an instance in a
/// cell may call it: one without a cell follows nothing. `live` is the
/// instance's own membership, `drain` how long an instance in no table
/// stays registered. `chooser` tells the hub, as this instance, that it
/// runs this follower, before the relay is first watched, of each
/// follow it holds no offer to once the hub marked it not offered, and of
/// each retired entry instance its tables hold. `runtime` gives the
/// chooser's damping windows and how long a cell's summary stays fresh.
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
    chooser: ChooserHub,
    runtime: &RuntimeConfig,
) -> Result<Option<(Cells, JoinHandle<()>)>> {
    let wanted = super::entries(layout, segment);
    if wanted.is_empty() {
        return Ok(None);
    }
    let watcher =
        CellWatcher::new().map_err(|e| Error::new(ErrorKind::Transport, e.to_string()))?;
    let address = hub_address(host, public_port);
    let cells = Cells::new(
        region,
        wanted.iter().flat_map(|(class, segments)| {
            segments
                .iter()
                .map(move |segment| (class.as_str(), segment.as_str()))
        }),
        Damping::new(runtime.cell_damping_first, runtime.cell_damping_max),
    );
    // Registered once, as the follower starts: the process has one
    // chooser, and its registry one set of the cells' families.
    let meters = runtime
        .metrics
        .as_ref()
        .map(CellMeters::register)
        .transpose()?;
    let follows = Follows::new(
        cells.clone(),
        node.clone(),
        Arc::clone(live),
        drain,
        wanted,
        runtime.cell_fresh_periods,
        meters,
    );
    let task = tokio::spawn(run(
        Hub {
            watcher,
            address,
            chooser,
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

/// One item of the relay.
pub(super) type Relayed = Watched<FollowEvent, UnknownFollowEvent>;

/// One item of a watch of the relay as its reader takes it: an item, or
/// the failure that ends the watch, which its backoff counts as one.
pub(super) type Relaying = std::result::Result<Relayed, String>;

/// Where the follower reads the relay from, and sends its
/// acknowledgements to.
pub(super) trait Source: Send + Sync + 'static {
    /// One watch of the relay: its snapshot, then each change, until it
    /// ends.
    fn watch(
        &self,
    ) -> impl Future<Output = std::result::Result<BoxStream<'static, Relaying>, String>> + Send;

    /// One watch of the relay's follow of the cell `cell` of `class`
    /// alone: its snapshot, then each change of it, until it ends.
    fn watch_of(
        &self,
        class: &str,
        cell: &str,
    ) -> impl Future<Output = std::result::Result<BoxStream<'static, Relaying>, String>> + Send;

    /// Tell the hub that this instance runs a follower of its relay:
    /// whether the hub took it, or why not.
    fn declare(&self) -> impl Future<Output = std::result::Result<(), String>> + Send;

    /// Tell the hub `unoffered`: whether it took it, or why not.
    fn acknowledge(
        &self,
        unoffered: Unoffered,
    ) -> BoxFuture<'static, std::result::Result<(), String>>;

    /// Tell the hub `avoids`: whether it took it, or why not.
    fn avoid(&self, avoids: Avoids) -> BoxFuture<'static, std::result::Result<(), String>>;
}

/// The instance's own hub.
struct Hub {
    watcher: CellWatcher,
    address: String,
    chooser: ChooserHub,
}

impl Source for Hub {
    async fn watch(&self) -> std::result::Result<BoxStream<'static, Relaying>, String> {
        self.watcher
            .follows(&self.address)
            .await
            .map(relayed)
            .map_err(|e| e.to_string())
    }

    async fn declare(&self) -> std::result::Result<(), String> {
        self.chooser.declare().await
    }

    fn acknowledge(
        &self,
        unoffered: Unoffered,
    ) -> BoxFuture<'static, std::result::Result<(), String>> {
        let chooser = self.chooser.clone();
        async move { chooser.acknowledge(&unoffered).await }.boxed()
    }

    fn avoid(&self, avoids: Avoids) -> BoxFuture<'static, std::result::Result<(), String>> {
        let chooser = self.chooser.clone();
        async move { chooser.avoid(&avoids).await }.boxed()
    }

    async fn watch_of(
        &self,
        class: &str,
        cell: &str,
    ) -> std::result::Result<BoxStream<'static, Relaying>, String> {
        self.watcher
            .follows_of(&self.address, class, cell)
            .await
            .map(relayed)
            .map_err(|e| e.to_string())
    }
}

/// The items of `events`, a watch of the relay, until it ends: one that
/// fails ends with why, its last item.
pub(super) fn relayed(
    events: WatchStream<FollowEvent, UnknownFollowEvent>,
) -> BoxStream<'static, Relaying> {
    events
        .map(|item| item.map_err(|failure| failure.to_string()))
        .boxed()
}

/// Follow `source` into `follows` until aborted: watch, read the snapshot
/// and then each change, and when the watch ends or cannot begin, watch
/// again after a wait from [`REWATCH`] that grows to [`REWATCH_CAP`] while
/// watches fail ([`Backoff`]). What the tables hold stays until a new
/// snapshot is complete. A watch refused again for the same reason is
/// not logged again, since a hub without the cell's extension refuses
/// every one. Unregistrations and acknowledgements go on throughout, a
/// watch's absence included. Before the first watch, the follower tells
/// the hub that it runs, and asks again, after the same waits, until the
/// hub takes it: from then
/// the hub waits for this instance before it closes a follow, and nothing
/// is offered to a cell before it, since the tables fill only from a
/// watch.
pub(super) async fn run<S: Source, R: Registry>(source: S, mut follows: Follows<R>) {
    let mut chores = Chores {
        acks: JoinSet::new(),
        avoids: JoinSet::new(),
        refused: None,
        avoid_refused: None,
    };
    let mut refused: Option<String> = None;
    // The declaration opens the follower's first watch: a hub that refuses
    // it, as one without the cell's extension refuses every one, is asked
    // as a hub that refuses the watch is.
    let mut backoff = Backoff::new(REWATCH, REWATCH_CAP);
    while let Err(error) = source.declare().await {
        if refused.as_deref() != Some(error.as_str()) {
            tracing::warn!(%error, "the hub did not take that this instance follows its relay");
            refused = Some(error);
        }
        tokio::time::sleep(backoff.wait(None, WatchEnd::Failed, Instant::now())).await;
    }
    refused = None;
    loop {
        // A hub can take the connection and not answer: the deadlines are
        // kept while the watch is asked for.
        let opening = source.watch();
        tokio::pin!(opening);
        let opened = loop {
            let (due, poll, avoid) = (
                follows.next_due(),
                follows.next_poll(),
                follows.next_avoid(),
            );
            tokio::select! {
                opened = &mut opening => break opened,
                chore = chores.next(due, poll, avoid) => chores.run(chore, &mut follows, &source),
            }
        };
        let (synced, end) = match opened {
            Ok(events) => {
                if refused.take().is_some() {
                    tracing::info!("the hub's relay of the cells it follows is watched again");
                }
                read(&source, &mut follows, events, &mut chores).await
            }
            Err(error) => {
                if refused.as_deref() != Some(error.as_str()) {
                    tracing::warn!(%error, "the hub's relay of the cells it follows could not be watched");
                    refused = Some(error);
                }
                (None, WatchEnd::Failed)
            }
        };
        let pause = tokio::time::sleep(backoff.wait(synced, end, Instant::now()));
        tokio::pin!(pause);
        loop {
            let (due, poll, avoid) = (
                follows.next_due(),
                follows.next_poll(),
                follows.next_avoid(),
            );
            tokio::select! {
                () = &mut pause => break,
                chore = chores.next(due, poll, avoid) => chores.run(chore, &mut follows, &source),
            }
        }
    }
}

/// Read one watch of the relay into `follows` until it ends, and return
/// when its snapshot completed, if it did, and how it ended: its snapshot
/// apart, applied whole at its `synced`, then each change. A second
/// `synced` in one watch is not the protocol, and ends it. A cell that an
/// event of a kind this build does not know names is read from a watch of
/// its own, from `source`, beside this one, which goes on: each such watch
/// is let go of when the cell asks for another, and all of them when this
/// one ends or its snapshot is complete.
async fn read<S: Source, R: Registry>(
    source: &S,
    follows: &mut Follows<R>,
    mut events: BoxStream<'static, Relaying>,
    chores: &mut Chores,
) -> (Option<Instant>, WatchEnd) {
    let mut snapshot = Some(follows.snapshot());
    let mut synced = None;
    let mut own = Own::default();
    loop {
        if snapshot.is_none() {
            for asked in follows.asked() {
                own.open(source, asked);
            }
        }
        let (due, poll, avoid) = (
            follows.next_due(),
            follows.next_poll(),
            follows.next_avoid(),
        );
        tokio::select! {
            item = events.next() => {
                let item = match item {
                    Some(Ok(item)) => item,
                    Some(Err(failure)) => {
                        tracing::info!(%failure, "a watch of the hub's relay failed");
                        return (synced, WatchEnd::Failed);
                    }
                    None => return (synced, WatchEnd::Closed),
                };
                let event = match item {
                    Watched::Event(event) => event,
                    Watched::Unknown(event) => {
                        match snapshot.as_mut() {
                            Some(filling) => filling.unknown(event),
                            None => follows.unknown(event),
                        }
                        continue;
                    }
                };
                let marker = matches!(event, FollowEvent::Synced);
                match snapshot.take() {
                    Some(complete) if marker => {
                        let now = Instant::now();
                        follows.resnapshot(complete, now);
                        synced = Some(now);
                        own = Own::default();
                    }
                    Some(mut filling) => {
                        filling.apply(event, Instant::now());
                        snapshot = Some(filling);
                    }
                    None if marker => return (synced, WatchEnd::Failed),
                    None => follows.take(event, Instant::now()),
                }
            }
            Some((key, watch, opened)) = own.opening.next() => match opened {
                Ok(stream) if follows.wants(&key, watch) => own.read(key, watch, stream),
                Ok(_) => {}
                Err(error) => {
                    tracing::debug!(class = key.0, cell = key.1, %error, "a cell could not be read again");
                    follows.own(key, watch, Err(WatchEnd::Failed), Instant::now());
                }
            },
            Some((key, watch, item)) = own.reading.next() => {
                follows.own(key, watch, item, Instant::now());
            }
            chore = chores.next(due, poll, avoid) => chores.run(chore, follows, source),
        }
    }
}

/// What the follower does besides reading the relay: unregister what fell
/// due, and acknowledge the follows it holds no offer to, each in a task
/// of its own so that a hub slow to answer never holds up the relay.
struct Chores {
    /// The acknowledgements under way. Dropping the set ends them.
    acks: Acks,
    /// The acknowledgements of retirements under way, ended the same way.
    avoids: JoinSet<(Avoids, std::result::Result<(), String>)>,
    /// The last reason an acknowledgement was not taken, logged once until
    /// one is taken again.
    refused: Option<String>,
    /// The same, for the acknowledgements of retirements.
    avoid_refused: Option<String>,
}

/// One thing for [`Chores::run`] to do.
enum Chore {
    /// An unregistration fell due.
    Forget,
    /// Look for the acknowledgements due.
    Poll,
    /// Send the acknowledgements of retirements due.
    Avoid,
    /// The hub answered an acknowledgement.
    Answered(Unoffered, std::result::Result<(), String>),
    /// The hub answered an acknowledgement of a retirement.
    Avoided(Avoids, std::result::Result<(), String>),
}

/// The acknowledgements under way, each answering with whether the hub
/// took it.
type Acks = JoinSet<(Unoffered, std::result::Result<(), String>)>;

impl Chores {
    /// The next chore: an unregistration due at `due`, a look for
    /// acknowledgements at `poll`, acknowledgements of retirements due at
    /// `avoid`, or an answer. The last apart from the look, which passes
    /// over the follows: a retirement's acknowledgement needs no pass.
    async fn next(
        &mut self,
        due: Option<Instant>,
        poll: Option<Instant>,
        avoid: Option<Instant>,
    ) -> Chore {
        tokio::select! {
            () = until(due) => Chore::Forget,
            () = until(poll) => Chore::Poll,
            () = until(avoid) => Chore::Avoid,
            Some(Ok((unoffered, answer))) = self.acks.join_next() => Chore::Answered(unoffered, answer),
            Some(Ok((avoids, answer))) = self.avoids.join_next() => Chore::Avoided(avoids, answer),
        }
    }

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
            Chore::Avoid => {
                for avoids in follows.avoidances(now) {
                    let sent = source.avoid(avoids.clone());
                    self.avoids.spawn(async move { (avoids, sent.await) });
                }
            }
            Chore::Avoided(avoids, answer) => {
                match &answer {
                    Ok(()) => {
                        if self.avoid_refused.take().is_some() {
                            tracing::info!(
                                "the hub takes this instance's acknowledgements of retirements again"
                            );
                        }
                    }
                    Err(error) => {
                        if self.avoid_refused.as_deref() != Some(error.as_str()) {
                            tracing::warn!(%error, "the hub did not take that this instance avoids a followed cell's retired entry instance");
                            self.avoid_refused = Some(error.clone());
                        }
                    }
                }
                follows.avoided(avoids, answer.is_ok(), now);
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

/// The cells' own watches, as the task holds them.
type Opening<'a> = futures::future::BoxFuture<
    'a,
    (
        Key,
        u64,
        std::result::Result<BoxStream<'static, Relaying>, String>,
    ),
>;

/// One item of a cell's own watch, or its end, with how it ended.
type OwnItem = (Key, u64, std::result::Result<Relayed, WatchEnd>);

/// The cells' own watches: those being opened, those being read, and what
/// lets go of each that is read.
struct Own<'a> {
    opening: FuturesUnordered<Opening<'a>>,
    reading: futures::stream::SelectAll<BoxStream<'static, OwnItem>>,
    /// Dropped to let go of the cell's watch being read.
    stops: std::collections::HashMap<Key, tokio::sync::oneshot::Sender<()>>,
}

impl Default for Own<'_> {
    fn default() -> Self {
        Self {
            opening: FuturesUnordered::new(),
            reading: futures::stream::SelectAll::new(),
            stops: std::collections::HashMap::new(),
        }
    }
}

impl<'a> Own<'a> {
    /// Open the watch `asked` names, letting go of the one its cell is read
    /// from now.
    fn open<S: Source>(&mut self, source: &'a S, asked: Asked) {
        let Asked { key, watch, pause } = asked;
        self.stops.remove(&key);
        self.opening.push(
            async move {
                if !pause.is_zero() {
                    tokio::time::sleep(pause).await;
                }
                let opened = source.watch_of(&key.0, &key.1).await;
                (key, watch, opened)
            }
            .boxed(),
        );
    }

    /// Read `stream`, the cell `key`'s watch `watch`, until it ends or is
    /// let go of: its end is an item too, with how it ended, unless it was
    /// let go of. A failure is a watch's last item, and the cell then asks
    /// for a new watch, so the end that follows it names a watch the
    /// follower no longer reads, and changes nothing.
    fn read(&mut self, key: Key, watch: u64, stream: BoxStream<'static, Relaying>) {
        let (stop, stopped) = tokio::sync::oneshot::channel();
        self.stops.insert(key.clone(), stop);
        let ending = key.clone();
        let items = stream
            .map(move |item| {
                let item = item.map_err(|failure| {
                    tracing::info!(class = key.0, cell = key.1, %failure, "a watch of one cell of the hub's relay failed");
                    WatchEnd::Failed
                });
                (key.clone(), watch, item)
            })
            .chain(futures::stream::once(async move {
                (ending, watch, Err(WatchEnd::Closed))
            }))
            .take_until(stopped)
            .boxed();
        self.reading.push(items);
    }
}

/// Resolves at `at`, or never.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
