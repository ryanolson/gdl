//! The task that serves one class's watches through one handle, and the
//! state it keeps between its runs.
//!
//! The task reads the class's snapshot at one revision R, in pages, and
//! holds it until every page is read: if compaction passes R meanwhile,
//! it reads again from a new revision, and no reader has seen the first
//! attempt. It then watches the class from R + 1, and trusts the watch only
//! once it is created and, on the same connection, the store's identity is
//! the one it read and the store's revision is not below its own. Each
//! change goes into the class's view, into a ring of the latest changes
//! for watches that resume, and to each reader's bounded buffer.
//!
//! A reader that joins a running task is answered once the task's view
//! has every change of the class up to the class's newest key at a read
//! made after the reader joined: so its snapshot holds every write
//! acknowledged before it watched, though the task's watch may lag.
//!
//! When the store's watch breaks, the task watches again from the revision
//! after the last it applied, on the next endpoint, and readers see no
//! break. A compacted revision, a store whose identity differs, or one
//! whose revision is below the task's, gives every reader `Resnapshot`.
//! When the class's last reader drops, the task stops; the class's view
//! and ring stay, so that a later task resumes the store's watch, and
//! tokens stay served.

use std::{
    collections::{HashMap, VecDeque},
    hash::{BuildHasher, RandomState},
    sync::{
        Arc, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use etcd_client::proto::{PbEvent, PbKeyValue};
use gdl_pools::{DirectoryError, PoolEvent, PoolRecord, ResumeToken};
use serde_json::{Map, Value};
use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::AbortHandle,
};

use crate::{
    directory::Shared,
    value::{self, Name, Stored},
};

mod session;
mod stream;

pub use stream::EtcdWatch;

/// The first wait before the task watches again after a break, doubled
/// up to [`RETRY_MAX`].
const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);

/// The bytes a key and its revisions add to a value in a range response,
/// beyond the class's prefix: a pool name, `.list`, and the fields around.
const KEY_OVERHEAD: usize = 64 + value::LIST.len() + 64;

/// etcd's event type for a delete.
const DELETE: i32 = 1;

#[derive(Debug, Default)]
pub(crate) struct State {
    classes: HashMap<String, Class>,
    next_watcher: u64,
}

#[derive(Debug)]
struct Class {
    // The position of the class's latest change.
    head: u64,
    // The latest changes, at positions `head - history.len() + 1 ..= head`.
    // Shared with the watchers they are handed to, each of which copies its
    // own on its task, so that the lock holds no copy of a record.
    history: VecDeque<Arc<PoolEvent>>,
    watchers: HashMap<u64, Watcher>,
    // Running while the class has watchers, and only then.
    task: Option<Task>,
    tasks: u64,
    view: View,
}

/// The class as the store's watch has shown it.
#[derive(Debug, Default)]
struct View {
    // Each pool's last write, whole.
    entries: HashMap<String, Entry>,
    // Each pool's list key: its revision and value.
    lists: HashMap<String, (i64, Vec<u8>)>,
    // A record whose list has not arrived: its fields and its list's
    // revision. The pool keeps its entry until it does.
    pending: HashMap<String, (Map<String, Value>, i64)>,
    // Every change of the class up to this revision is applied; 0 before
    // the first snapshot.
    cursor: i64,
    // The identity of the store the cursor is a revision of.
    id: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
enum Entry {
    // Shared with each snapshot answered from the view.
    Record(Arc<PoolRecord>),
    // A tombstone: in no snapshot.
    Withdrawn,
}

#[derive(Debug)]
struct Task {
    generation: u64,
    kick: Arc<Notify>,
    abort: AbortHandle,
}

/// How the task answers a watch: with its snapshot, the view's records,
/// shared by every watch answered from the same view and copied by each
/// on its own task, or with the changes after its token.
#[derive(Debug)]
pub(crate) enum Reply {
    Snapshot {
        records: Arc<[Arc<PoolRecord>]>,
        token: ResumeToken,
    },
    Events(VecDeque<Arc<PoolEvent>>),
}

pub(crate) type Answer = Result<Reply, DirectoryError>;

#[derive(Debug)]
struct Watcher {
    events: mpsc::Sender<Result<Arc<PoolEvent>, DirectoryError>>,
    lagged: Arc<AtomicBool>,
    // Until the task answers it.
    start: Option<Start>,
}

#[derive(Debug)]
struct Start {
    from: Option<ResumeToken>,
    reply: oneshot::Sender<Answer>,
    // The revision the task's view must reach before it answers: the
    // class's newest key at a read made after the watch began.
    barrier: Option<i64>,
}

/// What a watcher's registration hands its watch.
pub(crate) struct Registered {
    pub(crate) id: u64,
    pub(crate) answer: oneshot::Receiver<Answer>,
    pub(crate) receiver: mpsc::Receiver<Result<Arc<PoolEvent>, DirectoryError>>,
    pub(crate) lagged: Arc<AtomicBool>,
}

impl State {
    fn class(&mut self, name: &str, base: u64) -> &mut Class {
        self.classes
            .entry(name.to_owned())
            .or_insert_with(|| Class {
                head: base,
                history: VecDeque::new(),
                watchers: HashMap::new(),
                task: None,
                tasks: 0,
                view: View::default(),
            })
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Every change to the state is made in one step under the lock.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn watchers(&self, class: &str) -> usize {
        self.lock()
            .classes
            .get(class)
            .map_or(0, |class| class.watchers.len())
    }

    /// Add a watcher of `name` from `from`, and start the class's task if
    /// it is stopped; None when the token cannot be served.
    pub(crate) fn register(
        self: &Arc<Self>,
        name: &str,
        from: Option<ResumeToken>,
    ) -> Option<Registered> {
        let mut state = self.lock();
        if let Some(token) = &from {
            let served = token.class() == name
                && state.classes.get(name).is_some_and(|class| {
                    after(&class.history, class.head, token.position()).is_some()
                });
            if !served {
                return None;
            }
        }
        let id = state.next_watcher;
        state.next_watcher += 1;
        let (events, receiver) = mpsc::channel(self.limits.buffer);
        let (reply, answer) = oneshot::channel();
        let lagged = Arc::new(AtomicBool::new(false));
        let class = state.class(name, self.base);
        class.watchers.insert(
            id,
            Watcher {
                events,
                lagged: Arc::clone(&lagged),
                start: Some(Start {
                    from,
                    reply,
                    barrier: None,
                }),
            },
        );
        match &class.task {
            Some(task) => task.kick.notify_one(),
            None => {
                class.tasks += 1;
                let generation = class.tasks;
                let kick = Arc::new(Notify::new());
                let run = run(
                    Arc::clone(self),
                    name.to_owned(),
                    generation,
                    Arc::clone(&kick),
                );
                class.task = Some(Task {
                    generation,
                    kick,
                    abort: tokio::spawn(run).abort_handle(),
                });
            }
        }
        Some(Registered {
            id,
            answer,
            receiver,
            lagged,
        })
    }

    /// Remove the watcher `id` of `class`; the last one's removal stops the
    /// class's task.
    pub(crate) fn unregister(&self, class: &str, id: u64) {
        if let Some(class) = self.lock().classes.get_mut(class) {
            class.watchers.remove(&id);
            if class.watchers.is_empty() {
                class.stop();
            }
        }
    }

    /// Run `act` on the class `name`, if the task of `generation` is its
    /// running task; None once it is stopped.
    fn with<T>(&self, name: &str, generation: u64, act: impl FnOnce(&mut Class) -> T) -> Option<T> {
        let mut state = self.lock();
        let class = state.classes.get_mut(name)?;
        class
            .task
            .as_ref()
            .filter(|task| task.generation == generation)?;
        Some(act(class))
    }

    #[cfg(feature = "etcd-local")]
    async fn hook(&self, point: crate::testing::Point) {
        let hook = self
            .hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook(point).await;
        }
    }
}

impl Class {
    /// Stop the task at once, so that nothing is applied after the last
    /// watch of the class ends.
    fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort.abort();
        }
    }

    /// Record `event`, a change of the class, and hand it to each watcher
    /// already answered.
    fn change(&mut self, name: &str, history: usize, make: impl FnOnce(ResumeToken) -> PoolEvent) {
        self.head += 1;
        let event = Arc::new(make(ResumeToken::new(name, self.head)));
        // A watcher still to be answered has the change in its snapshot or
        // its replay.
        self.watchers
            .retain(|_, watcher| watcher.start.is_some() || watcher.offer(&event));
        // The last watcher fell behind: nothing watches the class.
        if self.watchers.is_empty() {
            self.stop();
        }
        if history > 0 {
            if self.history.len() == history {
                self.history.pop_front();
            }
            self.history.push_back(event);
        }
    }

    /// Take `view`, a snapshot the task read, as the class's view. A view
    /// that replaces one the class held follows a spell in which changes
    /// may have been missed: no token from before can say what followed it,
    /// so a position is skipped.
    fn install(&mut self, view: View) {
        if !self.history.is_empty() || !self.view.entries.is_empty() {
            self.history.clear();
            self.head += 1;
        }
        self.view = view;
    }

    /// Give every watcher `Resnapshot`, and forget the view and the ring:
    /// the store's watch cannot resume.
    fn resnapshot(&mut self) {
        for (_, watcher) in self.watchers.drain() {
            match watcher.start {
                Some(start) => {
                    let _ = start.reply.send(Ok(Reply::Events(VecDeque::from([Arc::new(
                        PoolEvent::Resnapshot,
                    )]))));
                }
                // Dropped lagged: its watch says `Resnapshot` and ends.
                None => watcher.lagged.store(true, Ordering::Release),
            }
        }
        self.view = View::default();
        self.history.clear();
        self.head += 1;
        self.stop();
    }

    /// End every watch of the class with `error`.
    fn fail(&mut self, error: &DirectoryError) {
        for (_, watcher) in self.watchers.drain() {
            match watcher.start {
                Some(start) => {
                    let _ = start.reply.send(Err(error.clone()));
                }
                None => {
                    if let Err(mpsc::error::TrySendError::Full(_)) =
                        watcher.events.try_send(Err(error.clone()))
                    {
                        watcher.lagged.store(true, Ordering::Release);
                    }
                }
            }
        }
        self.stop();
    }

    /// Answer each watcher whose barrier the view has reached: with the
    /// view's records, or the changes after its token, or `Resnapshot`
    /// when the ring no longer holds them. A view that holds a record whose
    /// list it cannot read answers no snapshot: it fails it, since a
    /// snapshot without that pool would drop a stored pool from its reader.
    /// Under the lock, a snapshot costs a pointer per pool, once for every
    /// watch it answers.
    fn serve(&mut self, name: &str) {
        let Class {
            head,
            history,
            watchers,
            view,
            ..
        } = self;
        if view.cursor == 0 {
            return;
        }
        let mut records = None;
        let mut ended = Vec::new();
        for (&id, watcher) in watchers.iter_mut() {
            let ready = watcher
                .start
                .as_ref()
                .and_then(|start| start.barrier)
                .is_some_and(|barrier| barrier <= view.cursor);
            if !ready {
                continue;
            }
            let Some(start) = watcher.start.take() else {
                continue;
            };
            let answer = match start.from {
                None if !view.pending.is_empty() => {
                    ended.push(id);
                    let mut pools: Vec<&str> = view.pending.keys().map(String::as_str).collect();
                    pools.sort_unstable();
                    Err(DirectoryError::Unavailable(format!(
                        "class `{name}`: the records of {} name property lists their list keys do not hold",
                        pools.join(", ")
                    )))
                }
                None => Ok(Reply::Snapshot {
                    records: Arc::clone(records.get_or_insert_with(|| records_of(&view.entries))),
                    token: ResumeToken::new(name, *head),
                }),
                Some(token) => Ok(Reply::Events(
                    match after(history, *head, token.position()) {
                        Some(replay) => [Arc::new(PoolEvent::Synced {
                            token,
                            snapshot: false,
                        })]
                        .into_iter()
                        .chain(replay)
                        .collect(),
                        None => {
                            ended.push(id);
                            VecDeque::from([Arc::new(PoolEvent::Resnapshot)])
                        }
                    },
                )),
            };
            if start.reply.send(answer).is_err() {
                ended.push(id);
            }
        }
        for id in ended {
            watchers.remove(&id);
        }
        if self.watchers.is_empty() {
            self.stop();
        }
    }

    /// Apply `events`, one response of the store's watch: each pool's
    /// list into the view, and each record whose list is in hand, or each
    /// tombstone, as a change. Err when the class cannot go on: a key was
    /// deleted, which no write of the directory does.
    fn apply(
        &mut self,
        name: &str,
        class_prefix: &str,
        history: usize,
        events: Vec<PbEvent>,
    ) -> Result<(), String> {
        for event in events {
            let Some(PbKeyValue {
                key,
                mod_revision,
                value: stored,
                ..
            }) = event.kv
            else {
                continue;
            };
            self.view.cursor = self.view.cursor.max(mod_revision);
            if event.r#type == DELETE {
                return Err(format!(
                    "the key {} was deleted, which no write of the directory does",
                    String::from_utf8_lossy(&key)
                ));
            }
            let Some(named) = value::name(class_prefix, &key) else {
                continue;
            };
            match named {
                Name::List(pool) => {
                    let joined = match self.view.pending.get(pool) {
                        Some((fields, at)) if *at == mod_revision => value::join(fields, &stored),
                        _ => None,
                    };
                    self.view
                        .lists
                        .insert(pool.to_owned(), (mod_revision, stored));
                    if let Some(record) = joined {
                        self.view.pending.remove(pool);
                        self.put(name, history, record);
                    }
                }
                Name::Record(pool) => match value::decode(&stored) {
                    None => tracing::warn!(
                        class = name,
                        pool,
                        "a record key holds a value this backend did not write: skipped"
                    ),
                    Some(Stored::Withdrawn(withdrawal)) => {
                        self.view.pending.remove(pool);
                        self.view.entries.insert(pool.to_owned(), Entry::Withdrawn);
                        self.change(name, history, |token| PoolEvent::Withdrawn {
                            withdrawal,
                            token,
                        });
                    }
                    Some(Stored::Record { fields, list, .. }) => {
                        let at = list.unwrap_or(mod_revision);
                        let joined = self
                            .view
                            .lists
                            .get(pool)
                            .filter(|(revision, _)| *revision == at)
                            .and_then(|(_, list)| value::join(&fields, list));
                        match joined {
                            Some(record) => {
                                self.view.pending.remove(pool);
                                self.put(name, history, record);
                            }
                            None => {
                                self.view.pending.insert(pool.to_owned(), (fields, at));
                            }
                        }
                    }
                },
            }
        }
        Ok(())
    }

    fn put(&mut self, name: &str, history: usize, record: PoolRecord) {
        self.view
            .entries
            .insert(record.name.clone(), Entry::Record(Arc::new(record.clone())));
        self.change(name, history, |token| PoolEvent::Put { record, token });
    }
}

impl Watcher {
    /// Hand `event` to the watcher without waiting; false when it is gone
    /// or lagged, and is to be dropped.
    fn offer(&self, event: &Arc<PoolEvent>) -> bool {
        match self.events.try_send(Ok(Arc::clone(event))) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Set before the sender is dropped, which wakes the stream.
                self.lagged.store(true, Ordering::Release);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

/// How a run of the task with one connection ended.
enum Ended {
    /// The class has no watchers.
    Stopped,
    /// The store's watch cannot resume: every reader takes a new snapshot.
    Resnapshot(String),
    /// The connection broke, or the store did not answer: watch again.
    Broken {
        error: DirectoryError,
        // Whether the store's watch was trusted before it broke.
        trusted: bool,
    },
    /// What the class holds cannot be read with these limits.
    Failed(DirectoryError),
}

/// The task of one class: a run per connection, until the class has no
/// watchers, the store's watch cannot resume, or the class cannot be read.
async fn run(shared: Arc<Shared>, name: String, generation: u64, kick: Arc<Notify>) {
    let class_prefix = value::class_prefix(&shared.prefix, &name);
    let mut wait = RETRY_MIN;
    loop {
        let ended = once(&shared, &name, &class_prefix, generation, &kick).await;
        match ended {
            Ended::Stopped => return,
            Ended::Resnapshot(why) => {
                tracing::warn!(class = name, "every watch takes a new snapshot: {why}");
                shared.with(&name, generation, Class::resnapshot);
                return;
            }
            Ended::Failed(error) => {
                shared.with(&name, generation, |class| class.fail(&error));
                return;
            }
            Ended::Broken { error, trusted } => {
                tracing::debug!(class = name, %error, "the store's watch broke; watching again");
                if trusted {
                    wait = RETRY_MIN;
                    // The member that served the watch broke it: the next
                    // run tries the next member. An untrusted run moved on
                    // already.
                    shared.endpoint.fetch_add(1, Ordering::Relaxed);
                }
                let again = wait;
                wait = wait.saturating_mul(2).min(RETRY_MAX);
                tokio::time::sleep(again).await;
            }
        }
    }
}

/// One run of the task over one connection to the handle's next member:
/// the snapshot, if the class has none, then the store's watch from the
/// revision after the view's.
async fn once(
    shared: &Arc<Shared>,
    name: &str,
    class_prefix: &str,
    generation: u64,
    kick: &Notify,
) -> Ended {
    let limits = shared.limits;
    // The next run, or the next task, tries the next member unless this
    // one's watch is trusted: a run abandoned at its watches' bound, which
    // stops the task, then leaves the member behind too.
    let tried = shared.endpoint.fetch_add(1, Ordering::Relaxed);
    let endpoint = tried % shared.endpoints.len();
    let opened = session::open(shared, name, class_prefix, generation, endpoint);
    let (client, mut stream, cursor) = match opened.await {
        Ok(opened) => opened,
        Err(ended) => return ended,
    };
    // Trusted: later runs and tasks start on this member.
    let _ = shared.endpoint.compare_exchange(
        tried.wrapping_add(1),
        tried,
        Ordering::Relaxed,
        Ordering::Relaxed,
    );
    let what = format!("watching class `{name}`");
    let history = limits.history;
    let mut fragments = Vec::new();
    loop {
        let barrier = shared.with(name, generation, |class| {
            class
                .watchers
                .values()
                .any(|watcher| {
                    watcher
                        .start
                        .as_ref()
                        .is_some_and(|start| start.barrier.is_none())
                })
                .then_some(())
        });
        match barrier {
            None => return Ended::Stopped,
            Some(Some(())) => {
                // The watchers waiting now are answered once the view has
                // the class's newest key as of this read.
                let below = shared.lock().next_watcher;
                let newest = async {
                    #[cfg(feature = "etcd-local")]
                    shared.hook(crate::testing::Point::BeforeBarrier).await;
                    session::newest(&client, class_prefix, &limits).await
                };
                let newest = match newest.await {
                    Ok(newest) => newest,
                    Err(error) => {
                        return Ended::Broken {
                            error,
                            trusted: true,
                        };
                    }
                };
                shared.with(name, generation, |class| {
                    for (_, watcher) in class.watchers.iter_mut().filter(|(id, _)| **id < below) {
                        if let Some(start) = watcher.start.as_mut() {
                            start.barrier.get_or_insert(newest);
                        }
                    }
                });
            }
            Some(None) => {}
        }
        if shared
            .with(name, generation, |class| class.serve(name))
            .is_none()
        {
            return Ended::Stopped;
        }
        tokio::select! {
            message = stream.message() => {
                let response = match message {
                    Ok(Some(response)) => response,
                    Ok(None) => return Ended::Broken {
                        error: DirectoryError::Unavailable(format!("{what}: the watch ended")),
                        trusted: true,
                    },
                    Err(error) => return Ended::Broken {
                        error: DirectoryError::Unavailable(format!("{what}: {error}")),
                        trusted: true,
                    },
                };
                if response.canceled() {
                    if response.compact_revision() > 0 {
                        return Ended::Resnapshot(format!(
                            "the revisions after {cursor} were compacted, up to {}",
                            response.compact_revision()
                        ));
                    }
                    return Ended::Broken {
                        error: DirectoryError::Unavailable(format!(
                            "{what}: etcd canceled the watch: {}",
                            response.cancel_reason()
                        )),
                        trusted: true,
                    };
                }
                // A response larger than etcd's request limit comes in
                // fragments: applied together, as the one response.
                let more = response.0.fragment;
                fragments.extend(response.0.events);
                if more {
                    continue;
                }
                let events = std::mem::take(&mut fragments);
                match shared.with(name, generation, |class| {
                    class.apply(name, class_prefix, history, events)
                }) {
                    None => return Ended::Stopped,
                    Some(Err(why)) => return Ended::Resnapshot(why),
                    Some(Ok(())) => {}
                }
            }
            () = kick.notified() => {}
        }
    }
}

/// The records `entries` holds, sorted by pool name.
fn records_of(entries: &HashMap<String, Entry>) -> Arc<[Arc<PoolRecord>]> {
    let mut records: Vec<Arc<PoolRecord>> = entries
        .values()
        .filter_map(|entry| match entry {
            Entry::Record(record) => Some(Arc::clone(record)),
            Entry::Withdrawn => None,
        })
        .collect();
    records.sort_by(|a, b| a.name.cmp(&b.name));
    records.into()
}

/// The changes after `position`, if `history`, ending at `head`, still
/// holds all of them.
fn after(
    history: &VecDeque<Arc<PoolEvent>>,
    head: u64,
    position: u64,
) -> Option<impl Iterator<Item = Arc<PoolEvent>> + '_> {
    let oldest = head - history.len() as u64;
    let skip = position.checked_sub(oldest).filter(|_| position <= head)?;
    Some(history.iter().skip(skip as usize).cloned())
}

/// The position before every class's first change through one handle and
/// its clones. It is random, so that a token of another handle falls
/// outside the positions this one serves, but for a chance of one in 2^24
/// that the two bases match. The base's low 40 bits are clear, and its top
/// 24 bits are never all set, so that 2^40 changes fit above it and
/// `u64::MAX` is never a position.
pub(crate) fn base() -> u64 {
    const BASES: u64 = (1 << 24) - 1;
    let random = RandomState::new().hash_one((std::process::id(), SystemTime::now()));
    (random % BASES) << 40
}
