use std::{
    collections::{HashMap, VecDeque},
    hash::{BuildHasher, RandomState},
    io,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::AbortHandle,
};

use crate::{CellDirectory, CellEvent, CellRecord, DirectoryError, ResumeToken, Withdrawal};

mod file;
mod watch;

use file::{Files, Held};
pub use watch::DirWatch;

/// A cell directory in files, for the hubs on one host: one file per cell,
/// `<root>/<class>/<cell>.json`, under a directory every hub shares (a
/// volume, under Compose).
///
/// A write reads the cell's file and refuses a write that is not newer, as
/// every backend does, then writes a temporary file beside it and renames
/// it over the cell's, so that a reader in any process finds a whole file.
/// A withdrawal leaves a tombstone in the cell's file, which refuses a late
/// write from any process.
///
/// A watch is polled: every 50 ms, while a class has watches in this
/// process, one task lists the class's directory and reads each file that
/// may have changed, and the differences are the watch's changes. Two
/// writes of one cell between two polls are one change, the later; a
/// reader keeps a cell's newest record, so it misses no state. A file that
/// does not parse is skipped; a cell's file that cannot be read ends every
/// watch of its class with [`DirectoryError::Unavailable`], except a
/// watch whose buffer is full, which is lagged and reads
/// [`CellEvent::Resnapshot`] instead. Every file
/// operation runs on a blocking thread.
///
/// A resume token is a place in what this process watched, shared by its
/// clones: a token from another process, from before a restart, or from
/// before a spell in which the class went unwatched and its files changed,
/// a record going missing or bad among them, yields
/// [`CellEvent::Resnapshot`].
#[derive(Debug, Clone)]
pub struct DirDirectory {
    shared: Arc<Shared>,
}

/// How much a [`DirDirectory`] buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirLimits {
    /// The changes a watcher may have unread before it is cut off with
    /// [`CellEvent::Resnapshot`].
    pub buffer: usize,
    /// The latest changes of each class kept for watches that resume.
    pub history: usize,
}

impl Default for DirLimits {
    fn default() -> Self {
        Self {
            buffer: 256,
            history: 256,
        }
    }
}

/// How often a watched class's directory is read: the roster directory's
/// period.
const PERIOD: Duration = Duration::from_millis(50);

#[derive(Debug)]
struct Shared {
    root: PathBuf,
    limits: DirLimits,
    // The position before each class's first change in this process.
    base: u64,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    classes: HashMap<String, Class>,
    next_watcher: u64,
}

#[derive(Debug)]
struct Class {
    // Held by this process's writes of the class and by its scans, so that
    // a scan never finds one of the process's writes without the writes
    // before it.
    io: Arc<Mutex<()>>,
    files: Arc<Files>,
    // The position of the class's latest change, as in `memory`.
    head: u64,
    // The latest changes, at positions `head - history.len() + 1 ..= head`.
    history: VecDeque<CellEvent>,
    watchers: HashMap<u64, Watcher>,
    // Running while the class has watchers, and only then.
    poller: Option<Poller>,
    pollers: u64,
}

#[derive(Debug)]
struct Poller {
    generation: u64,
    kick: Arc<Notify>,
    task: AbortHandle,
    // Whether a scan of this poller has been applied: the first follows a
    // spell in which nothing watched the files.
    scanned: bool,
}

type Answer = Result<VecDeque<CellEvent>, DirectoryError>;

#[derive(Debug)]
struct Watcher {
    events: mpsc::Sender<Result<CellEvent, DirectoryError>>,
    lagged: Arc<AtomicBool>,
    // Until the first scan that began after the watch answers it, with its
    // snapshot or its replay.
    start: Option<(Option<ResumeToken>, oneshot::Sender<Answer>)>,
}

impl DirDirectory {
    /// The directory under `root`, with the default limits. Nothing is
    /// read or made until it is used; a missing `root` holds no cells.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_limits(root, DirLimits::default())
    }

    /// The directory under `root`, with `limits`. A buffer of 0 is taken
    /// as 1.
    pub fn with_limits(root: impl Into<PathBuf>, limits: DirLimits) -> Self {
        Self {
            shared: Arc::new(Shared {
                root: root.into(),
                limits,
                base: base(),
                state: Mutex::default(),
            }),
        }
    }

    /// How many watches of `class` this process has open.
    pub fn watchers(&self, class: &str) -> usize {
        self.shared
            .lock()
            .classes
            .get(class)
            .map_or(0, |class| class.watchers.len())
    }

    async fn write(&self, class: String, cell: String, held: Held) -> Result<(), DirectoryError> {
        let io = Arc::clone(&self.shared.lock().class(&class, self.shared.base).io);
        let dir = self.shared.root.join(&class);
        tokio::task::spawn_blocking(move || {
            let _io = io.lock().unwrap_or_else(PoisonError::into_inner);
            file::write(&dir, &class, &cell, held)
        })
        .await
        .map_err(|error| DirectoryError::Unavailable(error.to_string()))?
    }
}

impl CellDirectory for DirDirectory {
    type Watch = DirWatch;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        // Before any path is made of its names.
        record.check()?;
        let (class, cell) = (record.class.clone(), record.name.clone());
        self.write(class, cell, Held::Record(record)).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        withdrawal.check()?;
        let (class, cell) = (withdrawal.class.clone(), withdrawal.cell.clone());
        self.write(class, cell, Held::tombstone(withdrawal)).await
    }

    /// The watch begins when it is first polled, which must be inside a
    /// tokio runtime.
    fn watch(&self, class: &str, from: Option<ResumeToken>) -> DirWatch {
        DirWatch::new(Arc::clone(&self.shared), class, from)
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Every change to the state is made in one step under the lock.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Add a watcher of `name` from `from`, to be answered by the class's
    /// next scan, and start the class's poller if it is stopped; None when
    /// the token cannot be served, whatever a scan finds.
    fn register(
        self: &Arc<Self>,
        name: &str,
        from: Option<ResumeToken>,
    ) -> Option<watch::Registered> {
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
        let (events, receiver) = mpsc::channel(self.limits.buffer.max(1));
        let (reply, answer) = oneshot::channel();
        let lagged = Arc::new(AtomicBool::new(false));
        let class = state.class(name, self.base);
        let watcher = Watcher {
            events,
            lagged: Arc::clone(&lagged),
            start: Some((from, reply)),
        };
        class.watchers.insert(id, watcher);
        match &class.poller {
            Some(poller) => poller.kick.notify_one(),
            None => {
                class.pollers += 1;
                let generation = class.pollers;
                let kick = Arc::new(Notify::new());
                let poll = poll(
                    Arc::clone(self),
                    name.to_owned(),
                    generation,
                    Arc::clone(&kick),
                );
                class.poller = Some(Poller {
                    generation,
                    kick,
                    task: tokio::spawn(poll).abort_handle(),
                    scanned: false,
                });
            }
        }
        Some(watch::Registered {
            id,
            answer,
            receiver,
            lagged,
        })
    }

    /// What a scan of `name` by the poller of `generation` starts from: the
    /// class's lock and its files, and the watchers it will answer, those
    /// registered before it. None once the poller is stopped.
    fn begin_scan(&self, name: &str, generation: u64) -> Option<(Arc<Mutex<()>>, Arc<Files>, u64)> {
        let state = self.lock();
        let class = state.classes.get(name)?;
        class
            .poller
            .as_ref()
            .filter(|p| p.generation == generation)?;
        Some((
            Arc::clone(&class.io),
            Arc::clone(&class.files),
            state.next_watcher,
        ))
    }

    /// Apply what a scan found, and answer the watchers registered before
    /// it; false when the poller is to stop.
    fn finish_scan(
        &self,
        name: &str,
        generation: u64,
        answers_below: u64,
        found: io::Result<Files>,
    ) -> bool {
        let history = self.limits.history;
        let mut state = self.lock();
        let Some(class) = state.classes.get_mut(name) else {
            return false;
        };
        // A poller stopped while it scanned: what it found was not watched.
        let Some(poller) = class.poller.as_mut().filter(|p| p.generation == generation) else {
            return false;
        };
        let first = !std::mem::replace(&mut poller.scanned, true);
        match found {
            Ok(files) => class.apply(name, files, first, history),
            Err(error) => {
                let dir = self.root.join(name);
                class.fail(DirectoryError::Unavailable(format!(
                    "reading {}: {error}",
                    dir.display()
                )));
                return false;
            }
        }
        class.answer(name, answers_below);
        if class.watchers.is_empty() {
            class.stop();
            return false;
        }
        true
    }
}

impl State {
    fn class(&mut self, name: &str, base: u64) -> &mut Class {
        self.classes
            .entry(name.to_owned())
            .or_insert_with(|| Class {
                io: Arc::default(),
                files: Arc::default(),
                head: base,
                history: VecDeque::new(),
                watchers: HashMap::new(),
                poller: None,
                pollers: 0,
            })
    }
}

impl Class {
    /// Turn the differences between the files last seen and `files` into
    /// changes, in the order they were written, and keep `files`.
    fn apply(&mut self, name: &str, files: Files, first: bool, history: usize) {
        let mut changed: Vec<_> = files
            .iter()
            .filter_map(|(cell, seen)| {
                let entry = seen.entry.as_ref()?;
                let before = self.files.get(cell).and_then(|seen| seen.entry.as_ref());
                (before != Some(entry)).then(|| (entry.order(), cell, Arc::clone(entry)))
            })
            .collect();
        changed.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let changed: Vec<_> = changed.into_iter().map(|(_, _, entry)| entry).collect();
        if first {
            // A record that went missing or bad is no change a watch can
            // report, but it is one of the files all the same.
            let lost = self.files.iter().any(|(cell, seen)| {
                seen.entry.is_some() && files.get(cell).is_none_or(|now| now.entry.is_none())
            });
            self.files = Arc::new(files);
            // The files changed while nothing in this process watched them:
            // no token from before can say what followed it. A position is
            // skipped, so that the latest token is refused too. Every
            // watcher is waiting for its answer, so none is owed the
            // changes.
            if lost || !changed.is_empty() {
                self.history.clear();
                self.head += 1;
            }
            return;
        }
        self.files = Arc::new(files);
        for entry in changed {
            self.head += 1;
            let event = entry.event(name, ResumeToken::new(name, self.head));
            // A watcher still to be answered has the change in its snapshot
            // or its replay.
            self.watchers
                .retain(|_, watcher| watcher.start.is_some() || watcher.offer(&event));
            if history > 0 {
                if self.history.len() == history {
                    self.history.pop_front();
                }
                self.history.push_back(event);
            }
        }
    }

    /// Answer each watcher registered before the scan just applied: with a
    /// snapshot of the files, or with the changes after its token, or, when
    /// those are no longer held, with `Resnapshot`.
    fn answer(&mut self, name: &str, below: u64) {
        let Class {
            files,
            head,
            history,
            watchers,
            ..
        } = self;
        let mut ended = Vec::new();
        for (&id, watcher) in watchers.iter_mut().filter(|(id, _)| **id < below) {
            let Some((from, reply)) = watcher.start.take() else {
                continue;
            };
            let events = match from {
                None => snapshot(files, name, *head),
                Some(token) => match after(history, *head, token.position()) {
                    Some(replay) => [CellEvent::Synced {
                        token,
                        snapshot: false,
                    }]
                    .into_iter()
                    .chain(replay)
                    .collect(),
                    None => {
                        ended.push(id);
                        VecDeque::from([CellEvent::Resnapshot])
                    }
                },
            };
            if reply.send(Ok(events)).is_err() {
                ended.push(id);
            }
        }
        for id in ended {
            watchers.remove(&id);
        }
    }

    /// End every watch of the class with `error`; a watch whose buffer is
    /// full is marked lagged instead, and reads a resnapshot.
    fn fail(&mut self, error: DirectoryError) {
        for (_, watcher) in self.watchers.drain() {
            match watcher.start {
                Some((_, reply)) => {
                    let _ = reply.send(Err(error.clone()));
                }
                None => {
                    // A full buffer has no room for the error: the watcher
                    // is lagged, so that its watch ends with `Resnapshot`
                    // and the next watch meets the error.
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

    /// Stop the poller at once, so that no scan is applied after the last
    /// watch of the class ends: what changes then is unwatched.
    fn stop(&mut self) {
        if let Some(poller) = self.poller.take() {
            poller.task.abort();
        }
    }
}

impl Watcher {
    /// Hand `event` to the watcher without waiting; false when it is gone
    /// or lagged, and is to be dropped.
    fn offer(&self, event: &CellEvent) -> bool {
        match self.events.try_send(Ok(event.clone())) {
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

/// The poller of one class: a scan at once, then one every period or
/// when a new watch asks, until the class has no watches.
async fn poll(shared: Arc<Shared>, name: String, generation: u64, kick: Arc<Notify>) {
    loop {
        let Some((io, previous, answers_below)) = shared.begin_scan(&name, generation) else {
            return;
        };
        let dir = shared.root.join(&name);
        let class = name.clone();
        let found = tokio::task::spawn_blocking(move || {
            let _io = io.lock().unwrap_or_else(PoisonError::into_inner);
            file::scan(&dir, &class, &previous)
        })
        .await
        .unwrap_or_else(|panic| Err(io::Error::other(panic.to_string())));
        if !shared.finish_scan(&name, generation, answers_below, found) {
            return;
        }
        tokio::select! {
            () = tokio::time::sleep(PERIOD) => {}
            () = kick.notified() => {}
        }
    }
}

/// The records `files` holds, by cell name, then the snapshot's end.
fn snapshot(files: &Files, name: &str, head: u64) -> VecDeque<CellEvent> {
    let mut records: Vec<&CellRecord> = files
        .values()
        .filter_map(|seen| seen.entry.as_ref()?.record())
        .collect();
    records.sort_by(|a, b| a.name.cmp(&b.name));
    records
        .into_iter()
        .map(|record| CellEvent::Current(record.clone()))
        .chain([CellEvent::Synced {
            token: ResumeToken::new(name, head),
            snapshot: true,
        }])
        .collect()
}

/// The changes after `position`, if `history`, ending at `head`, still
/// holds all of them.
fn after(
    history: &VecDeque<CellEvent>,
    head: u64,
    position: u64,
) -> Option<impl Iterator<Item = CellEvent> + '_> {
    let oldest = head - history.len() as u64;
    let skip = position.checked_sub(oldest).filter(|_| position <= head)?;
    Some(history.iter().skip(skip as usize).cloned())
}

/// The position before every class's first change in one directory
/// handle and its clones. It is random, so that a token another process
/// made, or this one before a restart, falls outside the positions this
/// one serves, but for a chance of one in 2^24 that the two bases match.
/// The base's low 40 bits are clear, and its top 24 bits are never all
/// set, so that 2^40 changes fit above it and `u64::MAX` is never a
/// position.
fn base() -> u64 {
    const BASES: u64 = (1 << 24) - 1;
    let random = RandomState::new().hash_one((std::process::id(), SystemTime::now()));
    (random % BASES) << 40
}
