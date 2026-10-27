use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use futures::Stream;
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::{
    CellDirectory, CellEvent, CellRecord, DirectoryError, ResumeToken, Version, Withdrawal,
};

/// An in-process cell directory, shared by every hub in one process
/// through its clones: the reference implementation of
/// [`CellDirectory`], for tests that run several cells in one process.
///
/// One lock guards the whole directory. It is held only for the length of
/// a write's fan-out or a watch's snapshot, never across an `.await`, and
/// a write costs work in proportion to its class's watchers: a send to
/// each that never waits.
#[derive(Debug, Clone, Default)]
pub struct MemoryDirectory {
    shared: Arc<Shared>,
}

/// How much a [`MemoryDirectory`] buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLimits {
    /// The changes a watcher may have unread before it is cut off with
    /// [`CellEvent::Resnapshot`].
    pub buffer: usize,
    /// The latest changes of each class kept for watches that resume.
    pub history: usize,
}

impl Default for MemoryLimits {
    fn default() -> Self {
        Self {
            buffer: 256,
            history: 256,
        }
    }
}

#[derive(Debug, Default)]
struct Shared {
    limits: MemoryLimits,
    // The position before each class's first change: 0, or for a
    // directory that stands for a restarted one, a base of its own, so
    // that a token of the one before falls outside what it serves.
    base: u64,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    classes: HashMap<String, Class>,
    next_watcher: u64,
}

#[derive(Debug, Default)]
struct Class {
    cells: HashMap<String, Held>,
    // The position of the class's latest change; the first is 1, so a
    // watch of a class with no changes yet starts from 0.
    head: u64,
    // The latest changes, at positions `head - history.len() + 1 ..= head`.
    history: VecDeque<CellEvent>,
    watchers: HashMap<u64, Watcher>,
}

#[derive(Debug)]
enum Held {
    // Boxed: a record is many times the size of a tombstone.
    Record(Box<CellRecord>),
    // The tombstone a withdrawal leaves, so that a late publish cannot
    // bring the record back.
    Withdrawn(Version),
}

#[derive(Debug)]
struct Watcher {
    events: mpsc::Sender<CellEvent>,
    lagged: Arc<AtomicBool>,
}

impl MemoryDirectory {
    /// An empty directory with the default limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty directory with `limits`. A buffer of 0 is taken as 1.
    pub fn with_limits(limits: MemoryLimits) -> Self {
        Self::with_base(limits, 0)
    }

    /// An empty directory with `limits` whose classes' positions start at
    /// `base`.
    pub(crate) fn with_base(limits: MemoryLimits, base: u64) -> Self {
        Self {
            shared: Arc::new(Shared {
                limits,
                base,
                state: Mutex::default(),
            }),
        }
    }

    /// The record the directory holds for `cell` of `class`, if any: not
    /// a tombstone.
    pub fn record(&self, class: &str, cell: &str) -> Option<CellRecord> {
        match self.shared.lock().classes.get(class)?.cells.get(cell)? {
            Held::Record(record) => Some(CellRecord::clone(record)),
            Held::Withdrawn(_) => None,
        }
    }

    /// How many watches of `class` are open.
    pub fn watchers(&self, class: &str) -> usize {
        self.shared
            .lock()
            .classes
            .get(class)
            .map_or(0, |class| class.watchers.len())
    }

    /// [`CellDirectory::publish`], which never waits: so that a caller on
    /// a blocking thread can write.
    pub(crate) fn put(&self, record: CellRecord) -> Result<(), DirectoryError> {
        record.check()?;
        let (class, cell) = (record.class.clone(), record.name.clone());
        self.write(
            &class,
            &cell,
            Held::Record(Box::new(record.clone())),
            |token| CellEvent::Put { record, token },
        )
    }

    /// [`CellDirectory::withdraw`], which never waits.
    pub(crate) fn remove(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        withdrawal.check()?;
        let (class, cell) = (withdrawal.class.clone(), withdrawal.cell.clone());
        let held = Held::Withdrawn(withdrawal.version());
        self.write(&class, &cell, held, |token| CellEvent::Withdrawn {
            withdrawal,
            token,
        })
    }

    /// The version of the latest write the directory took for `cell` of
    /// `class`, and whether it is a record rather than a withdrawal.
    pub(crate) fn held(&self, class: &str, cell: &str) -> Option<(Version, bool)> {
        let state = self.shared.lock();
        let held = state.classes.get(class)?.cells.get(cell)?;
        Some((held.version(), matches!(held, Held::Record(_))))
    }

    fn write(
        &self,
        class: &str,
        cell: &str,
        held: Held,
        event: impl FnOnce(ResumeToken) -> CellEvent,
    ) -> Result<(), DirectoryError> {
        let offered = held.version();
        let history = self.shared.limits.history;
        let base = self.shared.base;
        let mut state = self.shared.lock();
        let entry = state.class(class, base);
        if let Some(current) = entry.cells.get(cell)
            && offered <= current.version()
        {
            return Err(DirectoryError::NotNewer {
                class: class.to_owned(),
                cell: cell.to_owned(),
                held: current.version(),
                offered,
            });
        }
        entry.cells.insert(cell.to_owned(), held);
        entry.head += 1;
        let event = event(ResumeToken::new(class, entry.head));
        entry.watchers.retain(|_, watcher| watcher.offer(&event));
        if history > 0 {
            if entry.history.len() == history {
                entry.history.pop_front();
            }
            entry.history.push_back(event);
        }
        Ok(())
    }
}

impl CellDirectory for MemoryDirectory {
    type Watch = MemoryWatch;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        self.put(record)
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.remove(withdrawal)
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        let mut state = self.shared.lock();
        let id = state.next_watcher;
        // The snapshot or the replay, and the subscription, are taken under
        // the lock that writes take, so the watch has no gap and no
        // duplicate between them.
        let pending: VecDeque<CellEvent> = match from {
            None => {
                let entry = state.class(class, self.shared.base);
                let mut records: Vec<&CellRecord> = entry
                    .cells
                    .values()
                    .filter_map(|held| match held {
                        Held::Record(record) => Some(&**record),
                        Held::Withdrawn(_) => None,
                    })
                    .collect();
                records.sort_by(|a, b| a.name.cmp(&b.name));
                records
                    .into_iter()
                    .map(|record| CellEvent::Current(record.clone()))
                    .chain([CellEvent::Synced {
                        token: ResumeToken::new(class, entry.head),
                        snapshot: true,
                    }])
                    .collect()
            }
            Some(token) => {
                let Some(replay) = state
                    .classes
                    .get(class)
                    .filter(|_| token.class() == class)
                    .and_then(|entry| entry.after(token.position()))
                else {
                    return MemoryWatch::ended();
                };
                [CellEvent::Synced {
                    token,
                    snapshot: false,
                }]
                .into_iter()
                .chain(replay)
                .collect()
            }
        };
        state.next_watcher += 1;
        let (events, receiver) = mpsc::channel(self.shared.limits.buffer.max(1));
        let lagged = Arc::new(AtomicBool::new(false));
        let watcher = Watcher {
            events,
            lagged: Arc::clone(&lagged),
        };
        state
            .classes
            .get_mut(class)
            .expect("the class was found or made above")
            .watchers
            .insert(id, watcher);
        MemoryWatch {
            pending,
            receiver: Some(receiver),
            lagged,
            registration: Some(Registration {
                shared: Arc::clone(&self.shared),
                class: class.to_owned(),
                id,
            }),
        }
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic while the lock was held leaves the directory as the last
        // complete write left it: every write changes it in one step.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl State {
    /// `class`, made with its positions starting at `base` if it is new.
    fn class(&mut self, class: &str, base: u64) -> &mut Class {
        self.classes
            .entry(class.to_owned())
            .or_insert_with(|| Class {
                head: base,
                ..Class::default()
            })
    }
}

impl Class {
    /// The changes after `position`, if the history still holds all of
    /// them.
    fn after(&self, position: u64) -> Option<impl Iterator<Item = CellEvent> + '_> {
        let oldest = self.head - self.history.len() as u64;
        let skip = position
            .checked_sub(oldest)
            .filter(|_| position <= self.head)?;
        Some(self.history.iter().skip(skip as usize).cloned())
    }
}

impl Held {
    fn version(&self) -> Version {
        match self {
            Held::Record(record) => record.version(),
            Held::Withdrawn(version) => *version,
        }
    }
}

impl Watcher {
    /// Hand `event` to the watcher without waiting; false when it is gone
    /// or lagged, and is to be dropped.
    fn offer(&self, event: &CellEvent) -> bool {
        match self.events.try_send(event.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                // Set before the sender is dropped, which wakes the stream.
                self.lagged.store(true, Ordering::Release);
                false
            }
            Err(TrySendError::Closed(_)) => false,
        }
    }
}

/// A watch of one class of a [`MemoryDirectory`]. Dropping it removes its
/// watcher from the directory.
#[derive(Debug)]
pub struct MemoryWatch {
    // The snapshot or the replay, kept here rather than in the channel so
    // that a large class does not count against the buffer.
    pending: VecDeque<CellEvent>,
    receiver: Option<mpsc::Receiver<CellEvent>>,
    lagged: Arc<AtomicBool>,
    registration: Option<Registration>,
}

impl MemoryWatch {
    /// A watch that yields `Resnapshot` and ends, holding nothing.
    fn ended() -> Self {
        Self {
            pending: VecDeque::from([CellEvent::Resnapshot]),
            receiver: None,
            lagged: Arc::default(),
            registration: None,
        }
    }

    fn end(&mut self) {
        self.receiver = None;
        self.registration = None;
    }
}

impl Stream for MemoryWatch {
    type Item = Result<CellEvent, DirectoryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(event) = this.pending.pop_front() {
            return Poll::Ready(Some(Ok(event)));
        }
        let Some(receiver) = this.receiver.as_mut() else {
            return Poll::Ready(None);
        };
        // A lagged watcher's unread changes are superseded by the snapshot
        // it must take again, so it is told at once.
        if this.lagged.load(Ordering::Acquire) {
            this.end();
            return Poll::Ready(Some(Ok(CellEvent::Resnapshot)));
        }
        match receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(None) => {
                this.end();
                let lagged = this.lagged.load(Ordering::Acquire);
                Poll::Ready(lagged.then_some(Ok(CellEvent::Resnapshot)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A watch's place among its class's watchers, removed when the watch is
/// dropped or ends.
#[derive(Debug)]
struct Registration {
    shared: Arc<Shared>,
    class: String,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(class) = self.shared.lock().classes.get_mut(&self.class) {
            class.watchers.remove(&self.id);
        }
    }
}
