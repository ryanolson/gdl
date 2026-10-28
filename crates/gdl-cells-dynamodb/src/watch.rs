//! The poller of a watched class, and the watches it answers. The shape is
//! the `dir` backend's (`gdl_cells::DirDirectory`): one poller per class
//! while the class has watches, a scan's differences as changes, a
//! history of them for resumes, and a watcher cut off when its buffer is
//! full. What differs is the scan: a query of the class's items, and a
//! read of each list a changed record names.

use std::{
    collections::{HashMap, VecDeque},
    hash::{BuildHasher, RandomState},
    sync::{
        Arc, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use aws_sdk_dynamodb::types::AttributeValue;
use gdl_cells::{CellEvent, CellRecord, DirectoryError, ResumeToken};
use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::AbortHandle,
    time::Instant,
};

use crate::{
    directory::{Shared, unavailable},
    item::{self, Entry, StoredHeld},
};

mod stream;

pub use stream::DynamoWatch;

/// What a scan found of a class, by cell: each cell's newest write whose
/// list is in hand.
pub(crate) type Entries = HashMap<String, Arc<Entry>>;

/// The lock a handle's writes and scans of one class hold.
pub(crate) type Io = Arc<tokio::sync::Mutex<()>>;

/// The fastest a poller queries again after a scan that left a list
/// missing: the base of the hub's own watch of its directory.
const RETRY: Duration = Duration::from_millis(100);

#[derive(Debug, Default)]
pub(crate) struct State {
    classes: HashMap<String, Class>,
    next_watcher: u64,
}

#[derive(Debug)]
struct Class {
    // Held by this handle's writes of the class and by its scans, so that a
    // scan never finds one of the handle's writes without the writes
    // before it.
    io: Io,
    entries: Arc<Entries>,
    // The position of the class's latest change.
    head: u64,
    // The latest changes, at positions `head - history.len() + 1 ..= head`.
    history: VecDeque<CellEvent>,
    watchers: HashMap<u64, Watcher>,
    // Running while the class has watchers, and only then.
    poller: Option<Poller>,
    pollers: u64,
    // The cells whose lists the last scan did not find.
    missing: Vec<String>,
}

#[derive(Debug)]
struct Poller {
    generation: u64,
    kick: Arc<Notify>,
    task: AbortHandle,
    // Whether a scan of this poller has been applied: the first follows a
    // spell in which nothing watched the class.
    scanned: bool,
}

pub(crate) type Answer = Result<VecDeque<CellEvent>, DirectoryError>;

#[derive(Debug)]
struct Watcher {
    events: mpsc::Sender<Result<CellEvent, DirectoryError>>,
    lagged: Arc<AtomicBool>,
    // Until a scan that began after the watch answers it, with its
    // snapshot or its replay; with when the watch began.
    start: Option<(Option<ResumeToken>, oneshot::Sender<Answer>, Instant)>,
}

/// What one scan found: the class's entries, and the cells whose newest
/// record names a list not yet in this region's lists table.
#[derive(Debug)]
pub(crate) struct Found {
    entries: Entries,
    missing: Vec<String>,
}

/// What a watcher's registration hands its watch.
pub(crate) struct Registered {
    pub(crate) id: u64,
    pub(crate) answer: oneshot::Receiver<Answer>,
    pub(crate) receiver: mpsc::Receiver<Result<CellEvent, DirectoryError>>,
    pub(crate) lagged: Arc<AtomicBool>,
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

    /// The lock this handle's writes and scans of `class` hold.
    pub(crate) fn io(&self, class: &str) -> Io {
        Arc::clone(&self.lock().class(class, self.base).io)
    }

    /// Add a watcher of `name` from `from`, to be answered by a scan that
    /// begins after it, and start the class's poller if it is stopped;
    /// None when the token cannot be served, whatever a scan finds.
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
        let (events, receiver) = mpsc::channel(self.limits.buffer.max(1));
        let (reply, answer) = oneshot::channel();
        let lagged = Arc::new(AtomicBool::new(false));
        let class = state.class(name, self.base);
        class.watchers.insert(
            id,
            Watcher {
                events,
                lagged: Arc::clone(&lagged),
                start: Some((from, reply, Instant::now())),
            },
        );
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
        Some(Registered {
            id,
            answer,
            receiver,
            lagged,
        })
    }

    /// Remove the watcher `id` of `class`; the last one's removal stops
    /// the class's poller.
    pub(crate) fn unregister(&self, class: &str, id: u64) {
        if let Some(class) = self.lock().classes.get_mut(class) {
            class.watchers.remove(&id);
            if class.watchers.is_empty() {
                class.stop();
            }
        }
    }

    /// What a scan of `name` by the poller of `generation` starts from:
    /// the class's lock and its entries, and the watchers it will answer,
    /// those registered before it. None once the poller is stopped.
    fn begin_scan(&self, name: &str, generation: u64) -> Option<(Io, Arc<Entries>, u64)> {
        let state = self.lock();
        let class = state.classes.get(name)?;
        class
            .poller
            .as_ref()
            .filter(|p| p.generation == generation)?;
        Some((
            Arc::clone(&class.io),
            Arc::clone(&class.entries),
            state.next_watcher,
        ))
    }

    /// Apply what a scan found, and answer the watchers registered before
    /// it: whether every list was in hand, and by when the next scan must
    /// come for a snapshot that still waits; None when the poller is to
    /// stop.
    fn finish_scan(
        &self,
        name: &str,
        generation: u64,
        answers_below: u64,
        found: Result<Found, DirectoryError>,
    ) -> Option<(bool, Option<Instant>)> {
        let history = self.limits.history;
        let first_sync = self.limits.first_sync;
        let mut state = self.lock();
        let class = state.classes.get_mut(name)?;
        // A poller stopped while it scanned: what it found was not watched.
        let poller = class
            .poller
            .as_mut()
            .filter(|p| p.generation == generation)?;
        let first = !std::mem::replace(&mut poller.scanned, true);
        let found = match found {
            Ok(found) => found,
            Err(error) => {
                class.fail(error);
                return None;
            }
        };
        class.apply(name, found.entries, first, history);
        let due = class.answer(name, answers_below, &found.missing, first_sync);
        class.missing = found.missing.clone();
        if class.watchers.is_empty() {
            class.stop();
            return None;
        }
        Some((found.missing.is_empty(), due))
    }
}

impl Shared {
    /// End each watch of `name` whose snapshot has waited its bound, and
    /// say when the next one's bound comes; None once the poller of
    /// `generation` is to stop.
    fn end_overdue(&self, name: &str, generation: u64) -> Option<Option<Instant>> {
        let first_sync = self.limits.first_sync;
        let mut state = self.lock();
        let class = state.classes.get_mut(name)?;
        class
            .poller
            .as_ref()
            .filter(|p| p.generation == generation)?;
        let now = Instant::now();
        let overdue: Vec<u64> = class
            .watchers
            .iter()
            .filter_map(|(id, watcher)| match &watcher.start {
                Some((None, _, began)) if *began + first_sync <= now => Some(*id),
                _ => None,
            })
            .collect();
        for id in overdue {
            if let Some(Watcher {
                start: Some((_, reply, _)),
                ..
            }) = class.watchers.remove(&id)
            {
                let _ = reply.send(Err(unsynced(name, &class.missing, first_sync)));
            }
        }
        if class.watchers.is_empty() {
            class.stop();
            return None;
        }
        Some(
            class
                .watchers
                .values()
                .filter_map(|watcher| match &watcher.start {
                    Some((None, _, began)) => Some(*began + first_sync),
                    _ => None,
                })
                .min(),
        )
    }
}

impl State {
    fn class(&mut self, name: &str, base: u64) -> &mut Class {
        self.classes
            .entry(name.to_owned())
            .or_insert_with(|| Class {
                io: Arc::default(),
                entries: Arc::default(),
                head: base,
                history: VecDeque::new(),
                watchers: HashMap::new(),
                poller: None,
                pollers: 0,
                missing: Vec::new(),
            })
    }
}

impl Class {
    /// Turn the cells whose version rose since the entries last seen into
    /// changes, in the order they were written, and keep `entries`.
    fn apply(&mut self, name: &str, entries: Entries, first: bool, history: usize) {
        let mut changed: Vec<_> = entries
            .iter()
            .filter(|(cell, entry)| {
                self.entries
                    .get(*cell)
                    .is_none_or(|before| before.version < entry.version)
            })
            .map(|(cell, entry)| (entry.order, cell, Arc::clone(entry)))
            .collect();
        changed.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let changed: Vec<_> = changed.into_iter().map(|(_, _, entry)| entry).collect();
        if first {
            // An item that went missing is no change a watch can report,
            // but it is one of the class's items all the same.
            let lost = self.entries.keys().any(|cell| !entries.contains_key(cell));
            self.entries = Arc::new(entries);
            // The items changed while nothing through this handle watched
            // them: no token from before can say what followed it. A
            // position is skipped, so that the latest token is refused too.
            // Every watcher is waiting for its answer, so none is owed the
            // changes.
            if lost || !changed.is_empty() {
                self.history.clear();
                self.head += 1;
            }
            return;
        }
        self.entries = Arc::new(entries);
        for entry in changed {
            self.head += 1;
            let event = entry.event(ResumeToken::new(name, self.head));
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
    /// snapshot of the entries once no list is `missing`, or with the
    /// changes after its token, or, when those are no longer held, with
    /// `Resnapshot`. A snapshot that has waited `first_sync` for a list
    /// ends its watch instead.
    fn answer(
        &mut self,
        name: &str,
        below: u64,
        missing: &[String],
        first_sync: Duration,
    ) -> Option<Instant> {
        let Class {
            entries,
            head,
            history,
            watchers,
            ..
        } = self;
        let now = Instant::now();
        let mut ended = Vec::new();
        for (&id, watcher) in watchers.iter_mut().filter(|(id, _)| **id < below) {
            let waiting = matches!(&watcher.start, Some((None, _, _))) && !missing.is_empty();
            if waiting {
                let Some((_, _, began)) = &watcher.start else {
                    continue;
                };
                if now.saturating_duration_since(*began) < first_sync {
                    continue;
                }
            }
            let Some((from, reply, _)) = watcher.start.take() else {
                continue;
            };
            let events = if waiting {
                ended.push(id);
                Err(unsynced(name, missing, first_sync))
            } else {
                match from {
                    None => Ok(snapshot(entries, name, *head)),
                    Some(token) => match after(history, *head, token.position()) {
                        Some(replay) => Ok([CellEvent::Synced {
                            token,
                            snapshot: false,
                        }]
                        .into_iter()
                        .chain(replay)
                        .collect()),
                        None => {
                            ended.push(id);
                            Ok(VecDeque::from([CellEvent::Resnapshot]))
                        }
                    },
                }
            };
            if reply.send(events).is_err() {
                ended.push(id);
            }
        }
        for id in ended {
            watchers.remove(&id);
        }
        // The next scan must come by the earliest bound of a snapshot that
        // still waits, whatever the backoff says.
        watchers
            .values()
            .filter_map(|watcher| match &watcher.start {
                Some((None, _, began)) => Some(*began + first_sync),
                _ => None,
            })
            .min()
    }

    /// End every watch of the class with `error`; a watch whose buffer is
    /// full is marked lagged instead, and reads a resnapshot.
    fn fail(&mut self, error: DirectoryError) {
        for (_, watcher) in self.watchers.drain() {
            match watcher.start {
                Some((_, reply, _)) => {
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

/// The poller of one class: a scan at once, then one every period or when
/// a new watch asks, until the class has no watches. After a scan that
/// left a list missing, the next comes sooner, from [`RETRY`] doubling up
/// to the period.
async fn poll(shared: Arc<Shared>, name: String, generation: u64, kick: Arc<Notify>) {
    let period = shared.limits.period;
    let mut waits = Waits::new(period);
    loop {
        let Some((io, previous, answers_below)) = shared.begin_scan(&name, generation) else {
            return;
        };
        // A query that never answers, or a write of this handle that holds
        // the class's lock, would hold every watch of the class, and a
        // snapshot past its bound: so a scan, with its wait for the lock,
        // has the same bound.
        let bound = shared.limits.first_sync;
        let scanned = async {
            let _io = io.lock().await;
            scan(&shared, &name, &previous).await
        };
        let scanned = tokio::time::timeout(bound, scanned);
        tokio::pin!(scanned);
        // A snapshot that waits has its own bound, which can come before the
        // scan's: it ends at its bound, whatever the scan does.
        let found = loop {
            let Some(due) = shared.end_overdue(&name, generation) else {
                return;
            };
            tokio::select! {
                found = &mut scanned => break found.unwrap_or_else(|_| {
                    Err(DirectoryError::Unavailable(format!(
                        "class `{name}`: a scan did not finish within {bound:?}"
                    )))
                }),
                () = until(due) => {}
            }
        };
        let Some((complete, due)) = shared.finish_scan(&name, generation, answers_below, found)
        else {
            return;
        };
        let wait = if complete {
            waits.steady()
        } else {
            waits.retry()
        };
        let wait = due.map_or(wait, |due| {
            wait.min(due.saturating_duration_since(Instant::now()))
        });
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = kick.notified() => {}
        }
    }
}

/// Read every item of `name`, and join each record whose version rose
/// since `previous` to its list. A cell whose list is not in hand keeps
/// its entry from `previous`, if it had one, and is named missing.
async fn scan(shared: &Shared, name: &str, previous: &Entries) -> Result<Found, DirectoryError> {
    let mut entries = Entries::with_capacity(previous.len());
    let mut missing = Vec::new();
    let mut start = None;
    loop {
        let page = shared
            .client
            .query()
            .table_name(&shared.tables.records)
            .key_condition_expression("#class = :class")
            .expression_attribute_names("#class", item::CLASS)
            .expression_attribute_values(":class", AttributeValue::S(name.to_owned()))
            // A scan sees every write this region took before it began, so
            // a snapshot holds what a writer here was told it stored.
            .consistent_read(true)
            .set_exclusive_start_key(start.take())
            .send()
            .await
            .map_err(|error| unavailable(&format!("reading class `{name}`"), error))?;
        for found in page.items() {
            let Some(stored) = item::decode(name, found) else {
                continue;
            };
            let before = previous.get(&stored.cell);
            // Never back: global tables keep the last writer's item, which
            // may be an older version than one this reader has seen.
            if let Some(before) = before.filter(|before| stored.version <= before.version) {
                entries.insert(stored.cell, Arc::clone(before));
                continue;
            }
            let key = match &stored.held {
                StoredHeld::Record {
                    list: Some(key), ..
                } => *key,
                _ => {
                    entries.insert(
                        stored.cell.clone(),
                        Arc::new(Entry::new(name, stored, None)),
                    );
                    continue;
                }
            };
            let joined = match before.filter(|before| before.list == Some(key)) {
                Some(before) => before.record().map(|record| item::List {
                    models: record.models.clone(),
                    revoked: record.revoked.clone(),
                }),
                None => read_list(shared, name, &stored.cell, key).await?,
            };
            match joined {
                Some(list) => {
                    entries.insert(
                        stored.cell.clone(),
                        Arc::new(Entry::new(name, stored, Some(list))),
                    );
                }
                None => {
                    if let Some(before) = before {
                        entries.insert(stored.cell.clone(), Arc::clone(before));
                    }
                    missing.push(stored.cell);
                }
            }
        }
        match page.last_evaluated_key() {
            Some(key) if !key.is_empty() => start = Some(key.clone()),
            _ => break,
        }
    }
    missing.sort();
    Ok(Found { entries, missing })
}

/// The list `key` of `cell`, if this region's lists table holds it.
async fn read_list(
    shared: &Shared,
    class: &str,
    cell: &str,
    key: item::ListKey,
) -> Result<Option<item::List>, DirectoryError> {
    let got = shared
        .client
        .get_item()
        .table_name(&shared.tables.lists)
        .set_key(Some(item::list_key(class, cell, key)))
        .consistent_read(true)
        .send()
        .await
        .map_err(|error| {
            unavailable(
                &format!("reading the model list of cell `{cell}` of class `{class}`"),
                error,
            )
        })?;
    let list = got.item().and_then(|got| item::decode_list(key, got));
    if list.is_none() {
        tracing::debug!(
            class,
            cell,
            list = key.sort_key(),
            "a record's model list has not arrived: its cell keeps its earlier record"
        );
    }
    Ok(list)
}

/// The records `entries` holds, by cell name, then the snapshot's end.
fn snapshot(entries: &Entries, name: &str, head: u64) -> VecDeque<CellEvent> {
    let mut records: Vec<&CellRecord> = entries
        .values()
        .filter_map(|entry| entry.record())
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

/// The waits between one poller's scans, each drawn between half its step
/// and the whole step, as the hub's own watches draw theirs.
struct Waits {
    period: Duration,
    retry: Duration,
    state: u64,
}

impl Waits {
    fn new(period: Duration) -> Self {
        Self {
            period,
            retry: RETRY.min(period),
            state: RandomState::new().hash_one(SystemTime::now()),
        }
    }

    /// The wait after a scan that found every list: the period, and the
    /// next retry from its base.
    fn steady(&mut self) -> Duration {
        self.retry = RETRY.min(self.period);
        self.draw(self.period)
    }

    /// The wait after a scan that left a list missing: doubling, up to the
    /// period.
    fn retry(&mut self) -> Duration {
        let step = self.retry;
        self.retry = self.retry.saturating_mul(2).min(self.period);
        self.draw(step)
    }

    fn draw(&mut self, step: Duration) -> Duration {
        let half = step / 2;
        let span = u64::try_from((step - half).as_nanos()).unwrap_or(u64::MAX);
        half + Duration::from_nanos(self.next() % span.saturating_add(1))
    }

    /// splitmix64: cheap, and enough to spread waits.
    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
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

/// Wait until `at`; for ever without one.
async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Why a snapshot of `name` did not complete within `first_sync`.
fn unsynced(name: &str, missing: &[String], first_sync: Duration) -> DirectoryError {
    DirectoryError::Unavailable(if missing.is_empty() {
        format!("class `{name}`: a snapshot did not complete within {first_sync:?}")
    } else {
        format!(
            "class `{name}`: the model lists of {} have not arrived in this region within {first_sync:?}",
            missing.join(", ")
        )
    })
}
