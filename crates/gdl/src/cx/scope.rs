//! A context's stop and cancel scope: one node per context, with a parent
//! pointer, a word of flags, and a list of the waiters below it.
//!
//! Making, cloning, checking, and dropping a scope take no lock. A check
//! reads the flags of the node and of each node above it. A waiter
//! registers on its own node and on each node above it, so a stop or a
//! cancel wakes only the waiters registered where it happened, which are
//! the waiters below it. Why, and what was rejected, is in
//! docs/src/development/runtime-design.md.

use std::future::Future;
use std::marker::PhantomPinned;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr;
use std::sync::PoisonError;
use std::task::{Context, Poll, Waker};

use pin_project_lite::pin_project;

use sync::{Arc, AtomicU8, Mutex, MutexGuard, Ordering};

/// A stop was requested here.
pub(crate) const STOP: u8 = 1;
/// The scope was cancelled here.
pub(crate) const CANCEL: u8 = 2;
/// Some waiter has registered here. Never cleared: a stop or cancel that
/// finds it clear takes no lock.
const WAITED: u8 = 4;

/// Entries a stop takes from a list under one lock before it wakes them.
const BATCH: usize = 32;

/// The stop and cancel scope of one context. Cloning shares it.
#[doc(hidden)]
#[derive(Clone)]
pub struct Scope(Arc<Node>);

impl Scope {
    pub(crate) fn root() -> Self {
        Self(Arc::new(Node {
            state: AtomicU8::new(0),
            parent: None,
            waiters: Mutex::default(),
        }))
    }

    pub(crate) fn child(&self) -> Self {
        Self(Arc::new(Node {
            state: AtomicU8::new(0),
            parent: Some(Arc::clone(&self.0)),
            waiters: Mutex::default(),
        }))
    }

    /// Whether `parent` is this scope's parent.
    pub(crate) fn is_child_of(&self, parent: &Scope) -> bool {
        self.0
            .parent
            .as_ref()
            .is_some_and(|node| Arc::ptr_eq(node, &parent.0))
    }

    /// Whether `other` is this scope itself.
    pub fn same(&self, other: &Scope) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Cancel this scope and every scope below it.
    pub fn cancel(&self) {
        self.0.set(CANCEL);
    }

    /// Whether this scope, or one above it, was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.0.any(CANCEL)
    }

    /// A guard that cancels this scope when it is dropped.
    pub fn drop_guard(self) -> CancelOnDrop {
        CancelOnDrop(Some(self))
    }

    pub(crate) fn request_stop(&self) {
        self.0.set(STOP);
    }

    /// Whether a stop was requested, or a cancel made, here or above.
    pub(crate) fn is_stopping(&self) -> bool {
        self.0.any(STOP | CANCEL)
    }

    /// Whether some waiter has ever registered on this scope.
    pub(crate) fn waited(&self) -> bool {
        self.0.state.load(Ordering::Acquire) & WAITED != 0
    }

    /// Resolves when any bit of `interest` is set here or above.
    pub(crate) fn wait(&self, interest: u8) -> Waiter<&Node> {
        Waiter::new(&*self.0, interest)
    }

    /// [`Scope::wait`] for a cancel, holding the scope.
    pub(crate) fn cancelled_owned(self) -> Cancelled {
        Waiter::new(self.0, CANCEL)
    }
}

/// Cancels its scope when dropped, unless disarmed.
#[doc(hidden)]
pub struct CancelOnDrop(Option<Scope>);

impl CancelOnDrop {
    /// The scope, no longer cancelled by this guard.
    pub fn disarm(mut self) -> Scope {
        self.0
            .take()
            .expect("a guard holds its scope until disarmed")
    }

    /// The scope the guard will cancel.
    pub fn scope(&self) -> &Scope {
        self.0
            .as_ref()
            .expect("a guard holds its scope until disarmed")
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(scope) = &self.0 {
            scope.cancel();
        }
    }
}

/// A waiter for a cancel that holds its scope.
pub(crate) type Cancelled = Waiter<Arc<Node>>;

pub(crate) struct Node {
    // STOP, CANCEL, and WAITED; changed only by `fetch_or`, so that a
    // registration and a stop or cancel on this node always see each
    // other's bit, whichever comes second.
    state: AtomicU8,
    // Set when the node is made; taken only by `drop`.
    parent: Option<Arc<Node>>,
    // The waiters at or below this node. Dropped by `Node::drop`, and only
    // if a waiter has ever registered here: a node that never had one has
    // nothing to free, and the drop of the list would be a call on every
    // node of every request.
    waiters: Mutex<ManuallyDrop<Waiters>>,
}

struct Entry {
    // The pinned address of the waiter: unique among live waiters, since a
    // waiter removes its entries before its memory can be reused.
    key: usize,
    interest: u8,
    waker: Waker,
}

impl Node {
    /// Whether any bit of `bits` is set on this node or on a node above.
    fn any(&self, bits: u8) -> bool {
        let mut node = self;
        loop {
            if node.state.load(Ordering::Acquire) & bits != 0 {
                return true;
            }
            match &node.parent {
                Some(parent) => node = parent,
                None => return false,
            }
        }
    }

    fn set(&self, bit: u8) {
        if bit == CANCEL {
            self.cancel();
        } else {
            self.stop_in::<BATCH>();
        }
    }

    fn lock(&self) -> MutexGuard<'_, ManuallyDrop<Waiters>> {
        // No foreign code runs under this lock, but a poisoned list is
        // still a valid list.
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn cancel(&self) {
        let prev = self.state.fetch_or(CANCEL, Ordering::AcqRel);
        if prev & CANCEL != 0 || prev & WAITED == 0 {
            return;
        }
        let taken = std::mem::take(&mut *self.lock());
        WakeAll(ManuallyDrop::into_inner(taken).entries()).run();
    }

    /// A stop, waking at most `B` entries per lock.
    fn stop_in<const B: usize>(&self) {
        let prev = self.state.fetch_or(STOP, Ordering::AcqRel);
        // After a cancel the list holds no entries: every entry wants a
        // cancel, and the cancel took them.
        if prev & (STOP | CANCEL) != 0 || prev & WAITED == 0 {
            return;
        }
        StopDrain::<B> {
            node: self,
            batch: std::array::from_fn(|_| None),
            done: false,
        }
        .run();
    }

    /// Register a waiter here, unless a bit of its interest is already set.
    /// The waker was cloned before the lock; a waker rejected here is
    /// dropped after it.
    fn register(&self, key: usize, interest: u8, waker: Waker) -> bool {
        #[cfg(test)]
        REGISTERED.with(|count| count.set(count.get() + 1));
        let rejected = {
            let mut list = self.lock();
            let first = list.push(Entry {
                key,
                interest,
                waker,
            });
            let prev = self.state.fetch_or(WAITED, Ordering::AcqRel);
            if prev & interest == 0 {
                return true;
            }
            list.pop(first)
        };
        drop(rejected);
        false
    }

    /// Take a waiter's entry out, if a stop or cancel has not already.
    fn remove(&self, key: usize) -> Option<Entry> {
        self.lock().remove(key)
    }

    /// Replace a waiter's waker; `false` if a stop or cancel took its
    /// entry. The old waker is dropped after the lock.
    fn update(&self, key: usize, waker: Waker) -> bool {
        let old = {
            let mut list = self.lock();
            match list.get_mut(key) {
                Some(entry) => Some(std::mem::replace(&mut entry.waker, waker)),
                None => None,
            }
        };
        old.is_some()
    }

    /// Frees the list of a node that has had a waiter. Out of line, so that
    /// the drop of a node that never had one stays small enough to inline.
    #[cold]
    #[inline(never)]
    fn free_waiters(&mut self) {
        let waiters = self
            .waiters
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        drop(ManuallyDrop::into_inner(std::mem::take(waiters)));
    }

    #[cfg(test)]
    fn listed(&self) -> usize {
        self.lock().len()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // A context replaced by its child, over and over, holds a long
        // chain of parents that only it keeps. Dropped by recursion, the
        // chain would overflow the stack; unlink it one node at a time.
        // `into_inner` gives the last owner the node, even when two
        // children of one parent drop at once.
        debug_assert!(self.lock().is_empty(), "a node with a waiter is reachable");
        if self.state.load(Ordering::Acquire) & WAITED != 0 {
            self.free_waiters();
        } else {
            debug_assert_eq!(self.lock().rest.capacity(), 0, "a list with no waiter");
        }
        let mut up = self.parent.take();
        while let Some(parent) = up {
            up = match sync::into_inner(parent) {
                Some(mut node) => node.parent.take(),
                None => None,
            };
        }
    }
}

/// A node's waiters. The first is held in the node: most nodes never hold
/// more than one waiter at once, so a request that waits allocates no more
/// than one that does not. The `Vec` holds the rest, and allocates only
/// for a second waiter.
#[derive(Default)]
struct Waiters {
    first: Option<Entry>,
    rest: Vec<Entry>,
}

impl Waiters {
    /// Adds `entry`; `true` if it took the place held in the node.
    fn push(&mut self, entry: Entry) -> bool {
        if self.first.is_none() {
            self.first = Some(entry);
            true
        } else {
            self.rest.push(entry);
            false
        }
    }

    /// Takes the entry that `push` just added, which said where it went.
    fn pop(&mut self, first: bool) -> Option<Entry> {
        if first {
            self.first.take()
        } else {
            self.rest.pop()
        }
    }

    fn get_mut(&mut self, key: usize) -> Option<&mut Entry> {
        match &mut self.first {
            Some(entry) if entry.key == key => Some(entry),
            _ => self.rest.iter_mut().find(|entry| entry.key == key),
        }
    }

    fn remove(&mut self, key: usize) -> Option<Entry> {
        if self.first.as_ref().is_some_and(|entry| entry.key == key) {
            return self.first.take();
        }
        let at = self.rest.iter().position(|entry| entry.key == key)?;
        Some(self.rest.swap_remove(at))
    }

    /// Takes the next entry whose interest includes a bit of `bits`.
    /// `at` is where to go on in `rest`; it starts at 0 for each lock.
    fn take_next(&mut self, bits: u8, at: &mut usize) -> Option<Entry> {
        if self
            .first
            .as_ref()
            .is_some_and(|entry| entry.interest & bits != 0)
        {
            return self.first.take();
        }
        while *at < self.rest.len() {
            if self.rest[*at].interest & bits != 0 {
                return Some(self.rest.swap_remove(*at));
            }
            *at += 1;
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.first.is_none() && self.rest.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        usize::from(self.first.is_some()) + self.rest.len()
    }

    fn entries(self) -> Entries {
        self.first.into_iter().chain(self.rest)
    }
}

/// A node's waiters, taken out of it.
type Entries = std::iter::Chain<std::option::IntoIter<Entry>, std::vec::IntoIter<Entry>>;

/// Wakes the entries a cancel took. If a wake panics, the guard's drop
/// wakes the rest, so that one panicking waker strands no other waiter.
struct WakeAll(Entries);

impl WakeAll {
    fn run(mut self) {
        for entry in self.0.by_ref() {
            entry.waker.wake();
        }
    }
}

impl Drop for WakeAll {
    fn drop(&mut self) {
        for entry in self.0.by_ref() {
            entry.waker.wake();
        }
    }
}

/// Takes the entries that want a stop from a list, `B` at a time, and
/// wakes each batch after the lock. If a wake panics, the guard's drop
/// wakes the rest of the batch and goes on with the remaining batches.
struct StopDrain<'a, const B: usize> {
    node: &'a Node,
    batch: [Option<Waker>; B],
    done: bool,
}

impl<const B: usize> StopDrain<'_, B> {
    fn run(&mut self) {
        loop {
            {
                let mut list = self.node.lock();
                let mut at = 0;
                let mut taken = 0;
                while taken < B
                    && let Some(entry) = list.take_next(STOP, &mut at)
                {
                    self.batch[taken] = Some(entry.waker);
                    taken += 1;
                }
                if taken == 0 {
                    break;
                }
            }
            for waker in &mut self.batch {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
        }
        self.done = true;
    }
}

impl<const B: usize> Drop for StopDrain<'_, B> {
    fn drop(&mut self) {
        if !self.done {
            for waker in &mut self.batch {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
            self.run();
        }
    }
}

/// Removes a waiter's entries from `left` nodes, from `node` up. It moves
/// to the next node before it drops a removed waker, and its drop goes on
/// with the rest, so that one panicking waker drop leaves no entry behind.
struct Unlink<'a> {
    node: Option<&'a Node>,
    left: u32,
    key: usize,
}

impl Unlink<'_> {
    fn run(&mut self) {
        while self.left > 0 {
            let Some(node) = self.node else {
                return;
            };
            self.left -= 1;
            self.node = node.parent.as_deref();
            drop(node.remove(self.key));
        }
    }
}

impl Drop for Unlink<'_> {
    fn drop(&mut self) {
        self.run();
    }
}

fn unlink(node: &Node, key: usize, levels: &mut u32) {
    Unlink {
        node: Some(node),
        left: std::mem::take(levels),
        key,
    }
    .run();
}

pin_project! {
    /// Resolves when a bit of its interest is set on its node or above.
    /// Polled and not ready, it registers on its node and each node above;
    /// dropped, it removes those entries.
    pub(crate) struct Waiter<N: Deref<Target = Node>> {
        node: N,
        interest: u8,
        // The nodes it is registered on, from its own node up.
        levels: u32,
        // The waker its entries hold.
        waker: Option<Waker>,
        // Its address is its key.
        #[pin]
        pinned: PhantomPinned,
    }

    impl<N: Deref<Target = Node>> PinnedDrop for Waiter<N> {
        fn drop(this: Pin<&mut Self>) {
            let key = ptr::from_ref(&*this).addr();
            let this = this.project();
            unlink(this.node, key, this.levels);
        }
    }
}

impl<N: Deref<Target = Node>> Waiter<N> {
    fn new(node: N, interest: u8) -> Self {
        Self {
            node,
            interest,
            levels: 0,
            waker: None,
            pinned: PhantomPinned,
        }
    }
}

impl<N: Deref<Target = Node>> Future for Waiter<N> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let key = ptr::from_ref(&*self).addr();
        let this = self.project();
        let node: &Node = this.node;
        let interest = *this.interest;
        if node.any(interest) {
            unlink(node, key, this.levels);
            return Poll::Ready(());
        }
        if *this.levels == 0 {
            let mut at = Some(node);
            while let Some(here) = at {
                if !here.register(key, interest, cx.waker().clone()) {
                    unlink(node, key, this.levels);
                    return Poll::Ready(());
                }
                *this.levels += 1;
                at = here.parent.as_deref();
            }
            *this.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        if this
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()))
        {
            return Poll::Pending;
        }
        // A new waker: a stop or cancel may have taken an entry, and woken
        // the old waker, since the check above. A missing entry is that
        // check made again.
        let mut at = Some(node);
        for _ in 0..*this.levels {
            let Some(here) = at else { break };
            if !here.update(key, cx.waker().clone()) {
                unlink(node, key, this.levels);
                return Poll::Ready(());
            }
            at = here.parent.as_deref();
        }
        *this.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// std's types, or loom's when the node is model-checked.
mod sync {
    #[cfg(all(test, gdl_loom))]
    pub(super) use loom::sync::atomic::{AtomicU8, Ordering};
    #[cfg(all(test, gdl_loom))]
    pub(super) use loom::sync::{Arc, Mutex, MutexGuard};
    #[cfg(not(all(test, gdl_loom)))]
    pub(super) use std::sync::atomic::{AtomicU8, Ordering};
    #[cfg(not(all(test, gdl_loom)))]
    pub(super) use std::sync::{Arc, Mutex, MutexGuard};

    /// The value, if `arc` was its last reference.
    #[cfg(not(all(test, gdl_loom)))]
    pub(super) fn into_inner<T>(arc: Arc<T>) -> Option<T> {
        Arc::into_inner(arc)
    }

    /// loom's `Arc` has no `into_inner`. Two last drops at once can both
    /// fail here; the reference each drops then frees the value, one level
    /// deeper, which the models check.
    #[cfg(all(test, gdl_loom))]
    pub(super) fn into_inner<T>(arc: Arc<T>) -> Option<T> {
        Arc::try_unwrap(arc).ok()
    }
}

#[cfg(test)]
thread_local! {
    /// Registrations this thread has made, one per node, for tests that
    /// count them.
    pub(crate) static REGISTERED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(all(test, not(gdl_loom)))]
mod tests;

#[cfg(all(test, gdl_loom))]
mod models;
