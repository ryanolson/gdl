//! Property updates racing with startup must meet the declaration.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use gdl::{EntryState, InstanceProperties, PropertyKind, PropertyType};
use serde::{Deserialize, Serialize};

struct Pausing;

thread_local! {
    static PAUSE: Cell<Option<&'static Gate>> = const { Cell::new(None) };
}

unsafe impl GlobalAlloc for Pausing {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if let Some(gate) = PAUSE.with(Cell::take) {
            gate.arrived.store(true, Ordering::Release);
            while !gate.released.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        }
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Pausing = Pausing;

struct Gate {
    arrived: AtomicBool,
    released: AtomicBool,
}

struct Release(&'static Gate);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.released.store(true, Ordering::Release);
    }
}

fn declare_during_pause(
    properties: &InstanceProperties,
    provided: Vec<PropertyType>,
    release: Release,
) -> gdl::Result<()> {
    let concurrent = properties.clone();
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        sent.send(concurrent.declare(provided))
            .expect("declaration result");
    });
    // A declaration may wait for the update lock. Release the operation
    // in that case, so holding the lock across the check passes too.
    let first = received.recv_timeout(Duration::from_secs(1));
    drop(release);
    let declared = match first {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => received
            .recv_timeout(Duration::from_secs(3))
            .expect("declaration finishes after release"),
        Err(error) => panic!("declaration thread ended: {error}"),
    };
    worker.join().expect("declaration thread");
    declared
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    number: u32,
}

const SHARD: PropertyKind<Key> = PropertyKind::new("shard");

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Different {
    label: String,
}

#[test]
fn a_declaration_rejects_an_unprovided_want_before_and_after_it() {
    let before = InstanceProperties::new();
    before
        .wants(&SHARD, |_: &u32| Key { number: 1 })
        .expect("a want before declaration");
    assert!(before.declare(vec![]).is_err());

    let after = InstanceProperties::new();
    after.declare(vec![]).expect("no provided kinds");
    assert!(after.wants(&SHARD, |_: &u32| Key { number: 1 }).is_err());
}

#[test]
fn a_declaration_rejects_a_want_paused_after_its_check() {
    static GATE: Gate = Gate {
        arrived: AtomicBool::new(false),
        released: AtomicBool::new(false),
    };

    let properties = InstanceProperties::new();
    let concurrent = properties.clone();
    let release = Release(&GATE);
    let worker = std::thread::spawn(move || {
        // The want's Arc allocation follows its declaration check and
        // precedes the lock that publishes it.
        PAUSE.with(|pause| pause.set(Some(&GATE)));
        concurrent.wants(&SHARD, |_: &u32| Key { number: 1 })
    });

    let bound = Instant::now() + Duration::from_secs(3);
    while !GATE.arrived.load(Ordering::Acquire) && Instant::now() < bound {
        std::thread::yield_now();
    }
    assert!(
        GATE.arrived.load(Ordering::Acquire),
        "want reached its gate"
    );
    assert!(properties.wanted().is_empty(), "want is not published yet");
    let declared = declare_during_pause(&properties, vec![], release);

    let wanted = worker.join().expect("want thread");
    assert!(
        wanted.is_err() || declared.is_err(),
        "wants returned {wanted:?} and declare([]) returned {declared:?}; {} undeclared want is published",
        properties.wanted().len()
    );
}

#[test]
fn a_declaration_rejects_a_set_paused_after_its_check() {
    static GATE: Gate = Gate {
        arrived: AtomicBool::new(false),
        released: AtomicBool::new(false),
    };

    let properties = InstanceProperties::new();
    let concurrent = properties.clone();
    let release = Release(&GATE);
    let worker = std::thread::spawn(move || {
        let mut next = Some((
            Different {
                label: "wrong".into(),
            },
            EntryState::Ready,
        ));
        let entries = std::iter::from_fn(|| {
            let entry = next.take();
            if entry.is_none() {
                // Serialization has finished. The next allocation clones
                // a key after set_typed checks the declaration.
                PAUSE.with(|pause| pause.set(Some(&GATE)));
            }
            entry
        });
        concurrent.set(&PropertyKind::<Different>::new("shard"), entries)
    });

    let bound = Instant::now() + Duration::from_secs(3);
    while !GATE.arrived.load(Ordering::Acquire) && Instant::now() < bound {
        std::thread::yield_now();
    }
    assert!(GATE.arrived.load(Ordering::Acquire), "set reached its gate");
    assert!(
        properties.watch().borrow().is_empty(),
        "set is not published yet"
    );
    let declared =
        declare_during_pause(&properties, vec![PropertyType::of::<Key>("shard")], release);

    let set = worker.join().expect("setter thread");
    assert!(
        set.is_err() || declared.is_err(),
        "set returned {set:?} and declaring Key returned {declared:?}; a Different key is published: {:?}",
        properties.watch().borrow()["shard"][0].key()
    );
}
