//! The follower's watch of the cell directory, when it ends before its
//! snapshot is complete: the next watch's snapshot is the directory as it
//! is, and a cell only the interrupted one named is not kept. The
//! directory here is scripted, so that a watch fails exactly where the
//! test says.

mod common;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{GLOBAL, WAIT, global_default, hub_of, spec, timing};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellState, DirectoryError, MemoryDirectory, ResumeToken,
    Withdrawal,
};
use gdl_hub::CellTiming;
use gdl_watch::FollowEvent;
use tokio::sync::watch;

type Script = Vec<Result<CellEvent, DirectoryError>>;

/// A directory whose watches of `model_x` yield the scripts given, one
/// per watch, each then left open; every other class is a memory
/// directory's.
#[derive(Clone)]
struct Scripted {
    inner: MemoryDirectory,
    scripts: Arc<Mutex<VecDeque<Script>>>,
    /// How many watches of `model_x` have begun.
    watches: Arc<watch::Sender<usize>>,
}

impl CellDirectory for Scripted {
    type Watch = BoxStream<'static, Result<CellEvent, DirectoryError>>;

    async fn publish(&self, record: CellRecord) -> Result<(), DirectoryError> {
        self.inner.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.inner.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> Self::Watch {
        if class != "model_x" {
            return self.inner.watch(class, from).boxed();
        }
        self.watches.send_modify(|watches| *watches += 1);
        let script = self
            .scripts
            .lock()
            .expect("the scripts")
            .pop_front()
            .unwrap_or_default();
        let ends = script.last().is_some_and(Result::is_err);
        let events = futures::stream::iter(script);
        if ends {
            events.boxed()
        } else {
            events.chain(futures::stream::pending()).boxed()
        }
    }
}

/// A watch that names a child of the global and then fails, before its
/// snapshot is complete, then a watch whose complete snapshot is empty:
/// the cell was withdrawn meanwhile. The global follows the cell from the first
/// watch; its link fails, since no hub serves the cell, and once the
/// second snapshot is in, the global does not follow it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cell_only_an_interrupted_snapshot_named_is_not_kept() {
    let spec = spec();
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("a layout");
    let refused = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let nowhere = refused.local_addr().expect("an address").to_string();
    drop(refused);
    let cell = CellRecord {
        name: "model-x-07-us-east-1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some(GLOBAL.into()),
        contract: layout.contract_hash(),
        placement: layout.placement_hash(),
        state: CellState::Ready,
        epoch: 1,
        seq: 1,
        hub: nowhere,
        lease: Duration::from_secs(30),
        load: None,
    };
    let directory = Scripted {
        inner: MemoryDirectory::new(),
        scripts: Arc::new(Mutex::new(VecDeque::from([
            vec![
                Ok(CellEvent::Current(cell.clone())),
                Err(DirectoryError::Unavailable("the watch broke".into())),
            ],
            vec![Ok(CellEvent::Synced {
                token: ResumeToken::new("model_x", 1),
                snapshot: true,
            })],
        ]))),
        watches: Arc::new(watch::Sender::new(0)),
    };
    let mut watches = directory.watches.subscribe();
    let (hub, global) = hub_of(
        global_default(),
        directory.clone(),
        CellTiming {
            refollow: Duration::from_millis(20),
            ..timing()
        },
    )
    .await;
    let mut relay = global.follows();
    let key = ("model_x".to_string(), cell.name.clone());
    let mut following = global.following();
    tokio::time::timeout(WAIT, following.wait_for(|cells| cells.contains_key(&key)))
        .await
        .expect("the global follows the cell the first watch named")
        .expect("the extension lives");
    tokio::time::timeout(WAIT, watches.wait_for(|watches| *watches >= 2))
        .await
        .expect("the follower watches again")
        .expect("the directory lives");
    let waited = tokio::time::timeout(WAIT, async {
        following
            .wait_for(|cells| !cells.contains_key(&key))
            .await
            .map(|_| ())
    })
    .await;
    assert!(
        waited.is_ok(),
        "{GLOBAL} still follows the cell: {:?}",
        *following.borrow()
    );
    // A snapshot that does not hold the cell is no withdrawal: the
    // directory may have lost the record for any reason, an expiry
    // included, and silence is never a decommission.
    let withdrawn: Vec<FollowEvent> = std::iter::from_fn(|| relay.next().now_or_never().flatten())
        .filter(|event| matches!(event, FollowEvent::Withdrawn { .. }))
        .collect();
    assert!(withdrawn.is_empty(), "{withdrawn:?}");
    hub.shutdown().await.expect("the hub stops");
}

/// A withdrawal the directory's watch shows is relayed, naming the
/// cell's latest follow: the control for the test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawal_the_watch_shows_is_relayed() {
    let directory = MemoryDirectory::new();
    let cell = record();
    let (hub, global) = hub_of(global_default(), directory.clone(), timing()).await;
    let mut relay = global.follows();
    let key = ("model_x".to_string(), cell.name.clone());
    let mut following = global.following();
    directory.publish(cell.clone()).await.expect("stored");
    tokio::time::timeout(WAIT, following.wait_for(|cells| cells.contains_key(&key)))
        .await
        .expect("the global follows the cell")
        .expect("the extension lives");
    directory
        .withdraw(Withdrawal {
            class: cell.class.clone(),
            cell: cell.name.clone(),
            epoch: cell.epoch,
            seq: cell.seq + 1,
        })
        .await
        .expect("withdrawn");
    let withdrawn = tokio::time::timeout(WAIT, async {
        loop {
            if let FollowEvent::Withdrawn {
                class,
                cell,
                generation,
            } = relay.next().await.expect("the relay lives")
            {
                return (class, cell, generation);
            }
        }
    })
    .await
    .expect("the withdrawal relayed in time");
    // The follow begun from the record; its link to no hub fails, and a
    // later follow may have begun by then.
    assert_eq!(
        (withdrawn.0.as_str(), withdrawn.1.as_str()),
        ("model_x", &*cell.name)
    );
    assert!(withdrawn.2 >= 1, "{withdrawn:?}");
    hub.shutdown().await.expect("the hub stops");
}

/// A ready child of the global whose hub nobody serves.
fn record() -> CellRecord {
    let spec = spec();
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("a layout");
    let refused = std::net::TcpListener::bind("127.0.0.1:0").expect("a port");
    let nowhere = refused.local_addr().expect("an address").to_string();
    drop(refused);
    CellRecord {
        name: "model-x-07-us-east-1".into(),
        class: "model_x".into(),
        region: "us-east-1".into(),
        home: Some(GLOBAL.into()),
        contract: layout.contract_hash(),
        placement: layout.placement_hash(),
        state: CellState::Ready,
        epoch: 1,
        seq: 1,
        hub: nowhere,
        lease: Duration::from_secs(30),
        load: None,
    }
}

/// The most watches a follower asks for in `window` while every one
/// fails at once: each wait is at least half its step, and the steps
/// double from `base` to `cap`.
fn most_attempts(base: Duration, cap: Duration, window: Duration) -> usize {
    let (mut step, mut elapsed, mut attempts) = (base, Duration::ZERO, 1);
    loop {
        elapsed += step / 2;
        if elapsed > window {
            return attempts;
        }
        attempts += 1;
        step = (step * 2).min(cap);
    }
}

/// A child whose hub takes each connection and closes it is asked for its
/// watch a bounded number of times in 5 s, set by the follow link's cap,
/// rather than once every refollow wait. On the real clock, since the
/// watch client's own timers would run ahead on a paused one while it
/// waits on the socket; the 5 s window is the measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_child_whose_hub_closes_every_watch_is_asked_a_bounded_number_of_times() {
    const WINDOW: Duration = Duration::from_secs(5);
    let spec = spec();
    let layout = gdl_model::layout(&spec.graph, &spec.placement).expect("a layout");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let address = listener.local_addr().expect("an address").to_string();
    let (watched, watches) = watch::channel(0usize);
    let closer = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let watched = watched.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut head = [0u8; 256];
                let read = stream.read(&mut head).await.unwrap_or(0);
                if String::from_utf8_lossy(&head[..read]).contains("/watch") {
                    watched.send_modify(|watches| *watches += 1);
                }
            });
        }
    });
    let directory = MemoryDirectory::new();
    directory
        .publish(CellRecord {
            name: "model-x-07-us-east-1".into(),
            class: "model_x".into(),
            region: "us-east-1".into(),
            home: Some(GLOBAL.into()),
            contract: layout.contract_hash(),
            placement: layout.placement_hash(),
            state: CellState::Ready,
            epoch: 1,
            seq: 1,
            hub: address,
            lease: Duration::from_secs(30),
            load: None,
        })
        .await
        .expect("published");
    let (hub, _global) = hub_of(global_default(), directory, timing()).await;
    let mut counted = watches.clone();
    tokio::time::timeout(WAIT, counted.wait_for(|watches| *watches >= 1))
        .await
        .expect("the child is followed")
        .expect("the closer runs");
    let began = tokio::time::Instant::now();
    tokio::time::sleep_until(began + WINDOW).await;
    let attempts = *watches.borrow() - 1;
    let most = most_attempts(timing().refollow, CellTiming::REFOLLOW_CAP, WINDOW);
    hub.shutdown().await.expect("the hub stops");
    closer.abort();
    assert!(
        (2..most).contains(&attempts),
        "{attempts} more watches in {WINDOW:?}, fewer than {most} allowed"
    );
}

/// A directory whose every watch of the followed class fails at once is
/// watched a bounded number of times in 5 s, rather than once every
/// 100 ms: no more than a reader whose waits double from 100 ms to 1 s
/// would, since the directory's cap is longer than that. On the real
/// clock, since the hub serves over real sockets; the 5 s window is the
/// measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_directory_whose_watches_fail_is_watched_a_bounded_number_of_times() {
    const WINDOW: Duration = Duration::from_secs(5);
    let failing = vec![Err(DirectoryError::Unavailable("down".into()))];
    let directory = Scripted {
        inner: MemoryDirectory::new(),
        scripts: Arc::new(Mutex::new(std::iter::repeat_n(failing, 1000).collect())),
        watches: Arc::new(watch::Sender::new(0)),
    };
    let mut watches = directory.watches.subscribe();
    let (hub, _global) = hub_of(global_default(), directory, timing()).await;
    tokio::time::timeout(WAIT, watches.wait_for(|watches| *watches >= 1))
        .await
        .expect("the directory is watched")
        .expect("the directory lives");
    let began = tokio::time::Instant::now();
    tokio::time::sleep_until(began + WINDOW).await;
    let attempts = *watches.borrow() - 1;
    let most = most_attempts(Duration::from_millis(100), Duration::from_secs(1), WINDOW);
    hub.shutdown().await.expect("the hub stops");
    assert!(
        (2..most).contains(&attempts),
        "{attempts} more watches in {WINDOW:?}, fewer than {most} allowed"
    );
}
