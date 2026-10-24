//! The follower's watch of the cell directory, when it ends before its
//! snapshot is complete: the next watch's snapshot is the directory as it
//! is, and a cell only the interrupted one named is not kept. The
//! directory here is scripted, so that a watch fails exactly where the
//! test says.

mod common;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{GLOBAL, WAIT, global_requiring, hub_of, spec, timing};
use futures::StreamExt;
use futures::stream::BoxStream;
use gdl_cells::{
    CellDirectory, CellEvent, CellRecord, CellState, DirectoryError, MemoryDirectory, ResumeToken,
    Withdrawal,
};
use gdl_hub::CellTiming;
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
        global_requiring(0),
        directory.clone(),
        CellTiming {
            refollow: Duration::from_millis(20),
            ..timing()
        },
    )
    .await;
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
    hub.shutdown().await.expect("the hub stops");
}
