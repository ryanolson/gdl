//! A hub takes any pool directory, not only the backends its settings can
//! name: here one defined in this test, which stands for a database that
//! every hub reads. The child pool's hub publishes into it, and its
//! parent's hub finds the child there and follows it.
//!
//! Every hub runs in this process on OS-assigned ports; every wait is
//! bounded and for something observed.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Named, POOL, WAIT, config, get, global_default, meet_the_quorum, model, spec};
use gdl_hub::hub_builder_with;
use gdl_model::{DirectoryBackend, PoolSettings};
use gdl_pools::{
    DirectoryError, MemoryDirectory, MemoryWatch, PoolDirectory, PoolRecord, ResumeToken,
    Withdrawal,
};

/// One directory for every hub, which counts the writes and the watches
/// that reach it.
#[derive(Clone, Default)]
struct Database {
    records: MemoryDirectory,
    published: Arc<AtomicUsize>,
    watched: Arc<AtomicUsize>,
}

impl PoolDirectory for Database {
    type Watch = MemoryWatch;

    async fn publish(&self, record: PoolRecord) -> Result<(), DirectoryError> {
        self.published.fetch_add(1, Ordering::Relaxed);
        self.records.publish(record).await
    }

    async fn withdraw(&self, withdrawal: Withdrawal) -> Result<(), DirectoryError> {
        self.records.withdraw(withdrawal).await
    }

    fn watch(&self, class: &str, from: Option<ResumeToken>) -> MemoryWatch {
        self.watched.fetch_add(1, Ordering::Relaxed);
        self.records.watch(class, from)
    }
}

/// `pool` with no directory of its own named: its hub's program gives it.
fn given(pool: PoolSettings) -> PoolSettings {
    PoolSettings {
        directory: DirectoryBackend::DEFAULT,
        ..pool
    }
}

/// The parent's hub follows its child through a directory that no
/// setting can name: the child's hub publishes into it, and the parent's
/// hub watches it. The child names no parent's hub, which only the `relay`
/// backend reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_follows_its_child_through_a_directory_the_hub_is_given() {
    let database = Database::default();
    let directory = Named::new(database.clone());
    let parent = hub_builder_with(config(), spec(), given(global_default()), directory.clone())
        .expect("a hub with a given directory")
        .serve()
        .await
        .expect("the hub serves");
    directory.resolve("global-01-hub:1337", &parent);
    let child = hub_builder_with(
        config(),
        spec(),
        given(model(POOL, "us-east-1", "model-x-01-hub:1337")),
        directory.clone(),
    )
    .expect("a child that names no parent's hub")
    .serve()
    .await
    .expect("the hub serves");
    directory.resolve("model-x-01-hub:1337", &child);
    let _instances = meet_the_quorum(&child).await;

    // The parent's health requires one ready model pool, which it meets
    // only by following one.
    let followed = async {
        loop {
            let (_, body) = get(&parent, "/v1/ext/gdl-pool/health").await;
            let health: serde_json::Value = serde_json::from_str(&body).expect("a health body");
            if health.get("missing").is_none() {
                return health;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let health = tokio::time::timeout(WAIT, followed)
        .await
        .expect("the parent follows its child in time");
    assert_eq!(health["requires"]["model_x"]["met"], 1, "{health}");
    assert!(database.published.load(Ordering::Relaxed) >= 2);
    assert!(database.watched.load(Ordering::Relaxed) >= 1);
    child.shutdown().await.expect("the hub stops");
    parent.shutdown().await.expect("the hub stops");
}

/// Settings that name a backend of their own, or its keys, are refused
/// with a given directory, so that nothing they say is left unread.
#[test]
fn a_given_directory_refuses_settings_that_name_another() {
    let named = [
        DirectoryBackend::Memory,
        DirectoryBackend::Dir {
            path: PathBuf::from("/var/lib/gdl/pools"),
        },
        DirectoryBackend::Relay {
            parent_hub: Some("global-hub:1337".into()),
            state_dir: None,
        },
    ];
    for directory in named {
        let pool = PoolSettings {
            directory: directory.clone(),
            ..model(POOL, "us-east-1", "model-x-01-hub:1337")
        };
        let Err(error) = hub_builder_with(config(), spec(), pool, Database::default()) else {
            panic!("accepted {directory:?} beside a given directory");
        };
        assert!(error.to_string().contains("[pool] directory"), "{error}");
    }
}
