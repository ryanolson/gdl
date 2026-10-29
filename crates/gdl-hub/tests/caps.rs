//! A parent hub whose follow link a full pool watch refuses follows the
//! pool again, as it follows again after any link that does not begin, and
//! follows it once a place frees. Every hub runs in process on
//! OS-assigned ports, sharing one directory whose hub names a test
//! resolves once each hub serves; every wait is for something observed.

mod common;

use std::time::Duration;

use common::{
    GLOBAL, POOL, Records, Resolving, WAIT, global_default, health, meet_the_quorum, model,
    serve_pool, timing,
};
use futures::StreamExt;
use gdl_hub::{PoolReadiness, PoolTiming};
use gdl_model::WatchCaps;
use gdl_pools::PoolState;
use gdl_watch::PoolWatcher;

/// Wait until `pool` has refused `n` watches of itself.
async fn refused(pool: &PoolReadiness<Resolving>, n: u64) {
    let refusing = async {
        while pool.refused_watches().pool < n {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(WAIT, refusing)
        .await
        .unwrap_or_else(|_| panic!("{n} watches of the pool were not refused in time"));
}

/// The model pool's hub serves one watch of the pool, which the test
/// holds: the global's link is refused, and asked for again after its
/// backoff's first wait, and refused again. Once the test lets go, the global
/// follows the pool, its link counts among the pool's followers, and its
/// health meets the pool it requires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_follow_link_follows_the_pool_once_there_is_room() {
    let directory = Resolving::new();
    let mut records = Records::new(&directory.inner);
    let capped = PoolTiming {
        watch_caps: WatchCaps {
            pool: 1,
            ..WatchCaps::default()
        },
        ..timing()
    };
    let (model_hub, model_pool) = serve_pool(
        model(POOL, "us-east-1", "model-x-01-hub:1337"),
        &directory,
        capped,
    )
    .await;
    let (_instances, _) = meet_the_quorum(&model_hub).await;
    records.until(PoolState::Ready).await;

    let watcher = PoolWatcher::new().expect("a client");
    let address = format!("127.0.0.1:{}", model_hub.public_addr().port());
    let mut held = tokio::time::timeout(WAIT, watcher.watch(&address))
        .await
        .expect("the hub answers in time")
        .expect("the hub serves the pool's one watch");
    tokio::time::timeout(WAIT, held.next())
        .await
        .expect("the snapshot begins in time")
        .expect("the watch is served")
        .expect("the watch's first event is not a failure");

    let (global_hub, global_pool) = serve_pool(global_default(), &directory, timing()).await;
    // Refused twice: the link was asked for again after the first.
    refused(&model_pool, 2).await;
    assert!(model_pool.followers().borrow().is_empty());
    assert_eq!(
        global_pool.health().borrow().requires["model_x"].met,
        0,
        "a refused link met the pool"
    );

    drop(held);
    health(&global_pool, "the model pool followed and ready", |h| {
        h.requires["model_x"].met == 1
    })
    .await;
    let followers = model_pool.followers().borrow().clone();
    assert!(
        followers.contains(&("global".to_string(), GLOBAL.to_string())),
        "{followers:?}"
    );
    global_hub.shutdown().await.expect("the hub stops");
    model_hub.shutdown().await.expect("the hub stops");
}
