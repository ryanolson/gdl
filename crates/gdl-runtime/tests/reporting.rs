//! A serving body's work must progress while its drain report awaits an answer.

#![cfg(feature = "hub")]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::WAIT;
use common::pools::{Instance, Resolving, model_hub, model_pool, pool_hub, settings};
use common::proxy::Proxy;
use gdl::EntryState;
use gdl_runtime::{Drains, SegmentLoad};
use tokio::sync::oneshot;

/// The test property type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct Shard {
    name: String,
}

const SHARD: gdl::PropertyKind<Shard> = gdl::PropertyKind::new("test");

fn shard(name: &str) -> Shard {
    Shard { name: name.into() }
}

struct Working(Arc<AtomicUsize>);

impl Drains for Working {
    fn admitted(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    fn load(&self) -> Option<SegmentLoad> {
        Some(SegmentLoad {
            admitted: self.admitted(),
            limit: 4,
            declined: 0,
        })
    }
}

#[tokio::test]
async fn the_serving_body_progresses_while_a_stopping_instance_reports_its_drain() {
    const POOL: &str = "model-x-01-us-east-1";
    let directory = Resolving::new();
    let (hub, _) = pool_hub(model_hub(POOL, "m1-hub:1337"), &directory).await;
    let mut sink = Instance::ready(&hub, "d").await;
    sink.announce(gdl_model::SegmentPhase::Loaded).await;
    sink.announce(gdl_model::SegmentPhase::Serving).await;
    let control = Proxy::new(hub.control_addr()).await;

    for pending_drain in [false, true] {
        let mut config = settings(
            hub.public_addr().port(),
            control.port,
            5_000,
            &format!(
                "{}summary_ms = 1000\n[runtime]\nsetup_timeout_ms = 1000\ndrain_ms = 10000\n",
                model_pool(POOL),
            ),
        );
        config.runtime.properties = gdl::InstanceProperties::new();
        let properties = config.runtime.properties.clone();
        let (process, launch) = common::pools::launch(&config, "f")
            .await
            .expect("the entry launches");
        let working = Arc::new(AtomicUsize::new(1));
        let finished = Arc::clone(&working);
        let (began, started) = oneshot::channel();
        let (release, work) = oneshot::channel();
        let (answered, answer) = oneshot::channel();
        let serving = launch.serve(Working(working), move |_, shutdown| async move {
            began.send(()).expect("the test observes the body");
            work.await.expect("the request can finish");
            finished.store(0, Ordering::SeqCst);
            answered
                .send(())
                .expect("the consumer waits for its answer");
            shutdown.cancelled().await;
            Ok(())
        });
        tokio::pin!(serving);
        tokio::time::timeout(WAIT, async {
            tokio::select! {
                result = &mut serving => panic!("the entry ended before serving: {result:?}"),
                started = started => started.expect("the body started"),
            }
        })
        .await
        .expect("serving begins in time");

        if pending_drain {
            control.hold(b"\"draining\"");
            properties
                .set(&SHARD, [(shard("m"), EntryState::Draining)])
                .expect("the node drains its property");
            tokio::time::timeout(WAIT, control.holding())
                .await
                .expect("a drain report is awaiting its answer");
        }

        process.stop();
        assert!(futures::poll!(&mut serving).is_pending());
        release
            .send(())
            .expect("the existing request is ready to finish");
        let progressed = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = &mut serving => panic!("the entry ended before its answer: {result:?}"),
                answer = answer => answer.expect("the body answers its existing request"),
            }
        })
        .await;

        process.stop();
        let _ = tokio::time::timeout(WAIT, &mut serving)
            .await
            .expect("the second stop ends the entry");
        control.release();
        assert!(
            progressed.is_ok(),
            "an unanswered drain report froze the serving body (pending_drain={pending_drain})"
        );
    }
    // The proxy closes the instances' connections, which the hub would
    // otherwise wait out as it stops.
    control.set(common::proxy::Mode::Refuse);
    hub.shutdown().await.expect("the hub stops");
}
