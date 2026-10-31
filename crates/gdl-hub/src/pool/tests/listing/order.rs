use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use gdl_model::{SegmentPhase, segment_service};
use gdl_watch::InstancePhase;
use tokio::sync::Semaphore;
use velo_hub::velo::InstanceId;
use velo_hub::velo::discovery::{ServiceDiscovery, ServiceEvent};

use super::{Served, WAIT, list, member};

#[tokio::test]
async fn retirement_after_the_first_poll_never_regresses_the_listed_phase() {
    let served = Served::new().await;
    let (node, _client) = member(&served).await;
    let admission = Arc::clone(served.pool.admission.get().expect("admission attached"));
    let feed = Arc::clone(&served.pool.feed);
    let directory = Retirement {
        id: node.instance_id(),
        retired: AtomicBool::new(false),
        ready: Semaphore::new(1),
    };
    let before = list(&admission, &directory, &feed)
        .await
        .expect("the directory answers");
    assert_eq!(before.instances.len(), 1);
    assert_eq!(before.instances[0].phase, InstancePhase::Ready);

    let during = list(&admission, &directory, &feed);
    tokio::pin!(during);
    // A single poll holds ready and completes any reads started beside it.
    assert!(futures::poll!(&mut during).is_pending());
    directory.retired.store(true, Ordering::SeqCst);
    directory.ready.add_permits(2);
    let during = tokio::time::timeout(WAIT, during)
        .await
        .expect("the held read finishes")
        .expect("the directory answers");
    let after = list(&admission, &directory, &feed)
        .await
        .expect("the directory answers");
    assert_eq!(after.instances.len(), 1);
    assert_eq!(after.instances[0].phase, InstancePhase::Retired);
    assert_eq!(during.instances.len(), 1);
    assert!(admission.member(node.instance_id()).is_some());
    let phase = during.instances[0].phase;
    served.stop().await;
    assert!(
        phase >= before.instances[0].phase,
        "the same registration regressed from ready to {phase:?}"
    );
}

struct Retirement {
    id: InstanceId,
    retired: AtomicBool,
    ready: Semaphore,
}

impl ServiceDiscovery for Retirement {
    fn get_instances(&self, name: &str) -> BoxFuture<'_, anyhow::Result<Vec<InstanceId>>> {
        let name = name.to_string();
        Box::pin(async move {
            if name == segment_service("a", SegmentPhase::Ready) {
                self.ready
                    .acquire()
                    .await
                    .expect("the gate stays open")
                    .forget();
                return Ok(if self.retired.load(Ordering::SeqCst) {
                    Vec::new()
                } else {
                    vec![self.id]
                });
            }
            Ok(
                if name == segment_service("a", SegmentPhase::Retired)
                    && self.retired.load(Ordering::SeqCst)
                {
                    vec![self.id]
                } else {
                    Vec::new()
                },
            )
        })
    }

    fn list_services(&self) -> BoxFuture<'_, anyhow::Result<Vec<String>>> {
        Box::pin(async { unreachable!("listing reads each known phase") })
    }

    fn watch_instances(
        &self,
        _: &str,
    ) -> BoxFuture<'_, anyhow::Result<BoxStream<'static, ServiceEvent>>> {
        Box::pin(async { unreachable!("listing opens no watch") })
    }
}
