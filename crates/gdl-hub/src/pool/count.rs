//! The counting of a cell's usable instances, from its hub's own service
//! watches over loopback: the extension's only use of velo-hub's client.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use futures::stream::{BoxStream, SelectAll};
use futures::{StreamExt, TryFutureExt};
use gdl_model::{SegmentPhase, segment_service};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use velo_hub::client::HubClient;
use velo_hub::velo::InstanceId;
use velo_hub::velo::discovery::{ServiceDiscovery, ServiceEvent};

/// The counts not yet taken by the judgment before counting waits for it.
pub(super) const BUFFER: usize = 256;

/// How long to wait before watching again after a watch failed or ended,
/// as a segment's follower does.
const RETRY: Duration = Duration::from_millis(100);

/// The longest a watch may take to open and give its first set.
const OPEN: Duration = Duration::from_secs(5);

/// What the counting tells the publisher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Count {
    /// Every watch has its first set: the usable instances of each
    /// segment, in the class's order.
    Synced(Vec<u32>),
    /// The usable instances of the segment at this index changed.
    Changed(usize, u32),
    /// A watch ended: nothing is known until every watch has its first
    /// set again.
    Lost,
}

/// The address at which the hub's public listener is reached from its own
/// process: the loopback address of the same family when it is bound to
/// an unspecified address, else the address it is bound to.
pub(super) fn loopback(public: SocketAddr) -> SocketAddr {
    let ip = match public.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, public.port())
}

/// Count the usable instances of each of `segments` at the hub at
/// `public`, sending each change to the judgment, until `cancel`. Each
/// event is shown to `observe` before it is counted, with its segment's
/// index, so that what the counting has sent is never ahead of it.
pub(super) async fn run(
    public: SocketAddr,
    control_port: u16,
    segments: Vec<String>,
    counts: mpsc::Sender<Count>,
    observe: impl Fn(usize, Watched, &ServiceEvent) + Send,
    cancel: CancellationToken,
) {
    let client = match HubClient::builder()
        .host(public.ip().to_string())
        .public_port(public.port())
        .control_port(control_port)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::error!(%error, "the cell cannot watch its hub; it stays unavailable");
            return;
        }
    };
    let mut sets: Vec<Usable> = segments.iter().map(|_| Usable::default()).collect();
    let counting = async {
        loop {
            if let Some(mut watches) = open(&client, &segments, &mut sets, &observe).await {
                let synced = sets.iter().map(|set| set.usable).collect();
                if counts.send(Count::Synced(synced)).await.is_err() {
                    return;
                }
                while let Some((index, watched, event)) = watches.next().await {
                    if event == ServiceEvent::Disconnected {
                        break;
                    }
                    observe(index, watched, &event);
                    let set = &mut sets[index];
                    let before = set.usable;
                    set.apply(watched, event);
                    if set.usable != before
                        && counts
                            .send(Count::Changed(index, set.usable))
                            .await
                            .is_err()
                    {
                        return;
                    }
                }
                if counts.send(Count::Lost).await.is_err() {
                    return;
                }
            }
            tokio::time::sleep(RETRY).await;
        }
    };
    tokio::select! {
        () = counting => {}
        () = cancel.cancelled() => {}
    }
}

/// Which of a segment's services a watch follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Watched {
    Ready,
    Retired,
}

type Watch = BoxStream<'static, (usize, Watched, ServiceEvent)>;

/// Open a `ready` and a `retired` watch for each segment, setting each
/// segment's sets from the watches' first sets. `None` if a watch did not
/// open or give its first set in time.
async fn open(
    client: &HubClient,
    segments: &[String],
    sets: &mut [Usable],
    observe: &impl Fn(usize, Watched, &ServiceEvent),
) -> Option<SelectAll<Watch>> {
    let mut watches = SelectAll::new();
    for (index, segment) in segments.iter().enumerate() {
        let mut set = Usable::default();
        for (watched, phase) in [
            (Watched::Retired, SegmentPhase::Retired),
            (Watched::Ready, SegmentPhase::Ready),
        ] {
            let services = client.services();
            let name = segment_service(segment, phase);
            let first = async {
                let mut stream = services.watch_instances(&name).await.ok()?;
                match stream.next().await {
                    Some(ServiceEvent::Initial(ids)) => Some((stream, ids)),
                    _ => None,
                }
            };
            let (stream, ids) = tokio::time::timeout(OPEN, first)
                .unwrap_or_else(|_| None)
                .await?;
            let first = ServiceEvent::Initial(ids);
            observe(index, watched, &first);
            set.apply(watched, first);
            watches.push(stream.map(move |event| (index, watched, event)).boxed());
        }
        sets[index] = set;
    }
    Some(watches)
}

/// The ready and the retired instances of one segment, and how many are
/// usable: ready and not retired. A change costs a set operation, not a
/// pass over the instances.
#[derive(Debug, Default)]
struct Usable {
    ready: HashSet<InstanceId>,
    retired: HashSet<InstanceId>,
    usable: u32,
}

impl Usable {
    fn apply(&mut self, watched: Watched, event: ServiceEvent) {
        match (watched, event) {
            (Watched::Ready, ServiceEvent::Initial(ids)) => {
                self.ready = ids.into_iter().collect();
                self.recount();
            }
            (Watched::Retired, ServiceEvent::Initial(ids)) => {
                self.retired = ids.into_iter().collect();
                self.recount();
            }
            (Watched::Ready, ServiceEvent::Added(id)) => {
                if self.ready.insert(id) && !self.retired.contains(&id) {
                    self.usable += 1;
                }
            }
            (Watched::Ready, ServiceEvent::Removed(id)) => {
                if self.ready.remove(&id) && !self.retired.contains(&id) {
                    self.usable -= 1;
                }
            }
            (Watched::Retired, ServiceEvent::Added(id)) => {
                if self.retired.insert(id) && self.ready.contains(&id) {
                    self.usable -= 1;
                }
            }
            (Watched::Retired, ServiceEvent::Removed(id)) => {
                if self.retired.remove(&id) && self.ready.contains(&id) {
                    self.usable += 1;
                }
            }
            (_, ServiceEvent::Disconnected) => {}
        }
    }

    fn recount(&mut self) {
        let usable = self.ready.difference(&self.retired).count();
        self.usable = u32::try_from(usable).unwrap_or(u32::MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::{Count, ServiceEvent, Usable, Watched};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use velo_hub::velo::InstanceId;
    use velo_hub::{Hub, HubConfig};

    async fn hub(public: u16) -> Hub {
        let mut config = HubConfig::default();
        config.server.public_port = public;
        config.server.control_port = 0;
        Hub::builder(config).serve().await.expect("the hub serves")
    }

    async fn next(counts: &mut mpsc::Receiver<Count>) -> Count {
        tokio::time::timeout(Duration::from_secs(5), counts.recv())
            .await
            .expect("a count in time")
            .expect("the counting runs")
    }

    /// A watch that ends makes every count unknown, and the counting
    /// watches again until the hub answers, then counts from the new sets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_watch_that_ends_is_watched_again() {
        let first = hub(0).await;
        let public = first.public_addr();
        let (sender, mut counts) = mpsc::channel(super::BUFFER);
        let cancel = CancellationToken::new();
        let counting = tokio::spawn(super::run(
            super::loopback(public),
            first.control_addr().port(),
            vec!["d".into()],
            sender,
            |_, _, _| {},
            cancel.clone(),
        ));
        assert_eq!(next(&mut counts).await, Count::Synced(vec![0]));
        first.shutdown().await.expect("the hub stops");
        assert_eq!(next(&mut counts).await, Count::Lost);
        let second = hub(public.port()).await;
        assert_eq!(next(&mut counts).await, Count::Synced(vec![0]));
        cancel.cancel();
        counting.await.expect("the counting ends when cancelled");
        second.shutdown().await.expect("the hub stops");
    }

    /// The count follows each change in either set, in either order, and
    /// an event that repeats what the set holds changes nothing.
    #[test]
    fn usable_is_ready_and_not_retired() {
        let (a, b) = (InstanceId::new_v4(), InstanceId::new_v4());
        let mut set = Usable::default();
        set.apply(Watched::Retired, ServiceEvent::Initial(vec![b]));
        set.apply(Watched::Ready, ServiceEvent::Initial(vec![a, b]));
        assert_eq!(set.usable, 1, "b is retired");
        set.apply(Watched::Ready, ServiceEvent::Added(a));
        assert_eq!(set.usable, 1, "a repeated");
        set.apply(Watched::Retired, ServiceEvent::Added(a));
        assert_eq!(set.usable, 0);
        set.apply(Watched::Retired, ServiceEvent::Removed(b));
        assert_eq!(set.usable, 1, "b no longer retired");
        set.apply(Watched::Ready, ServiceEvent::Removed(b));
        assert_eq!(set.usable, 0);
        set.apply(Watched::Ready, ServiceEvent::Removed(a));
        assert_eq!(set.usable, 0, "a was retired, so not counted");
        set.apply(Watched::Retired, ServiceEvent::Removed(a));
        assert_eq!(set.usable, 0, "a is not ready");
        set.apply(Watched::Retired, ServiceEvent::Added(a));
        set.apply(Watched::Ready, ServiceEvent::Added(a));
        assert_eq!(set.usable, 0, "retired before it was ready");
    }
}
