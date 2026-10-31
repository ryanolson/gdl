//! The list of the hub's members, for a controller outside the pool
//! ([`gdl_watch::INSTANCES_ROUTE`]): each member's segment and peer from
//! admission, its furthest phase from the hub's own service directory,
//! read over loopback as the counting reads it, and an entry instance's
//! latest load report from the feed. The list is made when it is asked
//! for, from what the hub already holds, and nothing is kept for it: it
//! costs the hub nothing between requests, and nothing on a request's
//! path.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use gdl_model::{SegmentPhase, segment_service};
use gdl_watch::{InstancePhase, Instances, PoolInstance};
use velo_hub::client::HubServices;
use velo_hub::velo::discovery::ServiceDiscovery;
use velo_hub::velo::transports::utils::parse_endpoints;
use velo_hub::velo::{InstanceId, PeerInfo};

use super::feed::Feed;
use crate::GraphAdmission;

/// How long the hub's own service directory may take to answer every
/// read of one list.
const READ: Duration = Duration::from_secs(5);

/// The services read for each segment, each with the phase it shows.
const PHASES: [(SegmentPhase, InstancePhase); 4] = [
    (SegmentPhase::Ready, InstancePhase::Ready),
    (SegmentPhase::Detached, InstancePhase::Detached),
    (SegmentPhase::Retired, InstancePhase::Retired),
    (SegmentPhase::Drained, InstancePhase::Drained),
];

/// The list of `admission`'s members, with the phases `services`, the
/// hub's own directory, shows them in, and the loads `feed` holds. The
/// reason, when the directory does not answer within 5 s.
pub(super) async fn list(
    admission: &GraphAdmission,
    services: &HubServices,
    feed: &Feed,
) -> Result<Instances, String> {
    let members = admission.members();
    let segments: BTreeSet<&str> = members
        .iter()
        .map(|(segment, _)| segment.as_str())
        .collect();
    // One read per segment and phase, all at once: velo-hub keeps its
    // service directory to itself, and serves one service per read.
    let reads = segments.iter().flat_map(|segment| {
        PHASES.iter().map(move |&(phase, shown)| {
            let name = segment_service(segment, phase);
            async move { services.get_instances(&name).await.map(|ids| (shown, ids)) }
        })
    });
    let read = tokio::time::timeout(READ, try_join_all(reads))
        .await
        .map_err(|_| "the hub's service directory did not answer in time".to_string())?
        .map_err(|error| format!("the hub's service directory did not answer: {error:#}"))?;
    let mut phases: HashMap<InstanceId, InstancePhase> = HashMap::new();
    for (shown, ids) in read {
        for id in ids {
            let phase = phases.entry(id).or_insert(shown);
            *phase = (*phase).max(shown);
        }
    }
    let mut loads = feed.loads(Instant::now());
    let instances = members
        .into_iter()
        .map(|(segment, peer)| PoolInstance {
            phase: phases
                .get(&peer.instance_id)
                .copied()
                .unwrap_or(InstancePhase::Joining),
            load: loads.remove(&peer.instance_id),
            addresses: addresses(&peer),
            segment,
            peer,
        })
        .collect();
    Ok(Instances { instances })
}

/// The socket addresses `peer`'s velo TCP transport advertises: none when
/// it has no TCP entry, or one velo cannot read.
fn addresses(peer: &PeerInfo) -> Vec<SocketAddr> {
    let Ok(Some(entry)) = peer.worker_address.get_entry("tcp") else {
        return Vec::new();
    };
    parse_endpoints(&entry)
        .map(|endpoints| {
            endpoints
                .iter()
                .filter_map(|endpoint| endpoint.socket_addr())
                .collect()
        })
        .unwrap_or_default()
}
