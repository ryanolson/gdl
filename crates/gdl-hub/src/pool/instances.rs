//! The list of the hub's members, for a controller outside the pool
//! ([`gdl_watch::INSTANCES_ROUTE`]): each member's segment and peer from
//! admission, its furthest phase from the hub's own service directory,
//! read over loopback as the counting reads it, the incarnation of its
//! registration and whether a retire request is held for it, and an entry
//! instance's latest load report from the feed. The list is made when it is asked
//! for, from what the hub already holds, and nothing is kept for it: it
//! costs the hub nothing between requests, and nothing on a request's
//! path.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use futures::future::try_join_all;
use gdl_model::{SegmentPhase, segment_service};
use gdl_watch::{InstancePhase, Instances, PoolInstance};
use velo_hub::store::Incarnation;
use velo_hub::velo::discovery::ServiceDiscovery;
use velo_hub::velo::transports::utils::parse_endpoints;
use velo_hub::velo::{InstanceId, PeerInfo};

use super::feed::Feed;
use crate::GraphAdmission;

/// How long the hub's own service directory may take to answer every
/// read of one list.
const READ: Duration = Duration::from_secs(5);

/// The services read for each segment, each with the phase it shows, in
/// the order an instance announces them, which is the order they are
/// read in.
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
    services: &impl ServiceDiscovery,
    feed: &Feed,
) -> Result<Instances, String> {
    let members = admission.members();
    let segments: BTreeSet<&str> = members
        .iter()
        .map(|(_, segment, _)| segment.as_str())
        .collect();
    // velo-hub keeps its service directory to itself, and serves one
    // service per read. The segments are read at once, and each segment's
    // phases one after another, in the order an instance announces them,
    // so that a member's listed phase never goes back from one list to
    // the next while its announcements stand, without anything kept
    // between lists. It holds because, while they stand, an instance
    // withdraws only `ready`, and only once the hub has taken its
    // `retired` (docs/src/concepts/pools.md, "Listing the instances"): a
    // read of `ready` that misses it is followed by a read of `retired`
    // that sees it, and the later phases stay until the instance leaves.
    // An instance whose `retired` went unanswered withdraws it, and is
    // listed `ready` again, as the hub holds it.
    let reads = segments.iter().map(|segment| async move {
        let mut read = Vec::with_capacity(PHASES.len());
        for (phase, shown) in PHASES {
            let ids = services
                .get_instances(&segment_service(segment, phase))
                .await?;
            read.push((shown, ids));
        }
        anyhow::Ok(read)
    });
    let read = tokio::time::timeout(READ, try_join_all(reads))
        .await
        .map_err(|_| "the hub's service directory did not answer in time".to_string())?
        .map_err(|error| format!("the hub's service directory did not answer: {error:#}"))?;
    let mut phases: HashMap<InstanceId, InstancePhase> = HashMap::new();
    for (shown, ids) in read.into_iter().flatten() {
        for id in ids {
            let phase = phases.entry(id).or_insert(shown);
            *phase = (*phase).max(shown);
        }
    }
    // A member that left during the reads has lost its services, and
    // would read as `joining`, a phase before the one it had: it is left
    // out, as a list made a moment later would leave it out. A
    // registration is told by its incarnation as well as its id, since the
    // id can come back, even for another segment, while the reads run.
    let staying: HashSet<(InstanceId, Incarnation)> = admission
        .members()
        .into_iter()
        .map(|(incarnation, _, peer)| (peer.instance_id, incarnation))
        .collect();
    let mut loads = feed.loads(Instant::now());
    let requested = feed.retire_requests();
    let instances = members
        .into_iter()
        .filter(|(incarnation, _, peer)| staying.contains(&(peer.instance_id, *incarnation)))
        .map(|(incarnation, segment, peer)| {
            let phase = phases
                .get(&peer.instance_id)
                .copied()
                .unwrap_or(InstancePhase::Joining);
            // The feed learns of a retirement from its own watch, which can
            // lag the directory read above: a load is shown only with the
            // phase this list read, so a retired instance never shows one.
            let load = loads
                .remove(&peer.instance_id)
                .filter(|_| phase == InstancePhase::Ready);
            let incarnation = incarnation.as_u64();
            PoolInstance {
                phase,
                load,
                incarnation: Some(incarnation),
                retire_requested: requested.contains(&(peer.instance_id, incarnation)),
                addresses: addresses(&peer),
                segment,
                peer,
            }
        })
        .collect();
    Ok(Instances { instances })
}

/// The socket addresses `peer`'s velo TCP transports advertise, each once,
/// in the order of their transport keys. Every transport entry is
/// read, whatever its key, since a TCP transport can be registered under
/// a key of its own; an entry that is not a TCP endpoint is skipped.
fn addresses(peer: &PeerInfo) -> Vec<SocketAddr> {
    let address = &peer.worker_address;
    let mut keys = address.available_transports().unwrap_or_default();
    keys.sort();
    let mut addresses = Vec::new();
    for key in keys {
        let Ok(Some(entry)) = address.get_entry(&key) else {
            continue;
        };
        for found in endpoints(&entry) {
            if !addresses.contains(&found) {
                addresses.push(found);
            }
        }
    }
    addresses
}

/// The TCP endpoints of one transport entry: velo's list of endpoints, or
/// one `tcp://ip:port` or `ip:port`. A text entry is parsed here, never by
/// velo, whose fallback resolves a host name and would block the hub on
/// an entry such as `nats://host:4222`. velo's list is MessagePack, which
/// is never valid UTF-8.
fn endpoints(entry: &[u8]) -> Vec<SocketAddr> {
    match std::str::from_utf8(entry) {
        Ok(text) => text
            .strip_prefix("tcp://")
            .unwrap_or(text)
            .parse()
            .into_iter()
            .collect(),
        Err(_) => parse_endpoints(entry)
            .map(|endpoints| {
                endpoints
                    .iter()
                    .filter_map(|endpoint| endpoint.socket_addr())
                    .collect()
            })
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::addresses;
    use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

    /// A worker address of `entries`, encoded as velo encodes one: a
    /// MessagePack map of each key to its bytes, as an array (array16, so
    /// that a value may be longer than a fixarray's 15).
    fn address(entries: &[(&str, &[u8])]) -> WorkerAddress {
        let mut encoded = vec![0x80 | u8::try_from(entries.len()).expect("a few entries")];
        for (key, value) in entries {
            encoded.push(0xa0 | u8::try_from(key.len()).expect("a short key"));
            encoded.extend_from_slice(key.as_bytes());
            encoded.push(0xdc);
            let len = u16::try_from(value.len()).expect("a short value");
            encoded.extend_from_slice(&len.to_be_bytes());
            for &byte in *value {
                if byte >= 0x80 {
                    encoded.push(0xcc);
                }
                encoded.push(byte);
            }
        }
        WorkerAddress::from_encoded(encoded)
    }

    /// Every entry that is a TCP endpoint is listed, whatever its key, and
    /// each address once; one that is not is skipped, never resolved.
    #[test]
    fn every_tcp_entry_is_listed_once_and_others_are_skipped() {
        let peer = PeerInfo::new(
            InstanceId::new_v4(),
            address(&[
                ("a", b"tcp://127.0.0.1:9000"),
                ("b", b"nats://host:4222"),
                ("c", b"\xff\x00"),
                ("d", b"127.0.0.1:9000"),
                ("e", b"10.0.0.7:9001"),
            ]),
        );
        let listed: Vec<String> = addresses(&peer).iter().map(ToString::to_string).collect();
        assert_eq!(listed, ["127.0.0.1:9000", "10.0.0.7:9001"]);
    }
}
