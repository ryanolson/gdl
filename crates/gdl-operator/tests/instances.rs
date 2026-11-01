//! The operator reads the hub's instance list into a mirror of its own,
//! since the hub's type (`gdl_watch::Instances`) links velo for its peer.
//! The hub's encoding of every phase, with and without a load, must read
//! into the mirror with the same values.

use gdl_watch::{InstanceLoad, InstancePhase, Instances, PoolInstance};
use velo::{InstanceId, PeerInfo, WorkerAddress};

fn instance(phase: InstancePhase, address: &str, load: Option<InstanceLoad>) -> PoolInstance {
    PoolInstance {
        segment: "engine".into(),
        peer: PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(b"tcp://test".to_vec()),
        ),
        addresses: vec![address.parse().expect("an address")],
        phase,
        load,
    }
}

#[test]
fn the_hubs_list_reads_into_the_mirror() {
    let load = InstanceLoad {
        admitted: 3,
        limit: 8,
        refusals: 1,
        age_ms: 40,
        // The operator's mirror reads only the committed KV, and is not
        // refused for the rest.
        kv: gdl_pools::KvLoad::NONE
            .with_capacity(80_000)
            .with_committed(12_000)
            .with_eviction_age_ms(9_000),
    };
    let phases = [
        (InstancePhase::Joining, "joining"),
        (InstancePhase::Ready, "ready"),
        (InstancePhase::Detached, "detached"),
        (InstancePhase::Retired, "retired"),
        (InstancePhase::Drained, "drained"),
    ];
    let hub = Instances {
        instances: phases
            .iter()
            .enumerate()
            .map(|(n, (phase, _))| {
                let load = (*phase == InstancePhase::Ready).then_some(load);
                instance(*phase, &format!("10.0.0.{n}:7000"), load)
            })
            .collect(),
    };
    let text = serde_json::to_string(&hub).expect("the hub's list encodes");
    let mirror: gdl_operator::Instances = serde_json::from_str(&text).expect("the mirror reads it");
    assert_eq!(mirror.instances.len(), phases.len());
    for (n, (read, (_, phase))) in mirror.instances.iter().zip(phases).enumerate() {
        assert_eq!(read.segment, "engine");
        assert_eq!(read.phase, phase);
        assert_eq!(
            read.addresses,
            [format!("10.0.0.{n}:7000").parse().expect("an address")]
        );
        let expected = (phase == "ready").then_some(gdl_operator::InstanceLoad {
            admitted: 3,
            limit: 8,
            kv_committed: Some(12_000),
        });
        assert_eq!(read.load, expected);
    }
    assert_eq!(
        mirror.at("10.0.0.1").map(|i| i.phase.as_str()),
        Some("ready")
    );
}

#[test]
fn an_instance_without_kv_numbers_reads_as_no_committed_kv() {
    let load = InstanceLoad {
        admitted: 1,
        limit: 4,
        refusals: 0,
        age_ms: 0,
        kv: gdl_pools::KvLoad::NONE.with_capacity(80_000),
    };
    let hub = Instances {
        instances: vec![instance(InstancePhase::Ready, "10.0.0.1:7000", Some(load))],
    };
    let text = serde_json::to_string(&hub).expect("the hub's list encodes");
    let mirror: gdl_operator::Instances = serde_json::from_str(&text).expect("the mirror reads it");
    assert_eq!(
        mirror.instances[0].load,
        Some(gdl_operator::InstanceLoad {
            admitted: 1,
            limit: 4,
            kv_committed: None,
        })
    );
}
