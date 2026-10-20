use crate::{Error, ErrorKind, Result, Roster};
use gdl_model::SegmentRegistration;
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use velo::{PeerInfo, Velo, discovery::ServiceDiscovery};
use velo_hub::client::{HubClient, HubServiceRegistration};

/// A segment's hub registration and readiness services.
pub struct HubSession {
    client: HubClient,
    registration: SegmentRegistration,
    ready: Option<HubServiceRegistration>,
}

impl HubSession {
    /// Register the compiled graph identity before joining the deployment.
    pub async fn register(
        host: &str,
        public_port: u16,
        control_port: u16,
        node: &Velo,
        registration: SegmentRegistration,
    ) -> Result<Self> {
        let client = HubClient::builder()
            .host(host)
            .public_port(public_port)
            .control_port(control_port)
            .build()
            .map_err(failure)?;
        let payload = serde_json::to_value(&registration).map_err(failure)?;
        client
            .register(node, BTreeMap::from([("gdl".into(), payload)]))
            .await
            .map_err(failure)?;
        Ok(Self {
            client,
            registration,
            ready: None,
        })
    }

    /// Wait for the hub to admit exactly one instance of each segment.
    pub async fn roster(&self, segments: &[&str], wait: Duration) -> Result<Roster> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            self.client.send_heartbeat().await.map_err(failure)?;
            let members: Vec<Member> = self
                .client
                .ext("gdl")
                .get("/members")
                .await
                .map_err(failure)?;
            let mut roster = Roster::new();
            for member in members {
                if member.registration.contract != self.registration.contract
                    || member.registration.placement != self.registration.placement
                {
                    return Err(failure("hub returned a different deployment"));
                }
                roster.add(member.registration.segment, member.peer);
            }
            for segment in segments {
                if roster.instances(segment).len() > 1 {
                    return Err(failure(format!(
                        "multiple replicas of `{segment}`; this launch requires one"
                    )));
                }
            }
            if segments.iter().all(|s| roster.instances(s).len() == 1) {
                return Ok(roster);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failure("timed out waiting for hub registrations"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Advertise installed handlers, then wait for the admitted peers to do so.
    pub async fn ready(
        &mut self,
        roster: &Roster,
        segments: &[&str],
        wait: Duration,
    ) -> Result<()> {
        self.ready = Some(
            self.client
                .register_service(&service(&self.registration.segment))
                .await
                .map_err(failure)?,
        );
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            self.client.send_heartbeat().await.map_err(failure)?;
            let mut complete = true;
            for segment in segments {
                let ids = self
                    .client
                    .services()
                    .get_instances(&service(segment))
                    .await
                    .map_err(failure)?;
                complete &= roster
                    .instances(segment)
                    .iter()
                    .all(|peer| ids.contains(&peer.instance_id()));
            }
            if complete {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failure("timed out waiting for segment handlers"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Keep the lease alive. A failed lease ends this process instead of serving stale membership.
    pub async fn maintain(&self) -> Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            self.client.send_heartbeat().await.map_err(failure)?;
        }
    }
}

fn service(segment: &str) -> String {
    format!(
        "gdl-ready-{}",
        segment
            .bytes()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}
fn failure(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Transport, error.to_string())
}
#[derive(Deserialize)]
struct Member {
    registration: SegmentRegistration,
    peer: PeerInfo,
}

/// Bind a TCP node to this container's routable address.
///
/// Binding to an unspecified address would advertise `0.0.0.0` to peers.
pub async fn bind_container_node(port: u16) -> Result<Arc<Velo>> {
    let host = std::env::var("HOSTNAME").map_err(failure)?;
    let address = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(failure)?
        .find(|address| address.is_ipv4())
        .ok_or_else(|| failure("container hostname has no IPv4 address"))?;
    super::bind_node(address).await
}
