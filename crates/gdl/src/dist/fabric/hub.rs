use crate::{Error, ErrorKind, Result, Roster};
use gdl_model::SegmentRegistration;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use velo::{InstanceId, PeerInfo, Velo, discovery::ServiceDiscovery};
use velo_hub::client::{ClientError, HubClient, HubServiceRegistration};

/// A segment's hub registration, kept alive from the moment it is made,
/// and the two readiness services it announces: `loaded`, once its nodes
/// are ready to work, and `serving`, once its handlers are installed.
/// Peers route only to loaded instances, and send only once those are
/// serving.
pub struct HubSession {
    client: HubClient,
    registration: SegmentRegistration,
    loaded: Option<HubServiceRegistration>,
    serving: Option<HubServiceRegistration>,
    lease: tokio::task::JoinHandle<()>,
    lost: watch::Receiver<Option<String>>,
}

impl HubSession {
    /// Register the compiled graph identity, and from then on keep the
    /// registration alive with a heartbeat every `heartbeat`: the hub
    /// knows the instance is live from here, whether or not its nodes are
    /// ready, and a node that takes longer to load than the hub's lease is
    /// not reaped meanwhile. The hub admits the instance only if
    /// `registration` names the deployment it serves.
    pub async fn register(
        host: &str,
        public_port: u16,
        control_port: u16,
        node: &Velo,
        registration: SegmentRegistration,
        heartbeat: Duration,
    ) -> Result<Self> {
        let client = HubClient::builder()
            .host(host)
            .public_port(public_port)
            .control_port(control_port)
            .build()
            .map_err(failure)?;
        let payload = serde_json::to_value(&registration).map_err(failure)?;
        client
            .register(node, BTreeMap::from([("gdl".to_string(), payload)]))
            .await
            .map_err(failure)?;
        let (report, lost) = watch::channel(None);
        let lease = tokio::spawn(keep_lease(client.clone(), heartbeat, report));
        Ok(Self {
            client,
            registration,
            loaded: None,
            serving: None,
            lease,
            lost,
        })
    }

    /// Announce that this instance's nodes are ready, then wait until at
    /// least `min_replicas` loaded instances of each segment exist, and
    /// return them: the instances this one will route to. How many
    /// instances a segment has is not fixed, so more is never an error.
    /// Every instance announces before it waits, so none waits for another
    /// that is waiting for it. A hub call that fails is retried until
    /// `wait` runs out.
    pub async fn roster(
        &mut self,
        min_replicas: &[(&str, usize)],
        wait: Duration,
    ) -> Result<Roster> {
        let deadline = tokio::time::Instant::now() + wait;
        if self.loaded.is_none() {
            self.loaded = Some(self.announce(Phase::Loaded, deadline).await?);
        }
        let mut last = String::new();
        loop {
            self.lost()?;
            match tokio::time::timeout_at(deadline, self.loaded_members()).await {
                // A poll cut short by the deadline says nothing an earlier
                // answer did not; the shortfall it reported stands.
                Err(_) => {
                    if last.is_empty() {
                        last = "the hub did not answer in time".into();
                    }
                }
                Ok(Ok(roster)) => {
                    if min_replicas
                        .iter()
                        .all(|(segment, want)| roster.instances(segment).len() >= *want)
                    {
                        return Ok(roster);
                    }
                    let short: Vec<String> = min_replicas
                        .iter()
                        .filter(|(segment, want)| roster.instances(segment).len() < *want)
                        .map(|(segment, want)| {
                            format!("{segment} ({} of {want})", roster.instances(segment).len())
                        })
                        .collect();
                    last = format!("loaded instances missing at the hub: {}", short.join(", "));
                }
                Ok(Err(error)) => last = error.to_string(),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failure(last));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Announce that this instance's handlers are installed, then wait until
    /// every instance in `roster` of the segments this one sends to
    /// (`inner`) has announced the same. The announcement comes first: a
    /// forward path that leaves a segment and comes back makes two segments
    /// send to each other, and each would otherwise wait for the other. A
    /// request that reaches this instance before its wait is over waits at
    /// its held roster, so a peer's announcement is enough to route to it.
    /// An instance that left the hub meanwhile will never announce, and
    /// the requests routed to it would fail, so that is an error at once
    /// rather than a wait that ends the same way.
    pub async fn ready(&mut self, roster: &Roster, inner: &[&str], wait: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + wait;
        if self.serving.is_none() {
            self.serving = Some(self.announce(Phase::Serving, deadline).await?);
        }
        let mut last = String::new();
        loop {
            self.lost()?;
            match tokio::time::timeout_at(deadline, self.serving_members(inner)).await {
                // A poll cut short by the deadline says nothing an earlier
                // answer did not; the shortfall it reported stands.
                Err(_) => {
                    if last.is_empty() {
                        last = "the hub did not answer in time".into();
                    }
                }
                Ok(Ok((members, serving))) => {
                    let mut gone = Vec::new();
                    let mut missing = Vec::new();
                    for (segment, peers) in roster.segments() {
                        if !inner.contains(&segment) {
                            continue;
                        }
                        for peer in peers {
                            let id = peer.instance_id();
                            if !members.contains(&id) {
                                gone.push(format!("{segment}/{id}"));
                            } else if !serving.contains(&id) {
                                missing.push(format!("{segment}/{id}"));
                            }
                        }
                    }
                    if !gone.is_empty() {
                        return Err(failure(format!(
                            "instances left the hub before serving: {}; this roster cannot serve",
                            gone.join(", ")
                        )));
                    }
                    if missing.is_empty() {
                        return Ok(());
                    }
                    last = format!("instances not serving: {}", missing.join(", "));
                }
                Ok(Err(error)) => last = error.to_string(),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(failure(last));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Resolves when the lease is lost: the hub dropped the registration,
    /// or stopped answering for longer than it keeps one. The process is
    /// then invisible to new peers and will be probed as dead, so it should
    /// drain and stop, and let the deployment start it again.
    pub async fn maintain(&self) -> Result<()> {
        let mut lost = self.lost.clone();
        let lost = lost
            .wait_for(|lost| lost.is_some())
            .await
            .map_err(|_| failure("the lease task ended"))?;
        Err(failure(lost.as_deref().unwrap_or("lease lost")))
    }

    fn lost(&self) -> Result<()> {
        match &*self.lost.borrow() {
            Some(reason) => Err(failure(reason)),
            None => Ok(()),
        }
    }

    async fn announce(
        &self,
        phase: Phase,
        deadline: tokio::time::Instant,
    ) -> Result<HubServiceRegistration> {
        let name = service(&self.registration.segment, phase);
        let announced = self.client.register_service(&name);
        tokio::time::timeout_at(deadline, announced)
            .await
            .map_err(|_| failure("the hub did not answer in time"))?
            .map_err(failure)
    }

    /// The members of this deployment, from the hub.
    async fn members(&self) -> Result<Vec<Member>> {
        let members: Vec<Member> = self
            .client
            .ext("gdl")
            .get("/members")
            .await
            .map_err(failure)?;
        for member in &members {
            if member.registration.contract != self.registration.contract
                || member.registration.placement != self.registration.placement
            {
                return Err(failure("hub returned a different deployment"));
            }
        }
        Ok(members)
    }

    /// The instances of `segment` in `phase`.
    async fn instances(&self, segment: &str, phase: Phase) -> Result<HashSet<InstanceId>> {
        Ok(self
            .client
            .services()
            .get_instances(&service(segment, phase))
            .await
            .map_err(failure)?
            .into_iter()
            .collect())
    }

    /// The members that have announced `loaded`, as a roster. One query
    /// per segment: the hub answers for a segment, not for an instance.
    async fn loaded_members(&self) -> Result<Roster> {
        let members = self.members().await?;
        let segments: BTreeSet<String> = members
            .iter()
            .map(|m| m.registration.segment.clone())
            .collect();
        let mut loaded = BTreeMap::new();
        for segment in segments {
            let instances = self.instances(&segment, Phase::Loaded).await?;
            loaded.insert(segment, instances);
        }
        let mut roster = Roster::new();
        for member in members {
            if loaded[&member.registration.segment].contains(&member.peer.instance_id()) {
                roster.add(member.registration.segment, member.peer);
            }
        }
        Ok(roster)
    }

    /// The members, and those of them in `segments` that have announced
    /// `serving`.
    async fn serving_members(
        &self,
        segments: &[&str],
    ) -> Result<(HashSet<InstanceId>, HashSet<InstanceId>)> {
        let members = self.members().await?;
        let mut serving = HashSet::new();
        for segment in segments {
            serving.extend(self.instances(segment, Phase::Serving).await?);
        }
        let members = members.iter().map(|m| m.peer.instance_id()).collect();
        Ok((members, serving))
    }
}

impl Drop for HubSession {
    fn drop(&mut self) {
        self.lease.abort();
    }
}

/// Keep a registration alive: a heartbeat every `heartbeat`, each bounded
/// by the same, retried through failures the hub may recover from (a
/// reset connection, a `busy` answer, an answer that never comes) until
/// `grace` has passed since the last one the hub acknowledged. The lease
/// is reported lost then, or at once when the hub says it holds no
/// registration. The deployment's hub keeps registrations in memory, so
/// one that restarted holds none, and the next heartbeat ends the lease:
/// a segment does not outlive its hub, it drains and stops, and the
/// deployment starts it again.
async fn keep_lease(client: HubClient, heartbeat: Duration, report: watch::Sender<Option<String>>) {
    let grace = heartbeat * 5;
    let mut acknowledged = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(heartbeat).await;
        let result = match tokio::time::timeout(heartbeat, client.send_heartbeat()).await {
            Ok(result) => result.map_err(|e| (is_final(&e), e.to_string())),
            Err(_) => Err((false, format!("no answer within {heartbeat:?}"))),
        };
        match result {
            Ok(()) => acknowledged = tokio::time::Instant::now(),
            Err((final_loss, error)) => {
                if final_loss {
                    let _ = report.send(Some(error));
                    return;
                }
                if acknowledged.elapsed() >= grace {
                    let _ = report.send(Some(format!(
                        "the hub has not answered for {grace:?}: {error}"
                    )));
                    return;
                }
                tracing::warn!(%error, "hub heartbeat failed; retrying");
            }
        }
    }
}

/// Whether a failed hub call means the registration is gone, rather than
/// the hub being momentarily unreachable.
fn is_final(error: &ClientError) -> bool {
    match error {
        ClientError::NotRegistered | ClientError::AlreadyRegistered(_) => true,
        ClientError::Hub { status, .. } => status.as_u16() == 404 || status.as_u16() == 401,
        _ => false,
    }
}

const POLL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
enum Phase {
    Loaded,
    Serving,
}

/// The hub service that announces a segment's instances in `phase`. The
/// segment's name is hex, because a service name has a narrower alphabet.
fn service(segment: &str, phase: Phase) -> String {
    let phase = match phase {
        Phase::Loaded => "loaded",
        Phase::Serving => "serving",
    };
    format!(
        "gdl-{phase}-{}",
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

/// Bind a TCP node to this container's routable address: the address its
/// `HOSTNAME` resolves to, on `port`. Binding to an unspecified address
/// would advertise `0.0.0.0` to peers.
pub async fn bind_container_node(port: u16) -> Result<Arc<Velo>> {
    let host = std::env::var("HOSTNAME").map_err(|_| {
        failure("HOSTNAME is not set; under a hub a segment binds the address its container's hostname resolves to")
    })?;
    let address = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| failure(format!("resolving HOSTNAME {host:?}: {e}")))?
        .find(|address| address.is_ipv4())
        .ok_or_else(|| failure(format!("HOSTNAME {host:?} has no IPv4 address")))?;
    super::bind_node(address).await
}

#[cfg(test)]
mod tests {
    use super::{Phase, service};

    /// The segment name limit is set by the hub's service names: the
    /// longest name allowed fits, and one byte more does not.
    #[test]
    fn the_segment_name_limit_fits_the_hubs_service_names() {
        let fits = "s".repeat(gdl_model::SEGMENT_NAME_LIMIT);
        assert!(velo_hub::protocol::is_valid_service_name(&service(
            &fits,
            Phase::Serving
        )));
        assert!(velo_hub::protocol::is_valid_service_name(&service(
            &fits,
            Phase::Loaded
        )));
        let over = "s".repeat(gdl_model::SEGMENT_NAME_LIMIT + 1);
        assert!(!velo_hub::protocol::is_valid_service_name(&service(
            &over,
            Phase::Serving
        )));
    }
}
