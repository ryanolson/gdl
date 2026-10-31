//! What the pool controller asks a pool's hub, over its HTTP routes: its
//! health, whether its drained record is withdrawn, a drain, and a
//! presence. As a trait, so that the tests give a fake hub.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::probe::HttpProbe;

/// Where a pool's hub serves its routes, on either listener.
const ROUTES: &str = "/v1/ext/gdl-pool";

/// A pool's health, as its hub answers it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Health {
    /// Whether it answered 200: the pool is ready, its hub waits for
    /// nothing, and every class it requires is met.
    #[serde(skip)]
    pub healthy: bool,
    /// `ready`, `degraded`, `unavailable`, or `draining`. Read as text, so
    /// that a state a later hub adds does not fail the read.
    pub state: String,
    /// Each segment's usable instances and quorum.
    #[serde(default)]
    pub segments: BTreeMap<String, SegmentHealth>,
    /// Whether its drained record is withdrawn.
    #[serde(default)]
    pub withdrawn: bool,
    /// The presence its last stored record says: `serving` or
    /// `scaled_to_zero`. Absent before its first record, and from a hub of
    /// an earlier build.
    #[serde(default)]
    pub presence: Option<String>,
}

impl Health {
    /// The presence its last stored record says, when it says one this
    /// operator knows.
    pub fn presence(&self) -> Option<gdl_k8s::Presence> {
        match self.presence.as_deref()? {
            "serving" => Some(gdl_k8s::Presence::Serving),
            "scaled_to_zero" => Some(gdl_k8s::Presence::ScaledToZero),
            _ => None,
        }
    }
}

/// One segment's count, as the hub's health says it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct SegmentHealth {
    /// Its usable instances.
    pub usable: u32,
    /// Its quorum.
    pub quorum: u32,
}

/// How a hub answered a drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drain {
    /// It drains, or already did.
    Draining,
    /// It refused (409): a pool with no parent, or one whose class no hop
    /// enters, cannot be drained.
    Refused(String),
}

/// A pool's hub, reached in the cluster.
pub trait Hubs {
    /// Pool `pool`'s health.
    fn health(&self, pool: &str) -> impl Future<Output = Result<Health, String>> + Send;

    /// Whether pool `pool`'s hub says its drained record is withdrawn.
    fn withdrawn(&self, pool: &str) -> impl Future<Output = Result<bool, String>> + Send;

    /// Ask pool `pool`'s hub to drain it.
    fn drain(&self, pool: &str) -> impl Future<Output = Result<Drain, String>> + Send;

    /// Ask pool `pool`'s hub to say `presence` in its record, and return
    /// once the directory has stored it.
    fn presence(
        &self,
        pool: &str,
        presence: gdl_k8s::Presence,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

impl HttpProbe {
    fn hub(&self, pool: &str, port: u16, route: &str) -> String {
        format!(
            "http://{pool}.{}.svc:{port}{ROUTES}{route}",
            self.namespace()
        )
    }
}

impl Hubs for HttpProbe {
    async fn health(&self, pool: &str) -> Result<Health, String> {
        let url = self.hub(pool, gdl_model::HUB_PUBLIC_PORT, "/health");
        let response = self
            .client()
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        let healthy = response.status() == reqwest::StatusCode::OK;
        let mut health: Health = response
            .json()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        health.healthy = healthy;
        Ok(health)
    }

    async fn withdrawn(&self, pool: &str) -> Result<bool, String> {
        #[derive(Deserialize)]
        struct Withdrawn {
            withdrawn: bool,
        }
        let url = self.hub(pool, gdl_model::HUB_PUBLIC_PORT, "/withdrawn");
        let response = self
            .client()
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("GET {url}: {}", response.status()));
        }
        let body: Withdrawn = response
            .json()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        Ok(body.withdrawn)
    }

    async fn drain(&self, pool: &str) -> Result<Drain, String> {
        let url = self.hub(pool, gdl_model::HUB_CONTROL_PORT, "/drain");
        let response = self
            .client()
            .post(&url)
            .send()
            .await
            .map_err(|e| format!("POST {url}: {e}"))?;
        match response.status() {
            status if status.is_success() => Ok(Drain::Draining),
            reqwest::StatusCode::CONFLICT => {
                Ok(Drain::Refused(response.text().await.unwrap_or_default()))
            }
            status => Err(format!("POST {url}: {status}")),
        }
    }

    async fn presence(&self, pool: &str, presence: gdl_k8s::Presence) -> Result<(), String> {
        let url = self.hub(pool, gdl_model::HUB_CONTROL_PORT, "/presence");
        let response = self
            .client()
            .post(&url)
            .json(&presence_body(presence))
            .send()
            .await
            .map_err(|e| format!("POST {url}: {e}"))?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("POST {url}: {status}: {}", body.trim()));
        }
        Ok(())
    }
}

/// The body of a hub's `POST presence`: a mirror of the hub's own
/// `SetPresence`, which a test holds to it, so the operator links no
/// velo.
pub fn presence_body(presence: gdl_k8s::Presence) -> serde_json::Value {
    let presence = match presence {
        gdl_k8s::Presence::Serving => "serving",
        gdl_k8s::Presence::ScaledToZero => "scaled_to_zero",
    };
    serde_json::json!({"presence": presence})
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The presence the hub's health says reads as the hub's own type
    /// encodes it.
    #[test]
    fn the_health_presence_is_the_hubs() {
        for (theirs, ours) in [
            (gdl_pools::Presence::Serving, gdl_k8s::Presence::Serving),
            (
                gdl_pools::Presence::ScaledToZero,
                gdl_k8s::Presence::ScaledToZero,
            ),
        ] {
            let body = serde_json::json!({"state": "ready", "presence": theirs});
            let health: Health = serde_json::from_value(body).expect("a health body");
            assert_eq!(health.presence(), Some(ours));
        }
        let earlier: Health = serde_json::from_value(serde_json::json!({"state": "ready"}))
            .expect("a health body of an earlier hub");
        assert_eq!(earlier.presence(), None);
    }

    /// What the operator posts reads as the hub's own request, for each
    /// presence.
    #[test]
    fn the_presence_body_is_the_hubs() {
        for (ours, theirs) in [
            (gdl_k8s::Presence::Serving, gdl_pools::Presence::Serving),
            (
                gdl_k8s::Presence::ScaledToZero,
                gdl_pools::Presence::ScaledToZero,
            ),
        ] {
            let expected = serde_json::to_value(gdl_watch::SetPresence { presence: theirs })
                .expect("the hub's body");
            assert_eq!(presence_body(ours), expected);
            let read: gdl_watch::SetPresence =
                serde_json::from_value(presence_body(ours)).expect("the hub reads it");
            assert_eq!(read.presence, theirs);
        }
    }
}
