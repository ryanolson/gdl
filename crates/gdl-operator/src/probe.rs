//! What the operator reads from a pool, and the one thing it asks of it:
//! the hub's list of its instances, each pod's `/metrics`, and the hub's
//! retire route. All are HTTP routes, so the operator links no velo: the
//! instance list is read into a mirror of the hub's type
//! (`gdl_watch::Instances`) with only the fields it uses, which a test
//! holds to the hub's type.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use serde::Deserialize;

use crate::{GdlScalerSpec, Handoff};

/// Where a pool's hub lists its instances, on its control port.
const INSTANCES: &str = "/v1/ext/gdl-pool/instances";

/// How long one read of a hub or a pod may take.
const TIMEOUT: Duration = Duration::from_secs(3);

/// The hub's list of its members.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Instances {
    /// Every member of the hub, in no order.
    pub instances: Vec<Instance>,
}

/// One member of a pool's hub, as the operator reads it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Instance {
    /// Its segment.
    pub segment: String,
    /// Its velo peer, of which the operator reads only the instance id.
    #[serde(default)]
    pub peer: Option<Peer>,
    /// The socket addresses its velo TCP transports advertise: under
    /// Kubernetes, its pod's IP.
    #[serde(default)]
    pub addresses: Vec<SocketAddr>,
    /// The furthest phase it has announced: `joining`, `ready`,
    /// `detached`, `retired`, or `drained`. Read as text, so that a phase
    /// a later hub adds does not fail the list.
    pub phase: String,
    /// An entry instance's latest load report, while it is ready.
    #[serde(default)]
    pub load: Option<InstanceLoad>,
    /// The incarnation of its current registration, which a retire
    /// request names. `None` from a hub of a build from before the retire
    /// route.
    #[serde(default)]
    pub incarnation: Option<u64>,
    /// Whether its hub holds a retire request for its current
    /// registration.
    #[serde(default)]
    pub retire_requested: bool,
}

/// A member's velo peer, as the operator reads it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Peer {
    /// Its instance id, as text.
    pub instance_id: String,
}

impl Instance {
    /// Its instance id and the incarnation of its registration: what a
    /// retire request names. `None` from a hub that lists neither.
    pub fn registration(&self) -> Option<(String, u64)> {
        Some((self.peer.as_ref()?.instance_id.clone(), self.incarnation?))
    }
}

/// An entry instance's latest load report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct InstanceLoad {
    /// The roots it held admitted.
    pub admitted: u64,
    /// Its admission limit.
    pub limit: u64,
    /// Its committed KV, in tokens: the KV of the sessions active within
    /// the pool's cache target, resident or evicted. `None` when its nodes
    /// set none, or from a hub of a build from before the numbers, which
    /// leaves the field out.
    #[serde(default)]
    pub kv_committed: Option<u64>,
}

impl Instances {
    /// The member whose current registration is `id` in `incarnation`.
    pub fn by_registration(&self, id: &str, incarnation: u64) -> Option<&Instance> {
        self.instances.iter().find(|instance| {
            instance.incarnation == Some(incarnation)
                && instance
                    .peer
                    .as_ref()
                    .is_some_and(|peer| peer.instance_id == id)
        })
    }

    /// The member whose velo address has `ip`: a pod's instance.
    pub fn at(&self, ip: &str) -> Option<&Instance> {
        self.instances.iter().find(|instance| {
            instance
                .addresses
                .iter()
                .any(|address| address.ip().to_string() == ip)
        })
    }
}

/// What a pod's `/metrics` says it holds, for choosing a victim and for
/// reading its drain's progress. Each is 0 when the series is absent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Held {
    /// `admission_admitted`: root requests its ledger holds.
    pub admitted: u64,
    /// `egress_in_use`: streams it produces that velo has not handed on.
    pub egress: u64,
    /// `active_anchors`: streams still to arrive.
    pub anchors: u64,
    /// `inflight_requests`, summed over its entries: requests it still
    /// has work for, a source's own included, which admission does not
    /// see.
    pub inflight: u64,
}

impl Held {
    /// Its load, for choosing a victim: the larger of its admitted roots
    /// and its requests in flight. A source's own requests are only in
    /// the second, and an entry's admitted requests outlast the second
    /// while velo still holds their output.
    pub fn load(&self) -> u64 {
        self.admitted.max(self.inflight)
    }
}

/// `text`, a Prometheus text exposition, read for the series of a segment
/// whose metric names `spec` prefixes.
pub fn held(text: &str, spec: &GdlScalerSpec) -> Held {
    let names = [
        spec.metric("admission_admitted"),
        spec.metric("egress_in_use"),
        spec.metric("active_anchors"),
        spec.metric("inflight_requests"),
    ];
    let mut sums = [0u64; 4];
    for (name, value) in samples(text) {
        if let Some(at) = names.iter().position(|wanted| wanted == name) {
            sums[at] = sums[at].saturating_add(value);
        }
    }
    Held {
        admitted: sums[0],
        egress: sums[1],
        anchors: sums[2],
        inflight: sums[3],
    }
}

/// What a pod's `/metrics` says of its leave hooks: the series
/// `leave_outcome{hook, outcome}`, read by its whole name. A hook that is
/// registered reads 1 at `outcome="pending"` until it ends, then 1 at the
/// one outcome it ended with. No series at all is no hook, which is also
/// what an instance of a build without hooks shows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Leave {
    /// The hooks it shows.
    pub hooks: usize,
    /// The hooks still `pending`.
    pub pending: usize,
    /// The worst outcome of those that ended; an outcome of a later build
    /// counts as an end, and for nothing here.
    pub worst: Option<Handoff>,
}

/// `text`, a Prometheus text exposition, read for the leave hooks of a
/// segment whose metric names `spec` prefixes.
pub fn leave(text: &str, spec: &GdlScalerSpec) -> Leave {
    let name = spec.metric("leave_outcome");
    let mut hooks: BTreeMap<String, bool> = BTreeMap::new();
    let mut worst = None;
    for line in text.lines() {
        let Some(rest) = line.trim().strip_prefix(name.as_str()) else {
            continue;
        };
        let Some((labels, value)) = rest.strip_prefix('{').and_then(labelled) else {
            continue;
        };
        let (Some(hook), Some(outcome)) = (labels.get("hook"), labels.get("outcome")) else {
            continue;
        };
        let set = value
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<f64>().ok())
            == Some(1.0);
        let pending = hooks.entry(hook.clone()).or_insert(false);
        if outcome == "pending" {
            *pending |= set;
            continue;
        }
        if set {
            let ended = match outcome.as_str() {
                "nothing" => Some(Handoff::Nothing),
                "done" => Some(Handoff::Done),
                "partial" => Some(Handoff::Partial),
                "failed" => Some(Handoff::Failed),
                "timed_out" => Some(Handoff::TimedOut),
                _ => None,
            };
            worst = worst.max(ended);
        }
    }
    Leave {
        hooks: hooks.len(),
        pending: hooks.values().filter(|pending| **pending).count(),
        worst,
    }
}

/// A sample's labels, after its `{`, and the rest of its line after the
/// closing `}`. `None` for labels that do not parse.
fn labelled(text: &str) -> Option<(BTreeMap<String, String>, &str)> {
    let mut labels = BTreeMap::new();
    let mut rest = text.trim_start();
    loop {
        if let Some(after) = rest.strip_prefix('}') {
            return Some((labels, after));
        }
        let (key, after) = rest.split_once('=')?;
        let (value, after) = quoted(after.trim_start())?;
        labels.insert(key.trim().to_string(), value);
        let after = after.trim_start();
        rest = after.strip_prefix(',').unwrap_or(after).trim_start();
    }
}

/// The label value `text` opens with, quoted and escaped as the text
/// format writes it, and the text after its closing quote.
fn quoted(text: &str) -> Option<(String, &str)> {
    let body = text.strip_prefix('"')?;
    let mut value = String::new();
    let mut escaped = false;
    for (at, c) in body.char_indices() {
        if escaped {
            value.push(if c == 'n' { '\n' } else { c });
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == '"' {
            return Some((value, &body[at + 1..]));
        } else {
            value.push(c);
        }
    }
    None
}

/// How a pool's hub answered a retire request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Asked {
    /// 202: the hub holds it.
    Accepted,
    /// 404: the instance is no member of the pool's class.
    Unknown,
    /// 409: the incarnation is not the member's current one, or the pool
    /// drains. The hub's reason.
    Refused(String),
}

/// Each sample of `text` whose value is a whole number that is not
/// negative: its metric's name and its value. Comments, and samples of
/// any other value, are skipped.
pub fn samples(text: &str) -> impl Iterator<Item = (&str, u64)> {
    text.lines().filter_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let name_end = line.find(|c: char| c == '{' || c.is_whitespace())?;
        let name = &line[..name_end];
        // The value follows the labels' closing brace, which a label's
        // value cannot hold unescaped as the last `}` of the line.
        let rest = match line[name_end..].strip_prefix('{') {
            Some(_) => &line[line.rfind('}')? + 1..],
            None => &line[name_end..],
        };
        let value: f64 = rest.split_whitespace().next()?.parse().ok()?;
        (value >= 0.0 && value.fract() == 0.0 && value <= u64::MAX as f64)
            .then_some((name, value as u64))
    })
}

/// The sum of the samples of `name` in `text`.
#[cfg(test)]
fn sum(text: &str, name: &str) -> u64 {
    samples(text)
        .filter(|(sample, _)| *sample == name)
        .fold(0u64, |sum, (_, value)| sum.saturating_add(value))
}

/// Reads a pool's hub and its pods.
pub trait Probe {
    /// The instance list of `pool`'s hub.
    fn instances(&self, pool: &str) -> impl Future<Output = Result<Instances, String>> + Send;

    /// The `/metrics` text of the pod at `ip`, on its system port `port`.
    fn metrics(&self, ip: &str, port: u16) -> impl Future<Output = Result<String, String>> + Send;

    /// Ask `pool`'s hub to retire its member `instance` in the
    /// registration `incarnation` (`gdl_watch::RETIRE_ROUTE`). An error is
    /// a hub that did not answer, or answered otherwise.
    fn retire(
        &self,
        pool: &str,
        instance: &str,
        incarnation: u64,
    ) -> impl Future<Output = Result<Asked, String>> + Send;
}

/// [`Probe`] over HTTP, from inside the cluster: a pool's hub at its
/// Service, which has the pool's name, in the operator's namespace, and a
/// pod at its IP.
#[derive(Debug, Clone)]
pub struct HttpProbe {
    client: reqwest::Client,
    namespace: String,
}

impl HttpProbe {
    /// A probe of the pools in `namespace`.
    pub fn new(namespace: &str) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
        })
    }

    /// The HTTP client, with its timeout.
    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// The namespace whose pools it reaches.
    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    async fn get(&self, url: &str) -> Result<reqwest::Response, String> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("GET {url}: {}", response.status()));
        }
        Ok(response)
    }
}

impl HttpProbe {
    /// The control listener of `pool`'s hub, at its Service.
    fn control(&self, pool: &str) -> String {
        format!(
            "http://{pool}.{}.svc:{}",
            self.namespace,
            gdl_model::HUB_CONTROL_PORT
        )
    }
}

impl Probe for HttpProbe {
    async fn instances(&self, pool: &str) -> Result<Instances, String> {
        let url = format!("{}{INSTANCES}", self.control(pool));
        self.get(&url)
            .await?
            .json()
            .await
            .map_err(|e| format!("GET {url}: {e}"))
    }

    async fn metrics(&self, ip: &str, port: u16) -> Result<String, String> {
        let host = match ip.parse::<std::net::IpAddr>() {
            Ok(std::net::IpAddr::V6(_)) => format!("[{ip}]"),
            _ => ip.to_string(),
        };
        let url = format!("http://{host}:{port}/metrics");
        self.get(&url)
            .await?
            .text()
            .await
            .map_err(|e| format!("GET {url}: {e}"))
    }

    async fn retire(&self, pool: &str, instance: &str, incarnation: u64) -> Result<Asked, String> {
        let url = format!("{}{INSTANCES}/{instance}/retire", self.control(pool));
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "incarnation": incarnation }))
            .send()
            .await
            .map_err(|e| format!("POST {url}: {e}"))?;
        match response.status().as_u16() {
            202 => Ok(Asked::Accepted),
            404 => Ok(Asked::Unknown),
            409 => Ok(Asked::Refused(response.text().await.unwrap_or_default())),
            status => Err(format!("POST {url}: {status}")),
        }
    }
}

/// A pod's load as the hub and its `/metrics` give it: its metrics when
/// they answer, else the hub's latest report of it as an entry instance.
pub fn load_of(metrics: Option<&Held>, instance: Option<&Instance>) -> Option<u64> {
    metrics
        .map(Held::load)
        .or_else(|| instance.and_then(|i| i.load).map(|load| load.admitted))
}

/// A pod's committed KV, from its hub's latest load report of it as an
/// entry instance. `None` when the hub did not answer or list it, or its
/// report has no number. A report counts for up to three report periods,
/// so the number may be that old.
pub fn committed_of(instance: Option<&Instance>) -> Option<u64> {
    instance?.load?.kv_committed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(prefix: Option<&str>) -> GdlScalerSpec {
        let mut spec: GdlScalerSpec = serde_json::from_value(serde_json::json!({
            "pool": "p", "segment": "s",
            "targetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "p-s"},
        }))
        .expect("a spec");
        spec.metrics_prefix = prefix.map(str::to_string);
        spec
    }

    const SCRAPE: &str = r#"# HELP dynamo_component_admission_admitted Root requests.
# TYPE dynamo_component_admission_admitted gauge
dynamo_component_admission_admitted{dynamo_component="engine",worker_id="ab"} 3
dynamo_component_egress_in_use{dynamo_component="engine"} 2
dynamo_component_active_anchors 1
dynamo_component_inflight_requests{dynamo_endpoint="Engine",x="a}b"} 4
dynamo_component_inflight_requests{dynamo_endpoint="Other"} 1
dynamo_component_foo_admission_admitted 99
admission_admitted 77
dynamo_component_requests_total{dynamo_endpoint="Engine"} 12
process_cpu_seconds_total 1.5
"#;

    #[test]
    fn the_drain_series_are_read_by_their_exact_name() {
        let read = held(SCRAPE, &spec(None));
        assert_eq!(
            read,
            Held {
                admitted: 3,
                egress: 2,
                anchors: 1,
                inflight: 5,
            }
        );
        assert_eq!(read.load(), 5);
        assert_eq!(held(SCRAPE, &spec(Some(""))).admitted, 77);
        assert_eq!(held(SCRAPE, &spec(Some("other"))), Held::default());
        assert_eq!(sum(SCRAPE, "dynamo_component_requests_total"), 12);
        assert_eq!(sum(SCRAPE, "process_cpu_seconds_total"), 0);
    }

    /// The leave hooks are read by the series' whole name and its two
    /// labels: a hook at `pending` 1 has not ended; one at 0 that shows an
    /// outcome at 1 has. The worst outcome is taken, and an outcome of a
    /// later build counts as an end.
    #[test]
    fn the_leave_hooks_are_read_by_name_and_label() {
        let text = r#"# TYPE dynamo_component_leave_outcome gauge
dynamo_component_leave_outcome{hook="kv",outcome="pending"} 0
dynamo_component_leave_outcome{hook="kv",outcome="done"} 1
dynamo_component_leave_outcome{outcome="pending", hook="flush \"a,b}\""} 1
dynamo_component_leave_outcome{hook="flush \"a,b}\"",outcome="failed"} 0
dynamo_component_leave_outcome{hook="late",outcome="pending"} 0
dynamo_component_leave_outcome{hook="late",outcome="handed_over"} 1
dynamo_component_leave_outcome_total{hook="x",outcome="pending"} 1
dynamo_component_leave_seconds{hook="kv"} 0.25
other_leave_outcome{hook="y",outcome="pending"} 1
"#;
        let read = leave(text, &spec(None));
        assert_eq!(
            read,
            Leave {
                hooks: 3,
                pending: 1,
                worst: Some(Handoff::Done),
            }
        );
        let ended = text.replace(
            r#"outcome="pending", hook="flush \"a,b}\""} 1"#,
            r#"outcome="pending", hook="flush \"a,b}\""} 0"#,
        );
        let ended = ended.replace(
            r#"hook="flush \"a,b}\"",outcome="failed"} 0"#,
            r#"hook="flush \"a,b}\"",outcome="failed"} 1"#,
        );
        assert_eq!(
            leave(&ended, &spec(None)),
            Leave {
                hooks: 3,
                pending: 0,
                worst: Some(Handoff::Failed),
            }
        );
        assert_eq!(leave(SCRAPE, &spec(None)), Leave::default(), "no hook");
        assert_eq!(
            leave(text, &spec(Some("other"))),
            Leave {
                hooks: 1,
                pending: 1,
                worst: None,
            },
            "by the segment's prefix"
        );
    }

    #[test]
    fn a_pod_is_its_instance_by_ip() {
        let instances: Instances = serde_json::from_value(serde_json::json!({
            "instances": [
                {"segment": "engine", "peer": {"instance_id": "i-7", "worker_address": []}, "addresses": ["10.0.0.7:4000"], "phase": "ready",
                 "load": {"admitted": 2, "limit": 8, "refusals": 0, "age_ms": 10, "kv_committed": 640}},
                {"segment": "engine", "addresses": ["10.0.0.8:4000"], "phase": "retired"},
                {"segment": "engine", "addresses": [], "phase": "someLaterPhase"},
            ]
        }))
        .expect("a list");
        let seven = instances.at("10.0.0.7").expect("listed");
        assert_eq!(seven.load.map(|load| load.admitted), Some(2));
        assert_eq!(committed_of(Some(seven)), Some(640));
        assert_eq!(committed_of(instances.at("10.0.0.8")), None);
        assert_eq!(committed_of(None), None);
        assert!(instances.at("10.0.0.9").is_none());
        assert_eq!(load_of(None, Some(seven)), Some(2));
        let metrics = Held {
            admitted: 1,
            ..Held::default()
        };
        assert_eq!(load_of(Some(&metrics), Some(seven)), Some(1));
        assert_eq!(load_of(None, instances.at("10.0.0.8")), None);
    }
}
