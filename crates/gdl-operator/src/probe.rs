//! What the operator reads from a pool: the hub's list of its instances,
//! and each pod's `/metrics`. Both are HTTP routes, so the operator links
//! no velo: the instance list is read into a mirror of the hub's type
//! (`gdl_watch::Instances`) with only the fields it uses, which a test
//! holds to the hub's type.

use std::net::SocketAddr;
use std::time::Duration;

use serde::Deserialize;

use crate::GdlScalerSpec;

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
}

/// An entry instance's latest load report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct InstanceLoad {
    /// The roots it held admitted.
    pub admitted: u64,
    /// Its admission limit.
    pub limit: u64,
}

impl Instances {
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

impl Probe for HttpProbe {
    async fn instances(&self, pool: &str) -> Result<Instances, String> {
        let url = format!(
            "http://{pool}.{}.svc:{}{INSTANCES}",
            self.namespace,
            gdl_model::HUB_CONTROL_PORT
        );
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
}

/// A pod's load as the hub and its `/metrics` give it: its metrics when
/// they answer, else the hub's latest report of it as an entry instance.
pub fn load_of(metrics: Option<&Held>, instance: Option<&Instance>) -> Option<u64> {
    metrics
        .map(Held::load)
        .or_else(|| instance.and_then(|i| i.load).map(|load| load.admitted))
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

    #[test]
    fn a_pod_is_its_instance_by_ip() {
        let instances: Instances = serde_json::from_value(serde_json::json!({
            "instances": [
                {"segment": "engine", "peer": {"ignored": true}, "addresses": ["10.0.0.7:4000"], "phase": "ready",
                 "load": {"admitted": 2, "limit": 8, "refusals": 0, "age_ms": 10}},
                {"segment": "engine", "addresses": ["10.0.0.8:4000"], "phase": "retired"},
                {"segment": "engine", "addresses": [], "phase": "someLaterPhase"},
            ]
        }))
        .expect("a list");
        let seven = instances.at("10.0.0.7").expect("listed");
        assert_eq!(seven.load.map(|load| load.admitted), Some(2));
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
