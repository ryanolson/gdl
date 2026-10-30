//! The Kubernetes objects themselves, as JSON: each is also YAML, and a
//! file of them is a YAML stream that `kubectl apply` and `kubeconform`
//! read. Every object carries the labels `gdl.dev/fleet` and
//! `gdl.dev/cluster`, the fleet's name for the cluster it is rendered for:
//! `deploy` prunes only the objects of its own cluster, so that two
//! clusters whose contexts reach one namespace never prune each other's.
//! Every object of a pool also carries `gdl.dev/pool`, `gdl.dev/class`, and
//! `gdl.dev/segment` (`hub` for its hub's, which no segment can be named). A Deployment and a Service
//! select pods only by the labels that name them, `gdl.dev/fleet`,
//! `gdl.dev/pool`, and `gdl.dev/segment`, which no two pools' objects
//! share: a Deployment's selector cannot change, and a class's name can.

use std::collections::BTreeMap;

use gdl_model::{Cluster, Fleet, HUB_CONTROL_PORT, HUB_PUBLIC_PORT, PoolSettings};
use serde_json::{Map, Value, json};

use super::relay;
use crate::Error;

/// The variable that holds a hub's pod IP, which the hub's record names:
/// only the pod knows it, from the downward API.
pub(super) const POD_IP: &str = "GDL_POD_IP";

/// The address every segment's system listener binds in a pod: every
/// interface, so that the kubelet's probes reach it at the pod's IP.
pub(super) const SYSTEM_HOST: &str = "0.0.0.0";

/// Where a pod mounts its pool's ConfigMap.
const CONFIG_DIR: &str = "/etc/gdl/fleet";

/// Where a pod mounts its segment's secrets, each a file named for the
/// spec's secret, as Compose mounts them.
const SECRETS_DIR: &str = "/run/secrets";

/// The binary in every image, the hub's included.
pub(super) const BINARY: &str = "/usr/local/bin/segment";

/// The group a parent hub's claim is made writable for: the default
/// image's user's. Kubernetes copies nothing from the image into a new
/// claim, which a provisioner usually gives to root.
const STATE_GROUP: u32 = 65532;

/// The longest name of a Service: a DNS label's.
const LABEL_LIMIT: usize = 63;

/// One segment of a layout, as its pods run it.
pub(super) struct Segment {
    pub(super) name: String,
    /// Its image's repository: the package it builds.
    pub(super) repository: String,
    /// Its settings file.
    pub(super) config: String,
    /// How long its pod is let stop, in seconds: its drain bound and a
    /// margin.
    pub(super) grace: u64,
    pub(super) probes: Probes,
    /// The container ports it publishes.
    pub(super) ports: Vec<Port>,
    /// The spec's secrets it mounts.
    pub(super) secrets: Vec<String>,
}

/// Where a segment answers its probes: its system listener, as its
/// settings set it.
pub(super) struct Probes {
    port: u16,
    live: String,
    health: String,
}

impl Probes {
    /// The probes of a segment with settings `config`: refused
    /// when its system listener is off or takes an ephemeral port, since a
    /// probe needs a port it knows.
    pub(super) fn of(config: &toml::Table) -> Result<Self, String> {
        let system = config.get("system").and_then(toml::Value::as_table);
        let text = |key: &str, default: &str| {
            system
                .and_then(|system| system.get(key))
                .and_then(toml::Value::as_str)
                .unwrap_or(default)
                .to_string()
        };
        let port = system
            .and_then(|system| system.get("port"))
            .and_then(toml::Value::as_integer)
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port > 0)
            .ok_or("a pod's probes ask the segment's system listener, so it takes a fixed port")?;
        Ok(Self {
            port,
            live: text("live_path", "/live"),
            health: text("health_path", "/health"),
        })
    }
}

/// A container port that a segment publishes, from Compose's short form:
/// a ClusterIP Service of each pool that runs the segment publishes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Port {
    port: u16,
    protocol: &'static str,
}

impl Port {
    /// `port`, in Compose's short form `[[IP:][HOST]:]CONTAINER[/PROTOCOL]`,
    /// as a segment publishes it in a cluster: its container port.
    /// The host port is Compose's alone. A range is refused: a Service
    /// lists each port.
    pub(super) fn parse(port: &str) -> Result<Self, String> {
        let refuse = |why: &str| format!("`{port}` {why}");
        let (rest, protocol) = match port.rsplit_once('/') {
            Some((rest, protocol)) => (rest, protocol),
            None => (port, "tcp"),
        };
        let protocol = match protocol {
            "tcp" => "TCP",
            "udp" => "UDP",
            "sctp" => "SCTP",
            _ => return Err(refuse("names a protocol that is not tcp, udp, or sctp")),
        };
        let container = rest
            .rsplit_once(':')
            .map_or(rest, |(_, container)| container);
        match container.parse::<u16>() {
            Ok(number) if number > 0 => Ok(Self {
                port: number,
                protocol,
            }),
            _ => Err(refuse(
                "is not one container port: a Kubernetes Service lists each port it publishes, so publish a range as its ports",
            )),
        }
    }

    fn container(&self) -> Value {
        json!({"containerPort": self.port, "protocol": self.protocol})
    }

    fn service(&self) -> Value {
        json!({
            "name": format!("{}-{}", self.protocol.to_ascii_lowercase(), self.port),
            "port": self.port,
            "targetPort": self.port,
            "protocol": self.protocol,
        })
    }
}

/// The pods of one pool in its cluster.
pub(super) struct Pod<'a> {
    pub(super) fleet: &'a str,
    pub(super) pool: &'a PoolSettings,
    /// The fleet's name for the cluster.
    pub(super) cluster_name: &'a str,
    pub(super) cluster: &'a Cluster,
    pub(super) naming: &'a super::Naming<'a>,
}

impl Pod<'_> {
    /// The selector of the pool's pods of `segment`, `hub` for its hub's:
    /// the labels that name the pods, and not the class, which a fleet can
    /// rename while Kubernetes refuses a change to a Deployment's selector.
    fn selector(&self, segment: &str) -> Value {
        let labels = self.labels(Some(segment));
        ["gdl.dev/fleet", "gdl.dev/pool", "gdl.dev/segment"]
            .into_iter()
            .map(|key| (key.to_string(), labels[key].clone()))
            .collect::<Map<_, _>>()
            .into()
    }

    /// The labels of the pool's objects of `segment`, `hub` for its hub's.
    fn labels(&self, segment: Option<&str>) -> Value {
        self.named(segment)
            .into_iter()
            .map(|(key, value)| (key.to_string(), label_value(value).into()))
            .collect::<Map<_, _>>()
            .into()
    }

    /// Each label's key and its whole value, a label's value or not.
    fn named<'s>(&'s self, segment: Option<&'s str>) -> Vec<(&'static str, &'s str)> {
        let mut named = vec![
            ("gdl.dev/fleet", self.fleet),
            ("gdl.dev/cluster", self.cluster_name),
            ("gdl.dev/pool", self.pool.name.as_str()),
            ("gdl.dev/class", self.pool.class.as_str()),
        ];
        named.extend(segment.map(|segment| ("gdl.dev/segment", segment)));
        named
    }

    /// The annotations that hold each label's whole value where the label
    /// holds a shortened one ([`label_value`]), under the label's key.
    fn whole(&self, segment: Option<&str>) -> Map<String, Value> {
        self.named(segment)
            .into_iter()
            .filter(|(_, value)| value.len() > LABEL_LIMIT)
            .map(|(key, value)| (key.to_string(), value.into()))
            .collect()
    }

    /// The metadata of the pool's object `name` of `segment`, if it has
    /// one.
    fn meta(&self, name: &str, segment: Option<&str>) -> Value {
        let mut metadata = json!({
            "name": name,
            "namespace": self.cluster.namespace,
            "labels": self.labels(segment),
        });
        let whole = self.whole(segment);
        if !whole.is_empty() {
            metadata["annotations"] = whole.into();
        }
        metadata
    }

    /// The metadata of the pool's object `name` of `segment`.
    pub(super) fn metadata(&self, name: &str, segment: &str) -> Value {
        self.meta(name, Some(segment))
    }

    /// The pool's ConfigMap, named for the pool: its layout's deployment,
    /// which its hub admits, and each of its segments' settings.
    pub(super) fn config_map(&self, deployment: &str, segments: &[&Segment]) -> Value {
        let mut data = Map::new();
        data.insert("deployment.json".into(), deployment.into());
        for segment in segments {
            data.insert(
                format!("{}.toml", segment.name),
                segment.config.clone().into(),
            );
        }
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": self.meta(&self.pool.name, None),
            "data": data,
        })
    }

    /// The pool's hub: one replica, which `Recreate` stops before it starts
    /// another, since a pool's record has one writer. Its probes ask its own
    /// health, never the pool's, which a parent answers 503 until its
    /// children are ready: a parent that was never ready would have no
    /// Service endpoint for its children to report to.
    pub(super) fn hub(
        &self,
        variables: &Map<String, Value>,
        repository: &str,
        checksum: &str,
        state_dir: Option<&str>,
    ) -> Result<Value, Error> {
        let mut env = vec![json!({
            "name": POD_IP,
            "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
        })];
        env.extend(self.env(variables));
        let probe = |period: u32, failures: u32| {
            json!({
                "httpGet": {"path": "/v1/health", "port": HUB_PUBLIC_PORT},
                "periodSeconds": period,
                "timeoutSeconds": 2,
                "failureThreshold": failures,
            })
        };
        let mut mounts = vec![config_mount()];
        let mut volumes = vec![self.config_volume()];
        if let Some(state_dir) = state_dir {
            mounts.push(json!({"name": "state", "mountPath": state_dir}));
            volumes.push(json!({
                "name": "state",
                "persistentVolumeClaim": {"claimName": relay::state_claim(&self.pool.name)},
            }));
        }
        let container = json!({
            "name": "hub",
            "image": self.image(repository)?,
            "command": [BINARY, format!("{CONFIG_DIR}/deployment.json")],
            "env": env,
            "ports": [
                {"name": "public", "containerPort": HUB_PUBLIC_PORT},
                {"name": "control", "containerPort": HUB_CONTROL_PORT},
            ],
            "livenessProbe": probe(2, 30),
            "readinessProbe": probe(2, 3),
            "volumeMounts": mounts,
        });
        let mut pod = self.pod_spec(container, volumes);
        if state_dir.is_some() {
            pod["securityContext"] = json!({"fsGroup": STATE_GROUP});
        }
        Ok(self.deployment(&self.pool.name, "hub", 1, true, checksum, pod))
    }

    /// The Service of the pool's hub, named for the pool: where its
    /// segments, and its children's hubs, reach it.
    pub(super) fn hub_service(&self) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": self.metadata(&self.pool.name, "hub"),
            "spec": {
                "type": "ClusterIP",
                "selector": self.selector("hub"),
                "ports": [
                    {"name": "public", "port": HUB_PUBLIC_PORT, "targetPort": HUB_PUBLIC_PORT},
                    {"name": "control", "port": HUB_CONTROL_PORT, "targetPort": HUB_CONTROL_PORT},
                ],
            },
        })
    }

    /// The Deployment of the pool's `segment`: the pool's quorum of it, let
    /// stop for its drain bound and a margin, and probed on its system
    /// port, live from its start and ready while it serves.
    pub(super) fn segment(
        &self,
        segment: &Segment,
        variables: &Map<String, Value>,
        checksum: &str,
    ) -> Result<Value, Error> {
        let mut variables = variables.clone();
        variables.insert(
            "GDL_HUB__HOST".into(),
            crate::deployment::variables::string(&self.pool.name).into(),
        );
        let probe = |path: &str, period: u32| {
            json!({
                "httpGet": {"path": path, "port": segment.probes.port},
                "periodSeconds": period,
                "timeoutSeconds": 3,
                "failureThreshold": 3,
            })
        };
        let mut ports = vec![json!({"name": "system", "containerPort": segment.probes.port})];
        ports.extend(segment.ports.iter().map(Port::container));
        let mut mounts = vec![config_mount()];
        let mut volumes = vec![self.config_volume()];
        if !segment.secrets.is_empty() {
            mounts.push(json!({"name": "secrets", "mountPath": SECRETS_DIR, "readOnly": true}));
            let sources: Vec<Value> = segment
                .secrets
                .iter()
                .map(|secret| {
                    let holder = self
                        .cluster
                        .secrets
                        .get(secret)
                        .cloned()
                        .unwrap_or_default();
                    json!({"secret": {"name": holder, "items": [{"key": secret, "path": secret}]}})
                })
                .collect();
            volumes.push(json!({"name": "secrets", "projected": {"sources": sources}}));
        }
        let container = json!({
            "name": "segment",
            "image": self.image(&segment.repository)?,
            "command": [BINARY, format!("{CONFIG_DIR}/{}.toml", segment.name)],
            "env": self.env(&variables),
            "ports": ports,
            "livenessProbe": probe(&segment.probes.live, 10),
            "readinessProbe": probe(&segment.probes.health, 5),
            "volumeMounts": mounts,
        });
        let mut pod = self.pod_spec(container, volumes);
        pod["terminationGracePeriodSeconds"] = segment.grace.into();
        Ok(self.deployment(
            &object_name(&self.pool.name, &segment.name),
            &segment.name,
            self.pool.quorum(&segment.name),
            false,
            checksum,
            pod,
        ))
    }

    /// The ClusterIP Service of the ports the pool's `segment` publishes.
    pub(super) fn entry_service(&self, segment: &Segment) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": self.metadata(&object_name(&self.pool.name, &segment.name), &segment.name),
            "spec": {
                "type": "ClusterIP",
                "selector": self.selector(&segment.name),
                "ports": segment.ports.iter().map(Port::service).collect::<Vec<_>>(),
            },
        })
    }

    /// A Deployment of `replicas` pods of `segment` with `pod` as their
    /// spec, which `checksum`, the pool's ConfigMap's, replaces when the
    /// settings change: a ConfigMap's change alone restarts no pod.
    fn deployment(
        &self,
        name: &str,
        segment: &str,
        replicas: u32,
        recreate: bool,
        checksum: &str,
        pod: Value,
    ) -> Value {
        let labels = self.labels(Some(segment));
        let mut annotations = self.whole(Some(segment));
        annotations.insert("gdl.dev/config".into(), checksum.into());
        let mut spec = json!({
            "replicas": replicas,
            "selector": {"matchLabels": self.selector(segment)},
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": pod,
            },
        });
        if recreate {
            spec["strategy"] = json!({"type": "Recreate"});
        }
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": self.metadata(name, segment),
            "spec": spec,
        })
    }

    /// A pod of the fleet's service account, on nodes of the cluster's
    /// platform. Service links are off: they would add a variable per
    /// Service in the namespace to every container.
    fn pod_spec(&self, container: Value, volumes: Vec<Value>) -> Value {
        let platform = self.cluster.platform.as_deref().unwrap_or_default();
        let mut parts = platform.split('/');
        let os = parts.next().unwrap_or_default();
        let arch = parts.next().unwrap_or_default();
        json!({
            "serviceAccountName": self.fleet,
            "enableServiceLinks": false,
            "nodeSelector": {"kubernetes.io/os": os, "kubernetes.io/arch": arch},
            "containers": [container],
            "volumes": volumes,
        })
    }

    fn config_volume(&self) -> Value {
        json!({"name": "config", "configMap": {"name": self.pool.name}})
    }

    /// `variables`, and then the cluster's own, as a container's `env`.
    /// Kubernetes reads `$(NAME)` in a value as another variable's, and
    /// `$$` as `$`: a cluster's values are written so that they read as
    /// given. A pool's own values hold no `$`, except the hub's reference
    /// to its pod's IP.
    fn env(&self, variables: &Map<String, Value>) -> Vec<Value> {
        let mut env: Vec<Value> = variables
            .iter()
            .map(|(name, value)| json!({"name": name, "value": value}))
            .collect();
        env.extend(
            self.cluster
                .env
                .iter()
                .map(|(name, value)| json!({"name": name, "value": value.replace('$', "$$")})),
        );
        env
    }

    /// The image of `repository` in the cluster's registry, for its
    /// platform.
    fn image(&self, repository: &str) -> Result<String, Error> {
        let registry = self.cluster.registry.as_deref().unwrap_or_default();
        let platform = self.cluster.platform.as_deref().unwrap_or_default();
        let image = self.naming.image(registry, repository, platform)?;
        crate::deployment::validate_image(&image)?;
        Ok(image)
    }
}

/// The image `repository` at `tag` in `registry`, and at `digest` when it
/// is known. Every image is named here: each the objects run, and each
/// that `images` builds and pushes.
pub(in crate::deployment) fn image(
    registry: &str,
    repository: &str,
    tag: &str,
    digest: Option<&str>,
) -> String {
    let mut image = format!("{}/{repository}:{tag}", registry.trim_end_matches('/'));
    if let Some(digest) = digest {
        image.push('@');
        image.push_str(digest);
    }
    image
}

fn config_mount() -> Value {
    json!({"name": "config", "mountPath": CONFIG_DIR, "readOnly": true})
}

/// The address at which a pool's hub records itself: its pod's IP, which
/// the hub's pod reads from [`POD_IP`]. A pod IP is IPv4 here: an IPv6
/// address would need brackets before the port.
pub(super) fn pod_hub() -> String {
    format!("$({POD_IP}):{HUB_PUBLIC_PORT}")
}

/// The address at which pool `pool`'s segments reach its hub: its Service.
pub(super) fn hub_address(pool: &str) -> String {
    format!("{pool}:{HUB_PUBLIC_PORT}")
}

/// The name of pool `pool`'s object of segment `segment`.
fn object_name(pool: &str, segment: &str) -> String {
    format!("{pool}-{segment}")
}

/// The Namespace of `cluster`, which the fleet names `name`, and the
/// fleet's ServiceAccount in it, which carries the cluster's annotations,
/// for a cloud role.
pub(super) fn base(fleet: &str, name: &str, cluster: &Cluster) -> Vec<Value> {
    let labels = json!({
        "gdl.dev/fleet": label_value(fleet),
        "gdl.dev/cluster": label_value(name),
    });
    let mut account = json!({
        "name": fleet,
        "namespace": cluster.namespace,
        "labels": labels,
    });
    if !cluster.service_account_annotations.is_empty() {
        account["annotations"] = json!(cluster.service_account_annotations);
    }
    vec![
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": cluster.namespace, "labels": labels},
        }),
        json!({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": account}),
    ]
}

/// `objects` as a YAML stream: each a JSON document, which YAML reads,
/// with the characters YAML refuses escaped.
pub(super) fn stream(objects: &[Value]) -> serde_json::Result<String> {
    let mut text = String::new();
    for object in objects {
        text.push_str("---\n");
        text.push_str(&crate::deployment::yaml_safe(
            &serde_json::to_string_pretty(object)?,
        ));
        text.push('\n');
    }
    Ok(text)
}

/// A checksum of a ConfigMap's data, FNV-1a over its JSON text, as hex.
pub(super) fn checksum(config: &Value) -> String {
    format!("{:016x}", fnv(&config["data"].to_string()))
}

/// FNV-1a of `text`.
fn fnv(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// The characters of a long value that its label keeps.
const LABEL_PREFIX: usize = 52;

/// `value` as a label's value, which is at most 63 characters: itself when
/// it fits, else its first 52 characters, `-`, and 10 hex digits of its
/// hash, so that two long values that share their start still differ. The
/// whole value is then in the annotation of the label's key.
pub(in crate::deployment) fn label_value(value: &str) -> String {
    if value.len() <= LABEL_LIMIT {
        return value.to_string();
    }
    let hash = format!("{:016x}", fnv(value));
    format!("{}-{}", &value[..LABEL_PREFIX], &hash[..10])
}

/// Refuse a segment whose name a Kubernetes object cannot hold: its
/// Deployment and Service are named for it, and its ConfigMap key.
pub(super) fn check_segment(segment: &str) -> Result<(), String> {
    if dns_label(segment) {
        return Ok(());
    }
    Err(format!(
        "segment `{segment}` cannot name a Kubernetes object: to deploy on Kubernetes, name it with lowercase letters, digits, and `-`, starting with a letter and ending with a letter or digit"
    ))
}

/// Whether `name` is a DNS label that starts with a letter, as a Service's
/// name must be.
fn dns_label(name: &str) -> bool {
    name.len() <= LABEL_LIMIT
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Refuse a platform that is not `<os>/<arch>[/<variant>]`, from which a
/// pod's node selector is read.
pub(super) fn check_platform(platform: &str) -> Result<(), String> {
    let parts: Vec<&str> = platform.split('/').collect();
    let word = |part: &&str| {
        !part.is_empty()
            && part
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    };
    if (2..=3).contains(&parts.len()) && parts.iter().all(word) {
        return Ok(());
    }
    Err(format!(
        "`{platform}` is not `<os>/<arch>` as `docker buildx --platform` takes it, such as `linux/amd64`"
    ))
}

/// Refuse a registry that cannot start an image reference: a host, with a
/// port if it has one, and then path components, or path components alone,
/// which name a Docker Hub namespace. A scheme, a tag, or a digest would
/// make every image name one that Docker refuses.
pub(super) fn check_registry(registry: &str) -> Result<(), String> {
    let mut parts = registry.trim_end_matches('/').split('/').peekable();
    let first = parts.peek().copied().unwrap_or_default();
    let host = first.contains(['.', ':']) || first == "localhost";
    if host {
        parts.next();
    }
    let host_ok = !host || {
        let (name, port) = first.split_once(':').unwrap_or((first, "1"));
        let label = |label: &str| {
            !label.is_empty()
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        };
        name.split('.').all(label) && !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
    };
    // A path component: lowercase letters and digits, joined by `.`, `_`,
    // `__`, or runs of `-`.
    let component = |part: &str| {
        let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
        let separator =
            |run: &str| matches!(run, "." | "_" | "__") || run.chars().all(|c| c == '-');
        !part.is_empty()
            && part.starts_with(alnum)
            && part.ends_with(alnum)
            && part
                .split(alnum)
                .filter(|run| !run.is_empty())
                .all(separator)
    };
    if host_ok && parts.all(component) && !registry.is_empty() {
        return Ok(());
    }
    Err(format!(
        "`{registry}` is not a registry that an image reference can start with: write `host[:port][/path]`, with no scheme, tag, or digest, such as `registry.example:5000/gdl`"
    ))
}

/// Refuse a tag that an image reference cannot hold.
pub(super) fn check_tag(tag: &str) -> Result<(), Error> {
    let shaped = tag.len() <= 128
        && tag.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
        && tag
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c));
    if shaped {
        return Ok(());
    }
    Err(Error::new(format!(
        "`--tag {tag}` is not an image tag: letters, digits, `_`, `.`, and `-`, not starting with `.` or `-`, at most 128 characters"
    )))
}

/// The objects' names in each cluster by their kind, and the pool that
/// named each, so that two objects never share a name.
#[derive(Default)]
pub(super) struct Names(BTreeMap<(String, String, String), String>);

impl Names {
    /// Claim `object`'s name in `cluster` for `pool`: refused when another
    /// pool's object of its kind has it, or when it is a Service's name and
    /// longer than a DNS label.
    pub(super) fn claim(
        &mut self,
        cluster: &str,
        pool: &str,
        object: &Value,
    ) -> Result<(), String> {
        let kind = object["kind"].as_str().unwrap_or_default().to_string();
        let name = object["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if kind == "Service" && name.len() > LABEL_LIMIT {
            return Err(format!(
                "its Service `{name}` is longer than {LABEL_LIMIT} characters, the longest a Service's name may be: shorten the pool's or the segment's name"
            ));
        }
        let key = (cluster.to_string(), kind.clone(), name.clone());
        match self.0.get(&key) {
            Some(other) => Err(format!(
                "its {kind} `{name}` is also pool `{other}`'s in cluster `{cluster}`: rename one of the pools"
            )),
            None => {
                self.0.insert(key, pool.to_string());
                Ok(())
            }
        }
    }
}

/// The README of a fleet's objects: what to apply where, in what order,
/// and what each cluster must hold first.
pub(super) fn readme(fleet: &Fleet) -> String {
    let name = fleet.name().unwrap_or_default();
    let mut text = format!(
        "# Kubernetes objects of fleet `{name}`\n\n\
         One directory per cluster. `base.yaml` holds the cluster's Namespace and the fleet's ServiceAccount; `pools/<pool>.yaml` holds each pool's ConfigMap, its hub's Deployment and Service, a Deployment for each segment of its class, and a Service for each segment that publishes ports. Every object is labelled `gdl.dev/fleet={name}`.\n\n\
         Apply the base first, since every other object is in its Namespace:\n"
    );
    for (cluster, settings) in fleet.clusters() {
        text.push_str(&format!(
            "\n```bash\nkubectl --context {context} apply --server-side -f {cluster}/base.yaml\nkubectl --context {context} apply --server-side -R -f {cluster}/pools\n```\n",
            context = settings.context
        ));
        if !settings.secrets.is_empty() {
            let secrets: Vec<String> = settings
                .secrets
                .iter()
                .map(|(secret, holder)| format!("Secret `{holder}`, with key `{secret}`"))
                .collect();
            text.push_str(&format!(
                "\nBefore its pods start, namespace `{}` of cluster `{cluster}` must hold: {}. The objects name each Secret and hold no value.\n",
                settings.namespace,
                secrets.join(", ")
            ));
        }
    }
    text.push_str("\nEach image must be in its cluster's registry before its pods start: `cargo gdl images --push` builds and pushes them. A pool's hub's Service is named for the pool, and its segments' objects `<pool>-<segment>`.\n");
    text.push_str(&super::dynamodb::readme(fleet));
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A port is its container port in each of Compose's short forms, and
    /// a range or an unknown protocol is refused.
    #[test]
    fn a_port_is_its_container_port() {
        for (port, number, protocol) in [
            ("8080", 8080, "TCP"),
            ("9000:8080", 8080, "TCP"),
            ("127.0.0.1:9000:8080", 8080, "TCP"),
            ("127.0.0.1::8080", 8080, "TCP"),
            ("[::1]:9000:8080/udp", 8080, "UDP"),
            ("8080/sctp", 8080, "SCTP"),
        ] {
            let parsed = Port::parse(port).expect(port);
            assert_eq!((parsed.port, parsed.protocol), (number, protocol), "{port}");
        }
        for port in ["8080-8081", "80-90:80-90", "0", "8080/http", "x"] {
            assert!(Port::parse(port).is_err(), "{port}");
        }
    }

    /// A value of up to 63 characters is its own label; a longer one is its
    /// first 52 characters, `-`, and 10 hex digits of its hash, and two
    /// that share those 52 characters still differ.
    #[test]
    fn a_long_value_is_labelled_by_its_start_and_hash() {
        let fits = "m".repeat(63);
        assert_eq!(label_value(&fits), fits);
        let long = "m".repeat(64);
        let label = label_value(&long);
        assert_eq!(label.len(), 63, "{label}");
        let (start, hash) = label.split_at(LABEL_PREFIX);
        assert_eq!(start, &long[..LABEL_PREFIX]);
        assert!(hash.starts_with('-') && hash[1..].bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(label_value(&"m".repeat(65)), label);
    }

    /// A registry is a host and a path, or a path alone; a scheme, a tag,
    /// or a digest is refused.
    #[test]
    fn a_registry_starts_an_image_reference() {
        for registry in [
            "registry.example/gdl",
            "registry.example:5000/gdl/team",
            "localhost:5000",
            "localhost",
            "000000000000.dkr.ecr.us-east-1.amazonaws.com/gdl",
            "myorg",
            "my-org/my__sub.part",
            "registry.example/gdl/",
        ] {
            assert!(check_registry(registry).is_ok(), "{registry}");
        }
        for registry in [
            "",
            "https://registry.example/gdl",
            "registry.example/gdl:tag",
            "registry.example/gdl@sha256:00",
            "registry.example/GDL",
            "registry.example:/gdl",
            "registry.example//gdl",
            "-bad.example/gdl",
            "registry.example/gdl-",
        ] {
            assert!(check_registry(registry).is_err(), "{registry}");
        }
    }

    /// A platform is `<os>/<arch>`, with an optional variant; a tag is what
    /// an image reference can hold.
    #[test]
    fn a_platform_and_a_tag_are_shaped() {
        for platform in ["linux/amd64", "linux/arm64/v8"] {
            assert!(check_platform(platform).is_ok(), "{platform}");
        }
        for platform in ["arm64", "linux/", "Linux/amd64", "linux/amd64/v8/x"] {
            assert!(check_platform(platform).is_err(), "{platform}");
        }
        for tag in [
            "pinned",
            "v1.2.3",
            "_x",
            "0123456789abcdef-0123456789abcdef",
        ] {
            assert!(check_tag(tag).is_ok(), "{tag}");
        }
        for tag in ["", "-x", ".x", "a:b", &"x".repeat(129)] {
            assert!(check_tag(tag).is_err(), "{tag}");
        }
    }
}
