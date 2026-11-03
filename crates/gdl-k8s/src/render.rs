//! A pool's objects, as JSON. Every object carries the labels
//! `gdl.dev/fleet`, `gdl.dev/cluster`, `gdl.dev/pool`, `gdl.dev/class`,
//! `gdl.dev/region`, and, but for the ConfigMap, `gdl.dev/segment` (`hub`
//! for its hub's, which no segment can be named). A Deployment and a
//! Service select pods only by the labels that name them,
//! `gdl.dev/fleet`, `gdl.dev/pool`, and `gdl.dev/segment`, which no two
//! pools' objects share: a Deployment's selector cannot change, and a
//! class's name can. Every pod says where it serves its metrics, in the
//! `prometheus.io` annotations that a Prometheus's pod discovery reads by
//! convention.

use gdl_model::{HUB_CONTROL_PORT, HUB_PUBLIC_PORT};
use serde_json::{Map, Value, json};

use crate::labels::{LABEL_LIMIT, checksum, label_value};
use crate::pool::{Env, Pool, Port, Resources, Segment, Toleration};
use crate::settings::Settings;

/// The variable that holds a hub's pod IP, which the hub's record names:
/// only the pod knows it, from the downward API.
pub const POD_IP: &str = "GDL_POD_IP";

/// Where a pod mounts its pool's ConfigMap.
const CONFIG_DIR: &str = "/etc/gdl/fleet";

/// Where a segment's pod mounts its pool's rollover ConfigMap
/// ([`rollover`]): a whole directory, since a `subPath` mount never sees
/// the map change.
const ROLLOVER_DIR: &str = "/etc/gdl/rollover";

/// Where a pod mounts its segment's secrets, each a file named for the
/// spec's secret, as Compose mounts them.
const SECRETS_DIR: &str = "/run/secrets";

/// The binary in every image, the hub's included.
pub const BINARY: &str = "/usr/local/bin/segment";

/// The group a parent hub's claim is made writable for: the default
/// image's user's. Kubernetes copies nothing from the image into a new
/// claim, which a provisioner usually gives to root.
const STATE_GROUP: u32 = 65532;

/// The size of a parent hub's claim. A child's record is a few hundred
/// bytes, so the smallest size a provisioner commonly offers is ample.
const STATE_SIZE: &str = "1Gi";

/// Where a hub serves its metrics, on its public port.
pub const HUB_METRICS: &str = "/v1/ext/gdl-pool/metrics";

/// Where a segment serves its metrics, on its system port.
pub const SEGMENT_METRICS: &str = "/metrics";

/// The label a segment's pods carry under the operator, `true` while they
/// take connections, which their entry Service selects on.
pub const ROUTABLE: &str = "gdl.dev/routable";

/// The names of the container ports a hub and a segment serve their
/// metrics on, which a PodMonitor scrapes by name.
pub const PUBLIC: &str = "public";
/// See [`PUBLIC`].
pub const SYSTEM: &str = "system";

/// How a pool's segments are scaled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scaling {
    /// Each segment's Deployment runs the pool's quorum of it.
    Fixed,
    /// The gdl operator scales each segment, through a `GdlScaler` of its
    /// own: its Deployment carries no `replicas`, its pods say whether they
    /// take connections, and its entry Service selects only those that do.
    Operator,
}

/// The objects of `pool`, named `name` in namespace `namespace`: its
/// ConfigMap, its hub's claim under the relay directory when it is a
/// parent, its hub's Deployment and Service, for each segment a
/// Deployment, a Service when it publishes ports, and a `GdlScaler` under
/// [`Scaling::Operator`], and last its DGD when it has one, which runs
/// the segments placed in its components in place of their own objects.
/// Refused when a segment's settings are not TOML or give its probes no
/// fixed port, or when the DGD does not fit the pool.
pub fn children(
    name: &str,
    namespace: &str,
    pool: &Pool,
    scaling: Scaling,
) -> Result<Vec<Value>, String> {
    let render = Render {
        name,
        namespace,
        pool,
        scaling,
    };
    let settings: Vec<Settings> = pool
        .segments
        .iter()
        .map(|segment| {
            Settings::read(&segment.settings)
                .map_err(|e| format!("segment `{}`: {e}", segment.name))
        })
        .collect::<Result<_, _>>()?;
    let mut objects = Vec::new();
    let config = render.config_map();
    let checksum = checksum(&config);
    // The hub reads only the pool's deployment from the ConfigMap: a
    // change to a segment's settings replaces that segment's pods, and
    // never the hub, whose restart drains the pool.
    let hub_checksum =
        crate::labels::checksum(&json!({"data": {"deployment.json": pool.deployment}}));
    objects.push(config);
    objects.extend(render.state_claim());
    objects.push(render.hub(&hub_checksum));
    objects.push(render.hub_service());
    let dgd = crate::dgd::render(
        name,
        namespace,
        pool,
        render.labels(None).as_object().cloned().unwrap_or_default(),
        render.whole(None),
        &checksum,
    )?;
    for (segment, settings) in pool.segments.iter().zip(&settings) {
        // dynamo's operator runs the pods of a segment in the pool's DGD.
        // Its scaler writes its component's scaling adapter, and retires
        // each victim through the hub before the count goes down: the
        // segment has no entry Service to take it out of.
        if let Some(component) = &segment.component {
            if scaling == Scaling::Operator
                && let Some(dgd) = &dgd
            {
                let adapter = crate::dgd::adapter_name(dgd, name, component)?;
                let target = json!({
                    "apiVersion": crate::dgd::ADAPTER_API_VERSION,
                    "kind": crate::dgd::ADAPTER_KIND,
                    "name": adapter,
                });
                let mut scaler = render.scaler(segment, settings, target);
                scaler["spec"]["retire"] = "request".into();
                if let Some(fields) = scaler["spec"].as_object_mut() {
                    fields.remove("service");
                }
                objects.push(scaler);
            }
            continue;
        }
        objects.push(render.segment(segment, settings, &checksum));
        if !segment.ports.is_empty() {
            objects.push(render.entry_service(segment));
        }
        if scaling == Scaling::Operator {
            let target = json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": object_name(name, &segment.name),
            });
            objects.push(render.scaler(segment, settings, target));
        }
    }
    objects.extend(dgd);
    Ok(objects)
}

/// The `GdlPool` of `pool`, named `name` in namespace `namespace`, with
/// the labels its objects carry but `gdl.dev/segment`: the one object of
/// the pool that `cargo gdl` writes for a cluster the operator runs.
pub fn pool_object(name: &str, namespace: &str, pool: &Pool) -> Value {
    let render = Render {
        name,
        namespace,
        pool,
        scaling: Scaling::Operator,
    };
    json!({
        "apiVersion": "gdl.dev/v1alpha1",
        "kind": "GdlPool",
        "metadata": render.meta(name, None),
        "spec": pool,
    })
}

/// The name of pool `pool`'s object of segment `segment`.
pub fn object_name(pool: &str, segment: &str) -> String {
    format!("{pool}-{segment}")
}

/// The name of pool `pool`'s hub's claim.
pub fn state_claim(pool: &str) -> String {
    format!("{pool}-state")
}

/// The name of pool `pool`'s rollover ConfigMap, which its segments' pods
/// mount and the pool never writes: built from the name its own ConfigMap
/// takes, so that the two are named alike.
pub fn rollover(pool: &str) -> String {
    format!("{pool}-rollover")
}

/// The address at which a pool's hub records itself: its pod's IP, which
/// the hub's pod reads from [`POD_IP`]. A pod IP is IPv4 here: an IPv6
/// address would need brackets before the port.
pub fn pod_hub() -> String {
    format!("$({POD_IP}):{HUB_PUBLIC_PORT}")
}

/// The address at which pool `pool`'s segments reach its hub: its Service.
pub fn hub_address(pool: &str) -> String {
    format!("{pool}:{HUB_PUBLIC_PORT}")
}

/// One render of a pool.
struct Render<'a> {
    name: &'a str,
    namespace: &'a str,
    pool: &'a Pool,
    scaling: Scaling,
}

impl Render<'_> {
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
            ("gdl.dev/fleet", self.pool.fleet.as_str()),
            ("gdl.dev/cluster", self.pool.cluster.as_str()),
            ("gdl.dev/pool", self.name),
            ("gdl.dev/class", self.pool.class.as_str()),
            ("gdl.dev/region", self.pool.region.as_str()),
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
            "namespace": self.namespace,
            "labels": self.labels(segment),
        });
        let whole = self.whole(segment);
        if !whole.is_empty() {
            metadata["annotations"] = whole.into();
        }
        metadata
    }

    /// The metadata of the pool's object `name` of `segment`.
    fn metadata(&self, name: &str, segment: &str) -> Value {
        self.meta(name, Some(segment))
    }

    /// The pool's ConfigMap, named for the pool: its layout's deployment,
    /// which its hub admits, and each of its segments' settings.
    fn config_map(&self) -> Value {
        let mut data = Map::new();
        data.insert(
            "deployment.json".into(),
            self.pool.deployment.clone().into(),
        );
        for segment in &self.pool.segments {
            data.insert(
                format!("{}.toml", segment.name),
                segment.settings.clone().into(),
            );
        }
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": self.meta(self.name, None),
            "data": data,
        })
    }

    /// A parent hub's claim, so that it still holds its children's records,
    /// a silent child's included, after its pod is replaced.
    fn state_claim(&self) -> Option<Value> {
        self.pool.hub.state_dir.as_ref().map(|_| {
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": self.metadata(&state_claim(self.name), "hub"),
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": STATE_SIZE}},
                },
            })
        })
    }

    /// The pool's hub: one replica, which `Recreate` stops before it starts
    /// another, since a pool's record has one writer. Its probes ask its own
    /// health, never the pool's, which a parent answers 503 until its
    /// children are ready: a parent that was never ready would have no
    /// Service endpoint for its children to report to.
    fn hub(&self, checksum: &str) -> Value {
        let hub = &self.pool.hub;
        let mut env = vec![json!({
            "name": POD_IP,
            "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
        })];
        env.extend(environment(&hub.env));
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
        if let Some(state_dir) = &hub.state_dir {
            mounts.push(json!({"name": "state", "mountPath": state_dir}));
            volumes.push(json!({
                "name": "state",
                "persistentVolumeClaim": {"claimName": state_claim(self.name)},
            }));
        }
        let mut container = json!({
            "name": "hub",
            "image": hub.image,
            "command": [BINARY, format!("{CONFIG_DIR}/deployment.json")],
            "env": env,
            "ports": [
                {"name": PUBLIC, "containerPort": HUB_PUBLIC_PORT},
                {"name": "control", "containerPort": HUB_CONTROL_PORT},
            ],
            "livenessProbe": probe(2, 30),
            "readinessProbe": probe(2, 3),
            "volumeMounts": mounts,
        });
        resources(&mut container, hub.resources.as_ref());
        let mut pod = self.pod_spec(container, volumes);
        if hub.state_dir.is_some() {
            pod["securityContext"] = json!({"fsGroup": STATE_GROUP});
        }
        let scrape = Scrape {
            port: HUB_PUBLIC_PORT,
            path: HUB_METRICS,
        };
        self.deployment(self.name, "hub", Some(1), true, checksum, scrape, pod)
    }

    /// The Service of the pool's hub, named for the pool: where its
    /// segments, and its children's hubs, reach it.
    fn hub_service(&self) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": self.metadata(self.name, "hub"),
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
    fn segment(&self, segment: &Segment, settings: &Settings, checksum: &str) -> Value {
        let probe = |path: &str, period: u32| {
            json!({
                "httpGet": {"path": path, "port": settings.port},
                "periodSeconds": period,
                "timeoutSeconds": 3,
                "failureThreshold": 3,
            })
        };
        let mut ports = vec![json!({"name": SYSTEM, "containerPort": settings.port})];
        ports.extend(segment.ports.iter().map(container_port));
        // The rollover ConfigMap is optional, so a pod starts without it,
        // and outside `gdl.dev/config`, so writing it replaces no pod. The
        // hub never mounts it: a change to the hub's template restarts the
        // pool.
        let mut mounts = vec![
            config_mount(),
            json!({"name": "rollover", "mountPath": ROLLOVER_DIR, "readOnly": true}),
        ];
        let mut volumes = vec![
            self.config_volume(),
            json!({"name": "rollover", "configMap": {"name": rollover(self.name), "optional": true}}),
        ];
        if !segment.secrets.is_empty() {
            mounts.push(json!({"name": "secrets", "mountPath": SECRETS_DIR, "readOnly": true}));
            let sources: Vec<Value> = segment
                .secrets
                .iter()
                .map(|secret| {
                    json!({"secret": {"name": secret.holder, "items": [{"key": secret.name, "path": secret.name}]}})
                })
                .collect();
            volumes.push(json!({"name": "secrets", "projected": {"sources": sources}}));
        }
        let mut container = json!({
            "name": "segment",
            "image": segment.image,
            "command": [BINARY, format!("{CONFIG_DIR}/{}.toml", segment.name)],
            "env": environment(&segment.env),
            "ports": ports,
            "livenessProbe": probe(&settings.live, 10),
            "readinessProbe": probe(&settings.health, 5),
            "volumeMounts": mounts,
        });
        resources(&mut container, segment.resources.as_ref());
        let mut pod = self.pod_spec(container, volumes);
        pod["terminationGracePeriodSeconds"] = settings.grace().into();
        let scrape = Scrape {
            port: settings.port,
            path: SEGMENT_METRICS,
        };
        // Under the operator, the scaler owns `replicas`, and its pods say
        // that they take connections, which their entry Service selects
        // on.
        let operator = self.scaling == Scaling::Operator;
        let replicas = (!operator).then_some(segment.quorum);
        let mut deployment = self.deployment(
            &object_name(self.name, &segment.name),
            &segment.name,
            replicas,
            false,
            checksum,
            scrape,
            pod,
        );
        if operator {
            deployment["spec"]["template"]["metadata"]["labels"][ROUTABLE] = "true".into();
        }
        deployment
    }

    /// The `GdlScaler` of the pool's `segment`, which scales `target`,
    /// named `<pool>-<segment>`, without `replicas`, which only its
    /// bootstrap and then its HPA or a person write: the segment's quorum
    /// as its minimum, its drain bound, its entry Service when it
    /// publishes ports, and its metrics' prefix.
    fn scaler(&self, segment: &Segment, settings: &Settings, target: Value) -> Value {
        let name = object_name(self.name, &segment.name);
        let mut spec = json!({
            "pool": self.name,
            "segment": segment.name,
            "targetRef": target,
            "minReplicas": segment.quorum.max(1),
            "drainBound": format!("{}ms", settings.drain.as_millis()),
        });
        if !segment.ports.is_empty() {
            spec["service"] = name.clone().into();
        }
        if let Some(prefix) = &settings.metrics_prefix {
            spec["metricsPrefix"] = prefix.clone().into();
        }
        json!({
            "apiVersion": "gdl.dev/v1alpha1",
            "kind": "GdlScaler",
            "metadata": self.metadata(&name, &segment.name),
            "spec": spec,
        })
    }

    /// The ClusterIP Service of the ports the pool's `segment` publishes.
    /// Under the operator it selects only the pods that take connections
    /// (`gdl.dev/routable: "true"`), so that a pod the operator removes
    /// leaves its endpoints before it goes.
    fn entry_service(&self, segment: &Segment) -> Value {
        let mut selector = self.selector(&segment.name);
        if self.scaling == Scaling::Operator {
            selector[ROUTABLE] = "true".into();
        }
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": self.metadata(&object_name(self.name, &segment.name), &segment.name),
            "spec": {
                "type": "ClusterIP",
                "selector": selector,
                "ports": segment.ports.iter().map(service_port).collect::<Vec<_>>(),
            },
        })
    }

    /// A Deployment of `replicas` pods of `segment` with `pod` as their
    /// spec, which `checksum`, of what the pods read from the pool's
    /// ConfigMap, replaces when that changes: a ConfigMap's change alone
    /// restarts no pod. Each pod
    /// says where it serves its metrics (`scrape`). Without `replicas`, the
    /// Deployment leaves the field to the one manager that owns it.
    #[allow(clippy::too_many_arguments)]
    fn deployment(
        &self,
        name: &str,
        segment: &str,
        replicas: Option<u32>,
        recreate: bool,
        checksum: &str,
        scrape: Scrape,
        pod: Value,
    ) -> Value {
        let labels = self.labels(Some(segment));
        let mut annotations = self.whole(Some(segment));
        annotations.insert("gdl.dev/config".into(), checksum.into());
        annotations.insert("prometheus.io/scrape".into(), "true".into());
        annotations.insert("prometheus.io/port".into(), scrape.port.to_string().into());
        annotations.insert("prometheus.io/path".into(), scrape.path.into());
        let mut spec = json!({
            "selector": {"matchLabels": self.selector(segment)},
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": pod,
            },
        });
        if let Some(replicas) = replicas {
            spec["replicas"] = replicas.into();
        }
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

    /// A pod of the pool's service account, placed as the pool says.
    /// Service links are off: they would add a variable per Service in the
    /// namespace to every container.
    fn pod_spec(&self, container: Value, volumes: Vec<Value>) -> Value {
        let mut pod = json!({
            "serviceAccountName": self.pool.service_account_name,
            "enableServiceLinks": false,
            "containers": [container],
            "volumes": volumes,
        });
        place(&mut pod, &self.pool.node_selector, &self.pool.tolerations);
        pod
    }

    fn config_volume(&self) -> Value {
        json!({"name": "config", "configMap": {"name": self.name}})
    }
}

/// Where a pod serves its metrics.
struct Scrape {
    port: u16,
    path: &'static str,
}

/// Place the pod whose spec is `pod`: on nodes `node_selector` selects,
/// tolerating `tolerations`. Every pod gdl renders is placed here.
pub fn place(
    pod: &mut Value,
    node_selector: &std::collections::BTreeMap<String, String>,
    tolerations: &[Toleration],
) {
    pod["nodeSelector"] = json!(node_selector);
    if !tolerations.is_empty() {
        pod["tolerations"] = json!(tolerations);
    }
}

/// Give `container` its requests and limits, when it has any.
pub fn resources(container: &mut Value, resources: Option<&Resources>) {
    if let Some(resources) = resources {
        container["resources"] = json!(resources);
    }
}

/// `env` as a container's `env`.
fn environment(env: &[Env]) -> Vec<Value> {
    env.iter()
        .map(|var| json!({"name": var.name, "value": var.value}))
        .collect()
}

fn config_mount() -> Value {
    json!({"name": "config", "mountPath": CONFIG_DIR, "readOnly": true})
}

fn container_port(port: &Port) -> Value {
    json!({"containerPort": port.port, "protocol": port.protocol.name()})
}

fn service_port(port: &Port) -> Value {
    json!({
        "name": format!("{}-{}", port.protocol.name().to_ascii_lowercase(), port.port),
        "port": port.port,
        "targetPort": port.port,
        "protocol": port.protocol.name(),
    })
}
