//! The fleet's Kubernetes objects that are not a pool's own, and a
//! pool as [`gdl_k8s`] renders its objects: each is JSON, which is also
//! YAML, and a file of them is a YAML stream that `kubectl apply` and
//! `kubeconform` read. Every object carries the labels `gdl.dev/fleet` and
//! `gdl.dev/cluster`, the fleet's name for the cluster it is rendered for:
//! `deploy` prunes only the objects of its own cluster, so that two
//! clusters whose contexts reach one namespace never prune each other's.
//! A pool's objects carry more labels, which `gdl_k8s` names.

use std::collections::BTreeMap;

use gdl_k8s::{Env, LABEL_LIMIT, LABEL_PREFIX, Pool, Port, Protocol, Settings, fnv, label_value};
use gdl_model::{Fleet, KubernetesCluster, PoolSettings, Resources};
use serde_json::{Map, Value, json};

use crate::Error;
use crate::deployment::{Published, reference};

/// The address every segment's system listener binds in a pod: every
/// interface, so that the kubelet's probes reach it at the pod's IP.
pub(super) const SYSTEM_HOST: &str = "0.0.0.0";

/// One segment of a layout, as its pods run it: what [`gdl_k8s::Segment`]
/// takes from the spec, before a pool's cluster names its image and its
/// secrets' holders.
pub(super) struct Segment {
    pub(super) name: String,
    /// Its image's repository: the package it builds.
    pub(super) repository: String,
    /// Its settings file.
    pub(super) config: String,
    /// The container ports it publishes.
    pub(super) ports: Vec<Port>,
    /// The spec's secrets it mounts.
    pub(super) secrets: Vec<String>,
}

/// `port`, in Compose's short form `[[IP:][HOST]:]CONTAINER[/PROTOCOL]`,
/// as a segment publishes it in a cluster: its container port. The host
/// port is Compose's alone. A range is refused: a Service lists each port.
pub(super) fn parse_port(port: &str) -> Result<Port, String> {
    let refuse = |why: &str| format!("`{port}` {why}");
    let published = Published::of(port);
    let protocol = match published.protocol {
        "tcp" => Protocol::Tcp,
        "udp" => Protocol::Udp,
        "sctp" => Protocol::Sctp,
        _ => return Err(refuse("names a protocol that is not tcp, udp, or sctp")),
    };
    match published.container.parse::<u16>() {
        Ok(number) if number > 0 => Ok(Port {
            port: number,
            protocol,
        }),
        _ => Err(refuse(
            "is not one container port: a Kubernetes Service lists each port it publishes, so publish a range as its ports",
        )),
    }
}

/// Whether `port` is the port of the system listener that `settings` ask
/// the probes at, which the pod declares as `system`.
pub(super) fn is_system(port: &Port, settings: &Settings) -> bool {
    port.port == settings.port && port.protocol == Protocol::Tcp
}

/// The pods of one pool in its cluster, as the fleet places them.
pub(super) struct Pod<'a> {
    pub(super) fleet: &'a str,
    pub(super) pool: &'a PoolSettings,
    /// The fleet's name for the cluster.
    pub(super) cluster_name: &'a str,
    pub(super) cluster: &'a KubernetesCluster,
    pub(super) naming: &'a super::Naming<'a>,
    /// The fleet, for each pod's requests and limits.
    pub(super) model: &'a Fleet,
    /// The pool's `dgd` table and the DGD file it names, as JSON, when it
    /// has one.
    pub(super) dgd: Option<(&'a gdl_model::PoolDgd, &'a Value)>,
}

impl Pod<'_> {
    /// The pool as [`gdl_k8s::children`] renders it: its layout's
    /// `deployment`, its hub's image of `hub_repository` and `hub` its
    /// variables, a parent hub's `state_dir`, and `segments`, each with
    /// `variables`. A segment its `dgd` table names runs in that
    /// component, whose `main` container gets the segment's image.
    pub(super) fn pool(
        &self,
        deployment: &str,
        hub_repository: &str,
        hub: &Map<String, Value>,
        state_dir: Option<&str>,
        segments: &[&Segment],
        variables: &Map<String, Value>,
    ) -> Result<Pool, Error> {
        let mut variables = variables.clone();
        variables.insert(
            "GDL_HUB__HOST".into(),
            crate::deployment::variables::string(&self.pool.name).into(),
        );
        let mut dgd = self.dgd.map(|(_, file)| file.clone());
        let segments = segments
            .iter()
            .map(|segment| {
                let image = self.image(&segment.repository)?;
                let component = self
                    .dgd
                    .and_then(|(table, _)| table.components.get(&segment.name))
                    .cloned();
                if let (Some(component), Some(dgd)) = (&component, dgd.as_mut()) {
                    gdl_k8s::set_image(dgd, component, &image).map_err(|message| {
                        Error::new(format!("pool `{}`: {message}", self.pool.name))
                    })?;
                }
                Ok(gdl_k8s::Segment {
                    name: segment.name.clone(),
                    image,
                    settings: segment.config.clone(),
                    quorum: self.pool.quorum(&segment.name),
                    ports: segment.ports.clone(),
                    env: self.env(&variables),
                    secrets: segment
                        .secrets
                        .iter()
                        .map(|secret| gdl_k8s::Secret {
                            name: secret.clone(),
                            holder: self
                                .cluster
                                .secrets
                                .get(secret)
                                .cloned()
                                .unwrap_or_default(),
                        })
                        .collect(),
                    resources: self.resources(&segment.name),
                    component,
                })
            })
            .collect::<Result<_, Error>>()?;
        Ok(Pool {
            fleet: self.fleet.to_string(),
            cluster: self.cluster_name.to_string(),
            class: self.pool.class.clone(),
            region: self.pool.region.clone(),
            deployment: deployment.to_string(),
            hub: gdl_k8s::Hub {
                image: self.image(hub_repository)?,
                env: self.env(hub),
                resources: self.resources(gdl_model::HUB),
                state_dir: state_dir.map(str::to_string),
            },
            segments,
            service_account_name: self.fleet.to_string(),
            node_selector: node_selector(self.cluster),
            tolerations: self.cluster.tolerations.iter().map(Into::into).collect(),
            parent: self.pool.parent.as_ref().map(Into::into),
            dynamodb: gdl_k8s::Dynamodb::of(&self.pool.directory),
            pod_monitor: self.model.metrics().pod_monitor,
            presence: gdl_k8s::Presence::Serving,
            allow_pool_restart: false,
            dgd: dgd.map(|dgd| dgd.to_string()),
        })
    }

    /// The requests and limits of the pool's pods of `segment`, `hub` for
    /// its hub's, when the cluster gives any.
    fn resources(&self, segment: &str) -> Option<gdl_k8s::Resources> {
        self.model
            .pod_resources(self.cluster_name, Some(&self.pool.name), Some(segment))
            .as_ref()
            .map(Into::into)
    }

    /// `variables`, and then the cluster's own, as a container's `env`.
    /// Kubernetes reads `$(NAME)` in a value as another variable's, and
    /// `$$` as `$`: a cluster's values are written so that they read as
    /// given. A pool's own values hold no `$`, except the hub's reference
    /// to its pod's IP.
    fn env(&self, variables: &Map<String, Value>) -> Vec<Env> {
        let mut env: Vec<Env> = variables
            .iter()
            .map(|(name, value)| Env {
                name: name.clone(),
                value: value.as_str().unwrap_or_default().to_string(),
            })
            .collect();
        env.extend(self.cluster.env.iter().map(|(name, value)| Env {
            name: name.clone(),
            value: value.replace('$', "$$"),
        }));
        env
    }

    /// The image of `repository` in the cluster's registry, for its
    /// platform.
    fn image(&self, repository: &str) -> Result<String, Error> {
        let registry = self.cluster.registry.as_deref().unwrap_or_default();
        let platform = self.cluster.platform.as_deref().unwrap_or_default();
        let image = self.naming.image(registry, repository, platform)?;
        reference::check_image(&image).map_err(|why| {
            Error::new(format!("[cluster.{}] registry: {why}", self.cluster_name))
        })?;
        Ok(image)
    }
}

/// The PodMonitor of `fleet`'s pods in `cluster`, which the fleet names
/// `name`, for a cluster that runs the Prometheus operator
/// ([`gdl_k8s::pod_monitor`]).
pub(super) fn pod_monitor(fleet: &str, name: &str, cluster: &KubernetesCluster) -> Value {
    gdl_k8s::pod_monitor(
        &pod_monitor_name(fleet, name),
        &cluster.namespace,
        &[("gdl.dev/fleet", fleet), ("gdl.dev/cluster", name)],
    )
}

/// The name of `fleet`'s PodMonitor in cluster `cluster`: one of its own
/// for each pair, since two clusters' contexts, or two fleets' clusters,
/// can reach one namespace, where one name would hold only the last one's
/// selector. `<fleet>-<cluster>` alone is not one of its own: fleet `api`
/// with cluster `use1-east` and fleet `api-use1` with cluster `east` would
/// share it, as would a fitted name and a name that spells its hash. So it
/// is a readable start, `<fleet>-<cluster>` cut to fit, `-`, and always 10
/// hex digits of a hash of the pair with the fleet's length before it,
/// which no other pair spells: at most 63 characters.
pub(super) fn pod_monitor_name(fleet: &str, cluster: &str) -> String {
    let start: String = format!("{fleet}-{cluster}")
        .chars()
        .take(LABEL_PREFIX)
        .collect();
    let hash = format!("{:016x}", fnv(&format!("{}:{fleet}{cluster}", fleet.len())));
    format!("{}-{}", start.trim_end_matches('-'), &hash[..10])
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

/// The node selector of a pod of `cluster`: the cluster's own beside its
/// platform's OS and architecture, without a variant (`linux/arm64/v8`
/// runs on `arm64` nodes), as the nodes' `kubernetes.io/os` and
/// `kubernetes.io/arch` labels name them.
fn node_selector(cluster: &KubernetesCluster) -> BTreeMap<String, String> {
    let platform = cluster.platform.as_deref().unwrap_or_default();
    let mut parts = platform.split('/');
    let os = parts.next().unwrap_or_default();
    let arch = parts.next().unwrap_or_default();
    let mut selector = BTreeMap::from([
        ("kubernetes.io/os".to_string(), os.to_string()),
        ("kubernetes.io/arch".to_string(), arch.to_string()),
    ]);
    for (key, value) in &cluster.node_selector {
        selector.insert(key.clone(), value.clone());
    }
    selector
}

/// Place a pod of `cluster`, whose spec is `pod`, as a pool's pods are
/// placed: on the cluster's nodes ([`node_selector`]), with its
/// tolerations. Every pod the render writes is placed so, so that none is
/// left off a node group of the fleet's own.
pub(super) fn place(pod: &mut Value, cluster: &KubernetesCluster) {
    let tolerations: Vec<gdl_k8s::Toleration> =
        cluster.tolerations.iter().map(Into::into).collect();
    gdl_k8s::place(pod, &node_selector(cluster), &tolerations);
}

/// Give `container` its requests and limits, when its cluster gives any.
pub(super) fn resources(container: &mut Value, resources: Option<&Resources>) {
    gdl_k8s::resources(container, resources.map(Into::into).as_ref());
}

/// The Namespace of `cluster`, which the fleet names `name`, and the
/// fleet's ServiceAccount in it, which carries the cluster's annotations,
/// for a cloud role.
pub(super) fn base(fleet: &str, name: &str, cluster: &KubernetesCluster) -> Vec<Value> {
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
pub(in crate::deployment) fn stream(objects: &[Value]) -> serde_json::Result<String> {
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
pub(in crate::deployment) fn check_platform(platform: &str) -> Result<(), String> {
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

/// Refuse a registry that cannot start an image reference
/// ([`reference::check_registry`]). A scheme, a tag, or a digest would
/// make every image name one that Docker refuses.
pub(in crate::deployment) fn check_registry(registry: &str) -> Result<(), String> {
    reference::check_registry(registry).map_err(|why| {
        format!(
            "`{registry}` is not a registry that an image reference can start with ({why}): write `host[:port][/path]`, with no scheme, tag, or digest, such as `registry.example:5000/gdl`"
        )
    })
}

/// Refuse a tag that an image reference cannot hold.
pub(super) fn check_tag(tag: &str) -> Result<(), Error> {
    let shaped = reference::tag(tag);
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
         Apply the base first, since every other object is in its Namespace. Where a cluster's namespace already exists, leave its Namespace out of the apply and label the namespace instead, with `kubectl label namespace/<namespace> gdl.dev/fleet=<fleet> gdl.dev/cluster=<cluster>`: an apply would take over the fields its owner set, and drop the labels and annotations this Namespace does not list. `cargo gdl deploy` does so itself.\n"
    );
    for (cluster, settings) in super::kube_clusters(fleet) {
        text.push_str(&format!(
            "\n```bash\nkubectl --context {context} apply --server-side -f {cluster}/base.yaml\nkubectl --context {context} apply --server-side -R -f {cluster}/pools\n```\n",
            context = settings.context
        ));
        if settings.operator {
            text.push_str(&format!(
                "\nThe gdl operator runs the pools of cluster `{cluster}`: it needs the `GdlPool` and `GdlScaler` CRDs and the operator in namespace `{namespace}` (`gdl-operator manifests`). Each `pools/<pool>.yaml` there holds the pool's one `GdlPool`, from which the operator makes the pool's objects, and which it stops in order when it is deleted. Delete a `GdlPool` with background propagation, the default of `kubectl delete`, child pools first: `kubectl --context {context} --namespace {namespace} delete gdlpools.gdl.dev/<pool>`. A change that replaces a pool's hub is held until its `allowPoolRestart` is set.\n",
                namespace = settings.namespace,
                context = settings.context
            ));
            let dgds: Vec<String> = fleet
                .dgds()
                .filter(|(pool, _)| fleet.cluster_of(pool).is_some_and(|(c, _)| c == cluster))
                .map(|(pool, _)| format!("`{pool}`"))
                .collect();
            if !dgds.is_empty() {
                text.push_str(&format!(
                    "\nThe `GdlPool` of {} holds its DynamoGraphDeployment, which dynamo's operator runs: the cluster must serve `dynamographdeployments.nvidia.com` at `nvidia.com/v1beta1`, with dynamo's platform installed, before the gdl operator starts, or the pool applies nothing.\n",
                    dgds.join(", ")
                ));
            }
        }
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
    text.push_str("\nEvery pod says where it serves its metrics, in its `prometheus.io/scrape`, `prometheus.io/port`, and `prometheus.io/path` annotations.");
    if fleet.metrics().pod_monitor {
        if super::kube_clusters(fleet).any(|(_, cluster)| cluster.operator) {
            text.push_str(&format!(" Each `base.yaml` of a cluster the gdl operator does not run also holds a PodMonitor `{name}-<cluster>-<hash>` of the fleet's pods in its cluster, and in a cluster it runs each pool has a PodMonitor named after it. Both need the Prometheus operator's CRD (`monitoring.coreos.com/v1`) there: apply them where the operator runs."));
        } else {
            text.push_str(&format!(" Each `base.yaml` also holds a PodMonitor `{name}-<cluster>-<hash>` of the fleet's pods in its cluster, which needs the Prometheus operator's CRD (`monitoring.coreos.com/v1`) there: apply it where the operator runs."));
        }
    }
    text.push('\n');
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
            let parsed = parse_port(port).expect(port);
            assert_eq!(
                (parsed.port, parsed.protocol.name()),
                (number, protocol),
                "{port}"
            );
        }
        for port in ["8080-8081", "80-90:80-90", "0", "8080/http", "x"] {
            assert!(parse_port(port).is_err(), "{port}");
        }
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
