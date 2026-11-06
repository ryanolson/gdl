//! A fleet's Compose bundle: for each pool, one hub, named for the pool,
//! and a service for each segment of the pool's class in the pool's
//! layout, each configured as that pool. Each image is built once per
//! layout and segment, and every pool of that layout and class runs it.
//! Under `relay`, each parent pool's hub mounts a volume of its own, where
//! it keeps its children's records. Under a `[directory]` of DynamoDB, the
//! bundle runs one DynamoDB Local and the tables step instead, and every
//! hub waits for the step.

use std::collections::{BTreeMap, BTreeSet};

use gdl_model::{
    DirectoryBackend, Fleet, FleetDirectory, HUB_PUBLIC_PORT, Layout, Manifest, Parent,
    PoolSettings,
};
use serde_json::{Map, Value, json};

use super::variables::{pool_variables, string};
use super::{
    Bundle, Secrets, check_segment_name, hosts, hub_healthcheck, io, keep_stdin_open,
    segment_healthcheck, stdin_readme,
};
use crate::{Error, Files, Spec, emit, export::image_problem, validate::Layouts};

/// Why `spec` deploys only by pool, if it does: its placement has classes,
/// or it has several layouts, each of which only a pool names.
pub(super) fn by_pool(spec: &Spec) -> Option<String> {
    let layouts: Vec<_> = spec.layouts().collect();
    if layouts.len() > 1 {
        let names: Vec<String> = layouts
            .iter()
            .map(|laid| format!("`{}`", laid.name))
            .collect();
        let names = match names.split_last() {
            Some((last, [only])) => format!("{only} and {last}"),
            Some((last, rest)) => format!("{}, and {last}", rest.join(", ")),
            None => String::new(),
        };
        return Some(format!("of several layouts, {names},"));
    }
    layouts
        .iter()
        .any(|laid| laid.placement.classes().next().is_some())
        .then(|| "with classes".into())
}

/// The bundle of `fleet`'s pools, which [`Fleet::check`] accepted against
/// `spec`'s `layouts`. `stdin` is the source that reads requests on stdin,
/// if one does.
pub(super) fn bundle(
    spec: &Spec,
    fleet: &Fleet,
    mut bundle: Bundle,
    manifest: &Manifest,
    layouts: &Layouts,
    stdin: Option<String>,
) -> Result<Files, Error> {
    let pools: Vec<PoolSettings> = fleet.pools().collect();
    if pools.is_empty() {
        return Err(Error::new(
            "the fleet has no pools, so there is nothing to deploy",
        ));
    }
    let directory = fleet.directory();
    if let Some(directory) = directory {
        local_directory(directory, &pools)?;
    }
    let graph = bundle.graph.clone();
    let mut services = Map::new();
    // The pools that run each segment, by its name, which its deployment
    // settings, and so its ports, are keyed by in every layout.
    let mut runs: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    // The services whose source reads requests on stdin.
    let mut sources = BTreeSet::new();
    let mut secrets = Secrets::new();
    // Each parent pool's hub's own volume.
    let mut volumes = Map::new();
    for laid in spec.layouts() {
        let ours: Vec<&PoolSettings> = pools
            .iter()
            .filter(|pool| pool.layout.as_deref().unwrap_or(laid.name) == laid.name)
            .collect();
        if ours.is_empty() {
            continue;
        }
        let layout = layouts
            .get(laid.name)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::new(laid.named("the placement does not fit the graph".into())))?;
        // A spec of one layout keeps the names of a bundle without pools.
        let dir = laid.dir();
        let deployment = dir.join("deployment.json");
        let spec_of =
            gdl_model::DeploymentSpec::new(manifest.clone(), laid.placement.clone()).map_err(io)?;
        bundle.files.insert(
            deployment.clone(),
            serde_json::to_string_pretty(&spec_of).map_err(io)?,
        );
        let hub_dockerfile = dir.join("hub/Dockerfile");
        let text = match directory {
            None => bundle.hub_dockerfile(&deployment, Some(STATE_DIR), false),
            Some(_) => bundle.hub_dockerfile(&deployment, None, true),
        };
        bundle.files.insert(hub_dockerfile.clone(), text);
        let hub = Image {
            name: match laid.among_several() {
                true => format!("{}:{}", emit::package_name(&graph, "hub"), laid.name),
                false => emit::package_name(&graph, "hub"),
            },
            build: bundle.build(&hub_dockerfile),
        };
        // Each segment that a pool of the layout runs, built once.
        let mut images = BTreeMap::new();
        let source = laid
            .placement
            .segments()
            .find(|(_, nodes)| hosts(nodes, stdin.as_ref()))
            .map(|(segment, _)| segment);
        for (segment, nodes) in laid.placement.segments() {
            check_segment_name(segment)?;
            let class = layout.class_of(segment);
            if !ours.iter().any(|pool| class == Some(pool.class.as_str())) {
                continue;
            }
            let package = laid.package(&graph, segment);
            if let Some(problem) = image_problem(segment, &package) {
                return Err(Error::new(problem));
            }
            // A pool waits only for the segments of its own class.
            let unawaited = source.filter(|source| layout.class_of(source) == class);
            let grace = bundle.segment_image(&dir, segment, &package, None, unawaited)?;
            let image = Image {
                name: package,
                build: bundle.build(&dir.join(segment).join("Dockerfile")),
            };
            images.insert(segment, (image, grace, hosts(nodes, stdin.as_ref())));
        }
        let mut built = BTreeSet::new();
        for pool in ours {
            let parent = parent(layout, pool) && directory.is_none();
            let variables = variables(
                pool,
                format!("{}:{HUB_PUBLIC_PORT}", pool.name),
                // A fleet refuses a shared pool under `relay`, so only a home
                // has a parent's hub.
                match &pool.parent {
                    Some(Parent::Pool(home)) => Some(format!("{home}:{HUB_PUBLIC_PORT}")),
                    _ => None,
                },
                parent,
                directory.map(|_| format!("http://{LOCAL}:8000")),
            )?;
            let mut service = json!({
                "image": hub.name,
                // Compose refuses to scale a service whose container is
                // named, and a pool's record has one writer.
                "container_name": emit::package_name(&graph, &pool.name),
                "environment": variables,
                "healthcheck": hub_healthcheck(),
            });
            if parent {
                let volume = format!("{}-state", pool.name);
                service["volumes"] = json!([format!("{volume}:{STATE_DIR}")]);
                volumes.insert(volume, json!({}));
            }
            if directory.is_some() {
                service["depends_on"] =
                    json!({TABLES: {"condition": "service_completed_successfully"}});
            }
            hub.build(&mut service, &mut built);
            // The tables step runs from the hub's image, which a hub
            // service builds; it shares its pool's variables, and reads
            // only the directory's.
            if directory.is_some() && !services.contains_key(TABLES) {
                services.insert(
                    TABLES.into(),
                    json!({
                        "image": hub.name,
                        "entrypoint": ["/usr/local/bin/segment", "--tables"],
                        "environment": variables,
                        "depends_on": {LOCAL: {"condition": "service_started"}},
                    }),
                );
            }
            services.insert(pool.name.clone(), service);
            for (segment, (image, grace, reads)) in &images {
                if layout.class_of(segment) != Some(pool.class.as_str()) {
                    continue;
                }
                runs.entry(segment).or_default().insert(&pool.name);
                let mut variables = variables.clone();
                variables.insert("GDL_HUB__HOST".into(), string(&pool.name).into());
                // A segment waits only for its own pool's hub to be live:
                // never for another pool, nor for a pool's health, which a
                // parent answers 503 until its children are ready.
                let mut service = json!({
                    "image": image.name,
                    "depends_on": {&pool.name: {"condition": "service_healthy"}},
                    "environment": variables,
                    "init": true,
                    "stop_grace_period": format!("{grace}s"),
                    "healthcheck": segment_healthcheck(),
                    "ports": bundle.ports(segment),
                });
                if *reads {
                    keep_stdin_open(&mut service);
                    sources.insert(format!("{}_{segment}", pool.name));
                }
                bundle.mount_secrets(segment, &mut service, &mut secrets);
                image.build(&mut service, &mut built);
                services.insert(format!("{}_{segment}", pool.name), service);
            }
        }
    }
    for (segment, pools) in &runs {
        let fixed = bundle
            .ports(segment)
            .iter()
            .find(|port| binds_host_port(port));
        if let Some(port) = fixed.filter(|_| pools.len() > 1) {
            let pools: Vec<String> = pools.iter().map(|pool| format!("`{pool}`")).collect();
            return Err(Error::new(format!(
                "segment `{segment}` publishes host port `{port}`, and runs in pools {}: two containers on one host cannot bind one host port; publish it from a segment that runs in one pool, or let Docker choose the host port (`CONTAINER` alone, `IP::CONTAINER`, or host port 0)",
                pools.join(", ")
            )));
        }
    }
    if directory.is_some() {
        services.insert(
            LOCAL.into(),
            json!({
                "image": DYNAMODB_LOCAL,
                "command": ["-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-port", "8000"],
            }),
        );
    }
    let mut compose = json!({"services": services});
    if !volumes.is_empty() {
        compose["volumes"] = Value::Object(volumes);
    }
    if !secrets.is_empty() {
        compose["secrets"] = bundle.declare_secrets(&secrets);
    }
    let secrets_readme = bundle.secrets_readme(&secrets);
    let mut files = bundle.files;
    files.insert(
        "compose.yaml".into(),
        serde_json::to_string_pretty(&compose).map_err(io)?,
    );
    let mut readme = String::from(README);
    readme.push_str(match directory {
        None => RELAY_README,
        Some(_) => DYNAMODB_README,
    });
    readme.push_str(README_TAIL);
    for source in &sources {
        readme.push_str(&stdin_readme(source));
    }
    readme.push_str(&secrets_readme);
    files.insert("README.md".into(), readme);
    Ok(files)
}

/// An image of the bundle: its name, and the Dockerfile that builds it.
struct Image {
    name: String,
    /// The `build` of a service that builds it.
    build: Value,
}

impl Image {
    /// Give `service` the build of this image if no service in `built` has
    /// it yet: one service builds an image, and the others of its pools
    /// run it by its name, which Compose does not pull while a service of
    /// the bundle builds it.
    fn build(&self, service: &mut Value, built: &mut BTreeSet<String>) {
        if built.insert(self.name.clone()) {
            service["build"] = self.build.clone();
        }
    }
}

/// Where a parent pool's hub keeps its children's records in its
/// container: its own volume's mount.
pub(super) const STATE_DIR: &str = "/var/lib/gdl-hub";

/// The service of a DynamoDB directory's Local, and of its tables step:
/// names with a `-`, which a pool's name may also hold, so a pool of either
/// name is refused ([`local_directory`]).
const LOCAL: &str = "dynamodb-local";
const TABLES: &str = "dynamodb-tables";

/// DynamoDB Local 3.3.2, pinned by the digest of its multi-architecture
/// index, as `scripts/dynamodb-local.sh` runs it.
pub(in crate::deployment) const DYNAMODB_LOCAL: &str =
    "amazon/dynamodb-local@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86";

/// Whether the bundle can carry `directory` for `pools`: only with
/// `local = true`, since a Compose bundle carries no AWS role and no
/// credential goes into a file, and only when no pool takes the name of a
/// service the directory adds.
fn local_directory(directory: &FleetDirectory, pools: &[PoolSettings]) -> Result<(), Error> {
    if !directory.local {
        return Err(Error::new(
            "the fleet's [directory] backend = \"dynamodb\" needs [directory] local = true under Compose: a Compose bundle carries no AWS role for the tables, and no credential goes into its files. With local = true, the bundle runs one DynamoDB Local, which every hub uses",
        ));
    }
    if let Some(pool) = pools
        .iter()
        .find(|pool| [LOCAL, TABLES].contains(&pool.name.as_str()))
    {
        return Err(Error::new(format!(
            "pool `{}` takes the name of a service that the bundle's DynamoDB directory adds: rename the pool",
            pool.name
        )));
    }
    Ok(())
}

/// Whether `pool` is a parent in `layout`: its class sends to another, so
/// its hub holds the records of its children.
pub(super) fn parent(layout: &Layout, pool: &PoolSettings) -> bool {
    layout
        .boundaries()
        .iter()
        .any(|boundary| boundary.from == pool.class)
}

/// The `GDL_POOL__*` variables of `pool`'s hub and segments: its settings
/// as its fleet gives them, with `hub`, where other pools' hubs reach its
/// hub. Under the `relay` backend, `parent_hub`, where a child's hub
/// reports to its parent's, and, for a `parent`, the state directory its
/// hub keeps its children's records in. Under the fleet's DynamoDB
/// directory, which the pool's settings name, `endpoint`, the DynamoDB
/// Local every hub uses, if the fleet runs one. Compose and Kubernetes both
/// write a pool's variables here, so the two differ only in the addresses
/// each network spells.
pub(super) fn variables(
    pool: &PoolSettings,
    hub: String,
    parent_hub: Option<String>,
    parent: bool,
    endpoint: Option<String>,
) -> Result<Map<String, Value>, Error> {
    let mut pool = pool.clone();
    pool.directory = match pool.directory {
        DirectoryBackend::DynamoDb {
            records_table,
            lists_table,
            aws_region,
            ..
        } => DirectoryBackend::DynamoDb {
            records_table,
            lists_table,
            aws_region,
            endpoint,
        },
        _ => DirectoryBackend::Relay {
            parent_hub,
            state_dir: parent.then(|| STATE_DIR.into()),
        },
    };
    pool.hub = Some(hub);
    let toml::Value::Table(table) = toml::Value::try_from(&pool).map_err(io)? else {
        return Err(Error::new("a pool's settings are a table"));
    };
    Ok(pool_variables(&table))
}

/// Whether `port`, a port in Compose's short form,
/// `[[IP:][HOST]:]CONTAINER[/PROTOCOL]`, binds a host port it names: one
/// that two containers on one host cannot both bind. Without one, or with
/// host port 0, Docker chooses a free host port for each container. A form it does not know
/// counts as binding one, so that a conflict is never let through.
fn binds_host_port(port: &str) -> bool {
    let port = port.split_once('/').map_or(port, |(port, _)| port);
    let Some((host, _container)) = port.rsplit_once(':') else {
        return false;
    };
    let host = host.rsplit_once(':').map_or(host, |(_ip, host)| host);
    !host.is_empty() && host.parse::<u16>() != Ok(0)
}

/// The README of a fleet's bundle.
const README: &str = "# Docker Compose deployment of a fleet\n\n\
Run `docker compose up --build` from this directory, on one host. Each pool of the fleet has one hub, the service named for the pool, and a service for each segment of its class in its layout, named `<pool>_<segment>`. A pool's hub and segments take the pool's settings from `GDL_POOL__*` variables, and a segment finds its own pool's hub through `GDL_HUB__HOST`.\n\n\
A segment starts once its own pool's hub is healthy, and waits for nothing else. A hub's health check is its own liveness. Nothing waits for a pool's health, or for another pool: a parent pool's health answers 503 until its children are ready, so a wait on it would deadlock a fleet that starts together.\n\n\
A pool's hub must never run as two at once: a pool's record has one writer, its hub. Each hub's service names its container, so Compose refuses to scale it. Container names are global on the Docker host, so a host runs one copy of this bundle at a time. Compose's default recreate stops the old container before it starts the new one, so `docker compose up` after a change replaces a hub without two running at once.\n\n";

/// The README's paragraph on the `relay` directory backend.
const RELAY_README: &str = "Each pool's hub publishes its record under the `relay` directory backend: a child pool's hub reports it to its parent pool's hub (`GDL_POOL__PARENT_HUB`), and the parent's hub holds its children's records. The hubs share no volume, so the pools can run on several hosts, if each hub reaches its parent's hub by the name the bundle gives it. Each parent pool's hub keeps its children's records in a volume of its own, `<pool>-state`, mounted at `/var/lib/gdl-hub` (`GDL_POOL__STATE_DIR`), so it still holds them after it restarts, a silent child's included, until the child withdraws. `docker compose down --volumes` removes them. A parent's hub on another host needs that volume, or a directory of its own, on its host.\n\n\
";

/// The README's paragraph on a fleet's DynamoDB directory.
const DYNAMODB_README: &str = "Every pool's hub publishes its record to the fleet's DynamoDB directory: two tables in one DynamoDB Local, the `dynamodb-local` service (`GDL_POOL__DIRECTORY=dynamodb`, `GDL_POOL__ENDPOINT=http://dynamodb-local:8000`), which every hub reads, whatever its region. The hubs sign with fixed test credentials in their code, which DynamoDB Local never checks: no credential is in this bundle. DynamoDB Local runs with `-sharedDb`, so that every region sees one database, and in memory: `docker compose down` removes the directory, and so does a restart of `dynamodb-local`, after which the hubs cannot publish until `docker compose up` runs the tables step again. Before any hub starts, the `dynamodb-tables` service runs `gdl-hub --tables` from the hub's image, which makes the tables, and checks the keys of tables that exist. The hubs share no volume, and need no parent's hub. The hub's image is built with its `dynamodb` feature.\n\n";

/// The rest of the README of a fleet's bundle.
const README_TAIL: &str = "Each image is built once, from one Dockerfile per layout and segment, and every pool of that layout and class runs it. A segment's image is named for its package (`<graph>-<segment>`, or `<graph>-<layout>-<segment>` for a spec of several layouts), and a hub's is `<graph>-hub`, tagged with its layout for a spec of several.\n\n\
Each segment takes its roster from its pool's hub when it starts: the loaded instances of every segment of its class, at least `min_replicas` of each (one by default). An instance that starts later joins, so `docker compose up --scale <pool>_<segment>=N` adds instances of a segment that publishes no host port. A segment that publishes a host port it names (`HOST:CONTAINER`) runs in one pool; one whose host ports Docker chooses (`CONTAINER`, or host port 0) runs in several. An instance that is stopped retires: the others stop routing to it, and it leaves once it has drained. `cargo gdl k8s` writes a fleet's Kubernetes objects instead; srt-slurm is not generated.\n";

#[cfg(test)]
mod tests {
    use super::*;

    /// A pool's settings become one variable each, nested keys joined by
    /// `__`, and none for a setting that is unsaid.
    #[test]
    fn a_pools_settings_become_its_variables() {
        let fleet = Fleet::from_toml(
            "[class.model_x]\nready = { engine = 2 }\n\
             [pool.model-x-01]\nclass = \"model_x\"\nregion = \"us-east-1\"\nhome = \"global-us\"\n",
        )
        .expect("a fleet");
        let pool = fleet.pool("model-x-01").expect("a pool");
        let variables = variables(
            &pool,
            "model-x-01:1337".into(),
            Some("global-us:1337".into()),
            false,
            None,
        )
        .expect("variables");
        let expected = [
            ("GDL_POOL__CLASS", "model_x"),
            ("GDL_POOL__DIRECTORY", "relay"),
            ("GDL_POOL__HOME", "global-us"),
            ("GDL_POOL__HUB", "model-x-01:1337"),
            ("GDL_POOL__NAME", "model-x-01"),
            ("GDL_POOL__PARENT_HUB", "global-us:1337"),
            ("GDL_POOL__READY__ENGINE", "2"),
            ("GDL_POOL__REGION", "us-east-1"),
            ("GDL_POOL__SUMMARY_MS", "10000"),
        ];
        let found: Vec<(&str, &str)> = variables
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str().expect("a string")))
            .collect();
        assert_eq!(found, expected);

        // A parent's hub keeps its children's records in its volume.
        let parent =
            Fleet::from_toml("[pool.global-us]\nclass = \"global\"\nregion = \"us-east-1\"\n")
                .expect("a fleet")
                .pool("global-us")
                .expect("a pool");
        let kept = super::variables(&parent, "global-us:1337".into(), None, true, None)
            .expect("variables");
        assert_eq!(kept["GDL_POOL__STATE_DIR"], STATE_DIR);
        assert!(kept.get("GDL_POOL__PARENT_HUB").is_none(), "{kept:?}");
    }

    /// A port binds a host port only where it names one; a form that is
    /// not known counts as binding one.
    #[test]
    fn a_port_binds_a_host_port_only_where_it_names_one() {
        for port in [
            "9000",
            "9000/udp",
            "127.0.0.1::9000",
            "[::1]::9000/tcp",
            "0:9000",
            "127.0.0.1:0:9000",
            "[::1]:0:9000/tcp",
        ] {
            assert!(!binds_host_port(port), "{port}");
        }
        for port in [
            "9000:9000",
            "8000-8010:80-90",
            "127.0.0.1:9000:9000",
            "[::1]:9000:9000/tcp",
            "[::1]:9000",
        ] {
            assert!(binds_host_port(port), "{port}");
        }
    }
}
