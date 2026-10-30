//! A fleet's Kubernetes objects: one directory per cluster, of plain
//! objects only. Each cluster gets its Namespace and the fleet's
//! ServiceAccount (`<cluster>/base.yaml`), with a PodMonitor of the fleet's
//! pods when the fleet asks for one (`[metrics] pod_monitor`), and each of its pools one file
//! (`<cluster>/pools/<pool>.yaml`): a ConfigMap of the pool's settings, its
//! hub's Deployment and Service, a Deployment for each segment of its
//! class, and a Service for each segment that publishes ports. A pool's
//! hub and segments get the variables its Compose services get, from the
//! same function, with the addresses the cluster's network spells. See
//! `docs/src/guides/kubernetes.md`.

use std::collections::BTreeMap;
use std::path::Path;

use gdl_model::{Fleet, PoolSettings, Problem};

use super::images::{self, Digests, Image};
use super::{Bundle, Sources, check_segment_name, io, pools, stdin_source};
use crate::spec::Allocator;
use crate::{Error, Files, Offers, Spec, export::image_problem};

mod dynamodb;
mod objects;
mod refusals;
mod relay;

pub(super) use dynamodb::{LOCAL, PLACEHOLDER, TABLES};
use objects::{Names, Pod, Port, Segment};
pub(super) use objects::{image, label_value};

/// The objects of `fleet`'s pools, which run `spec`, by path from the
/// output directory: `<cluster>/base.yaml`, `<cluster>/pools/<pool>.yaml`
/// and a `README.md`. Each image is named in its cluster's registry,
/// tagged `tag`, or else with the sha256 of what it is built from, which
/// [`super::images`] tags it with; with `digests`, from a run of `images`,
/// each is also named by its digest there, and an image they do not hold
/// is refused. Refused, with every problem named, unless the fleet passes
/// [`Fleet::check`] against the spec and every pool can run on
/// Kubernetes: the fleet has a name, every pool a cluster, every cluster
/// a registry and a platform, its `secrets` only the spec's secrets and
/// every one a segment there mounts, and its `env` no variable the
/// render sets; under the relay directory each child is in its parent's
/// cluster; and the spec does not choose jemalloc.
pub fn k8s_files(
    spec: &Spec,
    fleet: &Fleet,
    target: &Path,
    tag: Option<&str>,
    digests: Option<&Digests>,
) -> Result<Files, Error> {
    if let Some(tag) = tag {
        objects::check_tag(tag)?;
    }
    let prepared = prepare(spec, fleet, target)?;
    let plan = images::plan(&prepared, fleet)?;
    render(prepared, fleet, &Naming::new(tag, &plan, digests))
}

/// Write `fleet`'s objects ([`k8s_files`]) into `out`, which must not
/// exist: all of them, or, on a failure, none.
pub fn k8s(
    spec: &Spec,
    fleet: &Fleet,
    out: &Path,
    target: &Path,
    tag: Option<&str>,
    digests: Option<&Digests>,
) -> Result<(), Error> {
    if out.exists() {
        return Err(exists());
    }
    let files = k8s_files(spec, fleet, target, tag, digests)?;
    write_new(out, &files)
}

fn exists() -> Error {
    Error::new("Kubernetes output already exists; choose a fresh directory")
}

/// Write `files` into `out`, which must not exist: all of them, or, on a
/// failure, none.
pub(super) fn write_new(out: &Path, files: &Files) -> Result<(), Error> {
    if out.exists() {
        return Err(exists());
    }
    if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(io)?;
    }
    // Claimed only now, and atomically: another process may have made it
    // while the objects were rendered.
    match std::fs::create_dir(out) {
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Err(exists()),
        other => other.map_err(io)?,
    }
    if let Err(error) = crate::write(out, files) {
        let _ = std::fs::remove_dir_all(out);
        return Err(error);
    }
    Ok(())
}

/// The images of `fleet`'s pools, once the fleet's objects are judged as
/// [`k8s_files`] judges them, so that no image is built for a fleet that
/// cannot run.
pub(super) fn judged(spec: &Spec, fleet: &Fleet, target: &Path) -> Result<Vec<Image>, Error> {
    let prepared = prepare(spec, fleet, target)?;
    let plan = images::plan(&prepared, fleet)?;
    render(prepared, fleet, &Naming::new(None, &plan, None))?;
    Ok(plan)
}

/// A spec as its fleet's images and objects are made from: its generated
/// crates, its manifest, and its layouts, every segment with mimalloc.
pub(super) struct Prepared {
    pub(super) spec: Spec,
    pub(super) bundle: Bundle,
    pub(super) manifest: gdl_model::Manifest,
    pub(super) layouts: crate::validate::Layouts,
}

/// `spec` prepared for `fleet`, refused as [`k8s_files`] says.
fn prepare(spec: &Spec, fleet: &Fleet, target: &Path) -> Result<Prepared, Error> {
    if pools::by_pool(spec).is_none() {
        return Err(Error::new(
            "a fleet on Kubernetes deploys a spec by pool: give its placement classes, and the fleet its pools",
        ));
    }
    // What needs no layout is judged first, so that a refusal below never
    // stands for a problem of the fleet itself, such as an unknown cluster.
    fleet
        .check(None)
        .map_err(|errors| Error::from(gdl_model::Problems::from(errors)))?;
    let settings = spec.deployment.clone().unwrap_or_default();
    let problems = refusals::fleet(fleet, &settings);
    if !problems.is_empty() {
        return Err(Error::all(problems));
    }
    // An image is built on one host and runs on nodes whose page size
    // may differ: jemalloc fixes the page size when it is built, mimalloc
    // reads it when it runs.
    let mut spec = spec.clone();
    spec.allocator = match spec.allocator {
        Allocator::Jemalloc => {
            return Err(Error::new(
                "[binary] allocator: jemalloc fixes the page size when it is built, so an image built on one host may not start on nodes of a larger page size; images link mimalloc, so choose `mimalloc` or leave it unset",
            ));
        }
        Allocator::System | Allocator::Mimalloc => Allocator::Mimalloc,
    };
    // The generated crates are not written: only their files are read.
    let scratch = tempfile::Builder::new()
        .prefix("gdl-k8s-")
        .tempdir()
        .map_err(io)?;
    let (spec, bundle) = Bundle::prepare(&spec, scratch.path(), target, Sources::Fetched)?;
    let (checked, layouts) = crate::checked(&spec, target, Offers::Read);
    let checked = checked?;
    fleet
        .check(Some(&layouts))
        .map_err(|errors| Error::from(gdl_model::Problems::from(errors)))?;
    if let Some(source) = stdin_source(&spec, &checked.manifest)? {
        return Err(Error::new(format!(
            "source `{source}` reads its requests on stdin, which a pod has no way to: pin a frontend on it to deploy on Kubernetes"
        )));
    }
    Ok(Prepared {
        spec,
        bundle,
        manifest: checked.manifest,
        layouts,
    })
}

/// The objects of `fleet` from `prepared`, each image named by `naming`.
fn render(prepared: Prepared, fleet: &Fleet, naming: &Naming<'_>) -> Result<Files, Error> {
    Render {
        fleet,
        bundle: prepared.bundle,
        names: Names::default(),
        problems: Vec::new(),
        files: Files::new(),
    }
    .run(
        &prepared.spec,
        &prepared.manifest,
        &prepared.layouts,
        naming,
    )
}

/// How the objects name each image: by `--tag`, else by its own tag, and
/// by its digest when a run of `images` recorded them.
pub(super) struct Naming<'a> {
    tag: Option<&'a str>,
    /// Each image's own tag, by its repository and platform.
    tags: BTreeMap<(String, String), String>,
    digests: Option<&'a Digests>,
}

impl<'a> Naming<'a> {
    fn new(tag: Option<&'a str>, plan: &[Image], digests: Option<&'a Digests>) -> Self {
        let tags = plan
            .iter()
            .map(|image| {
                (
                    (image.repository.clone(), image.platform.clone()),
                    image.tag.clone(),
                )
            })
            .collect();
        Self { tag, tags, digests }
    }

    /// The image of `repository` for `platform` in `registry`.
    pub(super) fn image(
        &self,
        registry: &str,
        repository: &str,
        platform: &str,
    ) -> Result<String, Error> {
        let tag = match self.tag {
            Some(tag) => tag,
            None => self
                .tags
                .get(&(repository.to_string(), platform.to_string()))
                .map(String::as_str)
                .ok_or_else(|| {
                    Error::new(format!(
                        "no image of `{repository}` is built for `{platform}`"
                    ))
                })?,
        };
        let digest = match self.digests {
            None => None,
            Some(digests) => {
                let reference = objects::image(registry, repository, tag, None);
                Some(digests.get(&reference).ok_or_else(|| {
                    Error::new(format!(
                        "image `{reference}` has no digest in the digests file: run `cargo gdl images --push` with this spec and fleet, and pass the file it writes"
                    ))
                })?)
            }
        };
        Ok(objects::image(registry, repository, tag, digest))
    }
}

/// One render of a fleet: what is written so far, and what is wrong.
struct Render<'f> {
    fleet: &'f Fleet,
    bundle: Bundle,
    /// The objects' names in each cluster, so that two never share one.
    names: Names,
    problems: Vec<Problem>,
    files: Files,
}

impl Render<'_> {
    fn run(
        mut self,
        spec: &Spec,
        manifest: &gdl_model::Manifest,
        layouts: &crate::validate::Layouts,
        naming: &Naming<'_>,
    ) -> Result<Files, Error> {
        let pools: Vec<PoolSettings> = self.fleet.pools().collect();
        if pools.is_empty() {
            return Err(Error::new(
                "the fleet has no pools, so there is nothing to deploy",
            ));
        }
        let graph = self.bundle.graph.clone();
        let hub_repository = crate::emit::package_name(&graph, "hub");
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
                .ok_or_else(|| {
                    Error::new(laid.named("the placement does not fit the graph".into()))
                })?;
            let deployment =
                gdl_model::DeploymentSpec::new(manifest.clone(), laid.placement.clone())
                    .map_err(io)?;
            let deployment = serde_json::to_string_pretty(&deployment).map_err(io)?;
            // Each segment that a pool of the layout runs, read once.
            let mut segments = BTreeMap::new();
            for (segment, _) in laid.placement.segments() {
                let class = layout.class_of(segment);
                if !ours.iter().any(|pool| class == Some(pool.class.as_str())) {
                    continue;
                }
                if let Some(read) = self.segment(&laid, segment)? {
                    segments.insert(segment, read);
                }
            }
            for pool in ours {
                let members: Vec<&Segment> = segments
                    .iter()
                    .filter(|(name, _)| layout.class_of(name) == Some(pool.class.as_str()))
                    .map(|(_, read)| read)
                    .collect();
                self.pool(
                    pool,
                    pools::parent(layout, pool),
                    &deployment,
                    &members,
                    &hub_repository,
                    naming,
                )?;
            }
        }
        let fleet_name = self.fleet.name().expect("refusals::fleet requires a name");
        for (name, cluster) in self.fleet.clusters() {
            let mut base = objects::base(fleet_name, name, cluster);
            if self.fleet.metrics().pod_monitor {
                base.push(objects::pod_monitor(fleet_name, name, cluster));
            }
            if let Some(directory) = self.fleet.directory()
                && dynamodb::runs_local(self.fleet, name)
            {
                base.extend(dynamodb::local(fleet_name, name, cluster));
                let registry = cluster.registry.as_deref().unwrap_or_default();
                let platform = cluster.platform.as_deref().unwrap_or_default();
                let image = naming.image(registry, &hub_repository, platform)?;
                crate::deployment::validate_image(&image)?;
                let job = dynamodb::tables_job(fleet_name, name, cluster, directory, &image);
                self.files.insert(
                    Path::new(name).join("tables.yaml"),
                    objects::stream(&[job]).map_err(io)?,
                );
            }
            self.files.insert(
                Path::new(name).join("base.yaml"),
                objects::stream(&base).map_err(io)?,
            );
        }
        if let Some(directory) = self.fleet.directory().filter(|d| !d.local) {
            self.files.insert(
                Path::new("dynamodb").join("tables.json"),
                dynamodb::tables_json(self.fleet, directory),
            );
        }
        if !self.problems.is_empty() {
            return Err(Error::all(self.problems));
        }
        self.files
            .insert("README.md".into(), objects::readme(self.fleet));
        Ok(self.files)
    }

    /// Segment `segment` of layout `laid` as its pods run it: its settings,
    /// its image's repository, and its published ports and secrets. `None`,
    /// with the problem noted, when Kubernetes cannot name or publish it.
    fn segment(
        &mut self,
        laid: &crate::laid::Laid<'_>,
        segment: &str,
    ) -> Result<Option<Segment>, Error> {
        check_segment_name(segment)?;
        if let Err(message) = objects::check_segment(segment) {
            self.problems.push(Problem::from(message));
            return Ok(None);
        }
        let package = laid.package(&self.bundle.graph, segment);
        if let Some(problem) = image_problem(segment, &package) {
            return Err(Error::new(problem));
        }
        let path = laid.dir().join(segment).join("config.toml");
        let grace = self.bundle.segment_config(&path, segment, None, None)?;
        let mut table: toml::Table = toml::from_str(&self.bundle.files[&path]).map_err(io)?;
        // The probes ask the pod's IP, so a listener on loopback alone
        // would never answer them.
        if let toml::Value::Table(system) = table
            .entry("system")
            .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        {
            system.insert("host".into(), objects::SYSTEM_HOST.into());
        }
        let config = toml::to_string_pretty(&table).map_err(io)?;
        let probes = match objects::Probes::of(&table) {
            Ok(probes) => probes,
            Err(message) => {
                self.problems.push(Problem::at(
                    Some(format!("deployment.segment.{segment}.config.system")),
                    Some("port".into()),
                    message,
                ));
                return Ok(None);
            }
        };
        let mut ports = Vec::new();
        for port in self.bundle.ports(segment) {
            match Port::parse(port) {
                Ok(port) if !ports.contains(&port) => ports.push(port),
                Ok(_) => {}
                Err(message) => self.problems.push(Problem::at(
                    Some(format!("deployment.segment.{segment}")),
                    Some("ports".into()),
                    message,
                )),
            }
        }
        Ok(Some(Segment {
            name: segment.to_string(),
            repository: package,
            config,
            grace,
            probes,
            ports,
            secrets: self.bundle.secrets(segment).to_vec(),
        }))
    }

    /// The objects of `pool`, a `parent` when its class sends to another,
    /// which runs `segments`, into its cluster's file. Its hub's record
    /// names the hub at its pod's IP, which other pools' hubs reach, and
    /// its segments reach the hub at its Service.
    fn pool(
        &mut self,
        pool: &PoolSettings,
        parent: bool,
        deployment: &str,
        segments: &[&Segment],
        hub_repository: &str,
        naming: &Naming<'_>,
    ) -> Result<(), Error> {
        let (cluster_name, cluster) = self
            .fleet
            .cluster_of(&pool.name)
            .expect("refusals::fleet requires every pool's cluster");
        let pod = Pod {
            fleet: self.fleet.name().expect("refusals::fleet requires a name"),
            pool,
            cluster_name,
            cluster,
            naming,
        };
        // A pod's mount of a secret its cluster does not map could never
        // be filled.
        for segment in segments {
            for secret in &segment.secrets {
                if !cluster.secrets.contains_key(secret) {
                    self.problems.push(Problem::at(
                        Some(format!("pool.{}", pool.name)),
                        None,
                        format!(
                            "segment `{}` mounts secret `{secret}`, which `[cluster.{cluster_name}] secrets` does not map to a Kubernetes Secret",
                            segment.name
                        ),
                    ));
                }
            }
        }
        // Under the DynamoDB directory a hub reports to no parent's hub and
        // keeps no children's records.
        let relay = relay::Relay::of(pool, parent && self.fleet.directory().is_none());
        let endpoint = dynamodb::endpoint(self.fleet, cluster_name);
        let variables = |hub: String| {
            pools::variables(
                pool,
                hub,
                relay.parent_hub(),
                relay.state_dir().is_some(),
                endpoint.clone(),
            )
        };
        let mut objects = Vec::new();
        let config = pod.config_map(deployment, segments);
        let checksum = objects::checksum(&config);
        objects.push(config);
        objects.extend(relay.state_claim(&pod));
        objects.push(pod.hub(
            &variables(objects::pod_hub())?,
            hub_repository,
            &checksum,
            relay.state_dir(),
        )?);
        objects.push(pod.hub_service());
        for segment in segments {
            let variables = variables(objects::hub_address(&pool.name))?;
            objects.push(pod.segment(segment, &variables, &checksum)?);
            if !segment.ports.is_empty() {
                objects.push(pod.entry_service(segment));
            }
        }
        for object in &objects {
            if let Err(message) = self.names.claim(cluster_name, &pool.name, object) {
                self.problems.push(Problem::at(
                    Some(format!("pool.{}", pool.name)),
                    None,
                    message,
                ));
            }
        }
        self.files.insert(
            Path::new(cluster_name)
                .join("pools")
                .join(format!("{}.yaml", pool.name)),
            objects::stream(&objects).map_err(io)?,
        );
        Ok(())
    }
}
