//! A fleet's Slurm sites: what `check` refuses of them beyond what
//! [`gdl_model::Fleet::check`] does, which needs the generator, and their
//! srt-slurm recipes (`cargo gdl slurm`). Each pool is one services-only
//! job, with its hub on the head node and each segment on nodes of its
//! own; the DynamoDB Local the pools find each other through is a job of
//! its own; and each segment of a pool has a recipe that adds instances of
//! it to the running pool as another job. See `docs/src/guides/slurm.md`.

use std::path::Path;

use gdl_model::{Fleet, Problem};

use super::images::Digests;
use super::k8s::{self, Naming, Prepared};
use super::{Sources, images};
use crate::{Error, Files, Spec};

mod recipe;

/// What `cargo gdl slurm` renders a fleet's Slurm sites from, beside the
/// spec and the fleet.
pub struct SlurmRender<'a> {
    /// Where programs are built.
    pub target: &'a Path,
    /// The tag each image's digest is recorded under in `digests`, in
    /// place of the tag `images` gives it.
    pub tag: Option<&'a str>,
    /// Each image's digest, as `cargo gdl images --push` records them: a
    /// Slurm site pulls every image by its digest.
    pub digests: &'a Digests,
    /// Where the images take gdl and the crates they build.
    pub sources: Sources,
}

/// The recipes and settings of `fleet`'s Slurm sites, which run `spec`,
/// by path from the output directory: for each site, `<site>/` holds what
/// goes in `<run_dir>/<fleet>/` there. Each pool has `<pool>/pool.yaml`,
/// its job; `<pool>/join-<segment>.yaml` for each segment of its class,
/// a job that adds an instance of it to the running pool; and the
/// settings each of its processes reads, mounted at `/gdl`:
/// `<pool>/deployment.json` and `<pool>/<segment>.toml`. The site that
/// runs the fleet's DynamoDB Local has `directory.yaml`, its job. Each
/// image is named by its digest in `how.digests`.
///
/// Refused, with every problem named, unless the fleet passes
/// [`Fleet::check`] against the spec and has a Slurm site, and every pool
/// on one can run there ([`refusals`]): the fleet has a name and a
/// `[directory]` with `local = true`; no segment of a pool there mounts a
/// secret or takes the name of a recipe's own service; and the processes
/// one job places on a node bind no port twice.
pub fn slurm_files(spec: &Spec, fleet: &Fleet, how: &SlurmRender<'_>) -> Result<Files, Error> {
    if let Some(tag) = how.tag {
        k8s::check_tag(tag)?;
    }
    let prepared = k8s::prepare(spec, fleet, how.target, how.sources, |fleet, _| {
        let mut problems = Vec::new();
        if fleet.name().is_none() {
            problems.push(Problem::from(
                    "the fleet has no `[fleet] name`: it names every job the fleet runs (`gdl-<fleet>-<pool>`), and the directory on the shared file system that holds the fleet's recipes".to_string(),
                ));
        }
        if sites(fleet).next().is_none() {
            problems.push(Problem::from(
                    "the fleet has no Slurm site, so `cargo gdl slurm` has nothing to render: give a `[cluster.<name>]` table `kind = \"slurm\"`, and run pools on it".to_string(),
                ));
        }
        // Each pool's images are built for its cluster's registry and
        // platform.
        for pool in fleet.pools() {
            if fleet.cluster_of(&pool.name).is_none() {
                problems.push(Problem::at(
                    Some(format!("pool.{}", pool.name)),
                    None,
                    "has no `cluster`: in a fleet with a Slurm site, every pool runs on one of the fleet's clusters, whose registry and platform its images are built for",
                ));
            }
        }
        problems.extend(refusals(fleet));
        problems
    })?;
    let plan = images::plan(&prepared, fleet)?;
    let naming = Naming::new(how.tag, &plan, Some(how.digests));
    recipe::render(&prepared, fleet, &naming)
}

/// Write `fleet`'s recipes ([`slurm_files`]) into `out`, which must not
/// exist: all of them, or, on a failure, none.
pub fn slurm(spec: &Spec, fleet: &Fleet, out: &Path, how: &SlurmRender<'_>) -> Result<(), Error> {
    if out.exists() {
        return Err(Error::new(
            "Slurm output already exists; choose a fresh directory",
        ));
    }
    let files = slurm_files(spec, fleet, how)?;
    k8s::write_new(out, &files)
}

/// Judge `fleet`'s Slurm sites as [`slurm_files`] does, when it has any,
/// without the digests a render needs: what `images` checks before it
/// builds.
pub(super) fn judge(prepared: &Prepared, fleet: &Fleet, naming: &Naming<'_>) -> Result<(), Error> {
    if sites(fleet).next().is_none() {
        return Ok(());
    }
    recipe::render(prepared, fleet, naming).map(drop)
}

/// Every Slurm site of `fleet`, by its name, in name order.
fn sites(fleet: &Fleet) -> impl Iterator<Item = (&str, &gdl_model::SlurmSite)> {
    fleet
        .clusters()
        .filter_map(|(name, cluster)| Some((name, cluster.slurm()?)))
}

/// Every problem that keeps `fleet`'s pools off its Slurm sites, before
/// anything is rendered: those
/// [`problems`] finds, and a fleet with a pool on a Slurm site that does
/// not find its pools through a store its site runs: a DynamoDB Local, or
/// etcd. A pool's job on Slurm
/// learns no other job's address before it runs, so a child's hub cannot
/// be given its parent's, as the relay directory needs; and a Slurm job has
/// no AWS role to reach DynamoDB on AWS with. Each pool's processes read
/// the store's address from the fleet's directory on their site's
/// `run_dir`, so the store runs on the site of every Slurm pool.
pub(super) fn refusals(fleet: &Fleet) -> Vec<Problem> {
    let mut found = problems(fleet);
    if let Some(directory) = fleet.directory().filter(|directory| directory.runs_store())
        && let Some(home) = directory.cluster.as_deref()
    {
        let store = match directory.backend {
            gdl_model::FleetBackend::Dynamodb { .. } => "DynamoDB Local",
            gdl_model::FleetBackend::Etcd => "etcd",
        };
        for pool in fleet.pools() {
            let Some((site, _)) = fleet
                .cluster_of(&pool.name)
                .filter(|(_, cluster)| cluster.slurm().is_some())
            else {
                continue;
            };
            if site != home {
                found.push(Problem::at(
                    Some("directory".into()),
                    Some("cluster".into()),
                    format!(
                        "is `{home}`, and pool `{}` runs on Slurm site `{site}`: a Slurm job reads the {store}'s address from the directory job's file in its own site's `run_dir`, which only a directory job on `{site}` writes. Run the directory on `{site}`",
                        pool.name
                    ),
                ));
                break;
            }
        }
    }
    let placed = fleet.pools().find(|pool| {
        fleet
            .cluster_of(&pool.name)
            .is_some_and(|(_, cluster)| cluster.slurm().is_some())
    });
    if let Some(pool) = placed {
        match fleet.directory() {
            None => found.push(Problem::at(
                Some(format!("pool.{}", pool.name)),
                None,
                format!(
                    "pool `{}` runs on a Slurm site, whose pools find each other through a directory that the fleet runs: give the fleet a `[directory]` with `backend = \"etcd\"`, or `backend = \"dynamodb\"` and `local = true`. Under the relay, a child's hub needs its parent's hub's address, which a job learns only once it runs",
                    pool.name
                ),
            )),
            Some(directory) if !directory.runs_store() => found.push(Problem::at(
                Some("directory".into()),
                Some("local".into()),
                format!(
                    "pool `{}` runs on a Slurm site, whose jobs have no AWS role to reach DynamoDB with: set `local = true`, so that a job of the fleet runs DynamoDB Local",
                    pool.name
                ),
            )),
            Some(_) => {}
        }
    }
    found
}

/// Every problem of `fleet`'s Slurm sites, each at its site's table: a
/// registry or a platform an image cannot be named with, a `base_image` or
/// `hub_image` that is not an image reference, and a `run_dir` srt-slurm
/// cannot mount.
pub(crate) fn problems(fleet: &Fleet) -> Vec<Problem> {
    let mut problems = Vec::new();
    for (name, site) in sites(fleet) {
        let at = |key: &str, message: String| {
            Problem::at(Some(format!("cluster.{name}")), Some(key.into()), message)
        };
        for (key, image) in [
            ("base_image", &site.base_image),
            ("hub_image", &site.hub_image),
        ] {
            if let Some(Err(why)) = image.as_deref().map(super::reference::check_image) {
                problems.push(at(key, why));
            }
        }
        if let Some(registry) = &site.registry
            && let Err(message) = k8s::check_registry(registry)
        {
            problems.push(at("registry", message));
        }
        // srt-slurm joins its mounts with `,`, splits each at `:`, and
        // expands `$` in them; Pyxis writes each mount's source in double
        // quotes in a shell command.
        if let Some(found) = site
            .run_dir
            .chars()
            .find(|c| matches!(c, ':' | ',' | '$' | '"' | '\\' | '`') || c.is_whitespace())
        {
            problems.push(at(
                "run_dir",
                format!(
                    "`{}` holds `{}`, which srt-slurm's or Pyxis's mount of it would read as more than a path: choose a directory without `:`, `,`, `$`, `\"`, `\\`, a backquote, or white space",
                    site.run_dir,
                    found.escape_default()
                ),
            ));
        }
        if let Some(platform) = &site.platform
            && let Err(message) = k8s::check_platform(platform)
        {
            problems.push(at("platform", message));
        }
    }
    problems
}
