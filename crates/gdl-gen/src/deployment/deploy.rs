//! `cargo gdl deploy` and `cargo gdl down`: a fleet applied to its
//! clusters, and taken off them; and `cargo gdl join` and `cargo gdl
//! leave`, which grow and shrink a running pool on Slurm. See `docs/src/guides/kubernetes.md` and
//! `docs/src/guides/slurm.md`.
//!
//! This module is the driver: it runs the steps in order and stops at the
//! first failure, stops pools children first across the fleet, drains each
//! pool it removes and waits for its withdrawal, and waits for each pool's
//! health. A site does each step where the pools run ([`site::Site`]): the
//! Kubernetes site, through `kubectl` and the kube contexts the user
//! already has ([`kubernetes`]); and the Slurm site, through srt-slurm and
//! Slurm's own commands on a login node ([`slurm`]). A fleet runs on one
//! kind of site.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use gdl_model::Fleet;
use serde_json::Value;

use super::images::{Digests, Docker, images};
use crate::{Error, Sources, Spec};

mod command;
mod kubernetes;
mod order;
mod site;
mod slurm;

use kubernetes::Kubernetes;
pub use kubernetes::{Kube, Kubectl, Reply};
use order::Order;
use site::{
    Answer, Driven, Readiness, RenderInput, Running, Scope, Site, Stopping, Which, Withdrawal,
};
use slurm::SlurmPlace;
pub use slurm::{JoinBy, Joiner, Launched, Slurm, SlurmCli, SlurmCommand};

/// What `deploy` deploys, and where it builds.
#[derive(Debug, Clone)]
pub struct Deploy {
    /// The spec's file.
    pub spec: PathBuf,
    /// The fleet's file.
    pub fleet: PathBuf,
    /// The target directory the images build in.
    pub target: PathBuf,
    /// Where to also write the objects it applies, as `k8s` writes them: a
    /// directory that must not exist.
    pub out: Option<PathBuf>,
    /// How long it waits for each thing it waits on.
    pub waits: Waits,
    /// Where the images take gdl and the crates they build: with
    /// [`Sources::Local`], from the spec's `[gdl] path` and its crates by
    /// path, as they are on this machine.
    pub sources: Sources,
    /// Whether a change that replaces a pool's hub, which drains the pool,
    /// may be applied where the gdl operator runs the pools: it sets each
    /// `GdlPool`'s `allowPoolRestart`. Without it, the operator holds such
    /// a change, and the deploy stops naming the pool and why. Kubernetes
    /// only.
    pub allow_pool_restart: bool,
    /// The digests file `cargo gdl images --push --out` wrote: the images
    /// are not built, and each is named by its digest there. Required for
    /// a fleet on Slurm, whose login node has no Docker; refused for one on
    /// Kubernetes, which builds its own.
    pub images: Option<PathBuf>,
    /// Whether a pool whose job runs another recipe than the render is
    /// replaced: drained, cancelled, and submitted again. Without it, the
    /// deploy refuses such a pool before it changes anything. Slurm only.
    pub replace: bool,
}

/// How long `deploy` and `down` wait for what they wait on, and how often
/// they look. A wait that runs out fails the command, naming what it
/// waited for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Waits {
    /// Between two reads of something awaited.
    pub interval: Duration,
    /// For every pool's health, after the apply.
    pub health: Duration,
    /// For a pool's drain, beyond the longest stop grace of its pods: the
    /// grace is its drain bound and 30 s.
    pub drain: Duration,
    /// For a namespace to be active, and for each delete.
    pub objects: Duration,
}

impl Default for Waits {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(2),
            health: Duration::from_secs(600),
            drain: Duration::from_secs(60),
            objects: Duration::from_secs(300),
        }
    }
}

/// Deploy the fleet of `how` with `docker` and `kube`, or, for a fleet on
/// Slurm, with `slurm` ([`deploy_slurm`] lists its steps), saying each step
/// to `say`. On Kubernetes, the steps run in order, and the first that
/// fails stops it:
///
/// 1. check the spec and the fleet, as `cargo gdl check --fleet` does, and
///    that each cluster's context may create Deployments in its namespace;
///    ask each cluster whether it has the PodMonitor CRD, and refuse a
///    fleet that asks for a PodMonitor where it does not;
/// 2. build the images and push them ([`images`]);
/// 3. render the objects, each image named by its pushed digest
///    ([`crate::k8s_files`]); then refuse a pool that the fleet places in
///    another cluster than one that holds any of its Deployments, before
///    anything is applied;
/// 4. apply each cluster's base objects, and wait until its namespace is
///    active. The Namespace is never applied, since an apply would take
///    over the fields its owner set and drop those the render does not
///    list: it is created where none exists, and an existing one only gets
///    the fleet's labels, with `kubectl label`. The DynamoDB directory's cluster goes first,
///    whose base holds the directory's DynamoDB Local, which is waited for
///    until it is ready.
///    Its pod's IP then replaces the placeholder in the other clusters'
///    objects, before their base is applied. The objects, so complete, are
///    written to `out` if given;
/// 5. the tables Job, where the fleet renders one: delete the last run,
///    apply it, and wait until it succeeds; a failure stops the deploy
///    with the Job's log, which names the table;
/// 6. apply each cluster's pools, server-side;
/// 7. prune each cluster: drain each pool that the fleet no longer places
///    there, then delete every object labelled with the fleet and the
///    cluster that this render did not write, and none that it applied in
///    any cluster. A drained pool is deleted once it has withdrawn its
///    record: under `relay` its parent's hub answers 404 for it, and under
///    the DynamoDB directory its own hub says so;
/// 8. wait until each pool's health answers 200, through the API server's
///    service proxy; on a timeout, the error names each pod of an
///    unhealthy pool that is not ready, why it waits, and its events.
pub fn deploy(
    how: &Deploy,
    docker: &mut dyn Docker,
    kube: &mut dyn Kube,
    slurm: &mut dyn Slurm,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let problems = crate::check_files(&how.spec, Some(&how.fleet), &how.target);
    if !problems.is_empty() {
        return Err(problems.into());
    }
    let fleet = read_fleet(&how.fleet)?;
    let dgds = super::Dgds::read(&fleet, &how.fleet)
        .map_err(|problems| Error::from(problems.in_file(how.fleet.display())))?;
    let spec = Spec::load(&how.spec).map_err(|e| Error::from(e.in_file(how.spec.display())))?;
    match Kind::of(&fleet)? {
        Kind::Kubernetes => {
            for (given, flag) in [
                (how.images.is_some(), "--images"),
                (how.replace, "--replace"),
            ] {
                if given {
                    return Err(Error::new(format!(
                        "`{flag}` is for a fleet on Slurm: a fleet on Kubernetes builds its own images, and applies each change in place"
                    )));
                }
            }
            let mut site = Kubernetes::new(kube, &fleet, how.waits, how.allow_pool_restart)?;
            site.check(&fleet)?;
            say("check: the spec, the fleet, and each cluster's context");
            for warning in super::tunables_warnings(&fleet, &dgds) {
                say(&warning);
            }
            let built = images(&spec, &fleet, &dgds, &how.target, true, how.sources, docker)?;
            say(&format!("images: {} in their registries", built.len()));
            drive(
                how,
                &mut site,
                &spec,
                &fleet,
                &dgds,
                &Digests::of(&built),
                say,
            )
        }
        Kind::Slurm => deploy_slurm(how, slurm, &spec, &fleet, &dgds, say),
    }
}

/// Deploy `fleet`, on a Slurm site, as `how` says, with `slurm`. The steps
/// run in order, and the first that fails stops it:
///
/// 1. check the spec and the fleet, as `cargo gdl check --fleet` does; that
///    `squeue` answers, and that srt-slurm's checkout and the run
///    directory are directories here;
/// 2. read each image's digest from `--images`: a login node has no
///    Docker, and nothing is built;
/// 3. render the recipes ([`crate::slurm_files`]), and reconcile them with
///    the fleet's jobs that `squeue` lists: a pool, or the directory, whose
///    job runs the rendered recipe is kept; one with no job is submitted.
///    Refused, before anything changes, for two jobs of one pool, for a
///    directory job of another recipe, and for a pool's job of another
///    recipe without `--replace`, which drains that pool and cancels its
///    job, as `down` does. Then, before anything else changes, the record
///    of each pool the fleet no longer runs whose job is gone is withdrawn
///    through a parent's hub, children first;
/// 4. write the recipes into `<run_dir>/<fleet>`, and to `out` when given;
/// 5. submit the directory's job when it has none, and wait until the
///    fleet's pointer names the file of the directory's job: never a file
///    that an earlier job left;
/// 6. submit each pool that has no job, with `srtctl apply`;
/// 7. prune: stop each pool that the fleet no longer runs, as `down` does;
/// 8. wait until each pool's health answers 200, at the address its hub
///    wrote; a job that ends meanwhile stops the deploy at once, naming
///    its id, how Slurm says it ended, and its logs.
fn deploy_slurm(
    how: &Deploy,
    slurm: &mut dyn Slurm,
    spec: &Spec,
    fleet: &Fleet,
    dgds: &super::Dgds,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    if how.allow_pool_restart {
        return Err(Error::new(
            "`--allow-pool-restart` is for a cluster the gdl operator runs: on Slurm, `--replace` replaces a pool whose recipe changed",
        ));
    }
    let Some(file) = &how.images else {
        return Err(Error::new(
            "a fleet on Slurm needs `--images`, the digests file `cargo gdl images --push --out` writes on a machine with Docker: a login node has none, and each job pulls its images by digest",
        ));
    };
    let mut site = SlurmPlace::new(slurm, fleet, how.waits, how.replace)?;
    site.check(fleet)?;
    say("check: the spec, the fleet, and the Slurm site");
    let text = std::fs::read_to_string(file)
        .map_err(|e| Error::new(format!("{}: {e}", file.display())))?;
    let digests =
        Digests::from_json(&text).map_err(|e| Error::new(format!("{}: {e}", file.display())))?;
    say(&format!("images: the digests in {}", file.display()));
    drive(how, &mut site, spec, fleet, dgds, &digests, say)
}

/// The steps of a deploy after its images: render, stop what the apply
/// replaces, apply, prune, and wait for health.
fn drive(
    how: &Deploy,
    site: &mut dyn Site,
    spec: &Spec,
    fleet: &Fleet,
    dgds: &super::Dgds,
    digests: &Digests,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let files = site.render(&RenderInput {
        spec,
        fleet,
        dgds,
        target: &how.target,
        digests,
        sources: how.sources,
    })?;
    // What the apply cannot change where it runs is stopped first, children
    // first, once the site has refused what it cannot apply.
    let (order, running) = live(site, fleet, Scope::Replaced)?;
    stop_in_order(site, &order, &running, &BTreeSet::new(), &how.waits, say)?;
    site.apply(fleet, files, how.out.as_deref(), say)?;

    // Every pool this run renders, in any cluster.
    let retained: BTreeSet<String> = fleet.pools().map(|pool| pool.name).collect();
    let (order, running) = live(site, fleet, Scope::Prune)?;
    stop_in_order(site, &order, &running, &retained, &how.waits, say)?;
    for (place, _) in &running {
        site.finish(place, Scope::Prune, say)?;
    }

    let pools: Vec<(String, String)> = fleet
        .pools()
        .map(|pool| {
            let (name, _) = fleet
                .cluster_of(&pool.name)
                .expect("the render requires every pool's cluster");
            (name.to_string(), pool.name)
        })
        .collect();
    healthy(site, &pools, &how.waits)?;
    say("wait: every pool's health answers 200");
    Ok(())
}

/// Take the fleet in the file `fleet` off its clusters with `kube`, saying
/// each step to `say`: in every cluster, drain each pool that has a parent
/// and delete the pools' objects; then, in every cluster, delete the rest
/// of the objects that carry the fleet's and the cluster's labels, and
/// remove both labels from the namespace, which stays, with any Secret in
/// it. The rest holds the DynamoDB directory's Local and tables Job, which
/// the hubs of every cluster use: so it goes only once no hub runs in any.
///
/// On Slurm, `down` stops each pool's job children first in the same way:
/// it cancels the jobs that wait to join the pool, drains a pool that has
/// a parent and waits for its withdrawal, lets the instances of a pool
/// with none leave on `SIGTERM` until its hub lists none, and cancels the
/// pool's job and those that joined it. A pool whose job is gone has its
/// record withdrawn once through a running parent's hub. The directory's
/// job goes last.
/// The fleet's directory on the shared file system stays.
pub fn down(
    fleet: &Path,
    waits: &Waits,
    kube: &mut dyn Kube,
    slurm: &mut dyn Slurm,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let fleet = read_fleet(fleet)?;
    fleet
        .check(None)
        .map_err(|errors| Error::from(gdl_model::Problems::from(errors)))?;
    let mut kubernetes;
    let mut slurm_site;
    let site: &mut dyn Site = match Kind::of(&fleet)? {
        Kind::Kubernetes => {
            kubernetes = Kubernetes::new(kube, &fleet, *waits, false)?;
            &mut kubernetes
        }
        Kind::Slurm => {
            slurm_site = SlurmPlace::new(slurm, &fleet, *waits, false)?;
            &mut slurm_site
        }
    };
    site.reach()?;
    // Every pool of the fleet stops, children first across the fleet,
    // whatever kind of cluster each runs in.
    let (order, running) = live(site, &fleet, Scope::Pools)?;
    stop_in_order(site, &order, &running, &BTreeSet::new(), waits, say)?;
    for (place, _) in &running {
        let rest = site.live(place, Scope::Rest)?;
        let pools: Vec<&Driven> = rest.driven.iter().collect();
        stop(site, place, &pools, Which::Rest, waits, say)?;
        site.finish(place, Scope::Rest, say)?;
    }
    Ok(())
}

/// What runs of `scope` in each cluster of `fleet`, and the order in which
/// pools stop: the fleet's, then each cluster's, as found.
fn live(
    site: &mut dyn Site,
    fleet: &Fleet,
    scope: Scope,
) -> Result<(Order, Vec<(String, Running)>), Error> {
    let mut order = Order::default();
    order.fleet(fleet);
    let mut running = Vec::new();
    for (place, _) in fleet.clusters() {
        let found = site.live(place, scope)?;
        for note in &found.notes {
            order.add(note);
        }
        running.push((place.to_string(), found));
    }
    Ok((order, running))
}

/// Stop the pools of `running`, children first across the fleet, by
/// `order`: every pool at one depth before any at the depth above,
/// whatever cluster each runs in. A pool the driver stops is drained from
/// here ([`stop`]); one the site stops itself is handed to it
/// ([`Site::stop`]). What is of no pool, or of a pool `retained`, goes
/// last, undrained.
fn stop_in_order(
    site: &mut dyn Site,
    order: &Order,
    running: &[(String, Running)],
    retained: &BTreeSet<String>,
    waits: &Waits,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let stopped = |pool: &Driven| (!retained.contains(&pool.pool)).then(|| order.depth(&pool.pool));
    let deepest = running
        .iter()
        .flat_map(|(_, found)| found.driven.iter().filter_map(stopped))
        .chain(
            running
                .iter()
                .flat_map(|(_, found)| found.own.iter().map(|pool| order.depth(pool))),
        )
        .max();
    if let Some(deepest) = deepest {
        for depth in (0..=deepest).rev() {
            // Pools made again since the list: the level finishes in every
            // cluster, and then the run stops before the level above.
            let mut replaced = Vec::new();
            for (place, found) in running {
                let batch: Vec<&Driven> = found
                    .driven
                    .iter()
                    .filter(|pool| stopped(pool) == Some(depth))
                    .collect();
                if !batch.is_empty() {
                    let names: Vec<&str> = batch.iter().map(|pool| pool.pool.as_str()).collect();
                    stop(site, place, &batch, Which::Pools(&names), waits, say)?;
                }
            }
            for (place, found) in running {
                let batch: Vec<&str> = found
                    .own
                    .iter()
                    .filter(|pool| order.depth(pool) == depth)
                    .map(String::as_str)
                    .collect();
                replaced.extend(site.stop(place, &batch, say)?);
            }
            if !replaced.is_empty() {
                return Err(Error::all(replaced));
            }
        }
    }
    for (place, _) in running {
        site.remove(place, Which::Rest)?;
    }
    Ok(())
}

/// Stop `pools` of `place`: drain each that has a parent, wait until its
/// record is withdrawn, and then remove `which`. A pool whose hub is gone
/// cannot drain: the site refuses it unless it is known to have withdrawn,
/// or withdraws it through a parent's hub, checked before any drain
/// starts, so that no pool drains when one cannot be shown to have
/// withdrawn.
fn stop(
    site: &mut dyn Site,
    place: &str,
    pools: &[&Driven],
    which: Which<'_>,
    waits: &Waits,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let (draining, hubless): (Vec<&Stopping>, Vec<&Stopping>) = pools
        .iter()
        .filter_map(|pool| pool.drain.as_ref())
        .partition(|pool| pool.hub);
    for pool in &hubless {
        site.withdrawn_without_hub(place, pool, say)?;
    }
    let names: Vec<&str> = pools.iter().map(|pool| pool.pool.as_str()).collect();
    site.stop_joining(place, &names, say)?;
    for pool in &draining {
        site.hub(place).drain(&pool.pool, waits.objects)?;
        say(&format!("drain: pool `{}` is draining", pool.pool));
    }
    for pool in &draining {
        // Under a shared directory no parent's hub holds the pool's record,
        // so only the pool's own hub can say it is withdrawn.
        match pool.seen {
            Withdrawal::Shared { directory } => {
                withdrawn_by_its_hub(site, place, pool, directory, waits)?
            }
            Withdrawal::Relay { .. } => withdrawn(site, place, pool, waits)?,
        }
        say(&format!("drain: pool `{}` has withdrawn", pool.pool));
    }
    site.remove(place, which)
}

/// Wait until the hub of `pool`, a pool under the shared `directory`, says
/// that its drained record is withdrawn (`GET /v1/ext/gdl-pool/withdrawn`):
/// the directory has stored the withdrawal. Its health says so too, but
/// answers 503 for a drained pool, and Kubernetes' service proxy hides the
/// body of a 503. The wait is bounded as the relay's, and names the pool
/// when it runs out.
fn withdrawn_by_its_hub(
    site: &mut dyn Site,
    place: &str,
    pool: &Stopping,
    directory: &str,
    waits: &Waits,
) -> Result<(), Error> {
    let bound = Duration::from_secs(pool.grace) + waits.drain;
    let deadline = Instant::now() + bound;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let answer = site.hub(place).withdrawn(&pool.pool, left)?;
        if let Answer::Body(body) = &answer
            && serde_json::from_str::<Value>(body).is_ok_and(|body| body["withdrawn"] == true)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let last = match &answer {
                Answer::Body(body) => format!("its hub still says {}", body.trim()),
                Answer::NotFound => "its hub does not answer".to_string(),
                Answer::NoHub(why) | Answer::Failed(why) => {
                    format!("its hub does not answer: {}", why.trim())
                }
            };
            return Err(Error::new(format!(
                "pool `{}` did not say within {} s that its record is withdrawn from the {directory} directory, so it is not deleted: {last}",
                pool.pool,
                bound.as_secs()
            )));
        }
        std::thread::sleep(waits.interval);
    }
}

/// Wait until the hub of `pool`'s parent no longer holds its record: the
/// pool's drain has ended, and its hub has withdrawn it. Only the parent's
/// hub's own 404 proves it. A parent holds a pool that stopped without a
/// drain, as missing, until an operator withdraws it, so a parent that
/// cannot be reached proves nothing: the wait stops at once when its hub
/// does not exist, and at its bound when it does not answer, naming the
/// pool either way. The relay directory is the one that serves this
/// record: another directory needs another proof.
fn withdrawn(
    site: &mut dyn Site,
    place: &str,
    pool: &Stopping,
    waits: &Waits,
) -> Result<(), Error> {
    let home = pool.relay_home()?;
    let bound = Duration::from_secs(pool.grace) + waits.drain;
    let deadline = Instant::now() + bound;
    let unproven = |why: &str| {
        Error::new(format!(
            "pool `{}` is not known to have withdrawn from its parent `{home}`, so it is not deleted: {why}",
            pool.pool
        ))
    };
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let answer = site
            .hub(place)
            .report(home, &pool.class, &pool.pool, left)?;
        match &answer {
            Answer::NotFound => return Ok(()),
            Answer::NoHub(why) => return Err(unproven(why)),
            Answer::Body(_) | Answer::Failed(_) => {}
        }
        if Instant::now() >= deadline {
            let last = match &answer {
                Answer::Failed(why) => format!("its hub does not answer: {}", why.trim()),
                _ => "its hub still holds the record".to_string(),
            };
            return Err(unproven(&format!("after {} s, {last}", bound.as_secs())));
        }
        std::thread::sleep(waits.interval);
    }
}

/// Wait until each of `pools`, each `(cluster, pool)`, is ready, as its
/// site says ([`Site::ready`]); on a timeout, the error names each pool
/// that is not, why it waits, and what its site says of it.
fn healthy(site: &mut dyn Site, pools: &[(String, String)], waits: &Waits) -> Result<(), Error> {
    let deadline = Instant::now() + waits.health;
    let mut waiting: Vec<&(String, String)> = pools.iter().collect();
    // Why each pool is not ready yet, as its site last said.
    let mut reasons: BTreeMap<String, String> = BTreeMap::new();
    loop {
        let mut still = Vec::new();
        for entry in waiting {
            let (place, pool) = entry;
            if let Readiness::Waiting(why) = site.ready(place, pool, deadline)? {
                if let Some(why) = why {
                    reasons.insert(pool.clone(), why);
                }
                still.push(entry);
            }
        }
        waiting = still;
        if waiting.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let mut message = format!(
                "the health of {} pool(s) did not answer 200 within {} s:",
                waiting.len(),
                waits.health.as_secs()
            );
            for (place, pool) in waiting {
                message.push_str(&format!("\n- pool `{pool}` in cluster `{place}`"));
                if let Some(why) = reasons.get(pool) {
                    message.push_str(&format!(": {why}"));
                }
                for line in site.why_not_ready(place, pool)? {
                    message.push_str(&format!("\n  {line}"));
                }
            }
            return Err(Error::new(message));
        }
        std::thread::sleep(waits.interval);
    }
}

/// Add instances of `segment` to running pool `pool` of the fleet in file
/// `fleet`, on its Slurm site, `by` a job of their own or a step on a
/// spare node of the pool's job; say each step to `say`. Returns what
/// started, which [`leave`] takes.
///
/// A joining job is submitted from the pool's `join-<segment>.yaml`, which
/// `deploy` wrote, with as many nodes as instances, and `sbatch
/// --deadline` set to the end `squeue` gives for the pool's job, with a
/// `--time-min` long enough to serve before it leaves: it never outlives
/// its pool. Its `resolve` step finds the pool's hub when it starts, and
/// fails the job at once when the pool is gone or draining.
///
/// A step on a spare node of the pool's job ([`JoinBy::Step`]) is refused
/// for now, before any Slurm command runs: its `srun` would run detached
/// on the login node, and a `down` that runs meanwhile can cancel the
/// joiner it starts unsignalled. It returns with a launcher inside the
/// pool's job; the code below the refusal stays, and is tested.
///
/// # Errors
///
/// A step on a spare node, refused; the fleet is not on Slurm; the pool
/// has no running job, or the render no recipe that joins `segment` to
/// it; or the pool's job ends too soon.
pub fn join(
    fleet: &Path,
    pool: &str,
    segment: &str,
    by: JoinBy,
    waits: &Waits,
    slurm: &mut dyn Slurm,
    say: &mut dyn FnMut(&str),
) -> Result<Joiner, Error> {
    if by == JoinBy::Step {
        return Err(Error::new(
            "`cargo gdl join --in-job` is not available yet: a joiner in the pool's job needs a launcher inside the job, which gdl does not have yet. A join by a job of its own works: run `cargo gdl join` without `--in-job`",
        ));
    }
    let fleet = slurm_fleet(fleet, "join")?;
    SlurmPlace::new(slurm, &fleet, *waits, false)?.join(&fleet, pool, segment, by, say)
}

/// Remove `joiner`, which [`join`] started in the fleet in file `fleet`:
/// one `SIGTERM` to its steps, on which its instances retire, drain, and
/// exit; then wait until Slurm no longer lists it. A joining job that
/// still waits in the queue is cancelled, which needs Slurm 23.11.6 or
/// later, as `down` does.
///
/// # Errors
///
/// The fleet is not on Slurm; `joiner` is not a joining job or a joiner's
/// step of the fleet; or it did not end within its stop grace and the
/// drain wait, or a job ended other than `COMPLETED`.
pub fn leave(
    fleet: &Path,
    joiner: &Joiner,
    waits: &Waits,
    slurm: &mut dyn Slurm,
    say: &mut dyn FnMut(&str),
) -> Result<(), Error> {
    let fleet = slurm_fleet(fleet, "leave")?;
    SlurmPlace::new(slurm, &fleet, *waits, false)?.leave_joiner(joiner, say)
}

/// The fleet in file `path`, checked, refused unless it runs on Slurm:
/// `command` is for a fleet on Slurm.
fn slurm_fleet(path: &Path, command: &str) -> Result<Fleet, Error> {
    let fleet = read_fleet(path)?;
    fleet
        .check(None)
        .map_err(|errors| Error::from(gdl_model::Problems::from(errors)))?;
    match Kind::of(&fleet)? {
        Kind::Slurm => Ok(fleet),
        Kind::Kubernetes => Err(Error::new(format!(
            "`cargo gdl {command}` is for a fleet on Slurm, and this fleet has no Slurm site: a pool on Kubernetes grows and shrinks with its `ready` counts and `cargo gdl deploy`"
        ))),
    }
}

/// The kind of site a fleet runs on: every cluster of a fleet is of one
/// kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Kubernetes,
    Slurm,
}

impl Kind {
    /// The kind of `fleet`'s clusters, refused for a fleet with both.
    fn of(fleet: &Fleet) -> Result<Self, Error> {
        let (mut slurm, mut kubernetes) = (Vec::new(), Vec::new());
        for (name, cluster) in fleet.clusters() {
            match cluster.slurm() {
                Some(_) => slurm.push(name),
                None => kubernetes.push(name),
            }
        }
        match (slurm.is_empty(), kubernetes.is_empty()) {
            (true, _) => Ok(Self::Kubernetes),
            (false, true) => Ok(Self::Slurm),
            (false, false) => Err(Error::new(format!(
                "the fleet has Kubernetes clusters (`{}`) and Slurm sites (`{}`): `cargo gdl deploy` and `cargo gdl down` run a fleet on one kind of site. Split it into a fleet for each",
                kubernetes.join("`, `"),
                slurm.join("`, `")
            ))),
        }
    }
}

fn read_fleet(path: &Path) -> Result<Fleet, Error> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| Error::new(format!("{}: {e}", path.display())))?;
    Fleet::from_toml(&text).map_err(|problems| problems.in_file(path.display()).into())
}
