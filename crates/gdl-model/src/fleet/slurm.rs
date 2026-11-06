//! A Slurm site, `[cluster.<name>]` with `kind = "slurm"`, a pool's
//! `slurm` table, and what the fleet's check refuses of them.

use serde::{Deserialize, Serialize};

use super::{FleetError, FleetTable};

/// A Slurm cluster that runs pools: `[cluster.<name>]` with
/// `kind = "slurm"`. Each pool runs as one job that srt-slurm submits,
/// and the site's account, partition, and network interface defaults stay
/// in srt-slurm's own `srtslurm.yaml` on the cluster, never in the fleet
/// file. Every gdl job on a site takes its nodes whole, so the site has no
/// key to share them: a pool's record carries only its hub's public
/// port, and two jobs on one node would bind the same ports.
/// See `docs/src/appendix/spec-reference.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlurmSite {
    /// The region of every pool on the site.
    pub region: String,
    /// The srt-slurm checkout that submits the jobs, an absolute path on
    /// the login node.
    pub srtctl: String,
    /// The directory of the rendered recipes and settings, an absolute
    /// path on a file system the login node and every compute node share.
    pub run_dir: String,
    /// The registry the site's nodes pull images from. Required: `check`
    /// refuses a site without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry: Option<String>,
    /// The site's nodes' platform, as `docker buildx --platform` takes
    /// it: `linux/arm64`. Required: `check` refuses a site without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    /// The image the site's segment images are built on, in place of the
    /// spec's `[deployment] base_image`: srt-slurm runs each command under
    /// `bash -c`, so it must hold bash. [`SlurmSite::DEFAULT_BASE_IMAGE`]
    /// when unsaid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_image: Option<String>,
    /// The image the site's hub image is built on; its `base_image` when
    /// unsaid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hub_image: Option<String>,
    /// The compute nodes' interface for traffic between nodes, which must
    /// match `srtslurm.yaml`'s; srt-slurm's when unsaid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_interface: Option<String>,
    /// How long before a job's time limit Slurm sends it SIGTERM, in
    /// seconds (`sbatch --signal=TERM@<lead>`): long enough for each pool
    /// to drain, and `check` refuses less than a pool's stop wait plus
    /// [`SlurmSite::SIGNAL_MARGIN_S`].
    #[serde(
        default = "SlurmSite::signal_lead_default",
        skip_serializing_if = "SlurmSite::signal_lead_is_default"
    )]
    pub signal_lead_s: u64,
    /// Whether a job keeps running when one of its nodes fails
    /// (`sbatch --no-kill`): the pool degrades rather than ends.
    #[serde(
        default = "SlurmSite::no_kill_default",
        skip_serializing_if = "SlurmSite::no_kill_is_default"
    )]
    pub no_kill: bool,
    /// Every job's time limit, in Slurm's time format (`08:00:00`), unless
    /// its pool's `slurm.time_limit` says otherwise; the partition's when
    /// unsaid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_limit: Option<String>,
    /// How `cargo gdl deploy` and `down`, on the login node, reach each
    /// pool's hub: [`SlurmReach::Direct`] unless the login node cannot
    /// reach the compute nodes' ports.
    #[serde(default, skip_serializing_if = "SlurmReach::is_default")]
    pub reach: SlurmReach,
}

/// How the login node reaches a hub on a Slurm site's compute nodes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlurmReach {
    /// Over the network, at the hub's address.
    #[default]
    Direct,
    /// With `curl` on the hub's node, run as a step of the pool's job:
    /// `srun --jobid <job> --overlap`.
    Srun,
}

impl SlurmReach {
    fn is_default(&self) -> bool {
        *self == Self::Direct
    }
}

impl SlurmSite {
    /// `base_image` when unsaid: Debian 12, which the default build image
    /// is built on too, with bash.
    pub const DEFAULT_BASE_IMAGE: &str = "debian:bookworm-slim";
    /// `signal_lead_s` when unsaid.
    pub const DEFAULT_SIGNAL_LEAD_S: u64 = 900;
    /// `no_kill` when unsaid.
    pub const DEFAULT_NO_KILL: bool = true;
    /// What `signal_lead_s` must exceed a pool's stop wait by, in
    /// seconds: the time for srt-slurm's teardown after the hub stops.
    pub const SIGNAL_MARGIN_S: u64 = 120;
    /// The longest `signal_lead_s` `sbatch --signal` takes, in seconds.
    pub const MAX_SIGNAL_LEAD_S: u64 = 65_535;
    /// The longest time limit, site's or pool's, in seconds: 365 days.
    /// Slurm counts a limit in 32-bit integers and wraps past them in more
    /// than one step of its parser, its rounding to minutes included; a
    /// year is far below every one, so no limit taken here wraps there.
    pub const MAX_TIME_LIMIT_S: u64 = 365 * 86_400;

    fn signal_lead_default() -> u64 {
        Self::DEFAULT_SIGNAL_LEAD_S
    }

    fn signal_lead_is_default(lead: &u64) -> bool {
        *lead == Self::DEFAULT_SIGNAL_LEAD_S
    }

    fn no_kill_default() -> bool {
        Self::DEFAULT_NO_KILL
    }

    fn no_kill_is_default(no_kill: &bool) -> bool {
        *no_kill == Self::DEFAULT_NO_KILL
    }
}

/// A pool's `slurm` table: how the pool's job runs on its Slurm site.
/// Refused on a pool of any other cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSlurm {
    /// The partition the pool's job runs in; `srtslurm.yaml`'s when
    /// unsaid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    /// The pool's job's time limit, over its site's `time_limit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_limit: Option<String>,
    /// Nodes the pool's job holds beyond its segments, for segments that
    /// join it later inside the job.
    #[serde(default, skip_serializing_if = "PoolSlurm::is_zero")]
    pub spare_nodes: u32,
}

impl PoolSlurm {
    fn is_zero(count: &u32) -> bool {
        *count == 0
    }
}

/// What Slurm site `name` cannot run, beside its name and region: a
/// missing `registry` or `platform`, a path that is not absolute, a signal
/// lead longer than `sbatch` takes, an interface that cannot be one, and a
/// time limit Slurm would not read or that is not longer than its signal
/// lead. Whether the lead covers each pool's stop wait needs its pools,
/// and is judged with them ([`super::Fleet::check`]); whether its images
/// are references, the generator's rule for an image, is judged by
/// `cargo gdl check`.
pub(super) fn check_site(name: &str, site: &SlurmSite, errors: &mut Vec<FleetError>) {
    let mut refuse = |key: &str, reason: String| {
        errors.push(FleetError::Slurm {
            table: FleetTable::Cluster(name.to_string()),
            key: Some(key.to_string()),
            reason,
        });
    };
    if site.registry.is_none() {
        refuse(
            "registry",
            "missing; a Slurm site's nodes pull its pools' images from it".into(),
        );
    }
    if site.platform.is_none() {
        refuse(
            "platform",
            "missing; a Slurm site's images are built for it, and its jobs run on its nodes".into(),
        );
    }
    for (key, path) in [("srtctl", &site.srtctl), ("run_dir", &site.run_dir)] {
        if !path.starts_with('/') {
            refuse(
                key,
                format!(
                    "`{path}` is not an absolute path; the login node and the jobs read it from any directory"
                ),
            );
        }
    }
    if site.signal_lead_s > SlurmSite::MAX_SIGNAL_LEAD_S {
        refuse(
            "signal_lead_s",
            format!(
                "is {} s, longer than {} s, the longest lead `sbatch --signal` takes",
                site.signal_lead_s,
                SlurmSite::MAX_SIGNAL_LEAD_S
            ),
        );
    }
    if let Some(interface) = &site.network_interface
        && !interface_name(interface)
    {
        refuse(
            "network_interface",
            format!(
                "`{interface}` is not a network interface's name: at most {INTERFACE_LIMIT} characters, without `/`, `:`, or white space"
            ),
        );
    }
    if let Some(limit) = &site.time_limit {
        check_time_limit(limit, site.signal_lead_s, &mut refuse);
    }
}

/// Refuse `limit`, a time limit, with `refuse` at `time_limit`, when Slurm
/// would not read it or it is not longer than the signal lead `lead`:
/// Slurm would signal the job as it starts.
pub(super) fn check_time_limit(limit: &str, lead: u64, refuse: &mut impl FnMut(&str, String)) {
    match slurm_seconds(limit) {
        None => refuse(
            "time_limit",
            format!(
                "`{limit}` is not a time limit in Slurm's format, above 0 and at most 365 days ({} s): minutes, minutes:seconds, hours:minutes:seconds, days-hours, days-hours:minutes, or days-hours:minutes:seconds",
                SlurmSite::MAX_TIME_LIMIT_S
            ),
        ),
        Some(seconds) if seconds <= lead => refuse(
            "time_limit",
            format!(
                "`{limit}` is {seconds} s, not longer than the site's `signal_lead_s` of {lead} s: Slurm would signal the job to stop as it starts"
            ),
        ),
        Some(_) => {}
    }
}

/// `text`, a time limit in Slurm's format, in seconds: `None` when Slurm's
/// format does not read it, when it is 0, which Slurm reads as no limit,
/// or when it is longer than [`SlurmSite::MAX_TIME_LIMIT_S`].
fn slurm_seconds(text: &str) -> Option<u64> {
    // Any number of digits, as Slurm reads them; a sum too large to count
    // in seconds is no limit it would take.
    let number = |part: &str| -> Option<u64> {
        let digits = !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
        digits.then(|| part.parse().ok()).flatten()
    };
    let (days, clock) = match text.split_once('-') {
        Some((days, clock)) => (Some(number(days)?), clock),
        None => (None, text),
    };
    let parts: Vec<u64> = clock.split(':').map(number).collect::<Option<_>>()?;
    let (days, hours, minutes, seconds) = match (days, parts.as_slice()) {
        (None, [minutes]) => (0, 0, *minutes, 0),
        (None, [minutes, seconds]) => (0, 0, *minutes, *seconds),
        (None, [hours, minutes, seconds]) => (0, *hours, *minutes, *seconds),
        (Some(days), [hours]) => (days, *hours, 0, 0),
        (Some(days), [hours, minutes]) => (days, *hours, *minutes, 0),
        (Some(days), [hours, minutes, seconds]) => (days, *hours, *minutes, *seconds),
        _ => return None,
    };
    let total = days
        .checked_mul(86_400)?
        .checked_add(hours.checked_mul(3600)?)?
        .checked_add(minutes.checked_mul(60)?)?
        .checked_add(seconds)?;
    (total > 0 && total <= SlurmSite::MAX_TIME_LIMIT_S).then_some(total)
}

/// The longest name of a Linux network interface.
const INTERFACE_LIMIT: usize = 15;

/// Whether `name` can name a Linux network interface.
fn interface_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= INTERFACE_LIMIT
        && name != "."
        && name != ".."
        && !name.contains(|c: char| c == '/' || c == ':' || c.is_whitespace())
}
