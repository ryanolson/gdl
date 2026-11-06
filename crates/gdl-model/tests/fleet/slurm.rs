//! A Slurm site: `[cluster.<name>]` with `kind = "slurm"`. It reads and
//! writes with its tag, takes only its own keys, and the check refuses
//! what a Slurm site cannot run.

use gdl_model::{Cluster, Fleet, PoolSlurm, SlurmReach, SlurmSite};
use serde_json::json;

use super::{fleet, laid, problems};

/// A global pool on a Kubernetes cluster and its child on a Slurm site,
/// every key of the site set.
const SITED: &str = r#"
[fleet]
name = "lab"

[cluster.k8s]
context = "kind-lab"
namespace = "gdl"
region = "us-east-1"

[cluster.lab]
kind = "slurm"
region = "slurm-lab"
srtctl = "/shared/srt-slurm"
run_dir = "/shared/gdl-runs"
registry = "registry.example/gdl"
platform = "linux/arm64"
base_image = "debian:bookworm"
hub_image = "debian:bookworm-slim"
network_interface = "ib0"
signal_lead_s = 1200
no_kill = false
time_limit = "08:00:00"
reach = "srun"

[pool.global]
class = "global"
cluster = "k8s"

[pool.model-x]
class = "model_x"
cluster = "lab"
home = "global"
slurm = { partition = "gpu", time_limit = "1-00:00:00", spare_nodes = 1 }
"#;

/// The smallest Slurm site the check takes.
const SITE: &str = "[cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = \"/s\"\nrun_dir = \"/r\"\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n";

/// A global pool and its child, both on the site `lab`, with `extra` in
/// the child's table.
fn on_site(site: &str, extra: &str) -> String {
    format!(
        "{site}\n[pool.global]\nclass = \"global\"\ncluster = \"lab\"\n\n[pool.model-x]\nclass = \"model_x\"\ncluster = \"lab\"\nhome = \"global\"\n{extra}"
    )
}

#[test]
fn a_slurm_site_reads_every_key() {
    let fleet = fleet(SITED);
    fleet.check(Some(&laid())).expect("a sound fleet");
    let lab = fleet.cluster("lab").expect("a cluster");
    assert_eq!(lab.kind(), "slurm");
    assert_eq!(lab.region(), "slurm-lab");
    assert!(lab.kubernetes().is_none());
    assert_eq!(
        lab.slurm().expect("a Slurm site"),
        &SlurmSite {
            region: "slurm-lab".into(),
            srtctl: "/shared/srt-slurm".into(),
            run_dir: "/shared/gdl-runs".into(),
            registry: Some("registry.example/gdl".into()),
            platform: Some("linux/arm64".into()),
            base_image: Some("debian:bookworm".into()),
            hub_image: Some("debian:bookworm-slim".into()),
            network_interface: Some("ib0".into()),
            signal_lead_s: 1200,
            no_kill: false,
            time_limit: Some("08:00:00".into()),
            reach: SlurmReach::Srun,
        }
    );
    assert_eq!(
        fleet.slurm("model-x"),
        Some(&PoolSlurm {
            partition: Some("gpu".into()),
            time_limit: Some("1-00:00:00".into()),
            spare_nodes: 1,
        })
    );
    assert_eq!(fleet.slurm("global"), None);
    // A pool takes its site's region, as a Kubernetes cluster's.
    assert_eq!(fleet.pool("model-x").expect("a pool").region, "slurm-lab");
    assert_eq!(
        fleet
            .cluster_of("model-x")
            .map(|(name, c)| (name, c.kind())),
        Some(("lab", "slurm"))
    );
}

#[test]
fn a_slurm_site_takes_its_defaults() {
    let fleet = fleet(&on_site(SITE, ""));
    fleet.check(Some(&laid())).expect("a sound fleet");
    let site = fleet
        .cluster("lab")
        .and_then(Cluster::slurm)
        .expect("a site");
    assert_eq!(site.signal_lead_s, SlurmSite::DEFAULT_SIGNAL_LEAD_S);
    assert_eq!(site.signal_lead_s, 900);
    assert!(site.no_kill);
    assert_eq!(
        (&site.base_image, &site.hub_image, &site.network_interface),
        (&None, &None, &None)
    );
    assert_eq!(site.time_limit, None);
    assert_eq!(site.reach, SlurmReach::Direct);
    assert_eq!(SlurmSite::DEFAULT_BASE_IMAGE, "debian:bookworm-slim");
}

/// The shape the composer's draft carries: a Slurm site writes its `kind`
/// first and only what is not a default; a Kubernetes cluster writes no
/// `kind`. Both read back as they were, from TOML and from JSON.
#[test]
fn a_slurm_site_writes_its_kind_first_and_reads_back() {
    let sited = fleet(SITED);
    let toml = toml::to_string(&sited).expect("toml");
    assert!(
        toml.contains("[cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\n"),
        "{toml}"
    );
    assert!(
        toml.contains("[cluster.k8s]\ncontext = \"kind-lab\"\n"),
        "{toml}"
    );
    assert_eq!(Fleet::from_toml(&toml).expect("read back"), sited);
    let json = serde_json::to_value(&sited).expect("json");
    assert_eq!(
        json["cluster"],
        json!({
            "k8s": {"context": "kind-lab", "namespace": "gdl", "region": "us-east-1"},
            "lab": {
                "kind": "slurm",
                "region": "slurm-lab",
                "srtctl": "/shared/srt-slurm",
                "run_dir": "/shared/gdl-runs",
                "registry": "registry.example/gdl",
                "platform": "linux/arm64",
                "base_image": "debian:bookworm",
                "hub_image": "debian:bookworm-slim",
                "network_interface": "ib0",
                "signal_lead_s": 1200,
                "no_kill": false,
                "time_limit": "08:00:00",
                "reach": "srun"
            }
        })
    );
    assert_eq!(
        json["pool"]["model-x"]["slurm"],
        json!({"partition": "gpu", "time_limit": "1-00:00:00", "spare_nodes": 1})
    );
    let text = serde_json::to_string(&sited).expect("json");
    assert!(text.contains(r#""lab":{"kind":"slurm","region""#), "{text}");
    assert_eq!(
        serde_json::from_str::<Fleet>(&text).expect("read back"),
        sited
    );
    // Defaults are not written.
    let small = toml::to_string(&fleet(&on_site(SITE, ""))).expect("toml");
    for key in ["signal_lead_s", "no_kill", "spare_nodes", "reach"] {
        assert!(!small.contains(key), "{key} in:\n{small}");
    }
}

#[test]
fn an_explicit_kubernetes_kind_reads_as_the_default() {
    let tagged = fleet(
        "[cluster.a]\nkind = \"kubernetes\"\ncontext = \"c\"\nnamespace = \"n\"\nregion = \"r\"\n",
    );
    let bare = fleet("[cluster.a]\ncontext = \"c\"\nnamespace = \"n\"\nregion = \"r\"\n");
    assert_eq!(tagged, bare);
    assert_eq!(tagged.cluster("a").map(Cluster::kind), Some("kubernetes"));
    let written = toml::to_string(&tagged).expect("toml");
    assert!(!written.contains("kind"), "{written}");
}

/// A key of one kind on a cluster of the other is refused by its name,
/// as any unknown key is.
#[test]
fn a_key_of_the_other_kind_is_refused_by_name() {
    let slurm =
        "[cluster.a]\nkind = \"slurm\"\nregion = \"r\"\nsrtctl = \"/s\"\nrun_dir = \"/r\"\n";
    for key in [
        "context = \"c\"",
        "namespace = \"n\"",
        "operator = true",
        "pins_local = true",
        "tolerations = []",
    ] {
        let name = key.split_once(' ').expect("a key").0;
        let problems = Fleet::from_toml(&format!("{slurm}{key}\n")).expect_err(key);
        let problem = &problems.0[0];
        assert_eq!(problem.table.as_deref(), Some("cluster.a"), "{key}");
        assert_eq!(problem.key.as_deref(), Some(name), "{key}");
        assert!(
            problem
                .message
                .starts_with(&format!("unknown field `{name}`")),
            "{problem}"
        );
    }
    let kubernetes = "[cluster.a]\ncontext = \"c\"\nnamespace = \"n\"\nregion = \"r\"\n";
    for key in ["srtctl = \"/s\"", "signal_lead_s = 900", "no_kill = true"] {
        let name = key.split_once(' ').expect("a key").0;
        let problems = Fleet::from_toml(&format!("{kubernetes}{key}\n")).expect_err(key);
        assert_eq!(problems.0[0].key.as_deref(), Some(name), "{key}");
    }
    // A pool's `slurm` table takes its own keys only.
    let problems = Fleet::from_toml(&on_site(SITE, "slurm = { nodes = 2 }\n")).expect_err("nodes");
    assert_eq!(
        problems.0[0].table.as_deref(),
        Some("pool.model-x.slurm"),
        "{problems:?}"
    );
    assert_eq!(problems.0[0].key.as_deref(), Some("nodes"));
    // A kind that is none.
    for kind in ["kind = \"nomad\"", "kind = 1"] {
        let problems =
            Fleet::from_toml(&format!("[cluster.a]\n{kind}\nregion = \"r\"\n")).expect_err(kind);
        assert_eq!(problems.0[0].key.as_deref(), Some("kind"), "{problems:?}");
    }
}

#[test]
fn a_slurm_site_without_registry_or_platform_is_refused() {
    let bare = SITE
        .replace("registry = \"registry.example/gdl\"\n", "")
        .replace("platform = \"linux/amd64\"\n", "");
    assert_eq!(
        problems(&on_site(&bare, "")),
        [
            "[cluster.lab] registry: missing; a Slurm site's nodes pull its pools' images from it",
            "[cluster.lab] platform: missing; a Slurm site's images are built for it, and its jobs run on its nodes",
        ]
    );
}

/// Every gdl job on a Slurm site takes its nodes whole, so a site has no
/// key to share them: `exclusive`, either way, is an unknown key, refused
/// by its name.
#[test]
fn a_slurm_site_has_no_key_to_share_its_nodes() {
    for value in ["false", "true"] {
        let text = on_site(&format!("{SITE}exclusive = {value}\n"), "");
        let problems = Fleet::from_toml(&text).expect_err(value);
        assert_eq!(problems.0[0].table.as_deref(), Some("cluster.lab"));
        assert_eq!(problems.0[0].key.as_deref(), Some("exclusive"));
        assert!(
            problems.0[0]
                .message
                .starts_with("unknown field `exclusive`"),
            "{problems:?}"
        );
    }
}

#[test]
fn a_slurm_site_refuses_paths_and_interfaces_it_cannot_use() {
    let site = SITE
        .replace("\"/s\"", "\"srt-slurm\"")
        .replace("\"/r\"", "\"runs\"");
    let site = format!("{site}network_interface = \"ib0:1\"\n");
    let problems = problems(&on_site(&site, ""));
    let starts = [
        "[cluster.lab] srtctl: `srt-slurm` is not an absolute path",
        "[cluster.lab] run_dir: `runs` is not an absolute path",
        "[cluster.lab] network_interface: `ib0:1` is not a network interface's name",
    ];
    assert_eq!(problems.len(), starts.len(), "{problems:?}");
    for (problem, start) in problems.iter().zip(starts) {
        assert!(problem.starts_with(start), "{start}\nin: {problem}");
    }
}

/// Slurm's time format, and a limit no longer than the signal lead, which
/// would stop the job as it starts: at the site and at a pool.
#[test]
fn a_time_limit_is_slurms_and_longer_than_the_signal_lead() {
    for good in [
        "60",
        "30:00",
        "08:00:00",
        "1-0",
        "1-12",
        "2-00:30",
        "1-00:00:00",
        "16",
    ] {
        let site = format!("{SITE}time_limit = \"{good}\"\n");
        fleet(&on_site(&site, ""))
            .check(Some(&laid()))
            .unwrap_or_else(|errors| panic!("{good}: {errors}"));
    }
    for bad in [
        "",
        "0",
        "00:00:00",
        "8h",
        "1-",
        "-1",
        "1:2:3:4",
        "UNLIMITED",
        "1-2:3:4:5",
    ] {
        let site = format!("{SITE}time_limit = \"{bad}\"\n");
        let problems = problems(&on_site(&site, ""));
        assert_eq!(problems.len(), 1, "{bad}: {problems:?}");
        assert!(
            problems[0].starts_with(&format!(
                "[cluster.lab] time_limit: `{bad}` is not a time limit in Slurm's format"
            )),
            "{problems:?}"
        );
    }
    // 15 minutes is 900 s, the default lead: the job would be signalled
    // as it starts.
    let site = format!("{SITE}time_limit = \"15\"\n");
    assert_eq!(
        problems(&on_site(&site, "")),
        [
            "[cluster.lab] time_limit: `15` is 900 s, not longer than the site's `signal_lead_s` of 900 s: Slurm would signal the job to stop as it starts"
        ]
    );
    let problems = problems(&on_site(SITE, "slurm = { time_limit = \"10:00\" }\n"));
    assert_eq!(
        problems,
        [
            "[pool.model-x] slurm.time_limit: `10:00` is 600 s, not longer than the site's `signal_lead_s` of 900 s: Slurm would signal the job to stop as it starts"
        ]
    );
}

#[test]
fn a_pools_slurm_table_is_refused_off_a_slurm_site() {
    let kubernetes = "[cluster.k8s]\ncontext = \"c\"\nnamespace = \"n\"\nregion = \"r\"\n\n[pool.global]\nclass = \"global\"\ncluster = \"k8s\"\n\n[pool.model-x]\nclass = \"model_x\"\ncluster = \"k8s\"\nhome = \"global\"\nslurm = { partition = \"gpu\" }\n";
    assert_eq!(
        problems(kubernetes),
        [
            "[pool.model-x] slurm: cluster `k8s` is a kubernetes cluster, which does not read it: leave the table out, or run the pool on a Slurm site"
        ]
    );
    let nowhere = "[pool.global]\nclass = \"global\"\nregion = \"r\"\n\n[pool.model-x]\nclass = \"model_x\"\nregion = \"r\"\nhome = \"global\"\nslurm = {}\n";
    assert_eq!(
        problems(nowhere),
        [
            "[pool.model-x] slurm: the pool names no cluster, so no Slurm site reads it: name the Slurm site that runs the pool, or leave the table out"
        ]
    );
    let blank = problems(&on_site(SITE, "slurm = { partition = \"\" }\n"));
    assert_eq!(
        blank,
        ["[pool.model-x] slurm.partition: `` is not a partition's name"]
    );
}

#[test]
fn a_dgd_or_kubernetes_resources_on_a_slurm_site_are_refused() {
    let dgd = problems(&on_site(
        SITE,
        "[pool.model-x.dgd]\nfile = \"dgd.yaml\"\ncomponents = { e = \"Worker\" }\n",
    ));
    assert_eq!(
        dgd,
        [
            "[pool.model-x] dgd: cluster `lab` is a Slurm site: a DynamoGraphDeployment runs only under dynamo's operator on Kubernetes. Run the pool on a Kubernetes cluster, or leave the table out"
        ]
    );
    let resources = problems(&on_site(
        SITE,
        "[pool.model-x.resources.segment.e]\nlimits = { memory = \"4Gi\" }\n",
    ));
    assert_eq!(
        resources,
        [
            "[pool.model-x] resources: cluster `lab` is a Slurm site, where each segment instance takes its nodes whole, and no pod's requests and limits apply: leave the table out"
        ]
    );
}

/// A site's `signal_lead_s` covers the longest stop wait of a pool on it,
/// 660 s when the pool's settings set none, plus 120 s; the check names
/// the lead, the wait, and the pool. A site that runs no pool has no wait
/// to cover.
#[test]
fn the_signal_lead_covers_each_pools_stop_wait() {
    assert_eq!(gdl_model::PoolSettings::DEFAULT_STOP_WAIT.as_secs(), 660);
    let short = format!("{SITE}signal_lead_s = 779\n");
    assert_eq!(
        problems(&on_site(&short, "")),
        [
            "[cluster.lab] signal_lead_s: is 779 s, below pool `global`'s stop wait of 660 s plus 120 s, 780 s: Slurm signals a job that long before its time limit, and the pool's hub drains the pool and waits up to its stop wait before it stops. Raise it to at least 780"
        ]
    );
    let enough = format!("{SITE}signal_lead_s = 780\n");
    fleet(&on_site(&enough, ""))
        .check(Some(&laid()))
        .expect("a lead of 780 s covers 660 s and 120 s");
    // No pool on the site: nothing to cover.
    let idle = format!(
        "{short}\n[pool.global]\nclass = \"global\"\nregion = \"r\"\n\n[pool.model-x]\nclass = \"model_x\"\nregion = \"r\"\nhome = \"global\"\n"
    );
    fleet(&idle)
        .check(Some(&laid()))
        .expect("a site with no pool");
}

/// `sbatch --signal` takes a lead of at most 65535 s: a longer one is
/// refused, naming the bound.
#[test]
fn a_signal_lead_fits_sbatch() {
    let site = |lead: u64| format!("{SITE}signal_lead_s = {lead}\ntime_limit = \"2-00:00:00\"\n");
    fleet(&on_site(&site(65_535), ""))
        .check(Some(&laid()))
        .expect("the longest lead sbatch takes");
    assert_eq!(
        problems(&on_site(&site(65_536), "")),
        [
            "[cluster.lab] signal_lead_s: is 65536 s, longer than 65535 s, the longest lead `sbatch --signal` takes"
        ]
    );
}

/// A time limit of any number of digits reads as Slurm reads it, leading
/// zeros included; one too large to count in seconds is refused.
#[test]
fn a_time_limit_takes_any_number_of_digits() {
    for good in ["0000000060", "000000000000000000001-0", "16:00000"] {
        let site = format!("{SITE}time_limit = \"{good}\"\n");
        fleet(&on_site(&site, ""))
            .check(Some(&laid()))
            .unwrap_or_else(|errors| panic!("{good}: {errors}"));
    }
    let huge = format!("{SITE}time_limit = \"99999999999999999999\"\n");
    let problems = problems(&on_site(&huge, ""));
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(
        problems[0].contains("is not a time limit in Slurm's format"),
        "{problems:?}"
    );
}

/// A time limit is at most 365 days, at the site and at a pool. Slurm
/// counts a limit in 32-bit integers, and wraps past them in more than one
/// step of its parser (`1073741825` minutes runs as 60 s, and
/// `35791394:07` as a negative limit); a year is far below every one of
/// them.
#[test]
fn a_time_limit_is_at_most_a_year() {
    assert_eq!(SlurmSite::MAX_TIME_LIMIT_S, 365 * 86_400);
    for good in ["365-0", "525600", "364-23:59:59", "0365-00:00:00"] {
        let site = format!("{SITE}time_limit = \"{good}\"\n");
        fleet(&on_site(&site, ""))
            .check(Some(&laid()))
            .unwrap_or_else(|errors| panic!("{good}: {errors}"));
    }
    for over in ["365-00:00:01", "525601", "1073741825", "35791394:07"] {
        let site = format!("{SITE}time_limit = \"{over}\"\n");
        let problems = problems(&on_site(&site, ""));
        assert_eq!(problems.len(), 1, "{over}: {problems:?}");
        assert!(
            problems[0].starts_with(&format!(
                "[cluster.lab] time_limit: `{over}` is not a time limit"
            )),
            "{problems:?}"
        );
        assert!(problems[0].contains("at most 365 days"), "{problems:?}");
        let pool = on_site(SITE, &format!("slurm = {{ time_limit = \"{over}\" }}\n"));
        let problems = super::problems(&pool);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].starts_with("[pool.model-x] slurm.time_limit:"),
            "{problems:?}"
        );
    }
}

/// How `cargo gdl` reaches a pool's hub on a Slurm site: directly, or
/// through `srun` inside the pool's job. Another value is refused by its
/// key.
#[test]
fn a_slurm_site_reaches_its_hubs_directly_or_through_srun() {
    for (value, reach) in [("direct", SlurmReach::Direct), ("srun", SlurmReach::Srun)] {
        let fleet = fleet(&on_site(&format!("{SITE}reach = \"{value}\"\n"), ""));
        fleet.check(Some(&laid())).expect("a sound fleet");
        let site = fleet
            .cluster("lab")
            .and_then(Cluster::slurm)
            .expect("a site");
        assert_eq!(site.reach, reach);
    }
    let problems =
        Fleet::from_toml(&on_site(&format!("{SITE}reach = \"ssh\"\n"), "")).expect_err("refused");
    assert_eq!(
        problems.0[0].table.as_deref(),
        Some("cluster.lab"),
        "{problems:?}"
    );
    assert_eq!(problems.0[0].key.as_deref(), Some("reach"), "{problems:?}");
}
