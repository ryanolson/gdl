//! A fleet's Slurm site under the etcd directory: the directory job runs
//! srt-slurm's typed `etcd` service with the backend's compaction and
//! quota, and writes etcd's address to the fleet's pointer, which each
//! pool's processes read when they start, as under a DynamoDB Local.

use super::*;

/// The api example's fleet on one Slurm site, under etcd.
const ETCD_FLEET: &str = "tests/fixtures/slurm/etcd/fleet.toml";

/// The pinned recipes and settings of [`ETCD_FLEET`]: what
/// `scripts/slurm-dry-run.sh` dry-runs.
const ETCD_PINNED: &str = "tests/fixtures/slurm/etcd/out";

fn etcd_text() -> String {
    std::fs::read_to_string(manifest_dir().join(ETCD_FLEET)).expect("the fleet")
}

#[test]
fn an_etcd_fleet_renders_as_pinned() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&etcd_text())).expect("the recipes");
    assert_pinned(&mirror, &files, ETCD_PINNED);
}

/// The directory job runs srt-slurm's own etcd, on the head node, with the
/// backend's flags as its `args`, and ends when etcd does. Then a step
/// writes etcd's address, to the job's own file and through it to the
/// fleet's pointer, as the DynamoDB job's tables step does.
#[test]
fn the_directory_job_runs_srt_slurms_etcd_and_writes_its_address() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&etcd_text())).expect("the recipes");
    let directory = recipe(&files, "lab/directory.yaml");
    assert_eq!(names(&directory), ["etcd", "pointer"]);
    let etcd = service(&directory, "etcd");
    assert_eq!(etcd["type"], "etcd");
    assert_eq!(etcd["placement"]["node"], "head");
    assert_eq!(etcd["terminal"], true);
    assert_eq!(
        words(&etcd["args"]),
        [
            "--auto-compaction-mode=periodic",
            "--auto-compaction-retention=1h",
            "--quota-backend-bytes=8589934592",
        ]
    );
    // srt-slurm's etcd builds its own command, from its own binary.
    assert!(etcd.get("command").is_none(), "{etcd}");

    let pointer = service(&directory, "pointer");
    assert_eq!(pointer["placement"]["node"], "head");
    assert_eq!(pointer["critical"], true);
    let command = words(&pointer["command"]);
    assert_eq!(command[..2], ["bash", "-c"]);
    let script = command[2];
    let scratch = tempfile::Builder::new()
        .prefix("gdl-slurm-etcd-")
        .tempdir()
        .expect("scratch");
    let run = script
        .replace("{node_ip}", "10.0.0.41")
        .replace("/fleet/", &format!("{}/", scratch.path().display()));
    let output = std::process::Command::new("bash")
        .args(["-c", &run])
        .env("SLURM_JOB_ID", "41")
        .output()
        .expect("bash");
    assert!(
        output.status.success(),
        "{run}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let read = |name: &str| std::fs::read_to_string(scratch.path().join(name)).ok();
    assert_eq!(
        read("directory.endpoint").as_deref(),
        Some("http://10.0.0.41:2379\n")
    );
    assert_eq!(
        read("directory.41.endpoint").as_deref(),
        Some("http://10.0.0.41:2379\n")
    );
    let teardown = words(&directory["host_setup"]["teardown"]);
    assert_eq!(
        teardown,
        ["rm -f '/shared/gdl-runs/api'/directory.$SLURM_JOB_ID.endpoint"]
    );
}

/// Each gdl process of a pool's job, and of a joining job, reads etcd's
/// address from the fleet's pointer when it starts, as the one member of
/// `GDL_POOL__ENDPOINTS`; with its variables, it reads as the pool's
/// settings under etcd. No recipe names etcd's address, so a new directory
/// job moves no pool's hash. A process with no address stops with one line
/// that names the file and etcd.
#[test]
fn a_jobs_processes_read_etcds_address_when_they_start() {
    let mirror = Mirror::new();
    let fleet = fleet(&etcd_text());
    let files = render(&mirror, &spec(&mirror), &fleet).expect("the recipes");
    let deployment: gdl_model::DeploymentSpec =
        serde_json::from_str(&files[Path::new("lab/model-x-01/deployment.json")])
            .expect("the hub's spec");
    let bin = tempfile::Builder::new()
        .prefix("gdl-slurm-etcd-address-")
        .tempdir()
        .expect("scratch");
    let mut read = 0;
    for path in [
        "lab/model-x-01/pool.yaml",
        "lab/model-x-01/join-engine.yaml",
    ] {
        let recipe = recipe(&files, path);
        for service in recipe["services"].as_array().expect("services") {
            let name = service["name"].as_str().expect("a name");
            if name == "spare" {
                continue;
            }
            let env = service["env"].as_object().expect("variables");
            assert_eq!(env["GDL_POOL__DIRECTORY"], "etcd", "{path}: {name}");
            for unsaid in [
                "GDL_POOL__ENDPOINTS",
                "GDL_POOL__ENDPOINT",
                "GDL_POOL__RECORDS_TABLE",
                "GDL_POOL__AWS_REGION",
            ] {
                assert!(env.get(unsaid).is_none(), "{path}: {name}: {unsaid}");
            }
            let own = service["preamble"].as_str().expect("a preamble");
            let (_held, preamble) = mounted(own, Some("http://10.0.0.7:2379"));
            let seen = exports(&preamble, &[("SLURM_JOB_ID", "42")], bin.path());
            assert_eq!(
                seen["GDL_POOL__ENDPOINTS"], "[\"http://10.0.0.7:2379\"]",
                "{path}: {name}"
            );
            let vars = env
                .iter()
                .filter(|(name, _)| name.starts_with("GDL_POOL__"))
                // srt-slurm fills in each node's address before the process starts.
                .map(|(name, value)| {
                    let value = value.as_str().expect("a string");
                    let value = value
                        .replace("{node_ip}", "10.0.0.8")
                        .replace("{head_ip}", "10.0.0.8");
                    (name.clone(), value)
                })
                .chain([(
                    "GDL_POOL__ENDPOINTS".to_string(),
                    seen["GDL_POOL__ENDPOINTS"].clone(),
                )])
                .map(|(name, value)| (name.into(), value.into()));
            let settings = gdl_hub::pool_from_vars(&deployment, vars)
                .unwrap_or_else(|error| panic!("{path}: {name}: {error}"))
                .expect("a pool");
            assert_eq!(
                settings.directory,
                gdl_model::DirectoryBackend::Etcd {
                    endpoints: vec!["http://10.0.0.7:2379".into()],
                    prefix: gdl_model::DirectoryBackend::ETCD_PREFIX.into(),
                },
                "{path}: {name}"
            );
            let (_held, preamble) = mounted(own, None);
            let output = std::process::Command::new("bash")
                .args(["-c", &format!("{preamble} && echo started")])
                .env("SLURM_JOB_ID", "42")
                .output()
                .expect("bash");
            let said = String::from_utf8_lossy(&output.stderr);
            assert!(!output.status.success(), "{path}: {name}");
            assert_eq!(said.lines().count(), 1, "{said}");
            assert!(
                said.contains("directory.endpoint") && said.contains("etcd"),
                "{said}"
            );
            read += 1;
        }
    }
    // A pool's hub, engine, and engine's launcher on its spare node, and a
    // joining job's resolve and engine.
    assert_eq!(read, 5);
}
