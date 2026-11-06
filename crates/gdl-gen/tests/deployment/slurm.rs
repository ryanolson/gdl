//! A fleet's Slurm sites as srt-slurm recipes: pinned file for file, read
//! for what srt-slurm does with them (the launch order, the nodes each
//! process runs on, and the ports it binds there), and refused, with the
//! reason named, where a site cannot run the fleet.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::k8s::assert_pinned;
use super::{Mirror, SPLIT, placed};

/// The api example's fleet on one Slurm site.
const FLEET: &str = "tests/fixtures/slurm/fleet.toml";

/// The pinned recipes and settings of [`FLEET`]: what
/// `scripts/slurm-dry-run.sh` dry-runs.
const PINNED: &str = "tests/fixtures/slurm/out";

/// The tag the pinned recipes' images are looked up by: the default tag
/// digests what each image is built from, which holds the test mirror's
/// URL and commit.
const TAG: &str = "pinned";

/// Each image the fleet runs, by the reference its digest is recorded
/// under, with a digest of its own.
const IMAGES: [&str; 3] = ["api-hub", "api-front", "api-engine"];

fn manifest_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

pub(super) fn fleet_text() -> String {
    std::fs::read_to_string(manifest_dir().join(FLEET)).expect("the fleet")
}

fn fleet(text: &str) -> gdl_model::Fleet {
    gdl_model::Fleet::from_toml(text).expect("a fleet")
}

/// The `[deployment]` tables of [`spec`]: a published port on the front,
/// and a drain bound on the engine.
const DEPLOYMENT: &str = "[deployment.segment.front]\nports = [\"8080\"]\n\
    [deployment.segment.engine.config.runtime]\ndrain_ms = 90000\n";

/// The api example in [`SPLIT`]'s two classes, with [`DEPLOYMENT`] and
/// `extra`.
fn spec_with(mirror: &Mirror, extra: &str) -> gdl_gen::Spec {
    placed(mirror, &format!("{SPLIT}{DEPLOYMENT}{extra}"))
}

pub(super) fn spec(mirror: &Mirror) -> gdl_gen::Spec {
    spec_with(mirror, "")
}

/// A digest for each of [`IMAGES`] in the site's registry, tagged [`TAG`].
fn digests() -> gdl_gen::Digests {
    let entries: Vec<String> = IMAGES
        .iter()
        .enumerate()
        .map(|(index, repository)| {
            format!(
                "\"registry.example/gdl/{repository}:{TAG}\": \"sha256:{:064x}\"",
                index + 1
            )
        })
        .collect();
    gdl_gen::Digests::from_json(&format!("{{{}}}", entries.join(", "))).expect("digests")
}

/// The recipes of `fleet` for `spec`, each image by its digest in
/// `digests`.
fn render_with(
    mirror: &Mirror,
    spec: &gdl_gen::Spec,
    fleet: &gdl_model::Fleet,
    digests: &gdl_gen::Digests,
) -> Result<gdl_gen::Files, gdl_gen::Error> {
    let target = mirror.target();
    gdl_gen::slurm_files(
        spec,
        fleet,
        &gdl_gen::SlurmRender {
            target: &target,
            tag: Some(TAG),
            digests,
            sources: gdl_gen::Sources::Fetched,
        },
    )
}

fn render(
    mirror: &Mirror,
    spec: &gdl_gen::Spec,
    fleet: &gdl_model::Fleet,
) -> Result<gdl_gen::Files, gdl_gen::Error> {
    render_with(mirror, spec, fleet, &digests())
}

/// A recipe the render wrote, read: each is one JSON object, which YAML
/// reads as it is.
fn recipe(files: &gdl_gen::Files, path: &str) -> Value {
    let text = files
        .get(Path::new(path))
        .unwrap_or_else(|| panic!("{path} in {:?}", files.keys().collect::<Vec<_>>()));
    serde_json::from_str(text).expect("a JSON object")
}

/// Every recipe of `files`, by its path.
fn recipes(files: &gdl_gen::Files) -> BTreeMap<PathBuf, Value> {
    files
        .iter()
        .filter(|(path, _)| path.extension().is_some_and(|e| e == "yaml"))
        .map(|(path, text)| {
            (
                path.clone(),
                serde_json::from_str(text).expect("a JSON object"),
            )
        })
        .collect()
}

/// `service` of `recipe`, by its name.
fn service<'r>(recipe: &'r Value, name: &str) -> &'r Value {
    recipe["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["name"] == name)
        .unwrap_or_else(|| panic!("service {name} in {recipe}"))
}

fn names(recipe: &Value) -> Vec<&str> {
    recipe["services"]
        .as_array()
        .expect("services")
        .iter()
        .map(|service| service["name"].as_str().expect("a name"))
        .collect()
}

fn words(value: &Value) -> Vec<&str> {
    value
        .as_array()
        .expect("a list")
        .iter()
        .map(|word| word.as_str().expect("a word"))
        .collect()
}

#[test]
fn the_api_fleet_renders_as_pinned() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    assert_pinned(&mirror, &files, PINNED);
}

/// A pool is one services-only job: its hub on the head node, which ends
/// the job when it exits, and each segment on nodes of its own, one
/// instance per node, ready once it is live; then the spare nodes, held by
/// a placeholder for segments that join inside the job.
#[test]
fn a_pool_is_one_job_with_its_hub_on_the_head_node() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let pool = recipe(&files, "lab/model-x-01/pool.yaml");
    assert_eq!(pool["name"], "gdl-api-model-x-01");
    assert_eq!(pool["frontend"]["type"], "none");
    // srt-slurm's preflight and its runtime refuse a model path that is
    // not a directory.
    assert_eq!(pool["model"]["path"], "/shared/gdl-runs/api");
    assert_eq!(
        words(&pool["extra_mount"]),
        [
            "/shared/gdl-runs/api/model-x-01:/gdl",
            "/shared/gdl-runs/api:/fleet:ro"
        ]
    );
    assert_eq!(pool["slurm"]["partition"], "gpu");
    assert_eq!(pool["slurm"]["time_limit"], "08:00:00");
    assert_eq!(names(&pool), ["hub", "engine", "spare"]);

    let hub = service(&pool, "hub");
    assert_eq!(hub["placement"]["node"], "head");
    assert_eq!(hub["critical"], true);
    assert_eq!(hub["terminal"], true);
    assert_eq!(
        hub["readiness"]["http"],
        serde_json::json!({"port": 1337, "path": "/v1/health", "status": 200})
    );
    assert_eq!(
        words(&hub["command"]),
        ["/usr/local/bin/segment", "/gdl/deployment.json"]
    );
    assert_eq!(hub["env"]["GDL_POOL__HUB"], "{node_ip}:1337");
    assert_eq!(hub["env"]["GDL_POOL__DRAIN_ON_STOP"], "true");
    assert_eq!(hub["env"]["GDL_POOL__NAME"], "model-x-01");
    assert_eq!(hub["env"]["GDL_POOL__PARENTS"], "global");
    assert_eq!(hub["env"]["GDL_POOL__REGION"], "slurm-lab");
    assert_eq!(hub["env"]["GDL_POOL__DIRECTORY"], "dynamodb");
    assert!(hub["env"].get("GDL_LISTEN").is_none(), "{hub}");

    // srt-slurm starts one instance at a time, each once the one before
    // is ready: an instance answers `/health` only once every segment of
    // its class has instances, so a class of two segments would wait on
    // `/health` forever.
    let engine = service(&pool, "engine");
    assert_eq!(engine["nodes"], 2);
    assert!(engine.get("critical").is_none(), "{engine}");
    assert!(engine.get("terminal").is_none(), "{engine}");
    assert_eq!(
        engine["readiness"]["http"],
        serde_json::json!({"port": 9090, "path": "/live", "status": 200})
    );
    assert_eq!(
        words(&engine["command"]),
        ["/usr/local/bin/segment", "/gdl/engine.toml"]
    );
    assert_eq!(engine["env"]["GDL_HUB__HOST"], "{head_ip}");
    assert_eq!(engine["env"]["GDL_LISTEN"], "{node_ip}:0");
    assert_eq!(engine["env"]["GDL_POOL__READY__ENGINE"], "2");
    assert!(
        engine["env"].get("GDL_POOL__DRAIN_ON_STOP").is_none(),
        "{engine}"
    );

    let spare = service(&pool, "spare");
    assert_eq!(spare["nodes"], 1);
    assert_eq!(words(&spare["command"]), ["sleep", "infinity"]);

    // Each image by its digest, as Pyxis names a registry's: the registry,
    // then `#`.
    let digest = |index: usize| format!("sha256:{index:064x}");
    assert_eq!(
        hub["container"],
        format!("registry.example#gdl/api-hub@{}", digest(1))
    );
    assert_eq!(
        engine["container"],
        format!("registry.example#gdl/api-engine@{}", digest(3))
    );
    assert_eq!(pool["model"]["container"], hub["container"]);

    let directives = pool["sbatch_directives"].as_object().expect("directives");
    assert_eq!(directives["exclusive"], "");
    assert_eq!(directives["no-requeue"], "");
    assert_eq!(directives["no-kill"], "");
    assert_eq!(directives["signal"], "TERM@900");
    let comment = directives["comment"].as_str().expect("a comment");
    let hash = comment
        .strip_prefix("gdl:api:model-x-01:pool:")
        .unwrap_or_else(|| panic!("{comment}"));
    assert!(
        hash.len() == 16 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
        "{comment}"
    );

    // The pool's own time limit, and no partition but srtslurm.yaml's.
    let shorter = recipe(&files, "lab/model-x-02/pool.yaml");
    assert_eq!(shorter["slurm"]["time_limit"], "04:00:00");
    assert!(shorter["slurm"].get("partition").is_none(), "{shorter}");
    assert_eq!(names(&shorter), ["hub", "engine"]);
    assert_eq!(service(&shorter, "engine")["nodes"], 1);
}

/// Every job is exclusive and never requeued: a requeued job keeps its id,
/// so an old endpoint file would look fresh, and a requeued hub would
/// restart with no registrations.
#[test]
fn every_job_is_exclusive_and_never_requeued() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let recipes = recipes(&files);
    assert_eq!(
        recipes.keys().collect::<Vec<_>>(),
        [
            "lab/directory.yaml",
            "lab/global-us/join-front.yaml",
            "lab/global-us/pool.yaml",
            "lab/model-x-01/join-engine.yaml",
            "lab/model-x-01/pool.yaml",
            "lab/model-x-02/join-engine.yaml",
            "lab/model-x-02/pool.yaml",
        ]
        .map(PathBuf::from)
        .iter()
        .collect::<Vec<_>>()
    );
    for (path, recipe) in &recipes {
        let directives = &recipe["sbatch_directives"];
        assert_eq!(directives["exclusive"], "", "{}", path.display());
        assert_eq!(directives["no-requeue"], "", "{}", path.display());
        assert_eq!(recipe["schema"], 2, "{}", path.display());
    }
}

/// The directory job runs DynamoDB Local, pinned by digest, and then the
/// tables step: it removes this job's old endpoint file, makes the tables,
/// and only then writes the endpoint, to a file beside it renamed into
/// place.
#[test]
fn the_directory_job_writes_its_endpoint_once_the_tables_exist() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let directory = recipe(&files, "lab/directory.yaml");
    assert_eq!(directory["name"], "gdl-api-directory");
    assert_eq!(names(&directory), ["dynamodb", "tables"]);
    assert_eq!(
        words(&directory["extra_mount"]),
        ["/shared/gdl-runs/api:/fleet"]
    );
    let local = service(&directory, "dynamodb");
    let image = local["container"].as_str().expect("an image");
    assert!(
        image.starts_with("amazon/dynamodb-local@sha256:"),
        "{image}"
    );
    assert_eq!(local["readiness"]["tcp"]["port"], 8000);
    assert_eq!(local["critical"], true);
    assert_eq!(local["terminal"], true);
    // The pools withdraw their records from the directory as they drain,
    // on the signal before their time limits: the directory takes none.
    assert!(
        directory["sbatch_directives"].get("signal").is_none(),
        "{directory}"
    );

    let tables = service(&directory, "tables");
    let command = words(&tables["command"]);
    assert_eq!(command[..2], ["bash", "-c"]);
    let script = command[2];
    let at = |text: &str| {
        script
            .find(text)
            .unwrap_or_else(|| panic!("{text} in {script}"))
    };
    let removed = at("rm -f /fleet/directory.$SLURM_JOB_ID.endpoint");
    let made = at("/usr/local/bin/segment --tables");
    let written = at("> /fleet/directory.$SLURM_JOB_ID.tmp");
    let renamed =
        at("mv /fleet/directory.$SLURM_JOB_ID.tmp /fleet/directory.$SLURM_JOB_ID.endpoint");
    assert!(
        removed < made && made < written && written < renamed,
        "{script}"
    );
    assert!(script.contains("http://{node_ip}:8000"), "{script}");
    assert_eq!(tables["env"]["GDL_POOL__ENDPOINT"], "http://{node_ip}:8000");
    assert_eq!(tables["env"]["GDL_POOL__RECORDS_TABLE"], "gdl-pools");
    assert_eq!(tables["placement"]["node"], "head");
    // Then the fleet's pointer, which the pools' processes read, after the
    // job's own file: one directory job after another, each pointer names
    // the latest job's Local; a job's end removes its own file alone, so a
    // pointer to it dangles, and a later job's is never touched.
    let pointed = at("/fleet/directory.endpoint");
    assert!(renamed < pointed, "{script}");
    let teardown = words(&directory["host_setup"]["teardown"]).join(" && ");
    let scratch = tempfile::Builder::new()
        .prefix("gdl-slurm-pointer-")
        .tempdir()
        .expect("scratch");
    let fleet_dir = scratch.path().join("api");
    std::fs::create_dir_all(&fleet_dir).expect("the fleet's directory");
    // A `cat` that, the first time it reads job 41's own file, runs the
    // racer first, as job 43, and marks that it did.
    let bin = scratch.path().join("bin");
    std::fs::create_dir_all(&bin).expect("a bin");
    let real = String::from_utf8(
        std::process::Command::new("bash")
            .args(["-c", "command -v cat"])
            .output()
            .expect("bash")
            .stdout,
    )
    .expect("a path");
    let raced = scratch.path().join("raced");
    std::fs::write(
        bin.join("cat"),
        format!(
            "#!/bin/bash\nif [ -n \"$GDL_RACER\" ] && [ ! -e {raced:?} ]; then case \"$*\" in *directory.41.endpoint*) touch {raced:?}; SLURM_JOB_ID=43 PATH={path:?} bash \"$GDL_RACER\" ;; esac; fi\nexec {real} \"$@\"\n",
            raced = raced.display().to_string(),
            path = std::env::var("PATH").expect("a PATH"),
            real = real.trim(),
        ),
    )
    .expect("a fake cat");
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(bin.join("cat"), std::fs::Permissions::from_mode(0o755))
        .expect("mode");
    let shell = |text: &str, job: &str, node: &str, racer: Option<&Path>| {
        let text = text
            .replace("/usr/local/bin/segment --tables", "true")
            .replace("{node_ip}", node)
            .replace("/fleet/", &format!("{}/", fleet_dir.display()))
            .replace(
                "'/shared/gdl-runs/api'",
                &format!("'{}'", fleet_dir.display()),
            );
        let path = format!(
            "{}:{}",
            bin.display(),
            std::env::var("PATH").expect("a PATH")
        );
        let output = std::process::Command::new("bash")
            .args(["-c", &text])
            .env("SLURM_JOB_ID", job)
            .env("PATH", path)
            .envs(racer.map(|racer| ("GDL_RACER", racer)))
            .output()
            .expect("bash");
        assert!(
            output.status.success(),
            "{text}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    let pointer = || std::fs::read_to_string(fleet_dir.join("directory.endpoint")).ok();
    shell(script, "41", "10.0.0.41", None);
    assert_eq!(pointer().as_deref(), Some("http://10.0.0.41:8000\n"));
    shell(script, "42", "10.0.0.42", None);
    shell(&teardown, "41", "10.0.0.41", None);
    assert_eq!(pointer().as_deref(), Some("http://10.0.0.42:8000\n"));
    shell(&teardown, "42", "10.0.0.42", None);
    assert_eq!(pointer(), None);

    // Job 43 publishes while job 41's teardown runs, at the moment it reads
    // anything of its own: a `cat` that publishes 43 first stands in.
    shell(script, "41", "10.0.0.41", None);
    let racer = scratch.path().join("racer");
    std::fs::write(
        &racer,
        script
            .replace("/usr/local/bin/segment --tables", "true")
            .replace("{node_ip}", "10.0.0.43")
            .replace("/fleet/", &format!("{}/", fleet_dir.display())),
    )
    .expect("the racer");
    shell(&teardown, "41", "10.0.0.41", Some(&racer));
    if !scratch.path().join("raced").exists() {
        shell(script, "43", "10.0.0.43", None);
    }
    assert_eq!(pointer().as_deref(), Some("http://10.0.0.43:8000\n"));
    assert!(words(&directory["host_setup"]["commands"]).is_empty());
}

/// A scratch directory standing in for `/gdl` and `/fleet` in a
/// container, with the fleet's pointer to the DynamoDB Local holding
/// `address` when there is one; and `preamble` with its paths there.
fn mounted(preamble: &str, address: Option<&str>) -> (tempfile::TempDir, String) {
    let scratch = tempfile::Builder::new()
        .prefix("gdl-slurm-mounts-")
        .tempdir()
        .expect("scratch");
    for dir in ["gdl", "fleet"] {
        std::fs::create_dir_all(scratch.path().join(dir)).expect("a mount");
    }
    if let Some(address) = address {
        std::fs::write(
            scratch.path().join("fleet/directory.endpoint"),
            format!("{address}\n"),
        )
        .expect("the pointer");
    }
    let root = scratch.path().display().to_string();
    let preamble = preamble
        .replace("/gdl/", &format!("{root}/gdl/"))
        .replace("/fleet/", &format!("{root}/fleet/"));
    (scratch, preamble)
}

/// The recipes name no DynamoDB Local's address: each gdl process reads
/// the one the fleet's directory job wrote, when it starts, so a new
/// directory job moves no pool's hash. A process with no address, or one
/// that is not `http://<host>:<port>`, stops with one line that names the
/// file.
#[test]
fn a_jobs_processes_read_the_directorys_address_when_they_start() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let bin = tempfile::Builder::new()
        .prefix("gdl-slurm-address-")
        .tempdir()
        .expect("scratch");
    let mut read = 0;
    for path in [
        "lab/model-x-01/pool.yaml",
        "lab/model-x-01/join-engine.yaml",
    ] {
        let recipe = recipe(&files, path);
        assert!(
            words(&recipe["extra_mount"]).contains(&"/shared/gdl-runs/api:/fleet:ro"),
            "{recipe}"
        );
        for service in recipe["services"].as_array().expect("services") {
            let name = service["name"].as_str().expect("a name");
            assert!(
                service["env"].get("GDL_POOL__ENDPOINT").is_none(),
                "{path}: {name}"
            );
            if name == "spare" {
                continue;
            }
            let own = service["preamble"].as_str().expect("a preamble");
            let (_held, preamble) = mounted(own, Some("http://10.0.0.7:8000"));
            let seen = exports(&preamble, &[("SLURM_JOB_ID", "42")], bin.path());
            assert_eq!(
                seen["GDL_POOL__ENDPOINT"], "http://10.0.0.7:8000",
                "{path}: {name}"
            );
            // No end time written: the process still starts, with none.
            let strict = format!("set -euo pipefail && {preamble}");
            let seen = exports(&strict, &[("SLURM_JOB_ID", "42")], bin.path());
            assert!(
                !seen.keys().any(|name| name.ends_with("LEAVE_BY")),
                "{seen:?}"
            );
            // As srt-slurm runs it, and after a cluster's preamble of
            // `set -euo pipefail`, which a read that fails must not end
            // before it says why.
            let strict = ["", "set -euo pipefail && "];
            for (address, cluster) in [None, Some("nowhere")]
                .into_iter()
                .flat_map(|address| strict.map(|cluster| (address, cluster)))
            {
                let (_held, preamble) = mounted(own, address);
                let output = std::process::Command::new("bash")
                    .args(["-c", &format!("{cluster}{preamble} && echo started")])
                    .env("SLURM_JOB_ID", "42")
                    .output()
                    .expect("bash");
                let said = String::from_utf8_lossy(&output.stderr);
                assert!(!output.status.success(), "{path}: {name}: {address:?}");
                assert!(!String::from_utf8_lossy(&output.stdout).contains("started"));
                assert_eq!(said.lines().count(), 1, "{said}");
                assert!(said.contains("directory.endpoint"), "{said}");
            }
            read += 1;
        }
    }
    // A pool's hub and engine, and a joining job's resolve and engine.
    assert_eq!(read, 4);

    // The hash covers what a pool runs, not where its directory landed.
    let again = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    assert_eq!(files, again);
}

/// A segment joins a running pool as a job of its own: `resolve` reads
/// where the pool's hub is from the directory when the job starts, and the
/// segment takes that file over its settings, so that it names no hub of
/// its own.
#[test]
fn a_join_recipe_resolves_its_pools_hub_when_it_starts() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let join = recipe(&files, "lab/model-x-01/join-engine.yaml");
    assert_eq!(join["name"], "gdl-api-model-x-01-join-engine");
    assert_eq!(names(&join), ["resolve", "engine"]);
    let resolve = service(&join, "resolve");
    // srt-slurm probes the file, then asks whether the process lives: a
    // `resolve` that exited in between would fail the job, so it waits
    // once it has written.
    assert_eq!(
        words(&resolve["command"]),
        [
            "bash",
            "-c",
            "/usr/local/bin/segment --resolve model_x/model-x-01 --write /logs/gdl-hub.toml && exec sleep infinity"
        ]
    );
    assert!(resolve.get("critical").is_none(), "{resolve}");
    assert_eq!(resolve["placement"]["node"], "head");
    assert_eq!(resolve["readiness"]["file"]["path"], "gdl-hub.toml");
    let engine = service(&join, "engine");
    assert_eq!(
        words(&engine["command"]),
        [
            "/usr/local/bin/segment",
            "/gdl/engine.toml",
            "/logs/gdl-hub.toml"
        ]
    );
    assert_eq!(engine["terminal"], true);
    assert_eq!(engine["nodes"], 1);
    assert_eq!(engine["env"]["GDL_LISTEN"], "{node_ip}:0");
    // A variable overrides a settings file: one naming the hub would win
    // over the resolved file.
    for name in ["GDL_HUB__HOST", "GDL_POOL__HUB"] {
        assert!(engine["env"].get(name).is_none(), "{name}: {engine}");
    }
    assert_eq!(
        words(&join["extra_mount"]),
        [
            "/shared/gdl-runs/api/model-x-01:/gdl",
            "/shared/gdl-runs/api:/fleet:ro"
        ]
    );
    let directives = &join["sbatch_directives"];
    assert_eq!(directives["signal"], "TERM@900");
    assert!(directives.get("no-kill").is_none(), "{directives}");
    let comment = directives["comment"].as_str().expect("a comment");
    assert!(comment.starts_with("gdl:api:model-x-01:join:"), "{comment}");
}

/// The nodes of a pool's job each process runs on, as srt-slurm places
/// them: a service that owns nodes takes the next ones, in the order
/// declared, and one placed on the head runs on the first.
fn placed_nodes(recipe: &Value) -> Vec<(String, Vec<usize>)> {
    let mut next = 0;
    let mut placed = Vec::new();
    for service in recipe["services"].as_array().expect("services") {
        let name = service["name"].as_str().expect("a name").to_string();
        match service["nodes"].as_u64() {
            Some(count) => {
                let count = usize::try_from(count).expect("a count");
                placed.push((name, (next..next + count).collect()));
                next += count;
            }
            None => {
                assert_eq!(service["placement"]["node"], "head", "{service}");
                placed.push((name, vec![0]));
            }
        }
    }
    placed
}

/// The ports a process of `service` binds on its node: the hub's two, a
/// segment's system port from its settings, its published ports, and the
/// port of `GDL_LISTEN` unless the OS chooses it.
fn listeners(
    files: &gdl_gen::Files,
    dir: &Path,
    service: &Value,
    published: &BTreeMap<&str, Vec<u16>>,
) -> Vec<u16> {
    let name = service["name"].as_str().expect("a name");
    let command = words(&service["command"]);
    if command == ["/usr/local/bin/segment", "/gdl/deployment.json"] {
        return vec![gdl_model::HUB_PUBLIC_PORT, gdl_model::HUB_CONTROL_PORT];
    }
    if command[0] != "/usr/local/bin/segment" {
        return Vec::new();
    }
    let settings = command[1].strip_prefix("/gdl/").expect("a settings file");
    let table: toml::Table = toml::from_str(&files[&dir.join(settings)]).expect("TOML");
    let mut ports =
        vec![u16::try_from(table["system"]["port"].as_integer().expect("a port")).expect("a port")];
    let listen = service["env"]["GDL_LISTEN"].as_str().expect("GDL_LISTEN");
    let port: u16 = listen
        .rsplit_once(':')
        .expect("a port")
        .1
        .parse()
        .expect("a port");
    if port != 0 {
        ports.push(port);
    }
    ports.extend(published.get(name).into_iter().flatten());
    ports
}

/// Every job takes its nodes whole and keeps the default ports, so the
/// processes one job places on a node must not share a port: the hub and
/// the first segment's first instance on the head node, and each other
/// instance alone.
#[test]
fn every_listener_on_a_node_has_a_port_of_its_own() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let published = BTreeMap::from([("front", vec![8080])]);
    let mut checked = 0;
    for (path, recipe) in recipes(&files) {
        let dir = path.parent().expect("a directory");
        let services = recipe["services"].as_array().expect("services");
        let mut on: BTreeMap<usize, Vec<(String, u16)>> = BTreeMap::new();
        for ((name, nodes), service) in placed_nodes(&recipe).into_iter().zip(services) {
            for node in nodes {
                for port in listeners(&files, dir, service, &published) {
                    on.entry(node).or_default().push((name.clone(), port));
                }
            }
        }
        for (node, ports) in &on {
            let mut seen = BTreeMap::new();
            for (name, port) in ports {
                if let Some(other) = seen.insert(*port, name) {
                    panic!(
                        "{}: node {node}: {name} and {other} both bind {port}",
                        path.display()
                    );
                }
                checked += 1;
            }
        }
    }
    // The hub's two ports and the front's two on the head node of
    // `global-us`, each pool's engines, and the joiners.
    assert!(checked >= 12, "{checked}");

    // A segment that would bind the hub's port beside it is refused.
    let clash = spec_with(
        &mirror,
        "[deployment.segment.front.config.system]\nport = 1337\n",
    );
    let refused = render(&mirror, &clash, &fleet(&fleet_text()))
        .expect_err("refused")
        .to_string();
    for named in ["global-us", "front", "1337", "hub"] {
        assert!(refused.contains(named), "{named} in {refused}");
    }
}

/// The comment's hash covers what a pool runs: the same render gives the
/// same hash, and a change to a pool's settings changes that pool's alone.
#[test]
fn a_pools_recipe_hash_moves_with_what_it_runs() {
    let mirror = Mirror::new();
    let comment = |files: &gdl_gen::Files, pool: &str| {
        recipe(files, &format!("lab/{pool}/pool.yaml"))["sbatch_directives"]["comment"]
            .as_str()
            .expect("a comment")
            .to_string()
    };
    let first = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let again = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let changed = render(
        &mirror,
        &spec_with(
            &mirror,
            "[deployment.segment.engine.config]\nrendezvous_ms = 90000\n",
        ),
        &fleet(&fleet_text()),
    )
    .expect("the recipes");
    for pool in ["global-us", "model-x-01", "model-x-02"] {
        assert_eq!(comment(&first, pool), comment(&again, pool));
    }
    assert_eq!(comment(&first, "global-us"), comment(&changed, "global-us"));
    for pool in ["model-x-01", "model-x-02"] {
        assert_ne!(comment(&first, pool), comment(&changed, pool), "{pool}");
    }
}

/// What a Slurm site cannot run is refused before anything is written,
/// each problem named.
#[test]
fn what_a_site_cannot_run_is_refused() {
    let mirror = Mirror::new();
    let text = fleet_text();
    let refusal = |spec: &gdl_gen::Spec, fleet_text: &str, digests: &gdl_gen::Digests| {
        render_with(&mirror, spec, &fleet(fleet_text), digests)
            .expect_err("refused")
            .to_string()
    };
    // An image the digests file does not hold.
    let refused = refusal(
        &spec(&mirror),
        &text,
        &gdl_gen::Digests::from_json("{}").expect("digests"),
    );
    assert!(refused.contains("has no digest"), "{refused}");

    // The relay directory, whose child hubs need their parent's address
    // before it runs.
    let start = text.find("[directory]").expect("a directory");
    let end = text.find("[class.global]").expect("a class");
    let relay = format!("{}{}", &text[..start], &text[end..]);
    let refused = refusal(&spec(&mirror), &relay, &digests());
    for named in ["[directory]", "DynamoDB"] {
        assert!(refused.contains(named), "{named} in {refused}");
    }

    // DynamoDB on AWS, which a Slurm job has no role for.
    let aws = text.replace("local = true\ncluster = \"lab\"\n", "");
    let refused = refusal(&spec(&mirror), &aws, &digests());
    assert!(refused.contains("local = true"), "{refused}");

    // A secret, which no Slurm job is given yet.
    let secret = manifest_dir().join("tests/fixtures/k8s/secret");
    let with_secret = spec_with(
        &mirror,
        &format!(
            "[deployment.secrets.fingerprint]\nfile = {:?}\n[deployment.segment.engine]\nsecrets = [\"fingerprint\"]\n",
            secret.display().to_string()
        ),
    );
    let refused = refusal(&with_secret, &text, &digests());
    for named in ["engine", "fingerprint", "secret"] {
        assert!(refused.contains(named), "{named} in {refused}");
    }

    // A run directory srt-slurm cannot mount.
    let refused = refusal(
        &spec(&mirror),
        &text.replace("/shared/gdl-runs", "/shared/gdl:runs"),
        &digests(),
    );
    for named in ["run_dir", "`:`"] {
        assert!(refused.contains(named), "{named} in {refused}");
    }

    // A DynamoDB Local that another cluster runs: a Slurm job reads its
    // address from the site's run directory, where no job writes it.
    let elsewhere = text.replace(
        "local = true\ncluster = \"lab\"\n",
        "local = true\ncluster = \"use1\"\n",
    ) + "\n[cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-api\"\nregion = \"us-east-1\"\nregistry = \"registry.example/gdl\"\nplatform = \"linux/arm64\"\n";
    let refused = refusal(&spec(&mirror), &elsewhere, &digests());
    for named in ["[directory] cluster", "use1", "lab"] {
        assert!(refused.contains(named), "{named} in {refused}");
    }

    // A fleet with no Slurm site renders nothing.
    let refused = refusal(&spec(&mirror), &super::k8s::fleet_text(), &digests());
    assert!(refused.contains("no Slurm site"), "{refused}");
}

/// A segment whose name a recipe's own service takes is refused.
#[test]
fn a_segment_named_for_a_recipes_own_service_is_refused() {
    let mirror = Mirror::new();
    let placement = SPLIT
        .replace("segment.engine", "segment.spare")
        .replace("segments = [\"engine\"]", "segments = [\"spare\"]");
    let spec = placed(&mirror, &placement);
    let text = fleet_text().replace("engine = ", "spare = ");
    let refused = render_with(&mirror, &spec, &fleet(&text), &digests())
        .expect_err("refused")
        .to_string();
    assert!(refused.contains("`spare`"), "{refused}");
}

/// Each service's environment as srt-slurm's bash wrapper sets it: one
/// `export` each, joined with `&&`. Ok when bash takes every one.
fn exported(service: &Value) -> Result<(), String> {
    let env = service["env"].as_object().expect("an environment");
    let mut exports: Vec<String> = env
        .iter()
        .map(|(name, value)| {
            let value = value.as_str().expect("a string").replace('\'', "'\"'\"'");
            format!("export {name}='{value}'")
        })
        .collect();
    exports.push("exec true".into());
    let output = std::process::Command::new("bash")
        .args(["-c", &exports.join(" && ")])
        .output()
        .expect("bash");
    match output.status.success() {
        true => Ok(()),
        false => Err(String::from_utf8_lossy(&output.stderr).into_owned()),
    }
}

/// srt-slurm exports each variable in bash, which takes only a name of
/// letters, digits, and `_`: a segment whose name holds `-` has its pool's
/// `ready` in one variable, a table that reads back whole.
#[test]
fn every_variable_is_one_bash_can_export() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    for (path, recipe) in recipes(&files) {
        for service in recipe["services"].as_array().expect("services") {
            if service.get("env").is_some() {
                exported(service).unwrap_or_else(|why| panic!("{}: {why}", path.display()));
            }
        }
    }

    let placement = format!("{SPLIT}{DEPLOYMENT}").replace("engine", "engine-a");
    let spec = placed(&mirror, &placement);
    let fleet = fleet(&fleet_text().replace("engine = ", "engine-a = "));
    let digests =
        gdl_gen::Digests::from_json(&digests().to_json().replace("api-engine", "api-engine-a"))
            .expect("digests");
    let files = render_with(&mirror, &spec, &fleet, &digests).expect("the recipes");
    let pool = recipe(&files, "lab/model-x-01/pool.yaml");
    for name in ["hub", "engine-a"] {
        exported(service(&pool, name)).unwrap_or_else(|why| panic!("{name}: {why}"));
    }
    assert_eq!(
        service(&pool, "hub")["env"]["GDL_POOL__READY"],
        "{\"engine-a\"=2}"
    );
}

/// Pyxis takes a registry's host before `#`, and an image with none from
/// Docker Hub: a registry that is a Docker Hub namespace has no host.
#[test]
fn an_image_on_docker_hub_names_no_host() {
    let mirror = Mirror::new();
    let pulled = |registry: &str| -> String {
        let text = fleet_text().replace("registry.example/gdl", registry);
        let named = gdl_gen::Digests::from_json(
            &digests()
                .to_json()
                .replace("registry.example/gdl", registry),
        )
        .expect("digests");
        let files =
            render_with(&mirror, &spec(&mirror), &fleet(&text), &named).expect("the recipes");
        let pool = recipe(&files, "lab/model-x-01/pool.yaml");
        let container = service(&pool, "hub")["container"]
            .as_str()
            .expect("an image");
        container.split_once('@').expect("a digest").0.to_string()
    };
    for (registry, expected) in [
        ("myteam", "myteam/api-hub"),
        ("localhost:5000/gdl", "localhost:5000#gdl/api-hub"),
        ("localhost", "localhost#api-hub"),
        ("registry.example", "registry.example#api-hub"),
        ("registry.example/gdl", "registry.example#gdl/api-hub"),
        // Docker reads a first part with a capital as a host, and
        // `docker.io` as Docker Hub, which Pyxis takes with no host.
        ("Registry/gdl", "Registry#gdl/api-hub"),
        ("docker.io/myteam", "myteam/api-hub"),
        ("index.docker.io/myteam", "myteam/api-hub"),
    ] {
        assert_eq!(pulled(registry), expected, "{registry}");
    }
}

/// A Kubernetes pool's DGD file is the Kubernetes render's: the Slurm
/// render of the same fleet takes none, and refuses nothing for it.
#[test]
fn a_kubernetes_pools_dgd_does_not_stop_the_slurm_render() {
    let mirror = Mirror::new();
    let fixture = manifest_dir().join("tests/fixtures/k8s/dgd/dynamo/dgd.yaml");
    let text = fleet_text()
        + &format!(
            "\n[cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-api\"\nregion = \"us-east-1\"\nregistry = \"registry.example/gdl\"\nplatform = \"linux/arm64\"\noperator = true\n\n[pool.dgd-use1]\nclass = \"model_x\"\ncluster = \"use1\"\nparents = \"global\"\n\n[pool.dgd-use1.dgd]\nfile = {:?}\ncomponents = {{ engine = \"Frontend\" }}\n",
            fixture.display().to_string()
        );
    let fleet = fleet(&text);
    fleet.check(None).expect("a fleet");
    let files = render(&mirror, &spec(&mirror), &fleet).expect("the recipes");
    assert!(files.contains_key(Path::new("lab/model-x-01/pool.yaml")));
    assert!(!files.keys().any(|path| path.starts_with("use1")));
}

/// A join job reads its own segment's settings alone: a change to another
/// segment of its class leaves its hash as it was, and moves the pool's.
#[test]
fn a_join_jobs_hash_moves_with_its_own_segment_alone() {
    let mirror = Mirror::new();
    let placement = format!(
        "{}[class.global]\nsegments = [\"front\", \"engine\"]\n{DEPLOYMENT}",
        super::SEGMENTS
    );
    let text = fleet_text();
    let prefix = &text[..text.find("[class.global]").expect("the classes")];
    let fleet = fleet(&format!(
        "{prefix}[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n"
    ));
    let comment = |files: &gdl_gen::Files, path: &str| {
        recipe(files, path)["sbatch_directives"]["comment"].clone()
    };
    let first = render(&mirror, &placed(&mirror, &placement), &fleet).expect("the recipes");
    let changed = render(
        &mirror,
        &placed(
            &mirror,
            &format!("{placement}[deployment.segment.front.config.runtime]\ndrain_ms = 12345\n"),
        ),
        &fleet,
    )
    .expect("the recipes");
    let join = "lab/global-us/join-engine.yaml";
    assert_eq!(comment(&first, join), comment(&changed, join));
    let pool = "lab/global-us/pool.yaml";
    assert_ne!(comment(&first, pool), comment(&changed, pool));
}

/// What `script` exports, run by bash in `dir` with `env` and `PATH`
/// first holding `bin`: each `GDL_` variable and its value.
fn exports(script: &str, env: &[(&str, &str)], bin: &Path) -> BTreeMap<String, String> {
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").expect("a PATH")
    );
    let output = std::process::Command::new("bash")
        .args(["-c", &format!("{script} && env")])
        .env_clear()
        .env("PATH", path)
        .env("TZ", "UTC")
        .envs(env.iter().copied())
        .output()
        .expect("bash");
    assert!(
        output.status.success(),
        "{script}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('='))
        .filter(|(name, _)| name.starts_with("GDL_"))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

/// A job's processes leave by the job's end time: a step on each node's
/// host writes it to the pool's directory before any service starts,
/// from `SLURM_JOB_END_TIME` where the cluster sets it, else from
/// `squeue`; each segment instance reads it as `[runtime] leave_by`, and
/// the hub as `[pool] leave_by`. With no end time, nothing is set.
#[test]
fn a_jobs_processes_leave_by_its_end_time() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let pool = recipe(&files, "lab/model-x-01/pool.yaml");
    let join = recipe(&files, "lab/model-x-01/join-engine.yaml");
    let directory = recipe(&files, "lab/directory.yaml");
    assert!(
        words(&directory["host_setup"]["commands"]).is_empty(),
        "{directory}"
    );
    assert!(service(&pool, "spare").get("preamble").is_none());
    let resolve = service(&join, "resolve")["preamble"]
        .as_str()
        .expect("a preamble");
    assert!(!resolve.contains("LEAVE_BY"), "{resolve}");

    let scratch = tempfile::Builder::new()
        .prefix("gdl-slurm-leave-")
        .tempdir()
        .expect("scratch");
    let dir = scratch.path().join("model-x-01");
    let bin = scratch.path().join("bin");
    std::fs::create_dir_all(&dir).expect("the pool's directory");
    std::fs::create_dir_all(&bin).expect("a bin");
    let on_host = |recipe: &Value| -> String {
        let setup = &recipe["host_setup"];
        assert_eq!(setup["nodes"], "all", "{setup}");
        words(&setup["commands"]).join(" && ").replace(
            "/shared/gdl-runs/api/model-x-01",
            &dir.display().to_string(),
        )
    };
    let fleet_dir = scratch.path().join("fleet");
    std::fs::create_dir_all(&fleet_dir).expect("the fleet's directory");
    std::fs::write(
        fleet_dir.join("directory.endpoint"),
        "http://10.0.0.7:8000\n",
    )
    .expect("the pointer");
    let preamble = |service: &Value| -> String {
        service["preamble"]
            .as_str()
            .expect("a preamble")
            .replace("/gdl/", &format!("{}/", dir.display()))
            .replace("/fleet/", &format!("{}/", fleet_dir.display()))
    };
    for recipe in [&pool, &join] {
        assert!(on_host(recipe).contains(&dir.display().to_string()));
    }
    let job = |env: &[(&str, &str)]| -> (BTreeMap<String, String>, BTreeMap<String, String>) {
        exports(&on_host(&pool), env, &bin);
        (
            exports(&preamble(service(&pool, "hub")), env, &bin),
            exports(&preamble(service(&pool, "engine")), env, &bin),
        )
    };

    // The cluster sets the end time, in seconds.
    let (hub, engine) = job(&[("SLURM_JOB_ID", "42"), ("SLURM_JOB_END_TIME", "1893456000")]);
    assert_eq!(hub["GDL_POOL__LEAVE_BY"], "1893456000000");
    assert!(!hub.contains_key("GDL_RUNTIME__LEAVE_BY"), "{hub:?}");
    assert_eq!(engine["GDL_RUNTIME__LEAVE_BY"], "1893456000000");
    assert!(!engine.contains_key("GDL_POOL__LEAVE_BY"), "{engine:?}");
    let joiner = exports(
        &preamble(service(&join, "engine")),
        &[("SLURM_JOB_ID", "42")],
        &bin,
    );
    assert_eq!(joiner["GDL_RUNTIME__LEAVE_BY"], "1893456000000");

    // It does not: `squeue` gives the end, as a date.
    let squeue = bin.join("squeue");
    std::fs::write(
        &squeue,
        "#!/bin/sh\n[ \"$*\" = \"-h -j 43 -o %e\" ] && echo 2030-01-01T00:00:00\n",
    )
    .expect("a fake squeue");
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(&squeue, std::fs::Permissions::from_mode(0o755)).expect("mode");
    let (hub, engine) = job(&[("SLURM_JOB_ID", "43")]);
    assert_eq!(hub["GDL_POOL__LEAVE_BY"], "1893456000000");
    assert_eq!(engine["GDL_RUNTIME__LEAVE_BY"], "1893456000000");

    // Neither knows one, as for a job with no time limit.
    std::fs::write(&squeue, "#!/bin/sh\necho Unknown\n").expect("a fake squeue");
    let (hub, engine) = job(&[("SLURM_JOB_ID", "44")]);
    assert!(!hub.contains_key("GDL_POOL__LEAVE_BY"), "{hub:?}");
    assert!(!engine.contains_key("GDL_RUNTIME__LEAVE_BY"), "{engine:?}");
}

/// A run directory holds any character a path may, but those srt-slurm's
/// mounts would read as more: the host step quotes it, so a `(` in it is
/// a path's, not the shell's.
#[test]
fn the_host_step_quotes_the_run_directory() {
    let mirror = Mirror::new();
    let scratch = tempfile::Builder::new()
        .prefix("gdl-slurm-run-dir-")
        .tempdir()
        .expect("scratch");
    let runs = scratch.path().join("runs(a)'b';&<c>*");
    let pool_dir = runs.join("api/model-x-01");
    std::fs::create_dir_all(&pool_dir).expect("the pool's directory");
    let text = fleet_text().replace(
        "\"/shared/gdl-runs\"",
        &format!("{:?}", runs.display().to_string()),
    );
    let files = render(&mirror, &spec(&mirror), &fleet(&text)).expect("the recipes");
    let pool = recipe(&files, "lab/model-x-01/pool.yaml");
    let run = |commands: &Value| {
        let output = std::process::Command::new("timeout")
            .args(["5", "bash", "-c", &words(commands).join(" && ")])
            .env("SLURM_JOB_ID", "42")
            .env("SLURM_JOB_END_TIME", "1893456000")
            .output()
            .expect("bash");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    run(&pool["host_setup"]["commands"]);
    let end = pool_dir.join("job.42.end");
    assert_eq!(
        std::fs::read_to_string(&end).expect("the end"),
        "1893456000\n"
    );
    run(&pool["host_setup"]["teardown"]);
    assert!(!end.exists());
}

/// Slurm's `TERM`, `signal_lead_s` before the time limit, reaches every
/// step of a joining job: `resolve`'s wait ends on it, and srt-slurm must
/// not take that end as a failure, which would tear the job down and stop
/// its segment's drain with a second signal. `resolve` is not critical: a
/// `resolve` that fails before it writes still fails the job, through its
/// readiness probe.
#[test]
fn a_joining_jobs_resolve_does_not_fail_on_the_jobs_signal() {
    use std::time::{Duration, Instant};

    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&fleet_text())).expect("the recipes");
    let join = recipe(&files, "lab/model-x-01/join-engine.yaml");
    assert_eq!(join["sbatch_directives"]["signal"], "TERM@900");
    let resolve = service(&join, "resolve");
    let command = words(&resolve["command"]);
    // The directory's answer is not what is under test: the wait after it is.
    let script = command[2].replace(
        "/usr/local/bin/segment --resolve model_x/model-x-01 --write /logs/gdl-hub.toml",
        "true",
    );
    let mut child = super::common::Running(
        std::process::Command::new(command[0])
            .args([command[1], &script])
            .spawn()
            .expect("the resolve step"),
    );
    let pid = child.0.id().to_string();
    let deadline = Instant::now() + Duration::from_secs(2);
    while std::fs::read_to_string(format!("/proc/{pid}/comm"))
        .map_or(true, |name| name.trim() != "sleep")
    {
        assert!(Instant::now() < deadline, "resolve did not reach its wait");
        std::thread::yield_now();
    }
    let sent = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("kill");
    assert!(sent.success());
    let status = child.0.wait().expect("resolve exits");
    assert!(
        resolve["critical"] != true || status.success(),
        "resolve ended {status} on the job's signal, and is critical"
    );
}

/// Each image a recipe names is a reference by Docker's grammar, as its
/// registry and repository make it: a repository path over 255 characters
/// is refused, naming the site's registry.
#[test]
fn an_image_name_too_long_for_a_reference_is_refused() {
    let mirror = Mirror::new();
    let registry = format!("registry.example/{}", "a".repeat(250));
    let text = fleet_text().replace("registry.example/gdl", &registry);
    let named = gdl_gen::Digests::from_json(
        &digests()
            .to_json()
            .replace("registry.example/gdl", &registry),
    )
    .expect("digests");
    let refused = render_with(&mirror, &spec(&mirror), &fleet(&text), &named)
        .expect_err("refused")
        .to_string();
    assert!(refused.contains("[cluster.lab] registry"), "{refused}");
}

/// A pool that names no cluster is refused by name, not passed to the
/// images it would need a cluster's registry and platform for.
#[test]
fn a_pool_with_no_cluster_is_refused() {
    let mirror = Mirror::new();
    let text = fleet_text().replacen(
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n",
        "[pool.global-us]\nclass = \"global\"\nregion = \"slurm-lab\"\n",
        1,
    );
    assert!(!text.contains("[pool.global-us]\nclass = \"global\"\ncluster"));
    let refused = render(&mirror, &spec(&mirror), &fleet(&text))
        .expect_err("refused")
        .to_string();
    for named in ["global-us", "cluster"] {
        assert!(refused.contains(named), "{named} in {refused}");
    }
}

/// Pyxis writes each mount's source inside double quotes in a shell
/// command, so a run directory with `"`, `\`, or a backquote is refused.
#[test]
fn a_run_directory_pyxis_cannot_quote_is_refused() {
    let mirror = Mirror::new();
    for bad in ["gdl-\\\"runs", "gdl-\\\\runs", "gdl-`runs"] {
        let text = fleet_text().replace("/shared/gdl-runs", &format!("/shared/{bad}"));
        let refused = render(&mirror, &spec(&mirror), &fleet(&text))
            .expect_err("refused")
            .to_string();
        assert!(refused.contains("run_dir"), "{bad}: {refused}");
    }
}

/// Each segment process a recipe starts gets `GLIBC_TUNABLES` as every
/// launcher gives it (`gdl_model::segment_tunables`): its preamble sets
/// `glibc.malloc.hugetlb=0` when the job's environment has none, and
/// keeps the user's value, as given, when it has one. A hub, `resolve`,
/// and the placeholders get none. Part of
/// `k8s::every_launcher_gives_a_segment_the_glibc_tunables`, which checks
/// every launcher.
pub(super) fn every_segment_a_recipe_starts_gets_the_glibc_tunables(mirror: &Mirror) {
    let files = render(mirror, &spec(mirror), &fleet(&fleet_text())).expect("the recipes");
    let bin = tempfile::Builder::new()
        .prefix("gdl-slurm-tunables-")
        .tempdir()
        .expect("scratch");
    let tunables = |preamble: &str, own: Option<&str>| -> Option<String> {
        let env: Vec<(&str, &str)> = own
            .map(|own| (gdl_model::GLIBC_TUNABLES, own))
            .into_iter()
            .chain([("SLURM_JOB_ID", "42")])
            .collect();
        let (_held, preamble) = mounted(preamble, Some("http://10.0.0.7:8000"));
        let script = format!("{preamble} && echo \"GDL_SEEN=${{GLIBC_TUNABLES-unset}}\"");
        let seen = exports(&script, &env, bin.path());
        seen.get("GDL_SEEN")
            .filter(|seen| *seen != "unset")
            .cloned()
    };
    let mut segments = 0;
    for (path, recipe) in recipes(&files) {
        for service in recipe["services"].as_array().expect("services") {
            let name = service["name"].as_str().expect("a name");
            assert!(
                service["env"].get(gdl_model::GLIBC_TUNABLES).is_none(),
                "{}: {name}",
                path.display()
            );
            let preamble = service["preamble"].as_str().unwrap_or("true");
            let segment = matches!(name, "front" | "engine");
            for own in [None, Some("glibc.malloc.arena_max=4"), Some("")] {
                let expected = match segment {
                    true => Some(gdl_model::segment_tunables(own)),
                    false => own.map(str::to_string),
                };
                assert_eq!(
                    tunables(preamble, own),
                    expected,
                    "{}: {name}, {own:?}",
                    path.display()
                );
            }
            segments += usize::from(segment);
        }
    }
    // Three pools and three join jobs, one segment service each.
    assert_eq!(segments, 6);
}
