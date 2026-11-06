//! A fleet whose `[directory]` names etcd: the bundle runs one etcd, with
//! the compaction and quota the backend needs, on a volume that keeps its
//! data; each hub publishes to it, with no state volume and no parent's
//! hub, and is built with the hub's `etcd` feature.

use std::path::Path;

use super::{Mirror, ONE_FLEET, SPLIT, assert_pinned, fleet_bundle, pinned, placed};

const DIRECTORY: &str = "[directory]\nbackend = \"etcd\"\n";

/// The pin of [`ONE_FLEET`]'s bundle under [`DIRECTORY`], over [`SPLIT`].
const ETCD_PIN: &str = "tests/fixtures/bundle/etcd.txt";

/// The flags the etcd backend needs of every member
/// (docs/src/concepts/pools.md, "Running etcd for the directory").
const FLAGS: [&str; 3] = [
    "--auto-compaction-mode=periodic",
    "--auto-compaction-retention=1h",
    "--quota-backend-bytes=8589934592",
];

#[test]
fn an_etcd_fleets_bundle_is_pinned() {
    let mirror = Mirror::new();
    let fleet = gdl_model::Fleet::from_toml(&format!("{ONE_FLEET}{DIRECTORY}")).expect("a fleet");
    let (files, _) = fleet_bundle(&mirror, &placed(&mirror, SPLIT), &fleet);
    assert_pinned(&pinned(&mirror, &files), Path::new(ETCD_PIN));
}

/// One etcd, pinned by digest, with the backend's flags, its data on a
/// volume of its own; every hub waits for it to start, and reads the same
/// directory as each of its segments.
#[test]
fn an_etcd_fleet_runs_one_etcd_that_every_hub_publishes_to() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let fleet = gdl_model::Fleet::from_toml(&format!("{ONE_FLEET}{DIRECTORY}")).expect("a fleet");
    let (files, compose) = fleet_bundle(&mirror, &spec, &fleet);
    let services = compose["services"].as_object().expect("services");

    let etcd = &services["etcd"];
    let image = etcd["image"].as_str().expect("an image");
    assert!(
        image.starts_with("gcr.io/etcd-development/etcd@sha256:"),
        "{image}"
    );
    let command: Vec<&str> = etcd["command"]
        .as_array()
        .expect("a command")
        .iter()
        .map(|arg| arg.as_str().expect("a word"))
        .collect();
    for flag in FLAGS {
        assert!(command.contains(&flag), "{flag}: {command:?}");
    }
    assert_eq!(
        etcd["volumes"],
        serde_json::json!(["etcd-data:/var/lib/etcd"])
    );
    assert_eq!(compose["volumes"], serde_json::json!({"etcd-data": {}}));
    assert!(services.get("dynamodb-local").is_none());
    assert!(services.get("dynamodb-tables").is_none());

    let dockerfile = &files[Path::new("hub/Dockerfile")];
    assert!(
        dockerfile.contains("--features etcd")
            && dockerfile.contains("protobuf-compiler libprotobuf-dev"),
        "{dockerfile}"
    );

    let deployment: gdl_model::DeploymentSpec =
        serde_json::from_str(&files[Path::new("deployment.json")]).expect("the hub's spec");
    for pool in fleet.pools() {
        let hub = &services[&pool.name];
        assert_eq!(
            hub["depends_on"],
            serde_json::json!({"etcd": {"condition": "service_started"}}),
            "{}",
            pool.name
        );
        assert!(hub.get("volumes").is_none(), "{}: {hub}", pool.name);
        let env = hub["environment"].as_object().expect("the hub's variables");
        assert!(env.get("GDL_POOL__PARENT_HUB").is_none(), "{env:?}");
        let vars = env
            .iter()
            .map(|(name, value)| (name.into(), value.as_str().expect("a string").into()));
        let read = gdl_hub::pool_from_vars(&deployment, vars)
            .unwrap_or_else(|error| panic!("{}: {error}", pool.name))
            .expect("a pool");
        let mut expected = pool.clone();
        expected.directory = gdl_model::DirectoryBackend::Etcd {
            endpoints: vec!["http://etcd:2379".into()],
            prefix: gdl_model::DirectoryBackend::ETCD_PREFIX.into(),
        };
        expected.hub = Some(format!("{}:1337", pool.name));
        assert_eq!(read, expected);
        for (name, service) in services {
            if !name.starts_with(&format!("{}_", pool.name)) {
                continue;
            }
            let vars = service["environment"]
                .as_object()
                .expect("the segment's variables")
                .iter()
                .filter(|(name, _)| name.starts_with("GDL_POOL__"))
                .map(|(name, value)| (name.into(), value.as_str().expect("a string").into()));
            let own = gdl_hub::pool_from_vars(&deployment, vars)
                .unwrap_or_else(|error| panic!("{name}: {error}"))
                .expect("a pool");
            assert_eq!(own, read, "{name}");
        }
    }
    let readme = &files[Path::new("README.md")];
    assert!(readme.contains("etcd-data"), "{readme}");
}

/// A pool cannot take the name of the bundle's etcd service.
#[test]
fn a_pool_named_for_the_etcd_service_is_refused() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let fleet = gdl_model::Fleet::from_toml(&format!(
        "{}{DIRECTORY}",
        ONE_FLEET.replace("model-x-02", "etcd")
    ))
    .expect("a fleet");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-etcd-")
        .tempdir()
        .expect("a directory");
    let Err(error) = gdl_gen::compose_files(
        &spec,
        Some(&fleet),
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    ) else {
        panic!("a pool named for a service of the bundle");
    };
    assert!(error.to_string().contains("`etcd`"), "{error}");
}
