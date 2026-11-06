//! A fleet whose `[directory]` names DynamoDB: the bundle runs one
//! DynamoDB Local, shared by every region, and the tables step from the hub
//! image before any hub; each hub publishes to the tables, with no state
//! volume and no parent's hub; and no file of the bundle holds a
//! credential. Without `local = true`, Compose refuses the fleet.

use std::path::Path;

use super::{Mirror, ONE_FLEET, SPLIT, assert_pinned, fleet_bundle, pinned, placed};

const DIRECTORY: &str = "[directory]\nbackend = \"dynamodb\"\nrecords_table = \"gdl-pools\"\nlists_table = \"gdl-pool-lists\"\nlocal = true\n";

/// The pin of [`ONE_FLEET`]'s bundle under [`DIRECTORY`], over [`SPLIT`].
const DYNAMODB_PIN: &str = "tests/fixtures/bundle/dynamodb.txt";

/// A fleet that keeps the DynamoDB directory composes byte for byte as
/// pinned: every file of the bundle, so that another backend changes
/// nothing of it.
#[test]
fn a_dynamodb_fleets_bundle_is_pinned() {
    let mirror = Mirror::new();
    let fleet = gdl_model::Fleet::from_toml(&format!("{ONE_FLEET}{DIRECTORY}")).expect("a fleet");
    let (files, _) = fleet_bundle(&mirror, &placed(&mirror, SPLIT), &fleet);
    assert_pinned(&pinned(&mirror, &files), Path::new(DYNAMODB_PIN));
}

#[test]
fn a_dynamodb_fleet_runs_one_local_and_the_tables_step_before_its_hubs() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let fleet = gdl_model::Fleet::from_toml(&format!("{ONE_FLEET}{DIRECTORY}")).expect("a fleet");
    let (files, compose) = fleet_bundle(&mirror, &spec, &fleet);
    let services = compose["services"].as_object().expect("services");

    // One DynamoDB Local, pinned, which every region shares.
    let local = &services["dynamodb-local"];
    let image = local["image"].as_str().expect("an image");
    assert!(
        image.starts_with("amazon/dynamodb-local@sha256:"),
        "{image}"
    );
    let command: Vec<&str> = local["command"]
        .as_array()
        .expect("a command")
        .iter()
        .map(|arg| arg.as_str().expect("a word"))
        .collect();
    assert!(command.contains(&"-sharedDb"), "{command:?}");

    // The tables step runs from the hub's image once Local has started.
    let tables = &services["dynamodb-tables"];
    let hub_image = services["global-us"]["image"].clone();
    assert_eq!(tables["image"], hub_image);
    assert_eq!(
        tables["entrypoint"],
        serde_json::json!(["/usr/local/bin/segment", "--tables"])
    );
    assert_eq!(
        tables["depends_on"],
        serde_json::json!({"dynamodb-local": {"condition": "service_started"}})
    );
    assert_eq!(
        tables["environment"]["GDL_POOL__RECORDS_TABLE"],
        "gdl-pools"
    );
    assert_eq!(
        tables["environment"]["GDL_POOL__LISTS_TABLE"],
        "gdl-pool-lists"
    );
    assert_eq!(
        tables["environment"]["GDL_POOL__ENDPOINT"],
        "http://dynamodb-local:8000"
    );

    // The hub's image takes the backend.
    let dockerfile = &files[Path::new("hub/Dockerfile")];
    assert!(
        dockerfile.contains("--features dynamodb") && dockerfile.contains("gdl-hub"),
        "{dockerfile}"
    );

    let deployment: gdl_model::DeploymentSpec =
        serde_json::from_str(&files[Path::new("deployment.json")]).expect("the hub's spec");
    for pool in fleet.pools() {
        let hub = &services[&pool.name];
        assert_eq!(
            hub["depends_on"],
            serde_json::json!({"dynamodb-tables": {"condition": "service_completed_successfully"}}),
            "{}",
            pool.name
        );
        assert!(hub.get("volumes").is_none(), "{}: {hub}", pool.name);
        let env = hub["environment"].as_object().expect("the hub's variables");
        let vars = env
            .iter()
            .map(|(name, value)| (name.into(), value.as_str().expect("a string").into()));
        let read = gdl_hub::pool_from_vars(&deployment, vars)
            .unwrap_or_else(|error| panic!("{}: {error}", pool.name))
            .expect("a pool");
        let mut expected = pool.clone();
        expected.directory = gdl_model::DirectoryBackend::DynamoDb {
            records_table: "gdl-pools".into(),
            lists_table: "gdl-pool-lists".into(),
            aws_region: pool.region.clone(),
            endpoint: Some("http://dynamodb-local:8000".into()),
        };
        expected.hub = Some(format!("{}:1337", pool.name));
        assert_eq!(read, expected);
        // Each of its segments reads the same `[pool]`, which fits an
        // instance of the segment.
        let layout =
            gdl_model::layout(&deployment.graph, &deployment.placement).expect("the layout");
        let mut segments = 0;
        for (name, service) in services {
            let Some(segment) = name.strip_prefix(&format!("{}_", pool.name)) else {
                continue;
            };
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
            own.check(&layout, segment)
                .unwrap_or_else(|problems| panic!("{name}: {problems}"));
            segments += 1;
        }
        assert!(segments > 0, "{}", pool.name);
    }
    assert!(compose.get("volumes").is_none(), "{compose}");

    // The fixed test credentials are in the hub's code, never in a file.
    for (path, text) in &files {
        for credential in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "aws_access_key_id",
            "aws_secret_access_key",
        ] {
            assert!(
                !text.contains(credential),
                "{} holds {credential}",
                path.display()
            );
        }
    }
}

/// A Compose bundle carries no AWS role, and no credential goes into a
/// file, so a DynamoDB directory under Compose is DynamoDB Local's.
#[test]
fn compose_refuses_a_dynamodb_directory_without_local() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let fleet = gdl_model::Fleet::from_toml(&format!(
        "{ONE_FLEET}{}",
        DIRECTORY.replace("local = true\n", "")
    ))
    .expect("a fleet");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-dynamodb-")
        .tempdir()
        .expect("a directory");
    let Err(error) = gdl_gen::compose_files(
        &spec,
        Some(&fleet),
        temp.path(),
        &mirror.target(),
        gdl_gen::Sources::Fetched,
    ) else {
        panic!("a bundle of AWS tables");
    };
    let error = error.to_string();
    assert!(
        error.contains("[directory] local = true") && error.contains("AWS role"),
        "{error}"
    );
}

/// A pool cannot take the name of the bundle's DynamoDB services.
#[test]
fn a_pool_named_for_a_directory_service_is_refused() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let fleet = gdl_model::Fleet::from_toml(&format!(
        "{}{DIRECTORY}",
        ONE_FLEET.replace("model-x-02", "dynamodb-tables")
    ))
    .expect("a fleet");
    let temp = tempfile::Builder::new()
        .prefix("gdl-compose-dynamodb-")
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
    assert!(error.to_string().contains("`dynamodb-tables`"), "{error}");
}

/// Compose runs on one host, so it ignores `[directory] cluster`: the same
/// DynamoDB fleet with its pools in a cluster, which runs the Local,
/// writes the same bundle.
#[test]
fn compose_ignores_the_directorys_cluster() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let plain = gdl_model::Fleet::from_toml(&format!("{ONE_FLEET}{DIRECTORY}")).expect("a fleet");
    let clustered = gdl_model::Fleet::from_toml(&format!(
        "[cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-api\"\nregion = \"us-east-1\"\n{}{DIRECTORY}cluster = \"use1\"\n",
        ONE_FLEET.replace("region = \"us-east-1\"", "cluster = \"use1\"")
    ))
    .expect("a fleet");
    assert_eq!(
        clustered.directory().and_then(|d| d.cluster.as_deref()),
        Some("use1")
    );
    let (plain, _) = fleet_bundle(&mirror, &spec, &plain);
    let (clustered, _) = fleet_bundle(&mirror, &spec, &clustered);
    assert_eq!(plain, clustered);
}

/// The tables step reads its variables as the hub does: a table name that
/// Compose quotes, because it would read as a number bare, reaches the
/// step as the name the fleet gives.
#[test]
fn the_tables_step_reads_a_quoted_table_name_as_the_fleet_gives_it() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let fleet = gdl_model::Fleet::from_toml(&format!(
        "{ONE_FLEET}{}",
        DIRECTORY.replace("\"gdl-pools\"", "\"123\"")
    ))
    .expect("a fleet");
    let (_, compose) = fleet_bundle(&mirror, &spec, &fleet);
    let step = compose["services"]["dynamodb-tables"]["environment"]
        .as_object()
        .expect("the step's variables");
    let vars = step
        .iter()
        .map(|(name, value)| (name.into(), value.as_str().expect("a string").into()));
    let read = gdl_hub::directory_from_vars(vars).unwrap_or_else(|error| {
        panic!(
            "{error}: the step reads {:?}",
            step["GDL_POOL__RECORDS_TABLE"]
        )
    });
    let gdl_model::DirectoryBackend::DynamoDb { records_table, .. } = read else {
        panic!("{read:?}");
    };
    assert_eq!(records_table, "123");
}

/// A shared pool, which every global pool follows, composes under the
/// DynamoDB directory: its hub publishes to the tables with its parents'
/// class, and names no parent's hub.
#[test]
fn a_shared_pool_composes_under_the_dynamodb_directory() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let fleet = gdl_model::Fleet::from_toml(&format!(
        "[class.model_x]\nready = {{ engine = 1 }}\n\
         [pool.global-us]\nclass = \"global\"\nregion = \"us-east-1\"\n\
         [pool.global-eu]\nclass = \"global\"\nregion = \"eu-west-1\"\n\
         [pool.model-x-shared]\nclass = \"model_x\"\nregion = \"eu-west-1\"\nparents = \"global\"\n{DIRECTORY}"
    ))
    .expect("a fleet");
    let (files, compose) = fleet_bundle(&mirror, &spec, &fleet);
    let deployment: gdl_model::DeploymentSpec =
        serde_json::from_str(&files[Path::new("deployment.json")]).expect("the hub's spec");
    let env = compose["services"]["model-x-shared"]["environment"]
        .as_object()
        .expect("the hub's variables");
    assert!(env.get("GDL_POOL__PARENT_HUB").is_none(), "{env:?}");
    let vars = env
        .iter()
        .map(|(name, value)| (name.into(), value.as_str().expect("a string").into()));
    let read = gdl_hub::pool_from_vars(&deployment, vars)
        .expect("the hub reads its settings")
        .expect("a pool");
    assert_eq!(read.parent, Some(gdl_model::Parent::Class("global".into())));
    assert!(matches!(
        read.directory,
        gdl_model::DirectoryBackend::DynamoDb { .. }
    ));
}
