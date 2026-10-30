//! A fleet under the DynamoDB directory on Kubernetes: every hub publishes
//! to the tables, with none of the relay's state claims or parent's hubs,
//! so a child may run in another cluster than its parents. With `local`,
//! one DynamoDB Local and the tables Job run in the directory's cluster
//! alone: its hubs reach the Local by its Service, and the other
//! clusters' hubs by its pod's IP, which only `deploy` can read, so the
//! render writes a placeholder there. On AWS no Local and no Job are
//! rendered, and `dynamodb/tables.json` says what the owner makes.

use super::*;

const DYNAMODB_FLEET: &str = "tests/fixtures/k8s/dynamodb/fleet.toml";
const DYNAMODB_PINNED: &str = "tests/fixtures/k8s/dynamodb/out";

/// The endpoint of the hubs outside the directory's cluster until
/// `deploy` writes the Local's pod IP in its place.
const PLACEHOLDER: &str = "http://DYNAMODB_LOCAL_POD_IP:8000";

fn dynamodb_text() -> String {
    std::fs::read_to_string(manifest_dir().join(DYNAMODB_FLEET)).expect("the fleet")
}

/// The fleet on AWS: the same tables, and no Local.
fn aws_text() -> String {
    let text = dynamodb_text().replace("local = true\ncluster = \"use1\"\n", "");
    assert!(!text.contains("local = true"), "{text}");
    text
}

fn variables(objects: &BTreeMap<(String, String), Value>, name: &str) -> BTreeMap<String, String> {
    env(container(
        &objects[&("Deployment".to_string(), name.to_string())],
    ))
}

#[test]
fn a_dynamodb_fleets_objects_are_pinned() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&dynamodb_text())).expect("objects");
    assert_pinned(&mirror, &files, DYNAMODB_PINNED);
    for path in ["use1/base.yaml", "use1/tables.yaml", "euw1/base.yaml"] {
        assert!(files.contains_key(Path::new(path)), "{path}");
    }
    assert!(!files.contains_key(Path::new("euw1/tables.yaml")));
}

#[test]
#[ignore = "F2: the tables Job has no hub image when its cluster hosts no pools"]
fn a_directory_can_run_in_a_cluster_without_pools() {
    let mirror = Mirror::new();
    let mut text: toml::Value = toml::from_str(&dynamodb_text()).expect("the fleet");
    for (_, pool) in text["pool"].as_table_mut().expect("pools").iter_mut() {
        pool["cluster"] = "euw1".into();
    }
    let fleet = fleet(&toml::to_string(&text).expect("the fleet"));
    fleet.check(None).expect("a valid fleet");
    assert!(
        fleet
            .pools()
            .all(|pool| { fleet.cluster_of(&pool.name).map(|(name, _)| name) == Some("euw1") })
    );

    let spec = spec(&mirror);
    let tagged = render(&mirror, &spec, &fleet).expect("objects with an explicit tag");
    assert!(tagged.contains_key(Path::new("use1/tables.yaml")));
    let files = gdl_gen::k8s_files(&spec, &fleet, &mirror.target(), None, None)
        .expect("a directory cluster without pools still needs its tables Job's hub image");
    assert!(files.contains_key(Path::new("use1/tables.yaml")));
}

/// Every hub and segment publishes to the tables in its cluster's region:
/// the directory's cluster at the Local's Service, the other at the
/// placeholder. None reports to a parent's hub or keeps a state claim, so
/// the shared pool in `euw1`, whose parents run in both clusters, renders.
#[test]
fn every_pool_publishes_to_the_tables() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&dynamodb_text())).expect("objects");
    let objects = objects(&files);
    for (deployment, region, endpoint) in [
        ("global-us", "us-east-1", "http://dynamodb-local:8000"),
        ("global-us-front", "us-east-1", "http://dynamodb-local:8000"),
        ("model-x-us", "us-east-1", "http://dynamodb-local:8000"),
        (
            "model-x-us-engine",
            "us-east-1",
            "http://dynamodb-local:8000",
        ),
        ("global-eu", "eu-west-1", PLACEHOLDER),
        ("model-x-shared", "eu-west-1", PLACEHOLDER),
        ("model-x-shared-engine", "eu-west-1", PLACEHOLDER),
    ] {
        let env = variables(&objects, deployment);
        for (name, value) in [
            ("GDL_POOL__DIRECTORY", "dynamodb"),
            ("GDL_POOL__RECORDS_TABLE", "gdl-pools"),
            ("GDL_POOL__LISTS_TABLE", "gdl-pool-lists"),
            ("GDL_POOL__AWS_REGION", region),
            ("GDL_POOL__ENDPOINT", endpoint),
        ] {
            assert_eq!(
                env.get(name).map(String::as_str),
                Some(value),
                "{deployment}: {name}"
            );
        }
        for relay in ["GDL_POOL__PARENT_HUB", "GDL_POOL__STATE_DIR"] {
            assert!(!env.contains_key(relay), "{deployment}: {relay}");
        }
    }
    assert_eq!(
        variables(&objects, "model-x-shared")["GDL_POOL__PARENTS"],
        "global"
    );
    for ((kind, name), object) in &objects {
        assert_ne!(kind, "PersistentVolumeClaim", "{name}");
        assert!(
            object["spec"]["template"]["spec"]["securityContext"].is_null(),
            "{kind} {name}"
        );
    }
    let readme = &files[Path::new("README.md")];
    assert!(readme.contains(PLACEHOLDER), "{readme}");
}

/// One DynamoDB Local, shared by every region, runs in the directory's
/// cluster, behind a Service of its name; the tables Job runs there too,
/// once, from the hub's image, against that Service. The other cluster
/// runs neither.
#[test]
fn the_local_and_the_tables_job_run_in_the_directorys_cluster_only() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&dynamodb_text())).expect("objects");
    let objects = objects(&files);
    let local = &objects[&("Deployment".to_string(), "dynamodb-local".to_string())];
    assert_eq!(local["metadata"]["labels"]["gdl.dev/cluster"], "use1");
    let local = container(local);
    assert!(
        local["image"]
            .as_str()
            .expect("an image")
            .starts_with("amazon/dynamodb-local@sha256:"),
        "{local}"
    );
    let args: Vec<&str> = local["args"]
        .as_array()
        .expect("args")
        .iter()
        .map(|arg| arg.as_str().expect("a word"))
        .collect();
    assert!(
        args.contains(&"-sharedDb") && args.contains(&"-inMemory"),
        "{args:?}"
    );
    let service = &objects[&("Service".to_string(), "dynamodb-local".to_string())];
    assert_eq!(service["metadata"]["labels"]["gdl.dev/cluster"], "use1");
    assert_eq!(service["spec"]["ports"][0]["port"], 8000);
    assert!(!files[Path::new("euw1/base.yaml")].contains("dynamodb-local"));

    let tables = documents(&files[Path::new("use1/tables.yaml")]);
    let [job] = tables.as_slice() else {
        panic!("one Job: {tables:?}");
    };
    assert_eq!(job["kind"], "Job");
    assert_eq!(job["metadata"]["name"], "dynamodb-tables");
    assert_eq!(job["spec"]["backoffLimit"], 0);
    let pod = &job["spec"]["template"]["spec"];
    assert_eq!(pod["restartPolicy"], "Never");
    let step = &pod["containers"][0];
    let hub = container(&objects[&("Deployment".to_string(), "global-us".to_string())]);
    assert_eq!(step["image"], hub["image"]);
    assert_eq!(
        step["command"],
        serde_json::json!(["/usr/local/bin/segment", "--tables"])
    );
    let env = env(step);
    for (name, value) in [
        ("GDL_POOL__RECORDS_TABLE", "gdl-pools"),
        ("GDL_POOL__LISTS_TABLE", "gdl-pool-lists"),
        ("GDL_POOL__AWS_REGION", "us-east-1"),
        ("GDL_POOL__ENDPOINT", "http://dynamodb-local:8000"),
    ] {
        assert_eq!(env.get(name).map(String::as_str), Some(value), "{name}");
    }
}

/// On AWS the owner makes the global tables: no Local and no Job are
/// rendered, no hub names an endpoint, and `dynamodb/tables.json` gives the
/// tables, their keys, the lists table's TTL, and every region of the
/// fleet, each of which needs a replica.
#[test]
fn on_aws_the_owner_makes_the_tables_from_tables_json() {
    let mirror = Mirror::new();
    let files = render(&mirror, &spec(&mirror), &fleet(&aws_text())).expect("objects");
    let objects = objects(&files);
    assert!(
        !objects
            .keys()
            .any(|(_, name)| name.starts_with("dynamodb-")),
        "{:?}",
        objects.keys()
    );
    assert!(!files.keys().any(|path| path.ends_with("tables.yaml")));
    for deployment in ["global-us", "global-eu", "model-x-shared-engine"] {
        let env = variables(&objects, deployment);
        assert_eq!(env["GDL_POOL__DIRECTORY"], "dynamodb", "{deployment}");
        assert!(!env.contains_key("GDL_POOL__ENDPOINT"), "{deployment}");
    }
    let tables: Value =
        serde_json::from_str(&files[Path::new("dynamodb/tables.json")]).expect("JSON");
    assert_eq!(
        tables,
        serde_json::json!({
            "billing_mode": "PAY_PER_REQUEST",
            "regions": ["eu-west-1", "us-east-1"],
            "tables": [
                {
                    "name": "gdl-pools",
                    "partition_key": {"name": "class", "type": "S"},
                    "sort_key": {"name": "pool", "type": "S"},
                },
                {
                    "name": "gdl-pool-lists",
                    "partition_key": {"name": "owner", "type": "S"},
                    "sort_key": {"name": "list", "type": "S"},
                    "ttl_attribute": "expires_at",
                },
            ],
        })
    );
    let readme = &files[Path::new("README.md")];
    assert!(
        readme.contains("dynamodb/tables.json") && readme.contains("replica"),
        "{readme}"
    );
    // A fleet with its Local writes no tables file: the Job makes them.
    let local = render(&mirror, &spec(&mirror), &fleet(&dynamodb_text())).expect("objects");
    assert!(!local.contains_key(Path::new("dynamodb/tables.json")));
}

/// No rendered file holds the fleet's secret, or a static AWS credential:
/// the hubs reach AWS through the service account's role, and DynamoDB
/// Local through test credentials in the hub's code.
#[test]
fn no_dynamodb_object_holds_a_credential() {
    let mirror = Mirror::new();
    for text in [dynamodb_text(), aws_text()] {
        let files = render(&mirror, &spec(&mirror), &fleet(&text)).expect("objects");
        for (path, text) in &files {
            assert!(!text.contains(MARKER), "{}", path.display());
            for credential in [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "aws_access_key_id",
                "aws_secret_access_key",
            ] {
                assert!(
                    !text.contains(credential),
                    "{}: {credential}",
                    path.display()
                );
            }
        }
    }
}

/// A pool cannot take the name of the Local or the Job, whose objects it
/// would share.
#[test]
fn a_pool_named_for_the_directorys_objects_is_refused() {
    let mirror = Mirror::new();
    let spec = spec(&mirror);
    for name in ["dynamodb-local", "dynamodb-tables"] {
        let text = dynamodb_text().replace("model-x-us", name);
        let refusal = refused(&mirror, &spec, &text);
        assert!(refusal.contains(&format!("`{name}`")), "{refusal}");
    }
}
