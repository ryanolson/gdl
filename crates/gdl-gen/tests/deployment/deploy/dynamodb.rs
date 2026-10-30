//! `deploy` and `down` of a fleet under the DynamoDB directory, in one
//! DynamoDB Local that cluster `use1` runs: the directory's cluster's base
//! goes first, with the Local, which is waited for until it is ready; its
//! pod IP replaces the placeholder in the other cluster's hubs; the tables
//! Job runs, replaced, and is waited for before any pool is applied, and a
//! failed Job stops the deploy with its log. A removed pool is never
//! deleted on its parent's 404, which proves nothing outside the relay.

use super::*;

const DYNAMODB_FLEET: &str = "tests/fixtures/k8s/dynamodb/fleet.toml";
const PLACEHOLDER: &str = "DYNAMODB_LOCAL_POD_IP";

fn fleet_text() -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(DYNAMODB_FLEET))
        .expect("the fleet")
}

fn fakes(log: &Log) -> (Docker, Kube) {
    (
        Docker {
            log: Rc::clone(log),
            fail: false,
        },
        Kube::new(log),
    )
}

/// What each kubectl call did, by its context: `base`, `local` (the
/// Local's pods read), `delete-job`, `tables` (the Job applied), `job` (its
/// status read), `pools`, and the rest as [`steps`] names them.
fn order(log: &Log) -> Vec<String> {
    let mut order: Vec<String> = Vec::new();
    for call in log.borrow().iter() {
        let Call::Kube { args, input } = call else {
            continue;
        };
        let has = |word: &str| args.iter().any(|a| a == word);
        let what = if has("apply") && input.contains("\"Namespace\"") {
            "base"
        } else if has("apply") && input.contains("\"Job\"") {
            "tables"
        } else if has("apply") {
            "pools"
        } else if has("pods") && args.iter().any(|a| a.contains("dynamodb-local")) {
            "local"
        } else if has("delete") && args.iter().any(|a| a == "job/dynamodb-tables") {
            "delete-job"
        } else if has("job") {
            "job"
        } else if has("logs") {
            "logs"
        } else if has("wait") {
            "wait-namespace"
        } else {
            continue;
        };
        let step = format!("{}:{what}", args[1]);
        if order.last() != Some(&step) {
            order.push(step);
        }
    }
    order
}

/// The stdin of each `apply` of pools in `context`.
fn pools_applied(log: &Log, context: &str) -> String {
    log.borrow()
        .iter()
        .filter_map(|call| match call {
            Call::Kube { args, input }
                if args[1] == context
                    && args.iter().any(|a| a == "apply")
                    && !input.contains("\"Namespace\"")
                    && !input.contains("\"Job\"") =>
            {
                Some(input.clone())
            }
            _ => None,
        })
        .collect()
}

/// The directory's cluster goes first: its base, with the Local, then a
/// wait until the Local is ready, and only then the other cluster's base.
/// The tables Job is deleted, applied, and waited for before any pool.
/// The other cluster's hubs name the Local's pod IP; its own, the Local's
/// Service; no placeholder is applied.
#[test]
fn the_local_and_the_tables_job_come_before_every_pool() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    assert_eq!(
        order(&log),
        [
            "kind-use1:base",
            "kind-use1:wait-namespace",
            "kind-use1:local",
            "kind-euw1:base",
            "kind-euw1:wait-namespace",
            "kind-use1:delete-job",
            "kind-use1:tables",
            "kind-use1:job",
            "kind-euw1:pools",
            "kind-use1:pools",
        ]
    );
    let euw1 = pools_applied(&log, "kind-euw1");
    assert!(euw1.contains("http://10.1.2.3:8000"), "{euw1}");
    assert!(!euw1.contains(PLACEHOLDER), "{euw1}");
    let use1 = pools_applied(&log, "kind-use1");
    assert!(use1.contains("http://dynamodb-local:8000"), "{use1}");
    assert!(!use1.contains(PLACEHOLDER), "{use1}");
}

/// A Local that never becomes ready stops the deploy, naming it, before
/// any other cluster's objects or any pool are applied. The wait is the
/// fakes' objects bound, 5 s.
#[test]
fn a_local_that_is_never_ready_stops_the_deploy() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.local_pods = json!({"items": [local_pod(false)]});
    let error =
        deploy(&mirror, &files, &mut docker, &mut kube).expect_err("a Local that is never ready");
    assert!(error.to_string().contains("dynamodb-local"), "{error}");
    let order = order(&log);
    assert!(
        !order.iter().any(|step| step.starts_with("kind-euw1")),
        "{order:?}"
    );
    assert!(
        !order.iter().any(|step| step.ends_with(":pools")),
        "{order:?}"
    );
}

/// A tables Job that fails stops the deploy before any pool is applied,
/// and the error carries its log, which names the table.
#[test]
fn a_failed_tables_job_stops_the_deploy_and_names_the_table() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.job = json!({"failed": 1});
    kube.job_log = "Error: table gdl-pools has the keys id (HASH, N), and the directory needs class (HASH, S), pool (RANGE, S)\n";
    let error = deploy(&mirror, &files, &mut docker, &mut kube).expect_err("a failed tables Job");
    let error = error.to_string();
    assert!(
        error.contains("dynamodb-tables") && error.contains("table gdl-pools has the keys"),
        "{error}"
    );
    let order = order(&log);
    assert!(
        !order.iter().any(|step| step.ends_with(":pools")),
        "{order:?}"
    );
}

/// A pool left by an earlier deploy under the DynamoDB directory, as its
/// cluster holds it.
fn left_dynamodb(name: &str, pool: &str, segment: &str) -> Value {
    let mut object = left("Deployment", name, pool, segment, Some("global-us"));
    object["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array_mut()
        .expect("env")
        .push(json!({"name": "GDL_POOL__DIRECTORY", "value": "dynamodb"}));
    object
}

/// A pool under the DynamoDB directory that the fleet no longer places is
/// not deleted: its parent's hub answers 404 for its record at once,
/// since only the relay serves that route, and that proves nothing. The
/// deploy stops, naming the pool, and deletes nothing.
#[test]
fn a_removed_dynamodb_pool_is_not_deleted_on_its_parents_404() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.held = Some(0);
    kube.holds(
        "kind-use1",
        left_dynamodb("model-x-old", "model-x-old", "hub"),
    );
    kube.holds(
        "kind-use1",
        left_dynamodb("model-x-old-engine", "model-x-old", "engine"),
    );
    let error =
        deploy(&mirror, &files, &mut docker, &mut kube).expect_err("a removed dynamodb pool");
    let error = error.to_string();
    assert!(
        error.contains("`model-x-old`") && error.contains("DynamoDB"),
        "{error}"
    );
    // Only the tables step's own delete of its last Job runs.
    let deleted = deletes(&log, "kind-use1");
    assert!(
        deleted
            .iter()
            .flatten()
            .all(|object| object == "job/dynamodb-tables"),
        "{deleted:?}"
    );
    assert!(
        !commands(&log)
            .iter()
            .any(|args| args.iter().any(|a| a.contains("/report/"))),
        "the parent's relay route was asked"
    );
}

/// `down` takes a DynamoDB fleet off its clusters: it drains each pool,
/// asks no parent for a record, deletes the segments, then the hubs, and
/// the Local and the Job last, after the hubs that use them. Only each
/// namespace stays.
#[test]
fn down_deletes_the_local_after_the_hubs() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    log.borrow_mut().clear();
    down(&files, &mut kube).expect("down");
    assert!(
        !commands(&log)
            .iter()
            .any(|args| args.iter().any(|a| a.contains("/report/"))),
        "the parent's relay route was asked"
    );
    let batches = deletes(&log, "kind-use1");
    let at = |object: &str| {
        batches
            .iter()
            .position(|batch| batch.iter().any(|o| o == object))
            .unwrap_or_else(|| panic!("{object} is not deleted: {batches:?}"))
    };
    assert!(at("deployment/global-us-front") < at("deployment/global-us"));
    assert!(at("deployment/global-us") < at("deployment/dynamodb-local"));
    assert!(at("deployment/global-us") < at("job/dynamodb-tables"));
    // Only each namespace stays.
    for context in ["kind-use1", "kind-euw1"] {
        let left: Vec<&(String, String)> = kube.objects[context].keys().collect();
        assert_eq!(
            left,
            [&("Namespace".to_string(), "gdl-api".to_string())],
            "{context}"
        );
    }
}
