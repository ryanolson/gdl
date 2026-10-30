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
        } else if has("delete") && args.iter().any(|a| a == "jobs.batch/dynamodb-tables") {
            "delete-job"
        } else if has("jobs.batch") {
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

/// The index of the first command that deletes `object` in `context`.
fn deleted_at(log: &Log, context: &str, object: &str) -> Option<usize> {
    commands(log).iter().position(|args| {
        args[1] == context && args.iter().any(|a| a == "delete") && args.iter().any(|a| a == object)
    })
}

/// The indexes of the reads of pool `pool`'s own `withdrawn` route.
fn withdrawn_reads(log: &Log, pool: &str) -> Vec<usize> {
    let route = format!("services/http:{pool}:public/proxy/v1/ext/gdl-pool/withdrawn");
    commands(log)
        .iter()
        .enumerate()
        .filter(|(_, args)| args.iter().any(|a| a.ends_with(&route)))
        .map(|(at, _)| at)
        .collect()
}

/// A pool under the DynamoDB directory that the fleet no longer places is
/// drained, and deleted only once its own hub, through its own Service,
/// says its record is withdrawn: never on its parent's 404, which only
/// the relay serves, and which here would come at once.
#[test]
fn a_removed_dynamodb_pool_is_deleted_only_once_its_own_hub_says_withdrawn() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.held = Some(0);
    kube.withdrawn = Some(2);
    kube.holds(
        "kind-use1",
        left_dynamodb("model-x-old", "model-x-old", "hub"),
    );
    kube.holds(
        "kind-use1",
        left_dynamodb("model-x-old-engine", "model-x-old", "engine"),
    );
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    assert!(
        !commands(&log)
            .iter()
            .any(|args| args.iter().any(|a| a.contains("/report/"))),
        "the parent's relay route was asked"
    );
    let drained = commands(&log)
        .iter()
        .position(|args| {
            args.iter()
                .any(|a| a.ends_with("http:model-x-old:control/proxy/v1/ext/gdl-pool/drain"))
        })
        .expect("the pool is drained");
    let reads = withdrawn_reads(&log, "model-x-old");
    assert_eq!(reads.len(), 3, "two false, then true: {reads:?}");
    assert!(reads[0] > drained, "read before the drain");
    for object in [
        "deployments.apps/model-x-old-engine",
        "deployments.apps/model-x-old",
    ] {
        let deleted = deleted_at(&log, "kind-use1", object).expect("deleted");
        assert!(
            deleted > *reads.last().expect("a read"),
            "{object} was deleted before the pool said withdrawn"
        );
    }
}

/// A pool under the DynamoDB directory whose hub never says withdrawn
/// stops the deploy at the bound, its drain grace and the drain wait,
/// naming the pool, and none of it is deleted.
#[test]
fn a_dynamodb_pool_that_never_says_withdrawn_stops_the_deploy() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.held = Some(0);
    kube.withdrawn = None;
    kube.holds(
        "kind-use1",
        left_dynamodb("model-x-old", "model-x-old", "hub"),
    );
    kube.holds(
        "kind-use1",
        left_dynamodb("model-x-old-engine", "model-x-old", "engine"),
    );
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("a pool that never says withdrawn");
    let error = error.to_string();
    assert!(
        error.contains("`model-x-old`") && error.contains("withdrawn"),
        "{error}"
    );
    assert!(withdrawn_reads(&log, "model-x-old").len() > 1);
    for object in [
        "deployments.apps/model-x-old-engine",
        "deployments.apps/model-x-old",
    ] {
        assert_eq!(deleted_at(&log, "kind-use1", object), None, "{object}");
    }
}

/// A pool under the DynamoDB directory whose hub's Deployment is gone has
/// no hub to drain it or to say that it withdrew, and its parent's 404
/// proves nothing: the deploy stops before it changes anything, naming
/// the pool.
#[test]
fn a_dynamodb_pool_without_its_hub_is_not_deleted() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.held = Some(0);
    kube.holds(
        "kind-use1",
        left_dynamodb("model-x-old-engine", "model-x-old", "engine"),
    );
    let error =
        deploy(&mirror, &files, &mut docker, &mut kube).expect_err("a pool without its hub");
    assert!(error.to_string().contains("`model-x-old`"), "{error}");
    assert_eq!(
        deleted_at(&log, "kind-use1", "deployments.apps/model-x-old-engine"),
        None
    );
    assert!(
        !commands(&log)
            .iter()
            .any(|args| args.iter().any(|a| a.contains("/report/"))),
        "the parent's relay route was asked"
    );
}

/// `down` takes a DynamoDB fleet off its clusters: it drains each pool
/// that has a parent, waits until its own hub says withdrawn, asks no
/// parent for a record, deletes the segments, then the hubs, and the
/// Local and the Job last, after the hubs that use them. Only each
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
    // A global pool has no parent, and its hub refuses a drain (409): it
    // is not drained. Each child is, and is waited for until it says
    // withdrawn.
    let drains: Vec<String> = commands(&log)
        .into_iter()
        .filter_map(|args| {
            args.into_iter()
                .find(|a| a.ends_with("/v1/ext/gdl-pool/drain"))
        })
        .collect();
    assert_eq!(drains.len(), 2, "{drains:?}");
    assert!(
        drains.iter().all(|path| path.contains("model-x")),
        "{drains:?}"
    );
    for pool in ["model-x-us", "model-x-shared"] {
        assert!(!withdrawn_reads(&log, pool).is_empty(), "{pool}");
    }
    let batches = deletes(&log, "kind-use1");
    let at = |object: &str| {
        batches
            .iter()
            .position(|batch| batch.iter().any(|o| o == object))
            .unwrap_or_else(|| panic!("{object} is not deleted: {batches:?}"))
    };
    assert!(at("deployments.apps/global-us-front") < at("deployments.apps/global-us"));
    assert!(at("deployments.apps/global-us") < at("deployments.apps/dynamodb-local"));
    assert!(at("deployments.apps/global-us") < at("jobs.batch/dynamodb-tables"));
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

#[test]
/// With the Local in `euw1`, which `down` reaches first, the Local still
/// goes only after `use1`'s hubs, which use it, are deleted.
fn down_keeps_the_local_until_every_clusters_hubs_stop() {
    let mirror = Mirror::new();
    let text = fleet_text().replace(
        "local = true\ncluster = \"use1\"",
        "local = true\ncluster = \"euw1\"",
    );
    let files = files(&mirror, &text);
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    log.borrow_mut().clear();
    down(&files, &mut kube).expect("down");

    let commands = commands(&log);
    let deletion = |context: &str, object: &str| {
        commands
            .iter()
            .position(|args| {
                args[1] == context
                    && args.iter().any(|arg| arg == "delete")
                    && args.iter().any(|arg| arg == object)
            })
            .unwrap_or_else(|| panic!("{context}: {object} was not deleted"))
    };
    let local = deletion("kind-euw1", "deployments.apps/dynamodb-local");
    for hub in ["deployments.apps/global-us", "deployments.apps/model-x-us"] {
        let stopped = deletion("kind-use1", hub);
        assert!(
            stopped < local,
            "the Local is deleted at command {local}, before {hub} at command {stopped} in kind-use1"
        );
    }
}
