//! `deploy` and `down` where the gdl operator runs a cluster's pools
//! (`use1` in the fixture fleet): one `GdlPool` per pool, deleted alone
//! and with background propagation, children first across the fleet; a
//! pool ready when its status says so; and the objects the operator made
//! left to it. A cluster without the operator (`euw1`) runs as before.

use super::*;

#[path = "operator/after.rs"]
mod after;

#[path = "operator/level.rs"]
mod level;

fn fakes(log: &Log) -> (Docker, Kube) {
    (
        Docker {
            log: Rc::clone(log),
            fail: false,
        },
        Kube::new(log),
    )
}

/// Deploy, with `--allow-pool-restart` when `allow`.
fn deploy_allowing(
    mirror: &Mirror,
    files: &Files,
    kube: &mut Kube,
    log: &Log,
    allow: bool,
) -> Result<Vec<String>, gdl_gen::Error> {
    let mut docker = Docker {
        log: Rc::clone(log),
        fail: false,
    };
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: allow,
    };
    let mut said = Vec::new();
    gdl_gen::deploy(&how, &mut docker, kube, &mut |step| {
        said.push(step.to_string())
    })?;
    Ok(said)
}

/// The kinds of `context`'s objects that carry `pool`'s label.
fn kinds_of(kube: &Kube, context: &str, pool: &str) -> Vec<String> {
    kube.objects[context]
        .values()
        .filter(|object| object["metadata"]["labels"]["gdl.dev/pool"] == pool)
        .map(|object| object["kind"].as_str().expect("a kind").to_string())
        .collect()
}

/// An object the operator made for pool `pool` in `use1`, as it holds it:
/// labelled with the fleet, the cluster and the pool, and controlled by
/// the pool's `GdlPool`.
fn made(kind: &str, name: &str, pool: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": kind,
        "metadata": {
            "name": name, "namespace": "gdl-api",
            "labels": {"gdl.dev/fleet": "api", "gdl.dev/cluster": "use1", "gdl.dev/pool": pool,
                       "gdl.dev/class": "model_x", "gdl.dev/segment": "engine"},
            "ownerReferences": [{"apiVersion": "gdl.dev/v1alpha1", "kind": "GdlPool", "name": pool,
                                 "uid": format!("kind-use1/GdlPool/{pool}"), "controller": true}],
        },
        "spec": {"template": {"spec": {"containers": [{"name": "segment", "env": [{"name": "GDL_POOL__HOME", "value": "global-us"}]}]}}},
    })
}

/// The raw path of `GdlPool` `name`'s delete.
fn pool_path(name: &str) -> String {
    format!("/apis/gdl.dev/v1alpha1/namespaces/gdl-api/gdlpools/{name}")
}

/// Every delete command in `context`, whole.
fn delete_commands(log: &Log, context: &str) -> Vec<Vec<String>> {
    commands(log)
        .into_iter()
        .filter(|args| args[1] == context && args.contains(&"delete".to_string()))
        .collect()
}

/// A cluster the operator runs gets one `GdlPool` per pool, and none of a
/// pool's objects from `deploy`: the operator makes them. Its base holds
/// no PodMonitor, since each pool has its own. The cluster without the
/// operator gets its pools' objects, as before, and a pool there is ready
/// by its hub's health; one in `use1` by its `GdlPool`'s status.
#[test]
fn a_cluster_the_operator_runs_gets_one_gdl_pool_per_pool() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    for pool in ["global-us", "model-x-us"] {
        assert_eq!(kinds_of(&kube, "kind-use1", pool), ["GdlPool"], "{pool}");
        let held = &kube.objects["kind-use1"][&("GdlPool".to_string(), pool.to_string())];
        assert_eq!(held["spec"]["cluster"], "use1");
        assert!(held["spec"].get("presence").is_none());
        assert!(held["spec"].get("allowPoolRestart").is_none());
    }
    assert!(
        !kube.objects["kind-use1"]
            .keys()
            .any(|(kind, _)| kind == "PodMonitor"),
        "the operator makes each pool's PodMonitor"
    );
    assert!(kinds_of(&kube, "kind-euw1", "global-eu").contains(&"Deployment".to_string()));
    let health: Vec<Vec<String>> = commands(&log)
        .into_iter()
        .filter(|args| args.iter().any(|a| a.ends_with("/health")))
        .collect();
    assert!(
        health.iter().all(|args| args[1] == "kind-euw1"),
        "{health:?}"
    );
}

/// A pool's DGD file, named by the fleet file, is read beside it: the
/// applied `GdlPool` holds the DGD, with the `main` image of `Frontend` set
/// to the `engine` segment's pushed image, and places `engine` in it.
#[test]
fn deploy_applies_a_pools_dgd_from_the_fleet_file() {
    let mirror = Mirror::new();
    let fleet = format!(
        "{}[pool.model-x-us.dgd]\nfile = \"dynamo/dgd.yaml\"\ncomponents = {{ engine = \"Frontend\" }}\n",
        fleet_text()
    );
    let files = files(&mirror, &fleet);
    let dynamo = files.fleet.parent().expect("a directory").join("dynamo");
    std::fs::create_dir(&dynamo).expect("a directory");
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/k8s/dgd/dynamo/dgd.yaml");
    let dgd = std::fs::read_to_string(fixture)
        .expect("the DGD")
        .replace("name: dgd-use1", "name: model-x-us");
    std::fs::write(dynamo.join("dgd.yaml"), dgd).expect("the DGD");
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    let held = &kube.objects["kind-use1"][&("GdlPool".to_string(), "model-x-us".to_string())];
    let engine = &held["spec"]["segments"][0];
    assert_eq!(engine["name"], "engine");
    assert_eq!(engine["component"], "Frontend");
    let dgd: Value =
        serde_json::from_str(held["spec"]["dgd"].as_str().expect("the DGD")).expect("JSON");
    assert_eq!(dgd["metadata"]["name"], "model-x-us");
    let main = &dgd["spec"]["components"][0]["podTemplate"]["spec"]["containers"][0];
    assert_eq!(main["image"], engine["image"]);
    assert!(
        engine["image"]
            .as_str()
            .expect("an image")
            .contains("@sha256:"),
        "{engine}"
    );
}

/// A cluster that asks for the operator and has neither of its CRDs is
/// refused before anything is built.
#[test]
fn a_cluster_without_the_operators_crds_is_refused() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.without_scalers = vec!["kind-use1"];
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("refused")
        .to_string();
    assert!(error.contains("gdlpools.gdl.dev"), "{error}");
    assert!(!log.borrow().iter().any(|c| matches!(c, Call::Docker)));
}

/// A pool's first create needs no flag. A later change the operator holds
/// stops the deploy at once, naming the pool and the operator's reason;
/// `--allow-pool-restart` lets it through.
#[test]
fn a_held_restart_names_the_pool_and_its_reason() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (_, mut kube) = fakes(&log);
    kube.operator = Operator::Holds(
        "generation 2 replaces the pool's hub (its image changes from `a` to `b`), and a hub restart drains the pool: nothing of the generation was applied",
    );
    deploy_allowing(&mirror, &files, &mut kube, &log, false).expect("a first create");

    let changed = fleet_text().replace(
        "env = { RUST_LOG = \"info\" }",
        "env = { RUST_LOG = \"debug\" }",
    );
    let files = super::files(&mirror, &changed);
    let error = deploy_allowing(&mirror, &files, &mut kube, &log, false)
        .expect_err("held")
        .to_string();
    assert!(
        error.contains("pool `global-us` in cluster `use1`"),
        "{error}"
    );
    assert!(
        error.contains("its image changes from `a` to `b`"),
        "{error}"
    );
    assert!(error.contains("--allow-pool-restart"), "{error}");

    deploy_allowing(&mirror, &files, &mut kube, &log, true).expect("allowed");
    let held = &kube.objects["kind-use1"][&("GdlPool".to_string(), "global-us".to_string())];
    assert_eq!(held["spec"]["allowPoolRestart"], true);
}

/// `deploy` writes a pool's `presence` only when it is not `serving`, so a
/// redeploy leaves a person's `scaledToZero` in place.
#[test]
fn a_redeploy_keeps_a_hand_patched_presence() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    let key = ("GdlPool".to_string(), "model-x-us".to_string());
    kube.objects
        .get_mut("kind-use1")
        .expect("use1")
        .get_mut(&key)
        .expect("the pool")["spec"]["presence"] = "scaledToZero".into();
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    assert_eq!(
        kube.objects["kind-use1"][&key]["spec"]["presence"],
        "scaledToZero"
    );
}

/// A pool removed from the fleet in a cluster the operator runs goes as
/// its `GdlPool`, deleted alone with background propagation: its
/// finalizer stops it in order. Neither `deploy` nor `down` deletes, or
/// lists for its prune, an object the operator made.
#[test]
fn deploy_and_down_delete_gdl_pools_alone_and_leave_the_operators_objects() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    let mut old =
        kube.objects["kind-use1"][&("GdlPool".to_string(), "model-x-us".to_string())].clone();
    old["metadata"]["name"] = "model-x-old".into();
    old["metadata"]["labels"]["gdl.dev/pool"] = "model-x-old".into();
    kube.holds("kind-use1", old);
    for (kind, name, pool) in [
        ("Deployment", "model-x-old-engine", "model-x-old"),
        ("Deployment", "model-x-us-engine", "model-x-us"),
        ("Service", "model-x-us", "model-x-us"),
    ] {
        kube.holds("kind-use1", made(kind, name, pool));
    }
    log.borrow_mut().clear();
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    let deleted = delete_commands(&log, "kind-use1");
    assert_eq!(deleted.len(), 1, "{deleted:?}");
    assert!(deleted[0].contains(&pool_path("model-x-old")));
    for name in ["model-x-old-engine", "model-x-us-engine"] {
        assert!(
            kube.objects["kind-use1"].contains_key(&("Deployment".to_string(), name.to_string()))
        );
    }

    log.borrow_mut().clear();
    down(&files, &mut kube).expect("down");
    for args in delete_commands(&log, "kind-use1") {
        let objects: Vec<&String> = args
            .iter()
            .filter(|a| a.contains('/') && !a.starts_with("--"))
            .collect();
        assert!(
            objects
                .iter()
                .all(|o| o.starts_with("/apis/gdl.dev/") || o.starts_with("serviceaccounts/")),
            "{args:?}"
        );
    }
    let pools: Vec<String> = delete_commands(&log, "kind-use1")
        .into_iter()
        .flatten()
        .filter(|a| a.contains("/gdlpools/"))
        .collect();
    // The child before its parent.
    assert_eq!(pools, [pool_path("model-x-us"), pool_path("global-us")]);
}

/// A cluster that drops `operator` while the operator still runs pools of
/// the fleet there is refused before anything is applied; and one that
/// sets it over a deploy's own objects, which the operator would not
/// change, is refused too.
#[test]
fn a_cluster_that_changes_its_kind_over_live_pools_is_refused() {
    let mirror = Mirror::new();
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    let operated = files(&mirror, &fleet_text());
    deploy(&mirror, &operated, &mut docker, &mut kube).expect("deploy");
    let plain = files(&mirror, &plain_fleet());
    log.borrow_mut().clear();
    let error = deploy(&mirror, &plain, &mut docker, &mut kube)
        .expect_err("refused")
        .to_string();
    assert!(error.contains("GdlPool `global-us`"), "{error}");
    assert!(error.contains("`operator = true`"), "{error}");
    let applied = commands(&log)
        .iter()
        .any(|args| args.contains(&"apply".to_string()));
    assert!(!applied, "refused before any apply");

    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &plain, &mut docker, &mut kube).expect("deploy");
    log.borrow_mut().clear();
    let error = deploy(&mirror, &operated, &mut docker, &mut kube)
        .expect_err("refused")
        .to_string();
    assert!(error.contains("did not make"), "{error}");
    assert!(error.contains("deployments.apps/global-us"), "{error}");
    let applied = commands(&log)
        .iter()
        .any(|args| args.contains(&"apply".to_string()));
    assert!(!applied, "refused before any apply");
}

/// A pool the operator never acts on, as when it does not run, fails the
/// wait at its bound, saying so.
#[test]
fn a_pool_the_operator_never_acts_on_is_named() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.operator = Operator::Absent;
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("not ready")
        .to_string();
    assert!(
        error.contains("pool `global-us` in cluster `use1`"),
        "{error}"
    );
    assert!(
        error.contains("the operator has not acted on it"),
        "{error}"
    );
}

/// Under the DynamoDB directory a shared pool in a cluster without the
/// operator (`model-x-shared` in `euw1`) serves the global pools of both
/// clusters, one of which the operator runs (`global-us` in `use1`).
/// `down` keeps one order across the fleet: the child stops before either
/// parent, whatever kind of cluster each is in.
#[test]
fn down_stops_a_plain_child_before_its_operated_parent() {
    let fleet = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/k8s/dynamodb/fleet.toml"),
    )
    .expect("the fleet")
    .replacen(
        "platform = \"linux/amd64\"\n",
        "platform = \"linux/amd64\"\noperator = true\n",
        1,
    );
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet);
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    assert_eq!(kinds_of(&kube, "kind-use1", "global-us"), ["GdlPool"]);
    log.borrow_mut().clear();
    down(&files, &mut kube).expect("down");
    let calls = commands(&log);
    let at = |context: &str, object: &str| {
        calls
            .iter()
            .position(|args| {
                args[1] == context
                    && args.contains(&"delete".to_string())
                    && args.iter().any(|a| a == object)
            })
            .unwrap_or_else(|| panic!("no delete of {object} in {context}: {calls:?}"))
    };
    let child = at("kind-euw1", "deployments.apps/model-x-shared-engine");
    let operated_child = at("kind-use1", &pool_path("model-x-us"));
    let operated_parent = at("kind-use1", &pool_path("global-us"));
    let plain_parent = at("kind-euw1", "deployments.apps/global-eu-front");
    assert!(child < operated_parent, "{calls:?}");
    assert!(operated_child < operated_parent, "{calls:?}");
    assert!(child < plain_parent, "{calls:?}");
}

/// A pool whose `GdlPool` is gone when the wait reads it is named as such
/// at the wait's end, not taken for an unreadable answer.
#[test]
fn a_pool_whose_gdl_pool_is_gone_is_named() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.operator = Operator::Deletes;
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("not ready")
        .to_string();
    assert!(
        error.contains("pool `global-us` in cluster `use1`: its GdlPool does not exist"),
        "{error}"
    );
}

/// A cluster without the operator and without the gdl CRDs needs no right
/// on GdlPools: `deploy` and `down` make no GdlPool call there. Where the
/// kind exists and the deployer may not list it, both refuse before
/// anything changes, naming the right: the list is what shows that the
/// operator runs no pool of the fleet there.
#[test]
fn a_plain_cluster_needs_no_gdl_pool_rights() {
    let mirror = Mirror::new();
    let files = files(&mirror, &plain_fleet());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.without_scalers = vec!["kind-use1", "kind-euw1"];
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    down(&files, &mut kube).expect("down");
    let calls = commands(&log);
    assert!(
        calls
            .iter()
            .all(|args| args.iter().all(|a| !a.contains("gdlpools"))),
        "{calls:?}"
    );

    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.forbidden_pools = vec!["kind-use1", "kind-euw1"];
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("refused")
        .to_string();
    assert!(error.contains("may not list `gdlpools.gdl.dev`"), "{error}");
    let has = |args: &[String], word: &str| args.iter().any(|a| a == word);
    let changed = commands(&log).iter().any(|args| {
        ["apply", "delete", "label"].iter().any(|w| has(args, w))
            || (has(args, "create") && !has(args, "can-i"))
    });
    assert!(!changed, "refused before anything changed");
    let error = down(&files, &mut kube).expect_err("refused").to_string();
    assert!(error.contains("may not list `gdlpools.gdl.dev`"), "{error}");
}

/// A cluster the operator runs needs to list its GdlPools: a 403 there
/// stops the deploy.
#[test]
fn an_operated_cluster_needs_its_gdl_pool_rights() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.forbidden_pools = vec!["kind-use1"];
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("refused")
        .to_string();
    assert!(error.contains("may not list `gdlpools.gdl.dev`"), "{error}");
}

#[test]
fn an_opt_out_with_renamed_pools_is_refused_when_the_gdl_pool_list_is_forbidden() {
    let mirror = Mirror::new();
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    let operated = files(&mirror, &fleet_text());
    deploy(&mirror, &operated, &mut docker, &mut kube).expect("operated deploy");
    for (kind, name, pool) in [
        ("Deployment", "global-us", "global-us"),
        ("Deployment", "model-x-us-engine", "model-x-us"),
    ] {
        kube.holds("kind-use1", made(kind, name, pool));
    }
    let next = plain_fleet()
        .replace("global-us", "global-next")
        .replace("model-x-us", "model-x-next");
    let plain = files(&mirror, &next);
    log.borrow_mut().clear();
    deploy(&mirror, &plain, &mut docker, &mut kube)
        .expect_err("kind change is refused with list rights");
    assert!(
        commands(&log)
            .iter()
            .all(|args| !args.contains(&"apply".to_string()))
    );
    kube.forbidden_pools = vec!["kind-use1"];
    log.borrow_mut().clear();

    let result = deploy(&mirror, &plain, &mut docker, &mut kube);
    assert!(
        result.is_err(),
        "the kind change must be refused while the operator still owns global-us and model-x-us; deploy returned {result:?}"
    );
    assert!(
        commands(&log)
            .iter()
            .all(|args| !args.contains(&"apply".to_string())),
        "the refusal must precede every apply"
    );
}

#[test]
fn down_leaves_a_gdl_pool_recreated_by_another_fleet_after_the_list() {
    struct Recreated {
        kube: Kube,
        replaced: bool,
    }

    impl gdl_gen::Kube for Recreated {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if !self.replaced
                && args[1] == "kind-use1"
                && args.contains(&"delete".to_string())
                && args.contains(&pool_path("model-x-us"))
            {
                let key = ("GdlPool".to_string(), "model-x-us".to_string());
                let object = self
                    .kube
                    .objects
                    .get_mut("kind-use1")
                    .expect("cluster")
                    .get_mut(&key)
                    .expect("listed pool");
                object["metadata"]["uid"] = "replacement-pool".into();
                object["metadata"]["labels"]["gdl.dev/fleet"] = "another-fleet".into();
                object["spec"]["fleet"] = "another-fleet".into();
                self.replaced = true;
            }
            gdl_gen::Kube::run(&mut self.kube, args, input)
        }
    }

    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    let mut raced = Recreated {
        kube,
        replaced: false,
    };
    let result = gdl_gen::down(&files.fleet, &waits(), &mut raced, &mut |_| {});
    assert!(raced.replaced, "the pool was recreated after the list");
    let key = ("GdlPool".to_string(), "model-x-us".to_string());
    let replacement = raced.kube.objects["kind-use1"].get(&key);
    assert!(
        replacement.is_some_and(|object| object["metadata"]["uid"] == "replacement-pool"),
        "down deleted another fleet's replacement pool; result={result:?}, replacement={replacement:?}",
    );
}

#[test]
fn a_write_ahead_status_does_not_finish_the_deploy_before_the_apply() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (_, mut kube) = fakes(&log);
    deploy_allowing(&mirror, &files, &mut kube, &log, false).expect("first deployment is ready");
    kube.operator = Operator::Applying;

    let result = deploy_allowing(&mirror, &files, &mut kube, &log, true);
    let key = ("GdlPool".to_string(), "global-us".to_string());
    let pool = &kube.objects["kind-use1"][&key];
    assert_eq!(pool["metadata"]["generation"], 2);
    assert_eq!(pool["status"]["observedGeneration"], 2);
    assert_eq!(pool["status"]["conditions"][0]["type"], "Applied");
    assert_eq!(pool["status"]["conditions"][0]["status"], "False");
    assert!(
        result.is_err(),
        "the operator stopped before applying generation 2, but deploy reported success: {result:?}",
    );
}

fn assert_parent_survives_child_conflict(prune: bool) {
    struct Recreated {
        kube: Kube,
        replaced: bool,
    }

    impl gdl_gen::Kube for Recreated {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if !self.replaced
                && args[1] == "kind-use1"
                && args.contains(&"delete".to_string())
                && args.contains(&pool_path("model-x-us"))
            {
                let object = self
                    .kube
                    .objects
                    .get_mut("kind-use1")
                    .expect("cluster")
                    .get_mut(&("GdlPool".into(), "model-x-us".into()))
                    .expect("child pool");
                // The new child still belongs to this fleet and needs
                // global-us. Its replacement is not a completed stop.
                object["metadata"]["uid"] = "replacement-child".into();
                self.replaced = true;
            }
            gdl_gen::Kube::run(&mut self.kube, args, input)
        }
    }

    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    let mut raced = Recreated {
        kube,
        replaced: false,
    };
    let result = if prune {
        let mut next: toml::Table = toml::from_str(&fleet_text()).expect("fleet");
        let pools = next["pool"].as_table_mut().expect("pools");
        pools.remove("global-us");
        pools.remove("model-x-us");
        let next = super::files(&mirror, &toml::to_string(&next).expect("fleet text"));
        let how = gdl_gen::Deploy {
            spec: next.spec,
            fleet: next.fleet,
            target: mirror.target(),
            out: None,
            waits: waits(),
            sources: gdl_gen::Sources::Fetched,
            allow_pool_restart: false,
        };
        gdl_gen::deploy(&how, &mut docker, &mut raced, &mut |_| {})
    } else {
        gdl_gen::down(&files.fleet, &waits(), &mut raced, &mut |_| {})
    };
    assert!(raced.replaced, "replace the child after it is listed");
    let error = result
        .as_ref()
        .expect_err("a replaced child fails the run")
        .to_string();
    assert!(
        error.contains("pool `model-x-us` in cluster `use1`")
            && error.contains("replacement-child"),
        "{error}"
    );
    let held = &raced.kube.objects["kind-use1"];
    if held.contains_key(&("GdlPool".into(), "model-x-us".into())) {
        assert!(
            held.contains_key(&("GdlPool".into(), "global-us".into())),
            "the replacement child still needs global-us, but its parent was deleted: prune={prune}, result={result:?}",
        );
    }
}

#[test]
fn a_recreated_child_keeps_its_parent_while_down_leaves_the_child_running() {
    assert_parent_survives_child_conflict(false);
}

#[test]
fn a_recreated_child_keeps_its_parent_while_prune_leaves_the_child_running() {
    assert_parent_survives_child_conflict(true);
}
