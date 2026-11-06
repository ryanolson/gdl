//! `deploy` and `down` of a fleet under the etcd directory, in one etcd
//! that cluster `use1` runs: the directory's cluster's base goes first,
//! with etcd, which is waited for until it is ready; its pod IP replaces
//! the placeholder in the other cluster's hubs; no Job runs. A removed
//! pool is deleted only once its own hub says withdrawn, and `down`
//! deletes etcd, and its claim, after every hub.

use super::*;

const ETCD_FLEET: &str = "tests/fixtures/k8s/etcd/fleet.toml";
const PLACEHOLDER: &str = "ETCD_POD_IP";

fn fleet_text() -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(ETCD_FLEET))
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

/// What each kubectl call did, by its context: `base`, `etcd` (etcd's pods
/// read), `pools`, and `wait-namespace`.
fn order(log: &Log) -> Vec<String> {
    let mut order: Vec<String> = Vec::new();
    for call in log.borrow().iter() {
        let Call::Kube { args, input } = call else {
            continue;
        };
        let has = |word: &str| args.iter().any(|a| a == word);
        let what = if has("apply") && input.contains("\"ServiceAccount\"") {
            "base"
        } else if has("apply") {
            "pools"
        } else if has("pods") && args.iter().any(|a| a.contains("gdl.dev/directory=etcd")) {
            "etcd"
        } else if has("jobs.batch") {
            "job"
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
                    && !input.contains("\"ServiceAccount\"") =>
            {
                Some(input.clone())
            }
            _ => None,
        })
        .collect()
}

/// The directory's cluster goes first: its base, with etcd and its claim,
/// then a wait until etcd is ready, and only then the other cluster's
/// base. No Job runs. The other cluster's hubs name etcd's pod IP; its
/// own, etcd's Service; no placeholder is applied.
#[test]
fn etcd_comes_before_every_pool_and_its_ip_fills_the_other_clusters() {
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
            "kind-use1:etcd",
            "kind-euw1:base",
            "kind-euw1:wait-namespace",
            "kind-euw1:pools",
            "kind-use1:pools",
        ]
    );
    let euw1 = pools_applied(&log, "kind-euw1");
    assert!(euw1.contains("http://10.1.2.3:2379"), "{euw1}");
    assert!(!euw1.contains(PLACEHOLDER), "{euw1}");
    let use1 = pools_applied(&log, "kind-use1");
    assert!(use1.contains("http://etcd:2379"), "{use1}");
    assert!(!use1.contains(PLACEHOLDER), "{use1}");
}

/// An etcd that never becomes ready stops the deploy, naming it, before
/// any other cluster's objects or any pool are applied. The wait is the
/// fakes' objects bound, 5 s.
#[test]
fn an_etcd_that_is_never_ready_stops_the_deploy() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.local_pods = json!({"items": [local_pod(false)]});
    let error =
        deploy(&mirror, &files, &mut docker, &mut kube).expect_err("an etcd that is never ready");
    assert!(error.to_string().contains("etcd `etcd`"), "{error}");
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

/// An etcd whose rollout never finishes stops the deploy, naming it,
/// before its pod's IP is read or any pool is applied. The wait reads the
/// Deployment, which needs only `get`, and is the fakes' objects bound.
#[test]
fn an_etcd_whose_rollout_never_finishes_stops_the_deploy() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.stalled = true;
    let error = deploy(&mirror, &files, &mut docker, &mut kube).expect_err("a stalled rollout");
    assert!(
        error
            .to_string()
            .contains("etcd `etcd` did not finish its rollout"),
        "{error}"
    );
    let order = order(&log);
    assert!(
        !order.iter().any(|step| step.ends_with(":etcd")),
        "{order:?}"
    );
    assert!(
        !order.iter().any(|step| step.ends_with(":pools")),
        "{order:?}"
    );
    assert!(
        !commands(&log)
            .iter()
            .any(|args| args.iter().any(|a| a == "rollout" || a == "--watch")),
        "a watch"
    );
}

/// A pool left by an earlier deploy under the etcd directory, as its
/// cluster holds it.
fn left_etcd(name: &str, pool: &str, segment: &str) -> Value {
    let mut object = left("Deployment", name, pool, segment, Some("global-us"));
    object["spec"]["template"]["spec"]["containers"][0]["env"]
        .as_array_mut()
        .expect("env")
        .push(json!({"name": "GDL_POOL__DIRECTORY", "value": "etcd"}));
    object
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

/// A pool under the etcd directory that the fleet no longer places is
/// drained, and deleted only once its own hub says its record is
/// withdrawn: no parent's hub holds it, so its parent's 404 proves
/// nothing.
#[test]
fn a_removed_etcd_pool_is_deleted_only_once_its_own_hub_says_withdrawn() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.held = Some(0);
    kube.withdrawn = Some(2);
    kube.holds("kind-use1", left_etcd("model-x-old", "model-x-old", "hub"));
    kube.holds(
        "kind-use1",
        left_etcd("model-x-old-engine", "model-x-old", "engine"),
    );
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    assert!(
        !commands(&log)
            .iter()
            .any(|args| args.iter().any(|a| a.contains("/report/"))),
        "the parent's relay route was asked"
    );
    let reads = withdrawn_reads(&log, "model-x-old");
    assert_eq!(reads.len(), 3, "two false, then true: {reads:?}");
    let deleted = commands(&log)
        .iter()
        .position(|args| {
            args[1] == "kind-use1"
                && args.iter().any(|a| a == "delete")
                && args.iter().any(|a| a == "deployments.apps/model-x-old")
        })
        .expect("deleted");
    assert!(deleted > reads[2], "deleted before the pool said withdrawn");
}

/// `down` deletes etcd's Deployment, Service, and claim after every hub
/// that uses it.
#[test]
fn down_deletes_etcd_after_the_hubs() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    log.borrow_mut().clear();
    down(&files, &mut kube).expect("down");
    let at = |object: &str| {
        commands(&log)
            .iter()
            .position(|args| {
                args[1] == "kind-use1"
                    && args.iter().any(|a| a == "delete")
                    && args.iter().any(|a| a == object)
            })
            .unwrap_or_else(|| panic!("{object} is not deleted"))
    };
    for object in [
        "deployments.apps/etcd",
        "services/etcd",
        "persistentvolumeclaims/etcd-data",
    ] {
        assert!(at("deployments.apps/global-us") < at(object), "{object}");
    }
}

/// A redeploy that changes etcd's pod template gives the other clusters
/// the new pod's IP, not the old one's: applying the change does not retire
/// the old pod at once, which stays ready, at its IP, until the
/// Deployment controller starts the Recreate rollout. This fake shows the
/// new pod only once the deploy has waited for the rollout.
#[test]
fn a_changed_etcd_is_reached_at_its_new_pods_ip() {
    struct Replacing {
        kube: Kube,
        changed: bool,
        observed: bool,
    }

    impl gdl_gen::Kube for Replacing {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if args.iter().any(|a| a == "apply")
                && let Some(input) = input
            {
                let input = std::str::from_utf8(input).expect("JSON");
                for doc in input.split("---\n").filter(|doc| !doc.trim().is_empty()) {
                    let object: Value = serde_json::from_str(doc).expect("an object");
                    if object["kind"] == "Deployment" && object["metadata"]["name"] == "etcd" {
                        let held = &self.kube.objects["kind-use1"]
                            [&("Deployment".to_string(), "etcd".to_string())];
                        self.changed = held["spec"]["template"] != object["spec"]["template"];
                    }
                }
            }
            // The deploy reads the Deployment's rollout: from then on the
            // controller has replaced the pod.
            if args.iter().any(|a| a == "deployments.apps/etcd") {
                self.observed = true;
            }
            if self.changed
                && args.iter().any(|a| a == "pods")
                && args.iter().any(|a| a.contains("gdl.dev/directory=etcd"))
            {
                let mut pod = local_pod(true);
                pod["metadata"]["name"] = if self.observed {
                    "etcd-current"
                } else {
                    "etcd-preceding"
                }
                .into();
                pod["metadata"]["namespace"] = "gdl-api".into();
                pod["metadata"]["labels"] = json!({
                    "gdl.dev/fleet": "api",
                    "gdl.dev/cluster": "use1",
                    "gdl.dev/directory": "etcd",
                    "pod-template-hash": if self.observed { "current" } else { "preceding" },
                });
                pod["status"]["podIP"] = if self.observed {
                    "10.1.2.4"
                } else {
                    "10.1.2.3"
                }
                .into();
                self.kube.local_pods = json!({"items": [pod]});
                // The controller observes the updated Deployment after
                // this API read; the next read sees its replacement.
                self.observed = true;
            }
            self.kube.run(args, input)
        }
    }

    let mirror = Mirror::new();
    let text = fleet_text();
    let initial = files(&mirror, &text);
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &initial, &mut docker, &mut kube).expect("initial deployment");
    log.borrow_mut().clear();

    let changed = files(&mirror, &text.replace("cpu = \"100m\"", "cpu = \"200m\""));
    let mut kube = Replacing {
        kube,
        changed: false,
        observed: false,
    };
    let how = gdl_gen::Deploy {
        spec: changed.spec.clone(),
        fleet: changed.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: false,
        images: None,
        replace: false,
    };
    gdl_gen::deploy(&how, &mut docker, &mut kube, &mut NoSlurm, &mut |_| {}).expect("redeploy");
    assert!(kube.changed, "the resources change must replace etcd's pod");
    let remote = pools_applied(&log, "kind-euw1");
    assert!(
        remote.contains("http://10.1.2.4:2379") && !remote.contains("http://10.1.2.3:2379"),
        "remote hubs were given the preceding etcd pod's address"
    );
}

/// An etcd pool whose hub never says withdrawn stops the deploy at its
/// bound, and the message names etcd, the pool's directory, not another.
#[test]
fn an_etcd_pool_that_never_says_withdrawn_names_etcd() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.held = Some(0);
    kube.withdrawn = None;
    kube.holds("kind-use1", left_etcd("model-x-old", "model-x-old", "hub"));
    kube.holds(
        "kind-use1",
        left_etcd("model-x-old-engine", "model-x-old", "engine"),
    );
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("a pool that never says withdrawn")
        .to_string();
    assert!(
        error.contains("`model-x-old`") && error.contains("withdrawn from the etcd directory"),
        "{error}"
    );
    assert!(!error.contains("DynamoDB"), "{error}");
}

/// An etcd pool whose hub's Deployment is gone is refused before anything
/// changes, and the refusal names etcd, the pool's directory.
#[test]
fn an_etcd_pool_without_its_hub_names_etcd() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    kube.held = Some(0);
    kube.holds(
        "kind-use1",
        left_etcd("model-x-old-engine", "model-x-old", "engine"),
    );
    let error = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("a pool without its hub")
        .to_string();
    assert!(
        error.contains("`model-x-old`") && error.contains("withdrawn from the etcd directory"),
        "{error}"
    );
    assert!(!error.contains("DynamoDB"), "{error}");
}
