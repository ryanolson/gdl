//! `deploy` and `down` against a fake Docker and a fake kubectl. The fake
//! kubectl records each command line and its stdin, and keeps each
//! context's objects as a cluster would: what is applied is listed, and
//! what is deleted is gone. So these tests read the order of the steps, the
//! stop at the first failure, what a prune drains and deletes, and that no
//! command line carries a credential. `scripts/kind-fleet.sh` runs the
//! real kubectl against a kind cluster.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use serde_json::{Value, json};

use super::k8s::{MARKER, deployment, fleet_text};
use super::{Mirror, SEGMENTS, SPLIT};

/// One call to the fake Docker or the fake kubectl, in the order made.
#[derive(Debug, Clone)]
enum Call {
    Docker,
    Kube { args: Vec<String>, input: String },
}

type Log = Rc<RefCell<Vec<Call>>>;

/// A Docker whose registries hold nothing: each image is built and pushed.
struct Docker {
    log: Log,
    fail: bool,
}

impl gdl_gen::Docker for Docker {
    fn digest(&mut self, _reference: &str) -> Result<Option<String>, gdl_gen::Error> {
        Ok(None)
    }

    fn build(
        &mut self,
        _context: &Path,
        _platform: &str,
        reference: &str,
        _push: bool,
    ) -> Result<String, gdl_gen::Error> {
        assert!(reference.starts_with("registry.example/"), "{reference}");
        self.log.borrow_mut().push(Call::Docker);
        if self.fail {
            return Err(gdl_gen::Error::from(gdl_model::Problems::from(
                gdl_model::Problem::from("the build failed".to_string()),
            )));
        }
        Ok(format!("sha256:{:064x}", self.log.borrow().len()))
    }
}

/// A kubectl over clusters held in memory, one per context. Each answers
/// as the fields below say.
struct Kube {
    log: Log,
    /// Each context's objects, by kind and name, as JSON.
    objects: BTreeMap<String, BTreeMap<(String, String), Value>>,
    /// What `auth can-i` prints.
    can_i: &'static str,
    /// The command whose first word after the context and namespace is
    /// this one fails: `apply`, `delete`, ...
    fail: Option<&'static str>,
    /// How many reads of a parent's record answer 200 before it answers
    /// 404; `None`: always 200.
    held: Option<usize>,
    /// How many health reads fail before each answers 200; `None`: never.
    health: Option<usize>,
    /// What `get pods` and `get events` print.
    pods: Value,
    events: Value,
}

impl Kube {
    fn new(log: &Log) -> Self {
        Self {
            log: Rc::clone(log),
            objects: BTreeMap::new(),
            can_i: "yes",
            fail: None,
            held: Some(1),
            health: Some(0),
            pods: json!({"items": []}),
            events: json!({"items": []}),
        }
    }

    /// Put `object` in `context` as if an earlier deploy had applied it.
    fn holds(&mut self, context: &str, object: Value) {
        let key = key(&object);
        self.objects
            .entry(context.to_string())
            .or_default()
            .insert(key, object);
    }
}

fn key(object: &Value) -> (String, String) {
    (
        object["kind"].as_str().expect("a kind").to_string(),
        object["metadata"]["name"]
            .as_str()
            .expect("a name")
            .to_string(),
    )
}

impl gdl_gen::Kube for Kube {
    fn run(
        &mut self,
        args: &[String],
        input: Option<&[u8]>,
    ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        let input = input.map(|i| String::from_utf8_lossy(i).into_owned());
        self.log.borrow_mut().push(Call::Kube {
            args: args.to_vec(),
            input: input.clone().unwrap_or_default(),
        });
        assert_eq!(args[0], "--context", "{args:?}");
        let context = args[1].clone();
        let mut rest = &args[2..];
        if rest.first().map(String::as_str) == Some("--namespace") {
            rest = &rest[2..];
        }
        let verb = rest[0].as_str();
        if self.fail == Some(verb) {
            return Ok(gdl_gen::Reply::failed(format!("error: {verb} refused")));
        }
        let raw = rest
            .iter()
            .position(|a| a == "--raw")
            .map(|i| rest[i + 1].as_str());
        let reply = match (verb, raw) {
            ("auth", _) => gdl_gen::Reply::ok(format!("{}\n", self.can_i)),
            ("apply", _) => {
                for document in input.unwrap_or_default().split("---\n") {
                    if document.trim().is_empty() {
                        continue;
                    }
                    let object: Value = serde_json::from_str(document).expect("JSON");
                    self.holds(&context, object);
                }
                gdl_gen::Reply::ok("")
            }
            ("wait" | "label", _) => gdl_gen::Reply::ok(""),
            ("create", Some(path)) => {
                assert!(path.ends_with("/proxy/v1/ext/gdl-pool/drain"), "{path}");
                gdl_gen::Reply::ok("{\"state\":\"draining\"}")
            }
            ("get", Some(path)) if path.ends_with("/v1/ext/gdl-pool/health") => {
                match &mut self.health {
                    Some(0) => gdl_gen::Reply::ok("{\"state\":\"ready\"}"),
                    Some(n) => {
                        *n -= 1;
                        gdl_gen::Reply::failed("Error from server (ServiceUnavailable)")
                    }
                    None => gdl_gen::Reply::failed("Error from server (ServiceUnavailable)"),
                }
            }
            ("get", Some(path)) if path.contains("/v1/ext/gdl-pool/report/") => {
                match &mut self.held {
                    Some(0) => gdl_gen::Reply::failed(
                        "Error from server (NotFound): the server could not find the requested resource",
                    ),
                    Some(n) => {
                        *n -= 1;
                        gdl_gen::Reply::ok("{}")
                    }
                    None => gdl_gen::Reply::ok("{}"),
                }
            }
            ("get", None) if rest[1] == "pods" => gdl_gen::Reply::ok(self.pods.to_string()),
            ("get", None) if rest[1] == "events" => gdl_gen::Reply::ok(self.events.to_string()),
            ("get", None) => {
                let items: Vec<Value> = self
                    .objects
                    .get(&context)
                    .into_iter()
                    .flat_map(|objects| objects.values())
                    .filter(|object| object["kind"] != "Namespace")
                    .cloned()
                    .collect();
                gdl_gen::Reply::ok(json!({"kind": "List", "items": items}).to_string())
            }
            ("delete", _) => {
                for reference in rest
                    .iter()
                    .filter(|a| !a.starts_with("--") && *a != "delete")
                {
                    let (kind, name) = reference.split_once('/').expect("kind/name");
                    let objects = self.objects.entry(context.clone()).or_default();
                    let before = objects.len();
                    objects.retain(|(k, n), _| !(k.eq_ignore_ascii_case(kind) && n == name));
                    assert_eq!(objects.len() + 1, before, "{reference} is not held");
                }
                gdl_gen::Reply::ok("")
            }
            _ => panic!("an unexpected command: {args:?}"),
        };
        Ok(reply)
    }
}

/// The spec and fleet files of a deploy: the api example in two classes,
/// with a published port, a secret, and a drain bound, and the fleet of
/// two clusters that the k8s tests render.
struct Files {
    _dir: tempfile::TempDir,
    spec: PathBuf,
    fleet: PathBuf,
}

fn files(mirror: &Mirror, fleet: &str) -> Files {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("spec");
    let graph = root.join("examples/api/api.gdl");
    let text = mirror
        .portable(&text)
        .replace(SEGMENTS, &format!("{SPLIT}{}", deployment()))
        .replace(
            "path = \"api.gdl\"",
            &format!("path = {:?}", graph.display().to_string()),
        );
    let dir = tempfile::Builder::new()
        .prefix("gdl-deploy-")
        .tempdir()
        .expect("a directory");
    let spec = dir.path().join("gdl.toml");
    std::fs::write(&spec, text).expect("the spec");
    let path = dir.path().join("fleet.toml");
    std::fs::write(&path, fleet).expect("the fleet");
    Files {
        _dir: dir,
        spec,
        fleet: path,
    }
}

/// Waits that never sleep long: the fakes answer at once.
fn waits() -> gdl_gen::Waits {
    gdl_gen::Waits {
        interval: Duration::from_millis(1),
        health: Duration::from_millis(200),
        drain: Duration::from_millis(200),
        objects: Duration::from_secs(5),
    }
}

fn deploy(
    mirror: &Mirror,
    files: &Files,
    docker: &mut Docker,
    kube: &mut Kube,
) -> Result<Vec<String>, gdl_gen::Error> {
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
    };
    let mut said = Vec::new();
    gdl_gen::deploy(&how, docker, kube, &mut |step| said.push(step.to_string()))?;
    Ok(said)
}

fn down(files: &Files, kube: &mut Kube) -> Result<Vec<String>, gdl_gen::Error> {
    let mut said = Vec::new();
    gdl_gen::down(&files.fleet, &waits(), kube, &mut |step| {
        said.push(step.to_string())
    })?;
    Ok(said)
}

/// Each call as one word for its step: `docker`, or kubectl's context and
/// what it did.
fn steps(log: &Log) -> Vec<String> {
    let mut steps: Vec<String> = Vec::new();
    for call in log.borrow().iter() {
        let step = match call {
            Call::Docker => "docker".to_string(),
            Call::Kube { args, input } => {
                let context = &args[1];
                let what = if args.contains(&"can-i".to_string()) {
                    "can-i"
                } else if args.contains(&"apply".to_string()) && input.contains("\"Namespace\"") {
                    "base"
                } else if args.contains(&"apply".to_string()) {
                    "pools"
                } else if args.contains(&"wait".to_string()) {
                    "wait-namespace"
                } else if args.iter().any(|a| a.ends_with("/health")) {
                    "health"
                } else if args.iter().any(|a| a.ends_with("/drain")) {
                    "drain"
                } else if args.iter().any(|a| a.contains("/report/")) {
                    "held"
                } else if args.contains(&"delete".to_string()) {
                    "delete"
                } else if args.contains(&"label".to_string()) {
                    "unlabel"
                } else if args.contains(&"get".to_string()) {
                    "list"
                } else {
                    "other"
                };
                format!("{context}:{what}")
            }
        };
        // Repeats of one step read as one.
        if steps.last() != Some(&step) {
            steps.push(step);
        }
    }
    steps
}

/// The command lines kubectl ran, in order.
fn commands(log: &Log) -> Vec<Vec<String>> {
    log.borrow()
        .iter()
        .filter_map(|call| match call {
            Call::Kube { args, .. } => Some(args.clone()),
            Call::Docker => None,
        })
        .collect()
}

/// The deletes in `context`, each as its objects.
fn deletes(log: &Log, context: &str) -> Vec<Vec<String>> {
    commands(log)
        .into_iter()
        .filter(|args| args[1] == context && args.contains(&"delete".to_string()))
        .map(|args| {
            args.into_iter()
                .filter(|a| a.contains('/') && !a.starts_with("--"))
                .collect()
        })
        .collect()
}

/// A pool's object as an earlier deploy left it: labelled with the fleet,
/// the pool, its class, and its segment, and for a hub's Deployment the
/// pool's parent.
fn left(kind: &str, name: &str, pool: &str, segment: &str, home: Option<&str>) -> Value {
    let mut object = json!({
        "apiVersion": "v1",
        "kind": kind,
        "metadata": {
            "name": name,
            "namespace": "gdl-api",
            "labels": {
                "gdl.dev/fleet": "api",
                "gdl.dev/pool": pool,
                "gdl.dev/class": "model_x",
                "gdl.dev/segment": segment,
            },
        },
    });
    if kind == "Deployment" {
        let mut env = vec![json!({"name": "GDL_POOL__NAME", "value": pool})];
        env.extend(home.map(|home| json!({"name": "GDL_POOL__HOME", "value": home})));
        object["spec"] = json!({"template": {"spec": {
            "terminationGracePeriodSeconds": 0,
            "containers": [{"name": "c", "env": env}],
        }}});
    }
    object
}

/// `deploy` checks, builds and pushes, renders, applies each cluster's
/// base and waits for its namespace, applies its pools, prunes, and waits
/// for health, in that order.
#[test]
fn a_deploy_runs_its_steps_in_order() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = (
        Docker {
            log: Rc::clone(&log),
            fail: false,
        },
        Kube::new(&log),
    );
    let said = deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    assert_eq!(
        steps(&log),
        [
            "kind-euw1:can-i",
            "kind-use1:can-i",
            "docker",
            "kind-euw1:base",
            "kind-euw1:wait-namespace",
            "kind-use1:base",
            "kind-use1:wait-namespace",
            "kind-euw1:pools",
            "kind-use1:pools",
            "kind-euw1:list",
            "kind-use1:list",
            "kind-euw1:health",
            "kind-use1:health",
            "kind-euw1:health",
            "kind-use1:health",
        ]
    );
    // Each pool's health is read through the service proxy, in its
    // cluster's namespace.
    let health: Vec<String> = commands(&log)
        .into_iter()
        .filter(|args| args.iter().any(|a| a.ends_with("/health")))
        .map(|args| args[4].clone())
        .collect();
    for pool in ["global-eu", "model-x-eu", "global-us", "model-x-us"] {
        let path = format!(
            "/api/v1/namespaces/gdl-api/services/http:{pool}:public/proxy/v1/ext/gdl-pool/health"
        );
        assert!(health.contains(&path), "{health:?}");
    }
    // Nothing was pruned: every object it lists, it rendered.
    assert!(deletes(&log, "kind-use1").is_empty() && deletes(&log, "kind-euw1").is_empty());
    assert_eq!(said.len(), 5, "{said:?}");
}

/// The first step that fails stops the deploy: nothing after it runs.
/// Six deploys, each of which prepares the spec's generated crates for its
/// check and render: about 10 s.
#[test]
fn a_deploy_stops_at_the_first_failure() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let run = |docker_fails: bool, setup: &dyn Fn(&mut Kube)| {
        let log = Log::default();
        let mut docker = Docker {
            log: Rc::clone(&log),
            fail: docker_fails,
        };
        let mut kube = Kube::new(&log);
        setup(&mut kube);
        let error = deploy(&mirror, &files, &mut docker, &mut kube).expect_err("a failure");
        (steps(&log), error.to_string())
    };

    // A context that may not create Deployments: nothing is built.
    let (steps_seen, error) = run(false, &|kube| kube.can_i = "no");
    assert_eq!(steps_seen, ["kind-euw1:can-i"], "{error}");
    assert!(
        error.contains("may not create Deployments in namespace `gdl-api`"),
        "{error}"
    );

    // A failed build: nothing is applied.
    let (steps_seen, _) = run(true, &|_| {});
    assert_eq!(steps_seen, ["kind-euw1:can-i", "kind-use1:can-i", "docker"]);

    // A failed apply of the first base: no other cluster is touched.
    let (steps_seen, error) = run(false, &|kube| kube.fail = Some("apply"));
    assert_eq!(
        steps_seen.last().map(String::as_str),
        Some("kind-euw1:base"),
        "{steps_seen:?}"
    );
    assert!(error.contains("applying to cluster `euw1`"), "{error}");

    // A namespace that never becomes active: no pool is applied.
    let (steps_seen, _) = run(false, &|kube| kube.fail = Some("wait"));
    assert_eq!(
        steps_seen.last().map(String::as_str),
        Some("kind-euw1:wait-namespace")
    );

    // A list that fails: no health is read.
    let (steps_seen, error) = run(false, &|kube| kube.fail = Some("get"));
    assert_eq!(
        steps_seen.last().map(String::as_str),
        Some("kind-euw1:list"),
        "{error}"
    );

    // A fleet that fails the check: neither Docker nor kubectl runs.
    let broken = self::files(
        &mirror,
        &fleet_text().replace("cluster = \"use1\"", "cluster = \"nowhere\""),
    );
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    let error = deploy(&mirror, &broken, &mut docker, &mut kube).expect_err("a bad fleet");
    assert!(log.borrow().is_empty(), "{:?}", log.borrow());
    assert!(error.to_string().contains("nowhere"), "{error}");
}

/// A pool that the fleet no longer places is drained, and deleted only
/// once its parent's hub no longer holds its record: segments first,
/// then its hub, then the rest. A pool with no parent is not drained.
/// Nothing the render wrote is deleted, nor any namespace.
#[test]
fn a_prune_drains_a_removed_pool_and_deletes_only_what_was_not_rendered() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = Some(2);
    for object in [
        left(
            "Deployment",
            "model-x-old",
            "model-x-old",
            "hub",
            Some("global-us"),
        ),
        left(
            "Deployment",
            "model-x-old-engine",
            "model-x-old",
            "engine",
            None,
        ),
        left("Service", "model-x-old", "model-x-old", "hub", None),
        left("ConfigMap", "model-x-old", "model-x-old", "hub", None),
        left("Deployment", "global-old", "global-old", "hub", None),
        left(
            "PersistentVolumeClaim",
            "global-old-state",
            "global-old",
            "hub",
            None,
        ),
    ] {
        kube.holds("kind-use1", object);
    }
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");

    let use1: Vec<String> = steps(&log)
        .into_iter()
        .filter(|step| step.starts_with("kind-use1:"))
        .collect();
    let at = use1
        .iter()
        .position(|s| s == "kind-use1:list")
        .expect("a list");
    assert_eq!(
        use1[at..],
        [
            "kind-use1:list",
            "kind-use1:drain",
            "kind-use1:held",
            "kind-use1:delete",
            "kind-use1:health",
            "kind-use1:health",
        ]
    );
    let drains: Vec<String> = commands(&log)
        .into_iter()
        .filter(|args| args.iter().any(|a| a.ends_with("/drain")))
        .map(|args| args[4].clone())
        .collect();
    assert_eq!(
        drains,
        [
            "/api/v1/namespaces/gdl-api/services/http:model-x-old:control/proxy/v1/ext/gdl-pool/drain"
        ]
    );
    let held = commands(&log)
        .into_iter()
        .filter(|args| args.iter().any(|a| a.contains("/report/")))
        .collect::<Vec<_>>();
    assert_eq!(held.len(), 3, "two answers of 200, then 404");
    assert_eq!(
        held[0][4],
        "/api/v1/namespaces/gdl-api/services/http:global-us:public/proxy/v1/ext/gdl-pool/report/model_x/model-x-old"
    );
    assert_eq!(
        deletes(&log, "kind-use1"),
        [
            vec!["deployment/model-x-old-engine".to_string()],
            vec![
                "deployment/global-old".into(),
                "deployment/model-x-old".into()
            ],
            vec![
                "configmap/model-x-old".into(),
                "persistentvolumeclaim/global-old-state".into(),
                "service/model-x-old".into(),
            ],
        ]
    );
    assert!(deletes(&log, "kind-euw1").is_empty());
    // What the render wrote is still there.
    let held: Vec<&(String, String)> = kube.objects["kind-use1"].keys().collect();
    assert!(held.contains(&&("Deployment".to_string(), "model-x-us-engine".to_string())));
    assert!(held.contains(&&("Namespace".to_string(), "gdl-api".to_string())));
    assert!(
        !held.iter().any(|(_, name)| name.contains("old")),
        "{held:?}"
    );
}

/// A removed pool whose parent still holds its record when the bound
/// runs out fails the deploy, naming the pool, and nothing is deleted.
#[test]
fn a_drain_that_does_not_end_stops_the_deploy() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = None;
    kube.holds(
        "kind-use1",
        left(
            "Deployment",
            "model-x-old",
            "model-x-old",
            "hub",
            Some("global-us"),
        ),
    );
    let error = deploy(&mirror, &files, &mut docker, &mut kube).expect_err("a drain that hangs");
    let error = error.to_string();
    assert!(
        error.contains("pool `model-x-old` did not withdraw from its parent `global-us`"),
        "{error}"
    );
    assert!(deletes(&log, "kind-use1").is_empty());
    assert!(!steps(&log).iter().any(|s| s.ends_with(":health")));
}

/// When a pool's health does not answer 200 in time, the error names each
/// of its pods that is not ready, why its container waits, and its latest
/// events, which name a missing Secret; no Secret is read.
#[test]
fn a_health_timeout_names_the_pods_and_their_events() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.health = None;
    kube.pods = json!({"items": [
        {
            "metadata": {"name": "global-us-front-abc"},
            "status": {
                "phase": "Pending",
                "conditions": [{"type": "Ready", "status": "False"}],
                "containerStatuses": [{"state": {"waiting": {
                    "reason": "CreateContainerConfigError",
                    "message": "secret \"api-fingerprint\" not found",
                }}}],
            },
        },
        {
            "metadata": {"name": "global-us-xyz"},
            "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
        },
    ]});
    kube.events = json!({"items": [
        {"type": "Warning", "reason": "Failed", "message": "Error: secret \"api-fingerprint\" not found"},
    ]});
    let error = deploy(&mirror, &files, &mut docker, &mut kube).expect_err("unhealthy");
    let error = error.to_string();
    for part in [
        "the health of 4 pool(s) did not answer 200",
        "pool `global-us` in cluster `use1`",
        "pod `global-us-front-abc`: Pending, waiting: CreateContainerConfigError (secret \"api-fingerprint\" not found)",
        "Warning Failed: Error: secret \"api-fingerprint\" not found",
    ] {
        assert!(error.contains(part), "{part} in {error}");
    }
    assert!(
        !error.contains("global-us-xyz"),
        "a ready pod is not named: {error}"
    );
    let pods = commands(&log)
        .into_iter()
        .find(|args| args.contains(&"pods".to_string()))
        .expect("pods were listed");
    assert!(
        pods.contains(&"gdl.dev/fleet=api,gdl.dev/pool=global-eu".to_string()),
        "{pods:?}"
    );
}

/// `down` drains each pool that has a parent, deletes every object with
/// the fleet's label, and removes the label from each namespace, which it
/// keeps, with the Secrets in it.
#[test]
fn down_drains_deletes_and_keeps_the_namespace() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = Some(0);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    log.borrow_mut().clear();
    down(&files, &mut kube).expect("down");
    assert_eq!(
        steps(&log),
        [
            "kind-euw1:list",
            "kind-euw1:drain",
            "kind-euw1:held",
            "kind-euw1:delete",
            "kind-euw1:unlabel",
            "kind-use1:list",
            "kind-use1:drain",
            "kind-use1:held",
            "kind-use1:delete",
            "kind-use1:unlabel",
        ]
    );
    let drains: Vec<String> = commands(&log)
        .into_iter()
        .filter(|args| args.iter().any(|a| a.ends_with("/drain")))
        .map(|args| args[4].clone())
        .collect();
    assert_eq!(drains.len(), 2, "only the children drain: {drains:?}");
    assert!(
        drains.iter().all(|d| d.contains("http:model-x-")),
        "{drains:?}"
    );
    for (context, objects) in &kube.objects {
        let kinds: Vec<&str> = objects.keys().map(|(kind, _)| kind.as_str()).collect();
        assert_eq!(kinds, ["Namespace"], "{context}");
    }
    let unlabel = commands(&log)
        .into_iter()
        .find(|args| args.contains(&"label".to_string()))
        .expect("an unlabel");
    assert_eq!(
        unlabel[2..],
        ["label", "namespace/gdl-api", "gdl.dev/fleet-"]
    );
    // Segments go first, so that each hub outlives its instances.
    assert_eq!(
        deletes(&log, "kind-use1")[..2],
        [
            vec![
                "deployment/global-us-front".to_string(),
                "deployment/model-x-us-engine".into(),
            ],
            vec![
                "deployment/global-us".into(),
                "deployment/model-x-us".into()
            ],
        ]
    );
}

/// No command line names a kubeconfig, a token, a user, a key, or a
/// certificate, nor reads kubectl's configuration or any Secret; each
/// names its context first; and nothing applied holds the fleet's secret.
#[test]
fn no_command_carries_a_credential() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.holds(
        "kind-use1",
        left(
            "Deployment",
            "model-x-old",
            "model-x-old",
            "hub",
            Some("global-us"),
        ),
    );
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    down(&files, &mut kube).expect("down");
    let calls = log.borrow();
    assert!(calls.len() > 20, "{}", calls.len());
    for call in calls.iter() {
        let Call::Kube { args, input } = call else {
            continue;
        };
        assert_eq!(args[0], "--context", "{args:?}");
        assert!(
            ["kind-use1", "kind-euw1"].contains(&args[1].as_str()),
            "{args:?}"
        );
        for arg in args {
            for banned in [
                "--kubeconfig",
                "--token",
                "--username",
                "--password",
                "--client-key",
                "--client-certificate",
                "--as",
                "--server",
                "--insecure",
            ] {
                let named = arg == banned || arg.starts_with(&format!("{banned}="));
                assert!(!named, "{args:?}");
            }
            assert!(
                !["secret", "secrets", "config"].contains(&arg.as_str()),
                "{args:?}"
            );
            assert!(
                !arg.contains("secrets/") && !arg.contains("secret/"),
                "{args:?}"
            );
        }
        assert!(!input.contains(MARKER), "{args:?}");
    }
}
