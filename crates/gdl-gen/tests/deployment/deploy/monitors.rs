//! A fleet's PodMonitors: `deploy` applies them where the fleet asks for
//! them, and prunes, as `down` deletes, the fleet's PodMonitors in each
//! cluster that has the Prometheus operator's CRD, whatever the fleet asks
//! now. A cluster without the CRD is never asked for PodMonitors.

use super::*;

#[path = "monitors/names.rs"]
mod names;

#[path = "monitors/upgrade.rs"]
mod upgrade;

/// [`fleet_text`], which asks for a PodMonitor, or the same without.
fn fleet_asking(pod_monitor: bool) -> String {
    let text = fleet_text();
    assert!(text.contains("[metrics]\npod_monitor = true\n"), "{text}");
    match pod_monitor {
        true => text,
        false => text.replace("[metrics]\npod_monitor = true\n", ""),
    }
}

fn docker(log: &Log) -> Docker {
    Docker {
        log: Rc::clone(log),
        fail: false,
    }
}

/// The command lines that name PodMonitors.
fn monitor_commands(log: &Log) -> Vec<Vec<String>> {
    commands(log)
        .into_iter()
        .filter(|args| args.iter().any(|a| a.contains("podmonitors")))
        .collect()
}

/// Where the clusters have the CRD, a fleet that asks gets its PodMonitor
/// in each, and `down` deletes them.
#[test]
fn a_pod_monitor_is_applied_where_asked_and_down_deletes_it() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(true));
    let log = Log::default();
    let mut kube = Kube::new(&log);
    deploy(&mirror, &files, &mut docker(&log), &mut kube).expect("deploy");
    // Each cluster's PodMonitor, by its name, which selects that cluster.
    let monitor = |kube: &Kube, context: &str, cluster: &str| {
        kube.objects[context]
            .iter()
            .find(|((kind, _), object)| {
                kind == "PodMonitor"
                    && object["spec"]["selector"]["matchLabels"]["gdl.dev/cluster"] == cluster
            })
            .map(|((_, name), _)| name.clone())
    };
    let mut names = BTreeMap::new();
    for (context, cluster) in [("kind-use1", "use1"), ("kind-euw1", "euw1")] {
        let name = monitor(&kube, context, cluster).unwrap_or_else(|| panic!("{context}"));
        names.insert(context, name);
    }
    assert!(steps(&log).contains(&"kind-use1:crds".to_string()));
    down(&files, &mut kube).expect("down");
    for context in ["kind-use1", "kind-euw1"] {
        let name = names[context].clone();
        let deleted: Vec<String> = deletes(&log, context).into_iter().flatten().collect();
        assert!(
            deleted.contains(&format!("podmonitors.monitoring.coreos.com/{name}")),
            "{deleted:?}"
        );
        assert!(!kube.objects[context].contains_key(&("PodMonitor".into(), name)));
    }
}

/// With the CRD, a PodMonitor an earlier deploy applied is pruned once
/// the fleet no longer asks for one.
#[test]
fn a_pod_monitor_the_fleet_no_longer_asks_for_is_pruned() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(false));
    let log = Log::default();
    let mut kube = Kube::new(&log);
    let earlier = json!({
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PodMonitor",
        "metadata": {
            "name": "api",
            "namespace": "gdl-api",
            "labels": {"gdl.dev/fleet": "api"},
        },
    });
    kube.holds("kind-use1", earlier);
    deploy(&mirror, &files, &mut docker(&log), &mut kube).expect("deploy");
    let deleted: Vec<String> = deletes(&log, "kind-use1").into_iter().flatten().collect();
    assert_eq!(deleted, ["podmonitors.monitoring.coreos.com/api"]);
    assert!(!kube.objects["kind-use1"].contains_key(&("PodMonitor".into(), "api".into())));
}

/// A cluster without the CRD is never asked for PodMonitors: `deploy` and
/// `down` list and delete only the other kinds there.
#[test]
fn a_cluster_without_the_crd_is_never_asked_for_pod_monitors() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(false));
    let log = Log::default();
    let mut kube = Kube::new(&log);
    kube.without_monitors = vec!["kind-use1", "kind-euw1"];
    deploy(&mirror, &files, &mut docker(&log), &mut kube).expect("deploy");
    down(&files, &mut kube).expect("down");
    assert_eq!(monitor_commands(&log), Vec::<Vec<String>>::new());
    // The control: with the CRD, the same deploy lists them.
    let log = Log::default();
    let mut kube = Kube::new(&log);
    deploy(&mirror, &files, &mut docker(&log), &mut kube).expect("deploy");
    assert!(!monitor_commands(&log).is_empty());
}

/// A fleet that asks for a PodMonitor in a cluster without the CRD is
/// refused before anything is built or applied, naming the cluster.
#[test]
fn a_pod_monitor_where_the_crd_is_missing_is_refused() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(true));
    let log = Log::default();
    let mut kube = Kube::new(&log);
    kube.without_monitors = vec!["kind-euw1"];
    let error = deploy(&mirror, &files, &mut docker(&log), &mut kube).expect_err("refused");
    let error = error.to_string();
    assert!(error.contains("cluster `euw1`"), "{error}");
    assert!(
        error.contains("podmonitors.monitoring.coreos.com"),
        "{error}"
    );
    let steps = steps(&log);
    assert!(
        !steps
            .iter()
            .any(|step| step == "docker" || step.ends_with(":base")),
        "{steps:?}"
    );
}

/// `kubectl api-resources` discovers every group before it filters by
/// one, and exits 1 when another group's discovery fails, as a stale
/// metrics server's does, after it prints what it found. That is no
/// reason to stop: the PodMonitor CRD is found, or known absent, from
/// what it printed. Only a failure of the operator's own group is one.
#[test]
fn another_groups_failed_discovery_does_not_stop_deploy_or_down() {
    let mirror = Mirror::new();
    for (pod_monitor, crd) in [(true, true), (false, false)] {
        let files = files(&mirror, &fleet_asking(pod_monitor));
        let log = Log::default();
        let mut kube = Kube::new(&log);
        kube.undiscovered = Some("metrics.k8s.io/v1beta1");
        if !crd {
            kube.without_monitors = vec!["kind-use1", "kind-euw1"];
        }
        deploy(&mirror, &files, &mut docker(&log), &mut kube).expect("deploy");
        down(&files, &mut kube).expect("down");
        assert_eq!(monitor_commands(&log).is_empty(), !crd);
    }
    // The operator's own group: whether the cluster has the CRD is not
    // known, so deploy stops and says why.
    let files = files(&mirror, &fleet_asking(false));
    let log = Log::default();
    let mut kube = Kube::new(&log);
    kube.undiscovered = Some("monitoring.coreos.com/v1");
    let error = deploy(&mirror, &files, &mut docker(&log), &mut kube).expect_err("unknown");
    assert!(
        error.to_string().contains("monitoring.coreos.com"),
        "{error}"
    );
}

#[test]
fn context_aliases_keep_a_pod_monitor_for_each_cluster() {
    struct Aliases(Kube);
    impl gdl_gen::Kube for Aliases {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let mut physical = args.to_vec();
            physical[1] = "shared-cluster".into();
            self.0.run(&physical, input)
        }
    }

    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(true));
    let log = Log::default();
    let mut kube = Aliases(Kube::new(&log));
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
    };
    gdl_gen::deploy(&how, &mut docker(&log), &mut kube, &mut |_| {})
        .expect("aliases permit a deploy");
    for name in ["global-eu", "global-us", "model-x-eu", "model-x-us"] {
        assert!(
            kube.0.objects["shared-cluster"].contains_key(&("Deployment".into(), name.into())),
            "{name} still runs"
        );
    }
    let mut clusters: Vec<&str> = kube.0.objects["shared-cluster"]
        .values()
        .filter(|object| object["kind"] == "PodMonitor")
        .map(|object| {
            object["spec"]["selector"]["matchLabels"]["gdl.dev/cluster"]
                .as_str()
                .expect("the selected cluster")
        })
        .collect();
    clusters.sort_unstable();
    assert_eq!(
        clusters,
        ["euw1", "use1"],
        "both clusters' pods still run, so both must be scraped"
    );
}

struct PartialOperator(Kube);

impl gdl_gen::Kube for PartialOperator {
    fn run(
        &mut self,
        args: &[String],
        input: Option<&[u8]>,
    ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        let mut reply = self.0.run(args, input)?;
        if args.iter().any(|arg| arg == "api-resources") {
            assert!(reply.stdout.contains("podmonitors.monitoring.coreos.com"));
            // Discovery can list a resource from another served version while v1 fails.
            reply.success = false;
            reply.stderr = "error: unable to retrieve the complete list of server APIs: monitoring.coreos.com/v1: operator discovery unavailable".into();
        }
        Ok(reply)
    }
}

#[test]
fn deploy_stops_when_operator_discovery_is_incomplete() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(true));
    let log = Log::default();
    let mut kube = PartialOperator(Kube::new(&log));
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
    };
    let result = gdl_gen::deploy(&how, &mut docker(&log), &mut kube, &mut |_| {});
    assert!(
        result.is_err() && !steps(&log).iter().any(|step| step == "docker"),
        "failed operator discovery must stop before image builds: {result:?}, {:?}",
        steps(&log)
    );
}

#[test]
fn down_stops_when_operator_discovery_is_incomplete() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(false));
    let log = Log::default();
    let mut kube = PartialOperator(Kube::new(&log));
    let result = gdl_gen::down(&files.fleet, &waits(), &mut kube, &mut |_| {});
    assert!(
        result.is_err() && commands(&log).iter().all(|args| args[2] == "api-resources"),
        "failed operator discovery must stop before listing or deleting: {result:?}, {:?}",
        commands(&log)
    );
}

struct MonitorGroups {
    inner: Kube,
    foreign: Option<Value>,
}

impl gdl_gen::Kube for MonitorGroups {
    fn run(
        &mut self,
        args: &[String],
        input: Option<&[u8]>,
    ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if args.iter().any(|arg| arg == "delete")
            && args.iter().any(|arg| arg == "podmonitor/retired")
        {
            // kubectl resolves an unqualified resource to the first matching API group.
            self.inner.log.borrow_mut().push(Call::Kube {
                args: args.to_vec(),
                input: String::new(),
            });
            self.foreign.take().expect("another team's PodMonitor");
            return Ok(gdl_gen::Reply::ok("podmonitor.another-team.dev deleted"));
        }
        let qualified: Vec<String> = args
            .iter()
            .map(|arg| match arg.as_str() {
                "podmonitor.monitoring.coreos.com/retired"
                | "podmonitors.monitoring.coreos.com/retired" => "podmonitor/retired".into(),
                _ => arg.clone(),
            })
            .collect();
        self.inner.run(&qualified, input)
    }
}

fn assert_monitor_group(prune: bool) {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(false));
    let log = Log::default();
    let mut kube = MonitorGroups {
        inner: Kube::new(&log),
        foreign: Some(json!({
            "apiVersion": "another-team.dev/v1",
            "kind": "PodMonitor",
            "metadata": {"name": "retired", "namespace": "gdl-api"},
        })),
    };
    kube.inner.holds(
        "kind-use1",
        json!({
            "apiVersion": "monitoring.coreos.com/v1",
            "kind": "PodMonitor",
            "metadata": {
                "name": "retired",
                "namespace": "gdl-api",
                "labels": {"gdl.dev/fleet": "api"},
            },
        }),
    );
    if prune {
        let how = gdl_gen::Deploy {
            spec: files.spec.clone(),
            fleet: files.fleet.clone(),
            target: mirror.target(),
            out: None,
            waits: waits(),
            sources: gdl_gen::Sources::Fetched,
        };
        gdl_gen::deploy(&how, &mut docker(&log), &mut kube, &mut |_| {}).expect("deploy");
    } else {
        gdl_gen::down(&files.fleet, &waits(), &mut kube, &mut |_| {}).expect("down");
    }
    assert!(
        kube.foreign.is_some(),
        "a qualified list must not become a delete in another API group: {:?}",
        deletes(&log, "kind-use1")
    );
    assert!(
        !kube.inner.objects["kind-use1"].contains_key(&("PodMonitor".into(), "retired".into())),
        "the fleet's operator PodMonitor must be deleted"
    );
}

#[test]
fn down_deletes_only_the_operator_pod_monitor() {
    assert_monitor_group(false);
}

#[test]
fn prune_deletes_only_the_operator_pod_monitor() {
    assert_monitor_group(true);
}
