use super::*;

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

fn apply_fleet(mirror: &Mirror, text: &str, log: &Log, kube: &mut dyn gdl_gen::Kube) {
    let files = files(mirror, text);
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
        allow_pool_restart: false,
    };
    gdl_gen::deploy(&how, &mut docker(log), kube, &mut |_| {})
        .expect("both cluster names permit a deploy");
}

fn assert_clusters(first: &str, second: &str) {
    let mirror = Mirror::new();
    let text = fleet_asking(true)
        .replace("use1", first)
        .replace("euw1", second);
    let log = Log::default();
    let mut kube = Aliases(Kube::new(&log));
    apply_fleet(&mirror, &text, &log, &mut kube);
    for name in ["global-eu", "global-us", "model-x-eu", "model-x-us"] {
        assert!(
            kube.0.objects["shared-cluster"].contains_key(&("Deployment".into(), name.into())),
            "{name} still runs"
        );
    }
    let mut selected: Vec<&str> = kube.0.objects["shared-cluster"]
        .values()
        .filter(|object| object["kind"] == "PodMonitor")
        .map(|object| {
            object["spec"]["selector"]["matchLabels"]["gdl.dev/cluster"]
                .as_str()
                .expect("the selected cluster")
        })
        .collect();
    selected.sort_unstable();
    let mut expected = [first, second];
    expected.sort_unstable();
    assert_eq!(
        selected, expected,
        "both clusters' pods still run, so both must be scraped"
    );
}

#[test]
fn a_long_cluster_and_a_short_cluster_keep_their_monitors() {
    assert_clusters(&"a".repeat(63), "second");
}

#[test]
fn a_short_name_cannot_replace_a_fitted_pod_monitor() {
    // Both names are valid cluster names; the shorter one matches a fitted suffix.
    assert_clusters(
        &"a".repeat(63),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-fdecec45b4",
    );
}

fn assert_fleets(first_cluster: &str) {
    struct SharedNamespace(Kube);
    impl gdl_gen::Kube for SharedNamespace {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let mut physical = args.to_vec();
            if args[1].starts_with("kind-use1") || args[1] == "kind-east" {
                physical[1] = "shared-cluster".into();
            }
            self.0.run(&physical, input)
        }
    }

    let mirror = Mirror::new();
    let first = fleet_asking(true).replace("use1", first_cluster);
    let second = fleet_asking(true)
        .replace("use1", "east")
        .replace("name = \"api\"", "name = \"api-use1\"")
        .replace("global-", "other-global-")
        .replace("model-x-", "other-model-x-");
    let log = Log::default();
    let mut kube = SharedNamespace(Kube::new(&log));
    // Two complete deploys keep both fleets' pods running while checking monitor ownership.
    apply_fleet(&mirror, &first, &log, &mut kube);
    apply_fleet(&mirror, &second, &log, &mut kube);
    for name in ["global-us", "model-x-us"] {
        for name in [name.to_string(), format!("other-{name}")] {
            assert!(
                kube.0.objects["shared-cluster"].contains_key(&("Deployment".into(), name.clone())),
                "{name} still runs"
            );
        }
    }
    let mut selected: Vec<(&str, &str)> = kube.0.objects["shared-cluster"]
        .values()
        .filter(|object| object["kind"] == "PodMonitor")
        .map(|object| {
            let labels = &object["spec"]["selector"]["matchLabels"];
            (
                labels["gdl.dev/fleet"].as_str().expect("the fleet"),
                labels["gdl.dev/cluster"].as_str().expect("the cluster"),
            )
        })
        .collect();
    selected.sort_unstable();
    let mut expected = [("api", first_cluster), ("api-use1", "east")];
    expected.sort_unstable();
    assert_eq!(
        selected, expected,
        "both fleets' running pods need their monitors"
    );
}

#[test]
fn two_fleets_keep_their_monitors_in_one_namespace() {
    assert_fleets("use1-west");
}

#[test]
fn fleet_and_cluster_boundaries_cannot_replace_another_fleets_monitor() {
    assert_fleets("use1-east");
}
