//! What a Prometheus needs to find a fleet's metrics on Kubernetes: each
//! pod says where it serves them, in the common `prometheus.io`
//! annotations, and carries its pool's region beside its other labels;
//! a fleet that asks for one gets a `PodMonitor` per cluster, which copies
//! each pod's pool, class, region and segment onto every series under
//! names gdl's own labels do not use.

use super::*;

/// [`FLEET`] with no secrets, and a PodMonitor or not.
fn monitored(pod_monitor: bool) -> gdl_model::Fleet {
    let text = unsecret();
    assert!(text.contains("[metrics]\npod_monitor = true\n"), "{text}");
    match pod_monitor {
        true => fleet(&text),
        false => fleet(&text.replace("[metrics]\npod_monitor = true\n", "")),
    }
}

/// The objects of the api example in two classes for `fleet`.
fn rendered(fleet: &gdl_model::Fleet) -> BTreeMap<(String, String), Value> {
    let mirror = Mirror::new();
    let spec = placed(
        &mirror,
        &format!("{SPLIT}[deployment.segment.front]\nports = [\"8080\"]\n"),
    );
    objects(&render(&mirror, &spec, fleet).expect("objects"))
}

/// Every pod of a pool says where it serves its metrics: a hub on its
/// public port at the pool extension's route, a segment on its system
/// port at `/metrics`. Each carries its pool's region.
#[test]
fn every_pod_says_where_it_serves_its_metrics() {
    let objects = rendered(&monitored(false));
    let mut seen = 0;
    for ((kind, name), object) in &objects {
        if kind != "Deployment" {
            continue;
        }
        let template = &object["spec"]["template"]["metadata"];
        let segment = template["labels"]["gdl.dev/segment"]
            .as_str()
            .expect("a segment");
        let (port, path) = match segment {
            "hub" => ("1337".to_string(), "/v1/ext/gdl-pool/metrics"),
            _ => {
                let system = container(object)["ports"]
                    .as_array()
                    .expect("ports")
                    .iter()
                    .find(|port| port["name"] == "system")
                    .expect("a system port");
                (system["containerPort"].to_string(), "/metrics")
            }
        };
        let annotations = &template["annotations"];
        assert_eq!(annotations["prometheus.io/scrape"], "true", "{name}");
        assert_eq!(annotations["prometheus.io/port"], port.as_str(), "{name}");
        assert_eq!(annotations["prometheus.io/path"], path, "{name}");
        let region = match template["labels"]["gdl.dev/cluster"].as_str() {
            Some("use1") => "us-east-1",
            Some("euw1") => "eu-west-1",
            other => panic!("{name}: cluster {other:?}"),
        };
        assert_eq!(template["labels"]["gdl.dev/region"], region, "{name}");
        assert_eq!(
            object["metadata"]["labels"]["gdl.dev/region"], region,
            "{name}"
        );
        seen += 1;
    }
    // Four pools, each a hub and one segment.
    assert_eq!(seen, 8);
    assert!(
        !objects.keys().any(|(kind, _)| kind == "PodMonitor"),
        "a PodMonitor the fleet did not ask for"
    );
}

/// A fleet with `[metrics] pod_monitor = true` gets one PodMonitor in each
/// cluster's namespace. It selects the fleet's pods in that cluster, and
/// scrapes a segment's system port and a hub's public port, each where it
/// serves. Each series gets the pod's fleet and cluster as `fleet` and
/// `cluster`, as the chapter's scrape job without the operator gives them,
/// and its pool, class, region and segment as
/// `in_pool`, `in_class`, `in_region` and `segment`: gdl's chooser labels
/// its own series with the pool, class and region it offered to. A label
/// shortened to fit Kubernetes' limit is read whole from its annotation.
#[test]
fn a_fleet_that_asks_gets_a_pod_monitor_in_each_cluster() {
    let objects = rendered(&monitored(true));
    for cluster in ["use1", "euw1"] {
        let monitor = &objects[&("PodMonitor".into(), format!("{cluster}/api"))];
        assert_eq!(monitor["apiVersion"], "monitoring.coreos.com/v1");
        assert_eq!(monitor["metadata"]["namespace"], "gdl-api");
        assert_eq!(monitor["metadata"]["labels"]["gdl.dev/fleet"], "api");
        assert_eq!(monitor["metadata"]["labels"]["gdl.dev/cluster"], cluster);
        assert_eq!(
            monitor["spec"]["selector"]["matchLabels"],
            serde_json::json!({"gdl.dev/fleet": "api", "gdl.dev/cluster": cluster})
        );
        let endpoints = monitor["spec"]["podMetricsEndpoints"]
            .as_array()
            .expect("endpoints");
        let served: Vec<(&str, &str)> = endpoints
            .iter()
            .map(|endpoint| {
                (
                    endpoint["port"].as_str().expect("a port"),
                    endpoint["path"].as_str().expect("a path"),
                )
            })
            .collect();
        assert_eq!(
            served,
            [
                ("system", "/metrics"),
                ("public", "/v1/ext/gdl-pool/metrics")
            ]
        );
        for endpoint in endpoints {
            let relabelings = endpoint["relabelings"].as_array().expect("relabelings");
            for (label, target) in [
                ("gdl_dev_fleet", "fleet"),
                ("gdl_dev_cluster", "cluster"),
                ("gdl_dev_pool", "in_pool"),
                ("gdl_dev_class", "in_class"),
                ("gdl_dev_region", "in_region"),
                ("gdl_dev_segment", "segment"),
            ] {
                let from = |source: &str| {
                    relabelings.iter().position(|relabeling| {
                        relabeling["sourceLabels"] == serde_json::json!([source])
                            && relabeling["targetLabel"] == target
                    })
                };
                let short = from(&format!("__meta_kubernetes_pod_label_{label}"))
                    .unwrap_or_else(|| panic!("{target} from the label: {relabelings:?}"));
                let whole = from(&format!("__meta_kubernetes_pod_annotation_{label}"))
                    .unwrap_or_else(|| panic!("{target} from the annotation: {relabelings:?}"));
                assert!(short < whole, "the annotation is read after the label");
                assert_eq!(relabelings[whole]["regex"], "(.+)");
            }
        }
    }
}
