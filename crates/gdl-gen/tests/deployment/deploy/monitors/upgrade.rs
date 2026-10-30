use super::*;

#[test]
fn fitted_monitor_names_remain_dns_labels() {
    let mirror = Mirror::new();
    let spec = super::super::super::k8s::spec(&mirror);
    for (fleet, cluster) in [
        ("a".into(), "b".into()),
        ("a".repeat(63), "b".repeat(63)),
        (format!("{}-x", "a".repeat(51)), "use1".into()),
        (format!("{}-----x", "a".repeat(45)), "use1".into()),
        ("api".into(), format!("{}-x", "a".repeat(47))),
        ("api".into(), format!("{}---x", "a".repeat(45))),
    ] {
        let text = fleet_asking(true)
            .replace("name = \"api\"", &format!("name = {fleet:?}"))
            .replace("use1", &cluster);
        let fleet = gdl_model::Fleet::from_toml(&text).expect("a fleet");
        let files = gdl_gen::k8s_files(
            &spec,
            &fleet,
            &mirror.target(),
            Some("pinned"),
            None,
            gdl_gen::Sources::Fetched,
        )
        .expect("valid names permit rendering");
        let objects = super::super::super::k8s::objects(&files);
        let monitors: Vec<_> = objects
            .values()
            .filter(|object| object["kind"] == "PodMonitor")
            .collect();
        assert_eq!(monitors.len(), 2);
        for monitor in monitors {
            let name = monitor["metadata"]["name"].as_str().expect("a name");
            assert!(!name.is_empty() && name.len() <= 63, "{name}");
            assert!(name.starts_with(|c: char| c.is_ascii_lowercase()), "{name}");
            assert!(
                name.ends_with(|c: char| c.is_ascii_alphanumeric()),
                "{name}"
            );
            assert!(
                name.bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'),
                "{name}"
            );
            let suffix = name.rsplit('-').next().expect("a hash");
            assert_eq!(suffix.len(), 10, "{name}");
            assert!(suffix.bytes().all(|c| c.is_ascii_hexdigit()), "{name}");
            assert_eq!(
                monitor["spec"]["selector"]["matchLabels"],
                monitor["metadata"]["labels"]
            );
        }
    }
}

#[test]
fn invalid_fleet_and_cluster_names_are_refused_before_rendering() {
    let mirror = Mirror::new();
    let spec = super::super::super::k8s::spec(&mirror);
    for invalid in ["Upper".to_string(), "a-".to_string(), "a".repeat(64)] {
        for text in [
            fleet_asking(true).replace("name = \"api\"", &format!("name = {invalid:?}")),
            fleet_asking(true).replace("use1", &invalid),
        ] {
            let fleet = gdl_model::Fleet::from_toml(&text).expect("valid TOML");
            let error = gdl_gen::k8s_files(
                &spec,
                &fleet,
                &mirror.target(),
                Some("pinned"),
                None,
                gdl_gen::Sources::Fetched,
            )
            .expect_err("invalid names are refused");
            assert!(error.to_string().contains("name"), "{error}");
        }
    }
}

#[test]
fn deploying_hashed_names_prunes_previous_monitor_names() {
    for (cluster, previous) in [
        ("use1".to_string(), "api-use1".to_string()),
        ("a".repeat(63), format!("api-{}-fdecec45b4", "a".repeat(48))),
    ] {
        assert_pruned(&cluster, &previous);
    }
}

fn assert_pruned(first_cluster: &str, first_previous: &str) {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_asking(true).replace("use1", first_cluster));
    let log = Log::default();
    let mut kube = Kube::new(&log);
    let first_context = format!("kind-{first_cluster}");
    let cases = [
        (first_context.as_str(), first_cluster, first_previous),
        ("kind-euw1", "euw1", "api-euw1"),
    ];
    for (context, cluster, previous) in cases {
        kube.holds(
            context,
            json!({
                "apiVersion": "monitoring.coreos.com/v1",
                "kind": "PodMonitor",
                "metadata": {
                    "name": previous,
                    "namespace": "gdl-api",
                    "labels": {"gdl.dev/fleet": "api", "gdl.dev/cluster": cluster},
                },
                "spec": {
                    "selector": {"matchLabels": {
                        "gdl.dev/fleet": "api", "gdl.dev/cluster": cluster,
                    }},
                },
            }),
        );
    }
    deploy(&mirror, &files, &mut docker(&log), &mut kube).expect("upgrade");
    for (context, cluster, previous) in cases {
        assert!(!kube.objects[context].contains_key(&("PodMonitor".into(), previous.into())));
        let deleted: Vec<_> = deletes(&log, context).into_iter().flatten().collect();
        assert!(deleted.contains(&format!("podmonitors.monitoring.coreos.com/{previous}")));
        let monitors: Vec<_> = kube.objects[context]
            .values()
            .filter(|object| object["kind"] == "PodMonitor")
            .collect();
        assert_eq!(monitors.len(), 1, "{context}");
        assert_eq!(
            monitors[0]["spec"]["selector"]["matchLabels"]["gdl.dev/cluster"],
            cluster
        );
    }
}
