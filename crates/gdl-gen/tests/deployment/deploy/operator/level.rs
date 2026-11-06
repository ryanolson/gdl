use super::*;

struct Conflict {
    kube: Kube,
    listed_uid: String,
    replaced: bool,
    peer_waited: bool,
}

impl gdl_gen::Kube for Conflict {
    fn run(
        &mut self,
        args: &[String],
        input: Option<&[u8]>,
    ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if !self.replaced
            && args[1] == "kind-euw1"
            && args.contains(&"delete".to_string())
            && args.contains(&pool_path("model-x-eu"))
        {
            self.kube
                .objects
                .get_mut("kind-euw1")
                .expect("cluster")
                .get_mut(&("GdlPool".into(), "model-x-eu".into()))
                .expect("listed child")["metadata"]["uid"] = "replacement-in-first-cluster".into();
            self.replaced = true;
        }
        if self.replaced
            && args[1] == "kind-euw1"
            && args.contains(&"get".to_string())
            && args.contains(&"gdlpools.gdl.dev/model-x-peer".to_string())
        {
            self.peer_waited = true;
        }
        gdl_gen::Kube::run(&mut self.kube, args, input)
    }
}

fn assert_level_finishes_before_the_conflict_is_returned(prune: bool) {
    // Deployment extracts the mirrored source, so this test takes several seconds.
    let mirror = Mirror::new();
    let mut fleet: toml::Table = toml::from_str(&fleet_text()).expect("fleet");
    fleet["cluster"]["euw1"]
        .as_table_mut()
        .expect("cluster")
        .insert("operator".into(), true.into());
    let files = files(&mirror, &toml::to_string(&fleet).expect("fleet text"));
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    let child = kube.objects["kind-euw1"][&("GdlPool".into(), "model-x-eu".into())].clone();
    let listed_uid = child["metadata"]["uid"]
        .as_str()
        .expect("listed uid")
        .to_string();
    let mut peer = child;
    peer["metadata"]["name"] = "model-x-peer".into();
    peer["metadata"]["labels"]["gdl.dev/pool"] = "model-x-peer".into();
    kube.holds("kind-euw1", peer);
    let mut raced = Conflict {
        kube,
        listed_uid,
        replaced: false,
        peer_waited: false,
    };
    let result = if prune {
        let pools = fleet["pool"].as_table_mut().expect("pools");
        for name in ["model-x-eu", "model-x-us"] {
            let pool = pools.remove(name).expect("child");
            pools.insert(format!("{name}-next"), pool);
        }
        let next = super::files(&mirror, &toml::to_string(&fleet).expect("fleet text"));
        let how = gdl_gen::Deploy {
            spec: next.spec,
            fleet: next.fleet,
            target: mirror.target(),
            out: None,
            waits: waits(),
            sources: gdl_gen::Sources::Fetched,
            allow_pool_restart: false,
            images: None,
            replace: false,
        };
        gdl_gen::deploy(&how, &mut docker, &mut raced, &mut NoSlurm, &mut |_| {})
    } else {
        gdl_gen::down(
            &files.fleet,
            &waits(),
            &mut raced,
            &mut NoSlurm,
            &mut |_| {},
        )
    };
    assert!(
        raced.replaced,
        "replace the child after the list: {result:?}"
    );
    let error = result
        .expect_err("the replacement fails the run")
        .to_string();
    assert!(
        error.contains("model-x-eu")
            && error.contains(&raced.listed_uid)
            && error.contains("replacement-in-first-cluster"),
        "{error}"
    );
    assert!(
        raced.peer_waited,
        "wait for the other child in the first cluster"
    );
    assert!(
        !raced.kube.objects["kind-euw1"].contains_key(&("GdlPool".into(), "model-x-peer".into()))
    );
    for (context, parent) in [("kind-euw1", "global-eu"), ("kind-use1", "global-us")] {
        assert!(
            raced.kube.objects[context].contains_key(&("GdlPool".into(), parent.into())),
            "keep every parent when a child is replaced: {context}, {parent}"
        );
    }
    assert!(
        !raced.kube.objects["kind-use1"].contains_key(&("GdlPool".into(), "model-x-us".into())),
        "the other cluster's child is at the same depth and must finish before the conflict is returned: prune={prune}, {error}"
    );
}

#[test]
fn down_stops_the_whole_level_when_a_child_in_the_first_cluster_is_replaced() {
    assert_level_finishes_before_the_conflict_is_returned(false);
}

#[test]
fn prune_stops_the_whole_level_when_a_child_in_the_first_cluster_is_replaced() {
    assert_level_finishes_before_the_conflict_is_returned(true);
}
