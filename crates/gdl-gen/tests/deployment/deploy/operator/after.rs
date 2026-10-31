use super::*;

struct RecreatedAfterDelete {
    kube: Kube,
    listed_uid: String,
    replaced: bool,
}

impl gdl_gen::Kube for RecreatedAfterDelete {
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
            let mut replacement = self.kube.objects["kind-use1"][&key].clone();
            let options: Value =
                serde_json::from_slice(input.expect("DeleteOptions")).expect("delete body");
            assert_eq!(options["preconditions"]["uid"], self.listed_uid);
            let reply = gdl_gen::Kube::run(&mut self.kube, args, input)?;
            assert!(reply.success, "the listed pool's delete succeeds");
            assert!(!self.kube.objects["kind-use1"].contains_key(&key));

            // Another deploy makes the child before the deletion wait reads it.
            replacement["metadata"]["uid"] = "replacement-after-delete".into();
            self.kube
                .objects
                .get_mut("kind-use1")
                .expect("cluster")
                .insert(key, replacement);
            self.replaced = true;
            return Ok(reply);
        }
        gdl_gen::Kube::run(&mut self.kube, args, input)
    }
}

fn assert_parent_survives_replacement_after_delete(prune: bool) {
    // Deployment extracts the mirrored source, so this test takes several seconds.
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let (mut docker, mut kube) = fakes(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    let listed_uid = kube.objects["kind-use1"][&("GdlPool".to_string(), "model-x-us".to_string())]
        ["metadata"]["uid"]
        .as_str()
        .expect("listed uid")
        .to_string();
    let mut raced = RecreatedAfterDelete {
        kube,
        listed_uid,
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
    assert!(
        raced.replaced,
        "replace only after the listed pool's delete succeeds"
    );
    let held = &raced.kube.objects["kind-use1"];
    assert_eq!(
        held[&("GdlPool".into(), "model-x-us".into())]["metadata"]["uid"],
        "replacement-after-delete",
        "leave the replacement child in place"
    );
    assert!(
        held.contains_key(&("GdlPool".into(), "global-us".into())),
        "the wait saw a replacement child that still needs global-us, but its parent was deleted: prune={prune}, result={result:?}"
    );
    let error = result
        .expect_err("a replacement leaves the level unfinished")
        .to_string();
    assert!(
        error.contains("model-x-us")
            && error.contains(&raced.listed_uid)
            && error.contains("replacement-after-delete"),
        "{error}"
    );
}

#[test]
fn down_keeps_a_parent_when_its_child_is_replaced_during_the_delete_wait() {
    assert_parent_survives_replacement_after_delete(false);
}

#[test]
fn prune_keeps_a_parent_when_its_child_is_replaced_during_the_delete_wait() {
    assert_parent_survives_replacement_after_delete(true);
}
