use super::*;

#[test]
fn changing_an_image_keeps_deployment_selectors() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let fleet = fleet(&unsecret());
    let before = objects(&render(&mirror, &spec, &fleet).expect("objects"));
    let after = objects(
        &gdl_gen::k8s_files(
            &spec,
            &fleet,
            &gdl_gen::Dgds::default(),
            &mirror.target(),
            Some("replacement"),
            None,
            gdl_gen::Sources::Fetched,
        )
        .expect("objects with another image tag"),
    );
    let mut compared = 0;
    for (key, prior) in &before {
        if key.0 != "Deployment" {
            continue;
        }
        let next = &after[key];
        assert_ne!(container(prior)["image"], container(next)["image"]);
        assert_eq!(prior["spec"]["selector"], next["spec"]["selector"]);
        compared += 1;
    }
    assert_eq!(compared, 8);
}

#[test]
fn renaming_a_class_keeps_deployment_selectors() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let before = objects(&render(&mirror, &spec, &fleet(&unsecret())).expect("objects"));
    let renamed = placed(&mirror, &SPLIT.replace("model_x", "model_y"));
    let renamed_fleet = fleet(&unsecret().replace("model_x", "model_y"));
    let after = objects(&render(&mirror, &renamed, &renamed_fleet).expect("renamed class"));
    let mut compared = 0;
    let mut changed = Vec::new();
    for (key, prior) in &before {
        if key.0 != "Deployment" || prior["metadata"]["labels"]["gdl.dev/class"] != "model_x" {
            continue;
        }
        let next = &after[key];
        assert_eq!(prior["metadata"]["name"], next["metadata"]["name"]);
        assert_eq!(
            prior["metadata"]["namespace"],
            next["metadata"]["namespace"]
        );
        assert_eq!(next["metadata"]["labels"]["gdl.dev/class"], "model_y");
        if prior["spec"]["selector"] != next["spec"]["selector"] {
            changed.push((
                &key.1,
                &prior["spec"]["selector"],
                &next["spec"]["selector"],
            ));
        }
        compared += 1;
    }
    assert_eq!(compared, 4);
    assert!(
        changed.is_empty(),
        "applying a class rename changes immutable Deployment selectors: {changed:?}"
    );
}
