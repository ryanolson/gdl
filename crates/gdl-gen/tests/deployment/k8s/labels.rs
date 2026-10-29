use super::*;

fn with_class(mirror: &Mirror, name: &str) -> Result<gdl_gen::Files, gdl_gen::Error> {
    gdl_model::check_class_name(name).expect("a model class name");
    let spec = placed(mirror, &SPLIT.replace("model_x", name));
    let text = unsecret().replace("model_x", name);
    render(mirror, &spec, &fleet(&text))
}

#[test]
fn a_class_at_the_label_limit_is_rendered() {
    let mirror = Mirror::new();
    let name = "m".repeat(63);
    let files = with_class(&mirror, &name).expect("objects");
    let objects = objects(&files);
    let hub = &objects[&("Deployment".into(), "model-x-us".into())];
    assert_eq!(hub["metadata"]["labels"]["gdl.dev/class"], name);
}

#[test]
fn class_labels_fit_the_kubernetes_limit() {
    let mirror = Mirror::new();
    let name = "m".repeat(64);
    let files = match with_class(&mirror, &name) {
        Err(error) => {
            assert!(error.to_string().contains(&name), "{error}");
            return;
        }
        Ok(files) => files,
    };
    for ((kind, name), object) in objects(&files) {
        for (key, value) in object["metadata"]["labels"].as_object().expect("labels") {
            let value = value.as_str().expect("a label value");
            assert!(
                value.len() <= 63,
                "{kind} {name}: {key} has {} characters",
                value.len()
            );
        }
    }
}

/// A class over 63 characters is labelled by its start and its hash, the
/// same on every object and pod template, and each object keeps its whole
/// name in the annotation of the label's key.
#[test]
fn a_long_class_keeps_its_whole_name_in_an_annotation() {
    let mirror = Mirror::new();
    let name = "m".repeat(64);
    let objects = objects(&with_class(&mirror, &name).expect("objects"));
    let hub = &objects[&("Deployment".into(), "model-x-us".into())];
    let label = hub["metadata"]["labels"]["gdl.dev/class"]
        .as_str()
        .expect("a label");
    assert_eq!(label.len(), 63, "{label}");
    assert!(label.starts_with(&name[..52]), "{label}");
    assert_eq!(hub["metadata"]["annotations"]["gdl.dev/class"], name);
    let template = &hub["spec"]["template"]["metadata"];
    assert_eq!(template["labels"]["gdl.dev/class"], label);
    assert_eq!(template["annotations"]["gdl.dev/class"], name);
    // A selector never holds the class, which a fleet can rename.
    assert!(
        hub["spec"]["selector"]["matchLabels"]
            .get("gdl.dev/class")
            .is_none()
    );
    let config = &objects[&("ConfigMap".into(), "model-x-us".into())];
    assert_eq!(config["metadata"]["annotations"]["gdl.dev/class"], name);
    // A class that fits carries no such annotation.
    let global = &objects[&("Deployment".into(), "global-us".into())];
    assert!(global["metadata"].get("annotations").is_none(), "{global}");
}
