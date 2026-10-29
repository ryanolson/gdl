use super::*;

#[test]
fn a_registry_must_form_a_valid_image_reference() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let mut accepted = Vec::new();
    for registry in ["https://registry.example/gdl", "registry.example/gdl:tag"] {
        let text = unsecret().replace(
            "registry = \"registry.example/gdl\"",
            &format!("registry = {registry:?}"),
        );
        match render(&mirror, &spec, &fleet(&text)) {
            Err(error) => assert!(error.to_string().contains(registry), "{error}"),
            Ok(files) => {
                let objects = objects(&files);
                let front = &objects[&("Deployment".into(), "global-us-front".into())];
                accepted.push(
                    container(front)["image"]
                        .as_str()
                        .expect("an image")
                        .to_string(),
                );
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "invalid images were rendered: {accepted:?}"
    );
}
