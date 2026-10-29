use super::*;

#[test]
fn aggregate_settings_cannot_override_the_rendered_hub_or_pool() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let mut accepted = Vec::new();
    for key in ["GDL_HUB", "GDL_POOL", "gdl_hub", "gdl_pool"] {
        let text = unsecret().replace("RUST_LOG = \"info\"", &format!("{key} = \"false\""));
        match render(&mirror, &spec, &fleet(&text)) {
            Err(error) => assert!(error.to_string().contains(key), "{error}"),
            Ok(files) => {
                let objects = objects(&files);
                let front = &objects[&("Deployment".into(), "global-us-front".into())];
                assert_eq!(env(container(front))[key], "false");
                accepted.push(key);
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "accepted reserved settings: {accepted:?}"
    );
}
