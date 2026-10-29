use super::*;

#[test]
fn secret_projection_paths_are_not_reserved_by_kubernetes() {
    let mirror = Mirror::new();
    let secret = manifest_dir().join(SECRET);
    let spec = placed(
        &mirror,
        &format!(
            "{SPLIT}[deployment.secrets.\"..fingerprint\"]\nfile = {:?}\n\
             [deployment.segment.front]\nsecrets = [\"..fingerprint\"]\n",
            secret.display().to_string()
        ),
    );
    let text = fleet_text().replace("fingerprint =", "\"..fingerprint\" =");
    let files = match render(&mirror, &spec, &fleet(&text)) {
        Err(error) => {
            assert!(error.to_string().contains("..fingerprint"), "{error}");
            return;
        }
        Ok(files) => files,
    };
    let objects = objects(&files);
    let front = &objects[&("Deployment".into(), "global-us-front".into())];
    let volume = front["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .expect("volumes")
        .iter()
        .find(|v| v["name"] == "secrets")
        .expect("the projected secrets");
    let path = volume["projected"]["sources"][0]["secret"]["items"][0]["path"]
        .as_str()
        .expect("the mounted file's path");
    assert!(!path.starts_with(".."), "reserved projection path: {path}");
}
