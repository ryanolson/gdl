use super::*;

#[test]
fn http_probes_can_reach_the_configured_system_listener() {
    let mirror = Mirror::new();
    let spec = placed(
        &mirror,
        &format!(
            "{SPLIT}[deployment.segment.engine.config.system]\nhost = \"127.0.0.1\"\nport = 9090\n"
        ),
    );
    let files = match render(&mirror, &spec, &fleet(&unsecret())) {
        Err(error) => {
            assert!(error.to_string().contains("system"), "{error}");
            return;
        }
        Ok(files) => files,
    };
    let objects = objects(&files);
    let config = &objects[&("ConfigMap".into(), "model-x-us".into())];
    let config: toml::Table =
        toml::from_str(config["data"]["engine.toml"].as_str().expect("settings")).expect("TOML");
    let host: std::net::IpAddr = config["system"]["host"]
        .as_str()
        .expect("system host")
        .parse()
        .expect("an IP address");
    let engine = &objects[&("Deployment".into(), "model-x-us-engine".into())];
    let pod = &engine["spec"]["template"]["spec"];
    assert_ne!(pod["hostNetwork"], true);
    for probe in ["livenessProbe", "readinessProbe"] {
        let probe = &container(engine)[probe];
        if probe.get("httpGet").is_some() {
            assert!(
                !host.is_loopback(),
                "HTTP probe {} cannot reach the system listener at {host}:9090",
                probe["httpGet"]
            );
        }
    }
}
