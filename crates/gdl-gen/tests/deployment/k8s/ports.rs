//! A segment's system port against the ports its spec publishes. The pod
//! already declares the system port as `system`, so a spec port that only
//! repeats it is refused, and a host publication of it, which is Compose's
//! only route to the system listener from the host, is left out of the pod.
//! No pod declares one port twice: Kubernetes refuses such a Deployment,
//! though its schema allows it.

use std::collections::BTreeSet;

use super::super::{ONE_FLEET, fleet_bundle};
use super::*;

/// The engine of [`SPLIT`], publishing `ports`, with whatever `extra`
/// tables add.
fn engine_publishes(mirror: &Mirror, ports: &str, extra: &str) -> gdl_gen::Spec {
    placed(
        mirror,
        &format!("{SPLIT}[deployment.segment.engine]\nports = [{ports}]\n{extra}"),
    )
}

/// What `check` says of `spec`: nothing when it passes.
fn checked(mirror: &Mirror, spec: &gdl_gen::Spec) -> String {
    match gdl_gen::check(spec, &mirror.target(), gdl_gen::Offers::Read) {
        Ok(_) => String::new(),
        Err(error) => error.to_string(),
    }
}

/// Each pod template of `object` whose container ports repeat a number
/// and protocol, with the repeated pair. A port that names no protocol is
/// TCP, as Kubernetes reads it.
fn repeated_ports(object: &Value) -> Vec<(u64, String)> {
    let mut repeated = Vec::new();
    let containers = object["spec"]["template"]["spec"]["containers"]
        .as_array()
        .into_iter()
        .flatten();
    for container in containers {
        let mut seen = BTreeSet::new();
        for port in container["ports"].as_array().into_iter().flatten() {
            let number = port["containerPort"].as_u64().expect("a container port");
            let protocol = port["protocol"].as_str().unwrap_or("TCP").to_string();
            if !seen.insert((number, protocol.clone())) {
                repeated.push((number, protocol));
            }
        }
    }
    repeated
}

/// A bare spec port equal to the system port adds nothing, so `check`
/// refuses it, naming the segment and the port and how to reach the
/// port; and so do `compose` and `k8s`, which check the spec first. The
/// same number over another protocol, or with the system port moved, is
/// another port.
#[test]
fn a_bare_system_port_is_refused() {
    let mirror = Mirror::new();
    for bare in ["9090", "9090/tcp"] {
        let spec = engine_publishes(&mirror, &format!("\"{bare}\""), "");
        let refusal = checked(&mirror, &spec);
        for part in [
            "[deployment.segment.engine]",
            &format!("port `{bare}` is segment `engine`'s system port"),
            "as port `system`",
            "`<host port>:9090`",
        ] {
            assert!(refusal.contains(part), "{part} in {refusal}");
        }
        let refusal = refused(&mirror, &spec, &unsecret());
        assert!(refusal.contains("system port"), "{refusal}");
        let temp = tempfile::Builder::new()
            .prefix("gdl-compose-ports-")
            .tempdir()
            .expect("a directory");
        let refusal = gdl_gen::compose_files(
            &spec,
            Some(&fleet(ONE_FLEET)),
            temp.path(),
            &mirror.target(),
            gdl_gen::Sources::Fetched,
        )
        .expect_err("a bare system port")
        .to_string();
        assert!(refusal.contains("system port"), "{refusal}");
    }
    let udp = engine_publishes(&mirror, "\"9090/udp\"", "");
    assert_eq!(checked(&mirror, &udp), "");
    let moved = engine_publishes(
        &mirror,
        "\"9090\"",
        "[deployment.segment.engine.config.system]\nport = 9191\n",
    );
    assert_eq!(checked(&mirror, &moved), "");
    let repeated = engine_publishes(
        &mirror,
        "\"9191\"",
        "[deployment.segment.engine.config.system]\nport = 9191\n",
    );
    assert!(
        checked(&mirror, &repeated).contains("port `9191` is segment `engine`'s system port"),
        "{}",
        checked(&mirror, &repeated)
    );
}

/// A host publication of the system port passes `check`, and Compose
/// publishes it. The pod leaves it out: it already declares the port as
/// `system`, and a pod publishes nothing to a host.
#[test]
fn a_host_publication_of_the_system_port_is_composes_alone() {
    let mirror = Mirror::new();
    let spec = engine_publishes(&mirror, "\"127.0.0.1:9091:9090\"", "");
    assert_eq!(checked(&mirror, &spec), "");
    // A fixed host port runs in one pool.
    let one = ONE_FLEET
        .split("[pool.model-x-02]")
        .next()
        .expect("a fleet");
    let (_, compose) = fleet_bundle(&mirror, &spec, &fleet(one));
    assert_eq!(
        compose["services"]["model-x-01_engine"]["ports"],
        serde_json::json!(["127.0.0.1:9091:9090"])
    );
    let files = render(&mirror, &spec, &fleet(&unsecret())).expect("objects");
    let objects = objects(&files);
    for pool in ["model-x-us", "model-x-eu"] {
        let engine = &objects[&("Deployment".to_string(), format!("{pool}-engine"))];
        assert_eq!(
            container(engine)["ports"],
            serde_json::json!([{"name": "system", "containerPort": 9090}]),
            "{pool}"
        );
        // It publishes no other port, so it has no entry Service.
        assert!(!objects.contains_key(&("Service".to_string(), format!("{pool}-engine"))));
    }
}

/// No pod template of a pinned render, or of the film example's ports on
/// the segments of the same names, declares one port twice. The film
/// example publishes each segment's system port to the host under
/// Compose. A pod built to repeat a port is caught, so the check is not
/// empty.
#[test]
fn no_pod_declares_one_port_twice() {
    let control = serde_json::json!({"spec": {"template": {"spec": {"containers": [{"ports": [
        {"name": "system", "containerPort": 9090},
        {"containerPort": 9090, "protocol": "TCP"},
        {"containerPort": 9090, "protocol": "UDP"},
    ]}]}}}});
    assert_eq!(repeated_ports(&control), [(9090, "TCP".to_string())]);

    let mut rendered = Vec::new();
    for pinned in [PINNED, "tests/fixtures/k8s/dynamodb/out"] {
        let mut stack = vec![manifest_dir().join(pinned)];
        while let Some(path) = stack.pop() {
            if path.is_dir() {
                for entry in std::fs::read_dir(&path).expect("a directory") {
                    stack.push(entry.expect("an entry").path());
                }
            } else if path.extension().is_some_and(|e| e == "yaml") {
                let text = std::fs::read_to_string(&path).expect("a file");
                rendered.extend(
                    documents(&text)
                        .into_iter()
                        .map(|object| (path.display().to_string(), object)),
                );
            }
        }
    }

    let mirror = Mirror::new();
    let film = std::fs::read_to_string(manifest_dir().join("../../examples/film/deploy.toml"))
        .expect("the film example");
    let film: toml::Table = toml::from_str(&film).expect("TOML");
    let mut tables = String::new();
    for segment in ["front", "engine"] {
        let ports = &film["deployment"]["segment"][segment]["ports"];
        let ports = ports.as_array().expect("ports");
        assert!(
            ports
                .iter()
                .any(|port| port.as_str().is_some_and(|port| port.ends_with(":9090"))),
            "the film example's {segment} publishes its system port"
        );
        tables.push_str(&format!(
            "[deployment.segment.{segment}]\nports = {ports}\n",
            ports = toml::Value::Array(ports.clone())
        ));
    }
    let spec = placed(&mirror, &format!("{SPLIT}{tables}"));
    assert_eq!(checked(&mirror, &spec), "");
    let files = render(&mirror, &spec, &fleet(&unsecret())).expect("objects");
    let film = objects(&files);
    let front = &film[&("Deployment".to_string(), "global-us-front".to_string())];
    assert_eq!(
        container(front)["ports"],
        serde_json::json!([
            {"name": "system", "containerPort": 9090},
            {"containerPort": 8082, "protocol": "TCP"},
        ])
    );
    rendered.extend(
        film.into_iter()
            .map(|((kind, name), object)| (format!("film {kind} {name}"), object)),
    );

    let pods = rendered
        .iter()
        .filter(|(_, object)| object["spec"]["template"]["spec"]["containers"].is_array())
        .count();
    assert!(pods >= 8, "{pods} pod templates");
    for (from, object) in &rendered {
        let repeated = repeated_ports(object);
        assert!(repeated.is_empty(), "{from}: {repeated:?}");
    }
}
