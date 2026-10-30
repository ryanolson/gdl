use super::*;

#[cfg(unix)]
#[test]
fn drain_deadline_bounds_a_stalled_kubectl() {
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    const CHILD: &str = "GDL_REVIEW_DRAIN_CHILD";
    if let Some(fleet) = std::env::var_os(CHILD) {
        let mut bounds = waits();
        bounds.drain = Duration::from_millis(20);
        let result = gdl_gen::down(
            Path::new(&fleet),
            &bounds,
            &mut gdl_gen::Kubectl,
            &mut |_| {},
        );
        if std::env::var("GDL_REVIEW_STALL").as_deref() == Ok("1") {
            assert!(result.is_err(), "a stalled report must time out");
        } else {
            result.expect("the responsive control completes");
        }
        return;
    }

    let dir = tempfile::Builder::new()
        .prefix("gdl-bounded-")
        .tempdir()
        .expect("a directory");
    let fleet = dir.path().join("fleet.toml");
    std::fs::write(&fleet, fleet_text()).expect("the fleet");
    let objects = dir.path().join("objects.json");
    let mut hub = left(
        "Deployment",
        "model-x-removed",
        "model-x-removed",
        "hub",
        Some("global-us"),
    );
    hub["metadata"]["uid"] = "uid-model-x-removed".into();
    std::fs::write(&objects, json!({"items": [hub]}).to_string()).expect("the live objects");
    let kubectl = dir.path().join("kubectl");
    std::fs::write(
        &kubectl,
        r#"#!/bin/sh
case "$*" in
  *"/report/"*)
    if [ "$GDL_REVIEW_STALL" = 1 ]; then exec sleep 10; fi
    echo 'Error from server (NotFound): the server could not find the requested resource' >&2
    exit 1
    ;;
  *"/drain"*) echo '{}' ;;
  *" get "*) cat "$GDL_REVIEW_OBJECTS" ;;
esac
"#,
    )
    .expect("the fake kubectl");
    std::fs::set_permissions(&kubectl, std::fs::Permissions::from_mode(0o755))
        .expect("an executable");
    let paths = std::env::join_paths(std::iter::once(dir.path().to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").expect("PATH")),
    ))
    .expect("a PATH");
    for stalled in [false, true] {
        let output = Command::new("timeout")
            .arg("2")
            .arg(std::env::current_exe().expect("the test binary"))
            .args([
                "--exact",
                "deploy::deadline::drain_deadline_bounds_a_stalled_kubectl",
                "--include-ignored",
                "--nocapture",
            ])
            .env(CHILD, &fleet)
            .env("GDL_REVIEW_STALL", if stalled { "1" } else { "0" })
            .env("GDL_REVIEW_OBJECTS", &objects)
            .env("PATH", &paths)
            .output()
            .expect("the bounded child test");
        assert!(
            output.status.success(),
            "stalled={stalled}: the 20 ms drain bound did not finish within 2 s: {:?}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
