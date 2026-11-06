//! The launcher's service, as a pool's recipe runs it, and the login
//! side's records.

use super::*;

/// The launcher is the segment's own service, with only its name, its
/// placement, and its command changed: whatever shapes the segment's
/// `srun`, its GPUs and CPUs among it, shapes the launcher's too. Its
/// command is the segment's binary as the launcher, with the segment's own
/// command after it.
#[test]
fn a_launcher_asks_for_what_its_segment_asks_for() {
    let segment = json!({
        "name": "engine",
        "type": "generic",
        "nodes": 2,
        "container": "registry.example/engine@sha256:aa",
        "command": ["/usr/local/bin/segment", "/gdl/engine.toml"],
        "env": {"GDL_LISTEN": "{node_ip}:0"},
        "preamble": "export A=1",
        "readiness": {"http": {"port": 9090}},
        "cpus_per_task": 16,
        "cpu_bind": "none",
        "srun_options": {"gres": "gpu:4", "mem": "0"},
        "placement": {"per": "node"},
    });
    let launcher = service(&segment, "/gdl");
    assert_eq!(launcher["name"], "launch_engine");
    assert_eq!(
        launcher["placement"],
        json!({"per": "node", "pool": "spare"})
    );
    for same in [
        "type",
        "container",
        "env",
        "preamble",
        "cpus_per_task",
        "cpu_bind",
        "srun_options",
    ] {
        assert_eq!(launcher[same], segment[same], "{same}");
    }
    for gone in ["nodes", "readiness", "terminal"] {
        assert!(launcher.get(gone).is_none(), "{gone}");
    }
    let command: Vec<&str> = launcher["command"]
        .as_array()
        .expect("a command")
        .iter()
        .map(|word| word.as_str().expect("a word"))
        .collect();
    assert_eq!(
        command,
        [
            "/usr/local/bin/segment",
            "--launch",
            "--root",
            "/gdl",
            "--node",
            "{node}",
            "--segment",
            "engine",
            "--",
            "/usr/local/bin/segment",
            "/gdl/engine.toml"
        ]
    );
}

/// A record is written once: a second write leaves the first as it was,
/// and no staged file stays behind.
#[test]
fn a_record_is_written_once() {
    let scratch = tempfile::Builder::new()
        .prefix("gdl-launcher-records-")
        .tempdir()
        .expect("a scratch directory");
    let dir = scratch.path();
    assert!(write_once(dir, "1.stop", "a1\n").expect("written"));
    assert!(!write_once(dir, "1.stop", "b2\n").expect("taken"));
    assert_eq!(read(dir, "1.stop").as_deref(), Some("a1\n"));
    assert_eq!(read(dir, "2.stop"), None);
    let left: Vec<String> = std::fs::read_dir(dir)
        .expect("the records")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(left, ["1.stop"]);
}

/// `EEXIST` from a link that made the record, as a retransmitted NFS
/// `LINK` answers, is this write's own record; from another write's
/// record, it is not.
#[test]
fn eexist_after_a_link_that_made_the_record_is_ours() {
    let scratch = tempfile::Builder::new()
        .prefix("gdl-launcher-link-")
        .tempdir()
        .expect("a scratch directory");
    let (staged, target) = (
        scratch.path().join(".1.run.tmp"),
        scratch.path().join("1.run"),
    );
    std::fs::write(&staged, "a1\n").expect("staged");
    std::fs::hard_link(&staged, &target).expect("linked");
    let eexist = || Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists));
    assert!(settle(&staged, &target, eexist()).expect("settled"));
    let other = scratch.path().join(".1.run.other.tmp");
    std::fs::write(&other, "b2\n").expect("another's");
    assert!(!settle(&other, &target, eexist()).expect("settled"));
    assert!(settle(&other, &target, Ok(())).expect("settled"));
}
