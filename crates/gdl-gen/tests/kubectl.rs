//! `Kubectl` against a stand-in on the `PATH`: a process that kubectl
//! starts can hold its output pipes open after kubectl exits, so the
//! deadline bounds the reads of its output too.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use gdl_gen::Kube as _;

fn exits_with_pipe(open: bool, test: &str) {
    const CHILD: &str = "GDL_REVIEW_PIPE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let result = gdl_gen::Kubectl
            .run(&["--request-timeout=100ms".to_string()], None)
            .expect("kubectl ran");
        if !open {
            assert!(result.success, "{result:?}");
            assert_eq!(result.stdout.trim(), "finished");
        }
        return;
    }

    let dir = tempfile::Builder::new()
        .prefix("gdl-output-pipe-")
        .tempdir()
        .expect("a directory");
    let kubectl = dir.path().join("kubectl");
    std::fs::write(
        &kubectl,
        "#!/bin/sh\nif [ \"$GDL_REVIEW_OPEN_PIPE\" = 1 ]; then sleep 10 >/dev/null & fi\necho finished\n",
    )
    .expect("the executable");
    std::fs::set_permissions(&kubectl, std::fs::Permissions::from_mode(0o755))
        .expect("permissions");
    let paths = std::env::join_paths(std::iter::once(dir.path().to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").expect("PATH")),
    ))
    .expect("PATH");
    let output = Command::new("timeout")
        .args(["--kill-after=1", "2"])
        .arg(std::env::current_exe().expect("the test binary"))
        .args(["--exact", test, "--include-ignored", "--nocapture"])
        .env(CHILD, "1")
        .env("GDL_REVIEW_OPEN_PIPE", if open { "1" } else { "0" })
        .env("PATH", paths)
        .output()
        .expect("the bounded test");
    assert!(
        output.status.success(),
        "open={open}: kubectl exited, but its 100 ms deadline did not bound pipe completion within 2 s: {:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A kubectl that exits and closes its pipes is answered in full.
#[test]
fn kubectl_completes_after_its_pipes_close() {
    exits_with_pipe(false, "kubectl_completes_after_its_pipes_close");
}

/// A kubectl whose pipes stay open after it exits ends at its deadline.
#[test]
fn kubectl_deadline_bounds_pipes_after_exit() {
    exits_with_pipe(true, "kubectl_deadline_bounds_pipes_after_exit");
}
