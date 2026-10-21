use super::*;

fn scratch(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("gdl-web-host-{name}-"))
        .tempdir()
        .expect("dir")
}

/// An executable `sh` script in `dir` that records its arguments, one per
/// line, in `args` beside it, then runs `body`.
///
/// A child `sh` writes the file, never this process: a test on another
/// thread that forks while this process holds the file open for writing
/// gives its child that descriptor until it execs, and running the script
/// meanwhile fails with "Text file busy".
fn fake(dir: &Path, name: &str, body: &str) -> PathBuf {
    use std::io::Write as _;
    let path = dir.join(name);
    let args = dir.join("args");
    let script = format!(
        "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\"; done >> {args:?}\n{body}\n",
        args = args.display().to_string()
    );
    let mut writer = std::process::Command::new("sh")
        .args(["-c", "cat > \"$0\" && chmod 755 \"$0\""])
        .arg(&path)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("sh");
    writer
        .stdin
        .take()
        .expect("stdin")
        .write_all(script.as_bytes())
        .expect("script");
    assert!(writer.wait().expect("sh").success());
    path
}

fn recorded(dir: &Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("args"))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

const DOCKER: &str = r#"case "$1 $2" in
  "compose version") echo 5.2.0 ;;
  "info --format") echo 29.6.2 ;;
  *) exit 2 ;;
esac"#;

#[tokio::test]
async fn compose_is_available_when_the_cli_and_the_daemon_answer() {
    let temp = scratch("compose");
    let deploy = Deploy {
        docker: fake(temp.path(), "docker", DOCKER),
        kubectl: temp.path().join("absent"),
        apply: true,
    };
    let found = targets(&deploy).await;
    assert_eq!(found.compose.version.as_deref(), Some("5.2.0"));
    assert_eq!(found.compose.daemon.as_deref(), Some("29.6.2"));
    assert!(found.compose.available);
    assert!(found.compose.apply);
    assert_eq!(found.compose.problem, None);
}

#[tokio::test]
async fn compose_without_a_daemon_is_not_available() {
    let temp = scratch("daemon");
    let deploy = Deploy {
        docker: fake(
            temp.path(),
            "docker",
            r#"case "$1" in
  compose) echo 5.2.0 ;;
  *) echo "Cannot connect to the Docker daemon" >&2; exit 1 ;;
esac"#,
        ),
        kubectl: temp.path().join("absent"),
        apply: true,
    };
    let found = targets(&deploy).await;
    assert_eq!(found.compose.version.as_deref(), Some("5.2.0"));
    assert!(!found.compose.available);
    // Apply asks for the daemon, not just the flag.
    assert!(!found.compose.apply);
    assert_eq!(
        found.compose.problem.as_deref(),
        Some("the Docker daemon did not answer: Cannot connect to the Docker daemon")
    );
}

#[tokio::test]
async fn a_missing_docker_is_reported_as_missing() {
    let temp = scratch("missing");
    let deploy = Deploy {
        docker: temp.path().join("docker"),
        kubectl: temp.path().join("kubectl"),
        apply: true,
    };
    let found = targets(&deploy).await;
    assert!(!found.compose.available);
    assert_eq!(found.compose.version, None);
    assert_eq!(
        found.compose.problem.as_deref(),
        Some("`docker` was not found")
    );
    assert!(!found.kubernetes.kubectl);
}

#[tokio::test]
async fn compose_is_detected_but_not_applied_without_the_flag() {
    let temp = scratch("flag");
    let deploy = Deploy {
        docker: fake(temp.path(), "docker", DOCKER),
        kubectl: temp.path().join("absent"),
        apply: false,
    };
    let found = targets(&deploy).await;
    assert!(found.compose.available);
    assert!(!found.compose.apply);
}

/// A daemon that never answers must not hold the page's request.
#[tokio::test]
async fn a_probe_that_hangs_is_bounded() {
    let temp = scratch("hang");
    let deploy = Deploy {
        docker: fake(temp.path(), "docker", "exec sleep 60"),
        kubectl: temp.path().join("absent"),
        apply: true,
    };
    let started = std::time::Instant::now();
    let found = tokio::time::timeout(Duration::from_secs(10), targets(&deploy))
        .await
        .expect("the probes are bounded");
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(!found.compose.available);
    assert!(
        found
            .compose
            .problem
            .as_deref()
            .is_some_and(|p| p.contains("did not answer within")),
        "{:?}",
        found.compose.problem
    );
}

#[tokio::test]
async fn kubernetes_reports_kubectl_and_its_current_context() {
    let temp = scratch("kube");
    let deploy = Deploy {
        docker: temp.path().join("absent"),
        kubectl: fake(temp.path(), "kubectl", "echo dev-cluster"),
        apply: false,
    };
    let found = targets(&deploy).await;
    assert!(found.kubernetes.kubectl);
    assert_eq!(found.kubernetes.context.as_deref(), Some("dev-cluster"));
    assert_eq!(
        recorded(temp.path()),
        ["config", "current-context"],
        "the probe reads the configuration and never contacts a cluster"
    );

    let temp = scratch("kube-unset");
    let deploy = Deploy {
        docker: temp.path().join("absent"),
        kubectl: fake(
            temp.path(),
            "kubectl",
            "echo 'error: current-context is not set' >&2; exit 1",
        ),
        apply: false,
    };
    let found = targets(&deploy).await;
    assert!(found.kubernetes.kubectl);
    assert_eq!(found.kubernetes.context, None);
}

#[test]
fn the_kubeconfig_is_the_first_existing_file_of_kubeconfig_else_the_home_default() {
    let temp = scratch("kubeconfig");
    let dir = temp.path();
    let home = dir.join("home");
    std::fs::create_dir_all(home.join(".kube")).expect("dir");
    assert_eq!(kubeconfig(None, Some(&home)), None);
    std::fs::write(home.join(".kube/config"), "").expect("file");
    assert_eq!(
        kubeconfig(None, Some(&home)),
        Some(home.join(".kube/config"))
    );
    std::fs::write(dir.join("b"), "").expect("file");
    let list = std::env::join_paths([dir.join("a"), dir.join("b")]).expect("paths");
    assert_eq!(
        kubeconfig(Some(list.as_os_str()), Some(&home)),
        Some(dir.join("b"))
    );
    // A set KUBECONFIG replaces the default, as kubectl reads it.
    let list = std::env::join_paths([dir.join("a")]).expect("paths");
    assert_eq!(kubeconfig(Some(list.as_os_str()), Some(&home)), None);
    // An empty one does not.
    assert_eq!(
        kubeconfig(Some(std::ffi::OsStr::new("")), Some(&home)),
        Some(home.join(".kube/config"))
    );
}

#[test]
fn a_project_is_a_name_compose_accepts() {
    assert_eq!(project("api"), "gdl-api");
    assert_eq!(project("Chat Demo.v2"), "gdl-chat-demo-v2");
    assert_eq!(project("a_b-c"), "gdl-a_b-c");
    assert_eq!(project(""), "gdl-");
}

#[test]
fn placing_a_deployment_replaces_only_one_the_server_placed() {
    let temp = scratch("place");
    let deployments = temp.path().join("deployments");
    let write = |text: &'static str| {
        move |dir: &Path| {
            std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            std::fs::write(dir.join("compose.yaml"), text).map_err(|e| e.to_string())
        }
    };
    let first = place(&deployments, "gdl-api", write("first")).expect("placed");
    assert_eq!(first, deployments.join("gdl-api"));
    std::fs::write(first.join("stale"), "").expect("file");
    let second = place(&deployments, "gdl-api", write("second")).expect("placed again");
    assert_eq!(second, first);
    assert_eq!(
        std::fs::read_to_string(second.join("compose.yaml")).expect("file"),
        "second"
    );
    assert!(!second.join("stale").exists(), "a re-apply starts fresh");

    // A directory the server did not place is never removed.
    let foreign = deployments.join("gdl-other");
    std::fs::create_dir_all(&foreign).expect("dir");
    std::fs::write(foreign.join("keep"), "").expect("file");
    let error = place(&deployments, "gdl-other", write("x")).expect_err("refused");
    assert!(error.contains("was not placed by this server"), "{error}");
    assert!(foreign.join("keep").exists());

    // A failed write leaves the last deployment in place.
    let error =
        place(&deployments, "gdl-api", |_: &Path| Err("no".to_string())).expect_err("failed");
    assert_eq!(error, "no");
    assert_eq!(
        std::fs::read_to_string(first.join("compose.yaml")).expect("file"),
        "second"
    );
}

#[test]
fn one_apply_runs_at_a_time() {
    let applying = Applying::default();
    let guard = applying.start().expect("first");
    assert!(applying.start().is_none(), "a second apply is refused");
    drop(guard);
    assert!(
        applying.start().is_some(),
        "the next one starts once it ends"
    );
}

async fn collect(mut events: tokio::sync::mpsc::Receiver<Value>) -> Vec<Value> {
    let mut all = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("an event")
    {
        all.push(event);
    }
    all
}

#[tokio::test]
async fn apply_builds_and_starts_the_project_detached_and_streams_its_output() {
    let temp = scratch("apply");
    let dir = temp.path().join("gdl-api");
    std::fs::create_dir_all(&dir).expect("dir");
    let docker = fake(
        temp.path(),
        "docker",
        "echo building; echo 'warming up' >&2; printf 'no newline'",
    );
    let applying = Applying::default();
    let guard = applying.start().expect("guard");
    let events = collect(compose_up(&docker, &dir, "gdl-api", guard)).await;
    let compose = dir.join("compose.yaml");
    assert_eq!(
        recorded(temp.path()),
        [
            "compose",
            "-p",
            "gdl-api",
            "--project-directory",
            dir.to_str().expect("utf-8"),
            "-f",
            compose.to_str().expect("utf-8"),
            "up",
            "--build",
            "--detach",
        ]
    );
    let mut lines: Vec<&str> = events.iter().filter_map(|e| e["line"].as_str()).collect();
    lines.sort_unstable();
    assert_eq!(lines, ["building", "no newline", "warming up"]);
    assert_eq!(
        events.last(),
        Some(&json!({"done": true, "ok": true, "code": 0}))
    );
    assert!(
        applying.start().is_some(),
        "the guard ends with the command"
    );
}

#[tokio::test]
async fn a_failed_apply_reports_its_exit_code() {
    let temp = scratch("apply-fails");
    let docker = fake(temp.path(), "docker", "echo 'no space left' >&2; exit 17");
    let applying = Applying::default();
    let events = collect(compose_up(
        &docker,
        temp.path(),
        "gdl-api",
        applying.start().expect("guard"),
    ))
    .await;
    assert_eq!(events[0], json!({"line": "no space left"}));
    assert_eq!(
        events.last(),
        Some(&json!({"done": true, "ok": false, "code": 17}))
    );
}

#[tokio::test]
async fn an_apply_whose_docker_is_missing_says_so() {
    let temp = scratch("apply-missing");
    let applying = Applying::default();
    let events = collect(compose_up(
        &temp.path().join("docker"),
        temp.path(),
        "gdl-api",
        applying.start().expect("guard"),
    ))
    .await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["done"], true);
    assert_eq!(events[0]["ok"], false);
    assert!(
        events[0]["problem"]
            .as_str()
            .is_some_and(|p| p.contains("`docker` was not found")),
        "{events:?}"
    );
    assert!(applying.start().is_some());
}

/// The page may leave while images build: the command still runs to its
/// end, and the next apply waits for it.
#[tokio::test]
async fn an_apply_outlives_the_page_that_started_it() {
    let temp = scratch("apply-leaves");
    let marker = temp.path().join("finished");
    let docker = fake(
        temp.path(),
        "docker",
        &format!(
            "i=0; while [ $i -lt 2000 ]; do echo line $i; i=$((i+1)); done; touch {:?}",
            marker.display().to_string()
        ),
    );
    let applying = Applying::default();
    let events = compose_up(
        &docker,
        temp.path(),
        "gdl-api",
        applying.start().expect("guard"),
    );
    drop(events);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while applying.start().is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the apply never ended"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(marker.exists(), "the command ran to its end");
}
