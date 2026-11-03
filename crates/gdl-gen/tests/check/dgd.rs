//! A pool's DGD, named by the fleet file (`[pool.<name>.dgd]`): `check`
//! reads the file beside the fleet file and refuses each DGD that the
//! pool could not own, one problem at the table that names it, and
//! `compose`, which has no DGD, refuses the table.

use super::*;

/// The chat graph in one class of two segments, `front` and `engine`.
fn spec() -> String {
    let chat = root().join("examples/chat");
    let path = |p: &Path| format!("{:?}", p.display().to_string());
    format!(
        "[graph]\npath = {graph}\n[messages]\ncrates = [{protocols}]\n\
         [nodes]\ncrates = [{front}, {engine}, {post}]\n[gdl]\npath = {gdl}\n\
         [segment.front]\nnodes = [\"Frontend\", \"Preprocess\", \"Truncate\", \"Number\", \"Detokenize\", \"Format\"]\n\
         [segment.engine]\nnodes = [\"Engine\"]\n\
         [class.global]\nsegments = [\"front\", \"engine\"]\n",
        graph = path(&chat.join("chat.gdl")),
        protocols = path(&chat.join("protocols")),
        front = path(&chat.join("front-nodes")),
        engine = path(&chat.join("engine-mock")),
        post = path(&chat.join("post-nodes")),
        gdl = path(&root().join("crates/gdl-runtime")),
    )
}

/// A pool of [`spec`] whose `engine` segment runs in the DGD's
/// `Frontend`, in a cluster the gdl operator runs.
const FLEET: &str = "[fleet]\nname = \"chat\"\n\
    [cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-chat\"\nregion = \"us-east-1\"\noperator = true\n\
    [class.global]\nready = { front = 1, engine = 1 }\n\
    [pool.chat-use1]\nclass = \"global\"\ncluster = \"use1\"\n\
    [pool.chat-use1.dgd]\nfile = \"dynamo/dgd.yaml\"\ncomponents = { engine = \"Frontend\" }\n";

/// A DGD that pool `chat-use1` can own: `Frontend` runs the segment, and
/// dynamo's `Worker` beside it.
const DGD: &str = "apiVersion: nvidia.com/v1beta1
kind: DynamoGraphDeployment
metadata:
  name: chat-use1
spec:
  components:
  - name: Frontend
    type: frontend
    scalingAdapter: {}
    runtimeVersionOverride: 1.7.0
    podTemplate:
      spec:
        containers:
        - name: main
          image: placeholder.invalid/chat-engine:dgd
  - name: Worker
    type: decode
    replicas: 3
";

/// The spec, `fleet`, and `dgd` at `dynamo/dgd.yaml` beside the fleet.
fn files(fleet: &str, dgd: &str) -> Files {
    let files = Files::new(&spec(), fleet);
    let dynamo = files.dir.path().join("dynamo");
    std::fs::create_dir(&dynamo).expect("a directory");
    std::fs::write(dynamo.join("dgd.yaml"), dgd).expect("the DGD");
    files
}

/// `check` of `fleet` and `dgd` finds exactly one problem, at the pool's
/// `dgd` table, `key` when given, that holds `needle`.
fn refused(fleet: &str, dgd: &str, key: Option<&str>, needle: &str) {
    let files = files(fleet, dgd);
    let check = files.check();
    assert_eq!(check.status.code(), Some(1), "{check:?}");
    let found = problems(&check);
    let f = files.fleet().display().to_string();
    let at = match key {
        Some(key) => format!("{f}: [pool.chat-use1.dgd] {key}: "),
        None => format!("{f}: [pool.chat-use1.dgd]: "),
    };
    assert_eq!(found.len(), 1, "{}", found.join("\n"));
    assert!(found[0].starts_with(&at), "{}\nnot at {at}", found[0]);
    assert!(found[0].contains(needle), "{}\nlacks {needle}", found[0]);
}

#[test]
fn a_pool_with_a_dgd_it_can_own_checks_clean() {
    let files = files(FLEET, DGD);
    let output = files.check();
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn check_refuses_a_missing_dgd_file() {
    let files = Files::new(&spec(), FLEET);
    let check = files.check();
    let found = problems(&check);
    let f = files.fleet().display().to_string();
    assert_eq!(found.len(), 1, "{}", found.join("\n"));
    assert!(
        found[0].starts_with(&format!("{f}: [pool.chat-use1.dgd] file: ")),
        "{}",
        found[0]
    );
    let dgd = files.dir.path().join("dynamo/dgd.yaml");
    assert!(
        found[0].contains(&format!("{} cannot be read", dgd.display())),
        "{}",
        found[0]
    );
}

#[test]
fn check_refuses_a_dgd_that_is_not_one_yaml_document() {
    refused(FLEET, "spec: [", Some("file"), "is not YAML");
    refused(
        FLEET,
        &format!("{DGD}---\n{DGD}"),
        Some("file"),
        "2 YAML documents",
    );
}

#[test]
fn check_refuses_a_dgd_of_another_api_version() {
    let dgd = DGD.replace("nvidia.com/v1beta1", "nvidia.com/v1alpha1");
    refused(
        FLEET,
        &dgd,
        Some("file"),
        "a pool owns a DynamoGraphDeployment of nvidia.com/v1beta1",
    );
}

#[test]
fn check_refuses_a_component_the_dgd_does_not_have() {
    let fleet = FLEET.replace("engine = \"Frontend\"", "engine = \"Router\"");
    refused(
        &fleet,
        DGD,
        Some("file"),
        "segment `engine` runs in component `Router`, which the pool's DGD does not have",
    );
}

#[test]
fn check_refuses_a_component_without_a_scaling_adapter() {
    let dgd = DGD.replace("    scalingAdapter: {}\n", "");
    refused(FLEET, &dgd, Some("file"), "give it `scalingAdapter: {}`");
}

/// gdl's image tags are never semantic versions, and dynamo's webhook
/// refuses a component whose tag is not one unless it names its version.
#[test]
fn check_refuses_a_component_without_a_runtime_version_override() {
    let dgd = DGD.replace("    runtimeVersionOverride: 1.7.0\n", "");
    refused(
        FLEET,
        &dgd,
        Some("file"),
        "component `Frontend` of the pool's DGD runs a segment, whose image tag is not a semantic version: give it `runtimeVersionOverride`",
    );
}

#[test]
fn check_refuses_a_dgd_whose_name_or_namespace_is_not_the_pools() {
    let renamed = DGD.replace("name: chat-use1", "name: chat-use2");
    refused(
        FLEET,
        &renamed,
        Some("file"),
        "has name `chat-use2`, and it must be `chat-use1`",
    );
    let moved = DGD.replace("name: chat-use1", "name: chat-use1\n  namespace: other");
    refused(
        FLEET,
        &moved,
        Some("file"),
        "has namespace `other`, and it must be `gdl-chat`",
    );
}

#[test]
fn check_refuses_a_component_named_by_two_segments() {
    let fleet = FLEET.replace(
        "components = { engine = \"Frontend\" }",
        "components = { engine = \"Frontend\", front = \"Frontend\" }",
    );
    refused(
        &fleet,
        DGD,
        Some("file"),
        "segments `engine` and `front` both run in component `Frontend`",
    );
}

#[test]
fn check_refuses_components_naming_a_segment_the_pool_does_not_have() {
    let fleet = FLEET.replace("engine = \"Frontend\"", "post = \"Frontend\"");
    refused(
        &fleet,
        DGD,
        Some("components.post"),
        "not a segment of class `global`",
    );
}

#[test]
fn check_refuses_a_component_whose_image_cannot_be_placed_exactly_once() {
    let nameless = DGD.replace("- name: main", "- name: app");
    refused(
        FLEET,
        &nameless,
        Some("file"),
        "has 0 containers named `main`",
    );
    let twice = DGD.replace(
        "          image: placeholder.invalid/chat-engine:dgd\n",
        "          image: placeholder.invalid/chat-engine:dgd\n        - name: main\n          image: other\n",
    );
    refused(FLEET, &twice, Some("file"), "has 2 containers named `main`");
}

#[test]
fn check_refuses_a_dgd_without_the_gdl_operator() {
    let plain = FLEET.replace("operator = true\n", "");
    refused(
        &plain,
        DGD,
        None,
        "a pool with a DGD needs the gdl operator, which owns the DGD, its stop, and its scaling: without it, `deploy`'s prune and `down` cannot find the DGD. Give `[cluster.use1]` `operator = true`",
    );
}

/// Compose runs no DGD, so a fleet that `check` passes is refused there.
#[test]
fn compose_refuses_a_dgd_table() {
    let files = files(FLEET, DGD);
    let out = files.dir.path().join("deployment");
    let compose = files.run("compose", &["--out".as_ref(), out.as_os_str()]);
    assert!(!compose.status.success(), "{compose:?}");
    let stderr = String::from_utf8_lossy(&compose.stderr);
    assert!(
        stderr.contains(
            "pool `chat-use1` has a DGD (`[pool.chat-use1.dgd]`), which only `cargo gdl k8s` and `deploy` render"
        ),
        "{stderr}"
    );
    assert!(!out.exists(), "compose wrote a bundle");
}

/// Dynamo's DGD schema requires a string in canonical MAJOR.MINOR.PATCH
/// form; an empty override also leaves the generated image unversioned.
#[test]
fn check_refuses_runtime_version_overrides_the_dgd_schema_rejects() {
    let control = files(FLEET, DGD).check();
    assert!(control.status.success(), "{control:?}");

    let mut accepted = Vec::new();
    for value in ["\"\"", "\"latest\"", "\"1.7\"", "true", "7"] {
        let dgd = DGD.replace(
            "runtimeVersionOverride: 1.7.0",
            &format!("runtimeVersionOverride: {value}"),
        );
        let output = files(FLEET, &dgd).check();
        if output.status.success() {
            accepted.push(value);
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.contains("runtimeVersionOverride"), "{stderr}");
        }
    }
    assert!(
        accepted.is_empty(),
        "check accepted runtimeVersionOverride values that Dynamo rejects: {accepted:?}"
    );
}
