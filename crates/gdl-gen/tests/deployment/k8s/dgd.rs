//! A pool that owns a DynamoGraphDeployment (DGD), from the fleet file
//! (`[pool.<name>.dgd]`): its `GdlPool` holds the DGD file, with the
//! `main` image of each component that runs a segment set to that
//! segment's own image, and names the component in the segment. The rest
//! of the file is left as written. A segment the table does not name
//! stays one of gdl's own Deployments.

use super::*;

const DGD_FLEET: &str = "tests/fixtures/k8s/dgd/fleet.toml";
const DGD_PINNED: &str = "tests/fixtures/k8s/dgd/out";

/// The fixture DGD as JSON, as a YAML reader other than gdl's writes it
/// (PyYAML's `safe_load`, then `json.dumps`): what the global router's
/// `kind.sh dynamo` put in its `GdlPool` by hand, before `cargo gdl` read
/// the file itself.
const DGD_JSON: &str = "tests/fixtures/k8s/dgd/dynamo/dgd.json";

/// The pool `dgd-use1`'s table in [`DGD_FLEET`].
const DGD_TABLE: &str =
    "[pool.dgd-use1.dgd]\nfile = \"dynamo/dgd.yaml\"\ncomponents = { engine = \"Frontend\" }\n";

/// [`DGD_FLEET`] and the DGD files it names, read beside it.
fn dgd_fleet() -> (gdl_model::Fleet, gdl_gen::Dgds) {
    let path = manifest_dir().join(DGD_FLEET);
    let fleet = fleet(&std::fs::read_to_string(&path).expect("the fleet"));
    let dgds = gdl_gen::Dgds::read(&fleet, &path).expect("the DGD files");
    (fleet, dgds)
}

/// The objects of `fleet`, whose DGD files are `dgds`, tagged [`TAG`].
fn render_dgd(
    mirror: &Mirror,
    spec: &gdl_gen::Spec,
    fleet: &gdl_model::Fleet,
    dgds: &gdl_gen::Dgds,
) -> gdl_gen::Files {
    gdl_gen::k8s_files(
        spec,
        fleet,
        dgds,
        &mirror.target(),
        Some(TAG),
        None,
        gdl_gen::Sources::Fetched,
    )
    .expect("objects")
}

/// The `GdlPool` of pool `name` in cluster `use1`.
fn pool_of(files: &gdl_gen::Files, name: &str) -> Value {
    let path = Path::new("use1/pools").join(format!("{name}.yaml"));
    let mut pools = documents(&files[&path]);
    assert_eq!(pools.len(), 1, "{pools:?}");
    let pool = pools.remove(0);
    assert_eq!(pool["kind"], "GdlPool");
    pool
}

/// `pool` with its `spec.dgd`, JSON text, as the JSON it holds: two
/// writers of one DGD may order its keys or space it differently.
fn parsed(mut pool: Value) -> Value {
    if let Some(text) = pool["spec"]["dgd"].as_str() {
        pool["spec"]["dgd"] = serde_json::from_str(text).expect("the DGD as JSON");
    }
    pool
}

/// The segment `name` of a `GdlPool`.
fn segment<'a>(pool: &'a mut Value, name: &str) -> &'a mut Value {
    pool["spec"]["segments"]
        .as_array_mut()
        .expect("segments")
        .iter_mut()
        .find(|segment| segment["name"] == name)
        .expect("the segment")
}

/// The golden: the DGD pool's `GdlPool` is what `kind.sh dynamo` assembled
/// by hand. That is gdl's render of the pool without the table, with the
/// DGD file as JSON in `spec.dgd`, the `main` image of its `Frontend`
/// rewritten to the `engine` segment's own image, and `engine` placed in
/// `Frontend`. The mockers' image, `runtimeVersionOverride`,
/// `scalingAdapter`, the workers' args and Grove off stay as written.
/// Every file the render writes for [`DGD_FLEET`] is pinned too, byte for
/// byte: the render is a pure function of the fleet, the DGD file and the
/// images. Each of the two renders takes seconds: it reads the test
/// mirror's sources.
#[test]
fn a_dgd_pool_is_its_pool_with_the_dgd_file_and_its_component() {
    let mirror = Mirror::new();
    let spec = spec(&mirror);
    let (fleet, dgds) = dgd_fleet();
    let files = render_dgd(&mirror, &spec, &fleet, &dgds);
    assert_pinned(&mirror, &files, DGD_PINNED);
    let rendered = pool_of(&files, "dgd-use1");

    let text = std::fs::read_to_string(manifest_dir().join(DGD_FLEET)).expect("the fleet");
    assert!(text.contains(DGD_TABLE), "{text}");
    let plain =
        render(&mirror, &spec, &self::fleet(&text.replace(DGD_TABLE, ""))).expect("objects");
    let mut expected = pool_of(&plain, "dgd-use1");
    let engine = segment(&mut expected, "engine");
    let image = engine["image"].as_str().expect("an image").to_string();
    assert_eq!(image, "registry.example/gdl/api-engine:pinned");
    engine["component"] = "Frontend".into();
    let mut dgd: Value = serde_json::from_str(
        &std::fs::read_to_string(manifest_dir().join(DGD_JSON)).expect("JSON"),
    )
    .expect("the DGD");
    let main = &mut dgd["spec"]["components"][0]["podTemplate"]["spec"]["containers"][0];
    assert_eq!(main["name"], "main");
    assert_eq!(main["image"], "placeholder.invalid/gdl/api-engine:dgd");
    main["image"] = image.into();
    expected["spec"]["dgd"] = dgd.to_string().into();

    assert_eq!(parsed(rendered), parsed(expected));
}

/// The api graph in one class of two segments, `front` and `engine`.
const ONE_CLASS: &str = "[segment.front]\nnodes = [\"Http\"]\n[segment.engine]\nnodes = [\"Engine\"]\n\
    [class.global]\nsegments = [\"front\", \"engine\"]\n";

/// A pool of two segments, one of them in its DGD: the operator renders a
/// Deployment for `front`, none for `engine`, whose pods dynamo's
/// operator runs from the DGD, with `engine`'s image. The render takes
/// seconds: it reads the test mirror's sources.
#[test]
fn a_segment_the_dgd_table_does_not_name_stays_a_deployment() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, &format!("{ONE_CLASS}{}", deployment()));
    let temp = tempfile::Builder::new()
        .prefix("gdl-k8s-dgd-")
        .tempdir()
        .expect("a directory");
    let fixture = manifest_dir().join("tests/fixtures/k8s/dgd/dynamo/dgd.yaml");
    let dgd = std::fs::read_to_string(fixture)
        .expect("the DGD")
        .replace("name: dgd-use1", "name: global-us");
    std::fs::create_dir(temp.path().join("dynamo")).expect("a directory");
    std::fs::write(temp.path().join("dynamo/dgd.yaml"), dgd).expect("the DGD");
    let text = "[fleet]\nname = \"api\"\n\
        [cluster.use1]\ncontext = \"kind-use1\"\nnamespace = \"gdl-api\"\nregion = \"us-east-1\"\n\
        registry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n\
        secrets = { fingerprint = \"api-fingerprint\" }\noperator = true\n\
        [class.global]\nready = { front = 1, engine = 1 }\n\
        [pool.global-us]\nclass = \"global\"\ncluster = \"use1\"\n\
        [pool.global-us.dgd]\nfile = \"dynamo/dgd.yaml\"\ncomponents = { engine = \"Frontend\" }\n";
    let path = temp.path().join("fleet.toml");
    std::fs::write(&path, text).expect("the fleet");
    let fleet = fleet(text);
    let dgds = gdl_gen::Dgds::read(&fleet, &path).expect("the DGD files");
    let pool = pool_of(&render_dgd(&mirror, &spec, &fleet, &dgds), "global-us");
    let made = operated_of(&pool);
    let find = |kind: &str, name: &str| {
        made.iter()
            .find(|o| o["kind"] == kind && o["metadata"]["name"] == name)
            .cloned()
    };
    let front = find("Deployment", "global-us-front").expect("front's Deployment");
    assert!(find("Deployment", "global-us-engine").is_none());
    let rendered = find("DynamoGraphDeployment", "global-us").expect("the DGD");
    let image = |segment: &str| {
        pool["spec"]["segments"]
            .as_array()
            .expect("segments")
            .iter()
            .find(|s| s["name"] == segment)
            .expect("the segment")["image"]
            .clone()
    };
    assert_eq!(container(&front)["image"], image("front"));
    let main = &rendered["spec"]["components"][0]["podTemplate"]["spec"]["containers"][0];
    assert_eq!(main["image"], image("engine"));
    assert_ne!(image("front"), image("engine"));
}

/// A fleet that names a DGD file renders only with the file read: the
/// render refuses to guess it.
#[test]
fn a_dgd_table_renders_only_with_its_file_read() {
    let mirror = Mirror::new();
    let (fleet, _) = dgd_fleet();
    let error = gdl_gen::k8s_files(
        &spec(&mirror),
        &fleet,
        &gdl_gen::Dgds::default(),
        &mirror.target(),
        Some(TAG),
        None,
        gdl_gen::Sources::Fetched,
    )
    .expect_err("refused");
    assert!(
        error
            .to_string()
            .contains("pool `dgd-use1` names a DGD file"),
        "{error}"
    );
}

fn render_file(mirror: &Mirror, text: &str) -> Value {
    let temp = tempfile::Builder::new()
        .prefix("gdl-dgd-yaml-")
        .tempdir()
        .expect("a directory");
    let root = manifest_dir().join("../..");
    let source = std::fs::read_to_string(root.join("examples/api/gdl.toml")).expect("the spec");
    let source = mirror
        .portable(&source)
        .replace(super::super::SEGMENTS, &format!("{SPLIT}{}", deployment()));
    let spec_path = temp.path().join("gdl.toml");
    std::fs::write(&spec_path, source).expect("the spec");
    std::fs::copy(
        root.join("examples/api/api.gdl"),
        temp.path().join("api.gdl"),
    )
    .expect("the graph");
    let fleet_path = temp.path().join("fleet.toml");
    let fleet_text = std::fs::read_to_string(manifest_dir().join(DGD_FLEET)).expect("the fleet");
    std::fs::write(&fleet_path, &fleet_text).expect("the fleet");
    std::fs::create_dir(temp.path().join("dynamo")).expect("a directory");
    std::fs::write(temp.path().join("dynamo/dgd.yaml"), text).expect("the DGD");
    let problems = gdl_gen::check_files(&spec_path, Some(&fleet_path), &mirror.target());
    assert!(problems.is_empty(), "{problems}");
    let spec = gdl_gen::Spec::load(&spec_path).expect("the spec");
    let fleet = fleet(&fleet_text);
    let dgds = gdl_gen::Dgds::read(&fleet, &fleet_path).expect("the DGD files");
    parsed(pool_of(
        &render_dgd(mirror, &spec, &fleet, &dgds),
        "dgd-use1",
    ))["spec"]["dgd"]
        .clone()
}

/// Kubernetes expands merge keys before decoding a node selector as
/// string values; an unexpanded merge becomes an invalid selector value.
#[test]
fn a_dgd_yaml_merge_keeps_the_inherited_node_selector() {
    let mirror = Mirror::new();
    let fixture = manifest_dir().join("tests/fixtures/k8s/dgd/dynamo/dgd.yaml");
    let source = std::fs::read_to_string(fixture).expect("the DGD");
    let anchored = source
        .replace(
            "terminationGracePeriodSeconds: 120",
            "terminationGracePeriodSeconds: 120\n        nodeSelector: &placement\n          kubernetes.io/arch: amd64",
        )
        .replace(
            "  - name: Worker\n    type: decode\n    replicas: 3\n    podTemplate:\n      spec:\n",
            "  - name: Worker\n    type: decode\n    replicas: 3\n    podTemplate:\n      spec:\n        nodeSelector:\n          <<: *placement\n          node.kubernetes.io/instance-type: gpu\n",
        );
    assert!(anchored.contains("<<: *placement"));
    let expanded = anchored.replace("<<: *placement", "kubernetes.io/arch: amd64");
    let expected = render_file(&mirror, &expanded);
    assert_eq!(
        expected["spec"]["components"][1]["podTemplate"]["spec"]["nodeSelector"]["kubernetes.io/arch"],
        "amd64"
    );
    let rendered = render_file(&mirror, &anchored);
    assert_eq!(
        rendered["spec"]["components"][1]["podTemplate"]["spec"]["nodeSelector"],
        expected["spec"]["components"][1]["podTemplate"]["spec"]["nodeSelector"]
    );
}
