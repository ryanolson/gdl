use super::*;

#[test]
/// A change to a spec outside the copied sources changes the generated
/// Rust, which the tag covers with every byte the image builds from: the
/// changed image is pushed again.
fn an_external_spec_change_moves_the_generated_images_tag() {
    let copy = Copy::new();
    let deployment = tempfile::Builder::new()
        .prefix("gdl-external-spec-")
        .tempdir()
        .expect("a deployment outside the selected sources");
    let api = copy.path().join("examples/api");
    let text = std::fs::read_to_string(api.join("gdl.toml"))
        .expect("a spec")
        .replace(SEGMENTS, SPLIT)
        .replace(
            "crates = [\".\"]",
            &format!(
                "crates = [{}]",
                toml::Value::String(api.display().to_string())
            ),
        )
        .replace(
            "path = \"../../crates/gdl-runtime\"",
            &format!(
                "path = {}",
                toml::Value::String(copy.path().join("crates/gdl-runtime").display().to_string())
            ),
        );
    let graph = std::fs::read_to_string(api.join("api.gdl")).expect("a graph");
    let spec = |text: &str| {
        std::fs::write(deployment.path().join("gdl.toml"), text).expect("a deployment spec");
        gdl_gen::Spec::from_text(text, &graph, deployment.path()).expect("a valid deployment spec")
    };
    let before = spec(&text);
    let mut first = Fake::default();
    let built = run_with(
        &before,
        &fleet(),
        &target(),
        true,
        gdl_gen::Sources::Local,
        &mut first,
    );
    let removed = "[entry.anthropic]\noperator = \"api_frontend::AnthropicAdapter\"\nnested = { target = \"Http\" }\n";
    assert!(text.contains(removed));
    let after = spec(
        &text
            .replace(removed, "")
            .replace("enabled_anthropic = true", "enabled_anthropic = false"),
    );
    let mut second = Fake::default();
    let changed = run_with(
        &after,
        &fleet(),
        &target(),
        false,
        gdl_gen::Sources::Local,
        &mut second,
    );
    let front = |docker: &Fake| {
        docker
            .contexts
            .iter()
            .find(|(reference, _)| reference.contains("/api-front:"))
            .expect("the front context")
            .1
            .iter()
            .filter(|(path, _)| {
                path.starts_with("target/gdl-images/front")
                    && path.extension().is_some_and(|extension| extension == "rs")
            })
            .map(|(path, text)| (path.clone(), text.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_ne!(
        front(&first),
        front(&second),
        "the generated Rust must change"
    );
    let cached = run_with(
        &after,
        &fleet(),
        &target(),
        true,
        gdl_gen::Sources::Local,
        &mut first,
    );
    let frontend = cached
        .iter()
        .find(|image| image.reference.contains("/api-front:"))
        .expect("the frontend image");
    assert_eq!(
        frontend.outcome,
        gdl_gen::Outcome::Pushed,
        "the changed frontend reused its published tag; before: {:?}; after: {:?}",
        tags(&built),
        tags(&changed)
    );
}
