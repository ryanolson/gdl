use super::*;

/// A change to a spec outside the copied sources changes the generated
/// Rust, which the tag covers with every byte the image builds from: the
/// changed image is pushed again.
#[test]
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

/// A link adds a type and a target to the digest. Those fields must not
/// read like the paths and contents of a different set of regular files.
#[cfg(unix)]
#[test]
fn different_file_and_link_trees_cannot_reuse_published_tags() {
    #[derive(Default)]
    struct Kinds {
        registry: BTreeMap<String, String>,
        seen: Vec<BTreeMap<String, String>>,
    }

    impl gdl_gen::Docker for Kinds {
        fn digest(&mut self, reference: &str) -> Result<Option<String>, gdl_gen::Error> {
            Ok(self.registry.get(reference).cloned())
        }

        fn build(
            &mut self,
            context: &Path,
            _: &str,
            reference: &str,
            push: bool,
        ) -> Result<String, gdl_gen::Error> {
            let mut seen = BTreeMap::new();
            for name in ["lia", "lib", "lic", "link"] {
                let path = context.join(name);
                if std::fs::symlink_metadata(&path).is_err() {
                    continue;
                }
                let value = match std::fs::read_link(&path) {
                    Ok(target) => format!("link:{}", target.display()),
                    Err(_) => format!("file:{}", std::fs::read_to_string(path).expect("a file")),
                };
                seen.insert(name.to_string(), value);
            }
            self.seen.push(seen);
            let digest = format!("sha256:{:064x}", self.seen.len());
            if push {
                self.registry.insert(reference.to_string(), digest.clone());
            }
            Ok(digest)
        }
    }

    let copy = Copy::new();
    for (name, text) in [("lia", "link"), ("lib", "lic"), ("link", "lid")] {
        std::fs::write(copy.path().join(name), text).expect("a regular source file");
    }
    copy.add();
    let mut docker = Kinds::default();
    let build = |push, docker: &mut Kinds| {
        gdl_gen::images(
            &copy.spec(),
            &fleet(),
            &target(),
            push,
            gdl_gen::Sources::Local,
            docker,
        )
        .expect("local images")
    };
    let first = build(true, &mut docker);
    for name in ["lia", "lib", "link"] {
        std::fs::remove_file(copy.path().join(name)).expect("remove the regular file");
    }
    for (name, target) in [("lia", "lib"), ("lic", "lid")] {
        std::os::unix::fs::symlink(target, copy.path().join(name))
            .expect("an internal relative link");
    }
    copy.add();
    let second = build(false, &mut docker);
    assert_eq!(docker.seen.len(), 6);
    assert_ne!(docker.seen[0], docker.seen[3], "the contexts differ");
    let cached = build(true, &mut docker);
    assert!(
        cached
            .iter()
            .all(|image| image.outcome == gdl_gen::Outcome::Pushed),
        "a different file/link tree reused published tags; before: {:?}; after: {:?}; outcomes: {cached:?}",
        tags(&first),
        tags(&second)
    );
}
