//! `deploy` takes no namespace over. It applies the Namespace only where
//! none exists. Where one does, it only adds the fleet's labels, with
//! `kubectl label`, and `down` only removes them. A server-side apply of
//! a namespace that another owner made with a client-side apply takes
//! over the fields that owner set, and removes those that gdl's object
//! does not list: the owner's labels and its last-applied annotation.

use super::*;

/// The namespace as its owner's client-side apply made it, with a label
/// and annotations of the owner's.
fn owned() -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "gdl-api",
            "uid": "owned",
            "labels": {"example.com/setup": "gr-aws"},
            "annotations": {
                "kubectl.kubernetes.io/last-applied-configuration": "{\"kind\":\"Namespace\"}",
                "example.com/owner": "platform",
            },
        },
    })
}

/// The Namespace that `context` holds.
fn namespace<'a>(kube: &'a Kube, context: &str) -> &'a Value {
    &kube.objects[context][&("Namespace".to_string(), "gdl-api".to_string())]
}

/// How many applies in `context` carried a Namespace.
fn namespace_applies(log: &Log, context: &str) -> usize {
    log.borrow()
        .iter()
        .filter(|call| match call {
            Call::Kube { args, input } => {
                args[1] == context
                    && args.contains(&"apply".to_string())
                    && input.contains("\"Namespace\"")
            }
            Call::Docker => false,
        })
        .count()
}

/// Each `kubectl label` in `context`, after the context and before the
/// request timeout.
fn labels(log: &Log, context: &str) -> Vec<Vec<String>> {
    commands(log)
        .into_iter()
        .filter(|args| args[1] == context && args.contains(&"label".to_string()))
        .map(|args| {
            args[2..]
                .iter()
                .filter(|a| !a.starts_with("--request-timeout="))
                .cloned()
                .collect()
        })
        .collect()
}

/// A deploy into an existing namespace applies no Namespace: it labels
/// the namespace with the fleet's labels, and the owner's labels and
/// annotations stay. `down` removes only the fleet's labels.
#[test]
fn an_existing_namespace_is_only_labelled() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = Some(0);
    for context in ["kind-euw1", "kind-use1"] {
        kube.objects
            .entry(context.to_string())
            .or_default()
            .insert(("Namespace".into(), "gdl-api".into()), owned());
    }
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    for (context, cluster) in [("kind-euw1", "euw1"), ("kind-use1", "use1")] {
        assert_eq!(namespace_applies(&log, context), 0, "{context}");
        assert_eq!(
            labels(&log, context),
            [vec![
                "label".to_string(),
                "namespace/gdl-api".into(),
                "--overwrite".into(),
                "gdl.dev/cluster=".to_string() + cluster,
                "gdl.dev/fleet=api".into(),
            ]],
            "{context}"
        );
        let held = namespace(&kube, context);
        assert_eq!(
            held["metadata"]["labels"],
            json!({
                "example.com/setup": "gr-aws",
                "gdl.dev/cluster": cluster,
                "gdl.dev/fleet": "api",
            }),
            "{context}"
        );
        assert_eq!(held["metadata"], {
            let mut metadata = owned()["metadata"].clone();
            metadata["labels"] = held["metadata"]["labels"].clone();
            metadata
        });
    }
    // The namespace is labelled before the base is applied, and is Active
    // before anything else.
    let steps = steps(&log);
    let at = |step: &str| steps.iter().position(|s| s == step).expect(step);
    assert!(
        at("kind-euw1:namespace") < at("kind-euw1:label"),
        "{steps:?}"
    );
    assert!(at("kind-euw1:label") < at("kind-euw1:base"), "{steps:?}");
    assert!(
        at("kind-euw1:base") < at("kind-euw1:wait-namespace"),
        "{steps:?}"
    );

    log.borrow_mut().clear();
    down(&files, &mut kube).expect("down");
    for context in ["kind-euw1", "kind-use1"] {
        assert_eq!(namespace_applies(&log, context), 0, "{context}");
        assert_eq!(
            labels(&log, context),
            [vec![
                "label".to_string(),
                "namespace/gdl-api".into(),
                "gdl.dev/fleet-".into(),
                "gdl.dev/cluster-".into(),
            ]],
            "{context}"
        );
        assert_eq!(namespace(&kube, context), &owned(), "{context}");
    }
    assert!(
        commands(&log)
            .iter()
            .all(|args| !args.contains(&"apply".to_string())),
        "down applies nothing"
    );
}

/// A deploy where the namespace does not exist applies it with the base,
/// and labels nothing. The next deploy finds it, and only labels it.
#[test]
fn a_missing_namespace_is_applied() {
    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    deploy(&mirror, &files, &mut docker, &mut kube).expect("deploy");
    for (context, cluster) in [("kind-euw1", "euw1"), ("kind-use1", "use1")] {
        assert_eq!(namespace_applies(&log, context), 1, "{context}");
        assert!(labels(&log, context).is_empty(), "{context}");
        assert_eq!(
            namespace(&kube, context)["metadata"]["labels"],
            json!({"gdl.dev/cluster": cluster, "gdl.dev/fleet": "api"}),
            "{context}"
        );
    }

    log.borrow_mut().clear();
    deploy(&mirror, &files, &mut docker, &mut kube).expect("a second deploy");
    for context in ["kind-euw1", "kind-use1"] {
        assert_eq!(namespace_applies(&log, context), 0, "{context}");
        assert_eq!(labels(&log, context).len(), 1, "{context}");
    }
}

/// A failed question whether the namespace exists stops the deploy before
/// anything is applied: it is never read as a namespace to make.
#[test]
fn an_unanswered_namespace_question_applies_nothing() {
    struct Unanswered(Kube);
    impl gdl_gen::Kube for Unanswered {
        fn run(
            &mut self,
            args: &[String],
            input: Option<&[u8]>,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            if args.iter().any(|a| a == "namespace/gdl-api") && args.contains(&"get".to_string()) {
                return Ok(gdl_gen::Reply::failed(
                    "Error from server (Forbidden): namespaces \"gdl-api\" is forbidden",
                ));
            }
            self.0.run(args, input)
        }
    }

    let mirror = Mirror::new();
    let files = files(&mirror, &fleet_text());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Unanswered(Kube::new(&log));
    let how = gdl_gen::Deploy {
        spec: files.spec.clone(),
        fleet: files.fleet.clone(),
        target: mirror.target(),
        out: None,
        waits: waits(),
        sources: gdl_gen::Sources::Fetched,
    };
    let error = gdl_gen::deploy(&how, &mut docker, &mut kube, &mut |_| {})
        .expect_err("an unanswered question")
        .to_string();
    assert!(error.contains("is forbidden"), "{error}");
    assert!(
        commands(&log).iter().all(
            |args| !args.contains(&"apply".to_string()) && !args.contains(&"label".to_string())
        ),
        "{:?}",
        commands(&log)
    );
}
