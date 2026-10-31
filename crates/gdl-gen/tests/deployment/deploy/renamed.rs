//! A pool that the fleet still places keeps serving while its renamed
//! segment's old objects are pruned: it is neither drained nor asked
//! about at its parent.

use super::*;

/// Deploy, rename `segment` to `replacement`, and deploy again: only
/// `pool`'s objects of the old segment are deleted.
fn rename_segment(segment: &str, replacement: &str, pool: &str) {
    let mirror = Mirror::new();
    let files = files(&mirror, &plain_fleet());
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    kube.held = None;
    deploy(&mirror, &files, &mut docker, &mut kube).expect("the initial deployment");
    let replaced: Vec<String> = kube.objects["kind-euw1"]
        .keys()
        .filter(|(_, name)| name == &format!("{pool}-{segment}"))
        .map(|(kind, name)| match kind.as_str() {
            // Each by its resource, qualified by its group, as deletes name it.
            "Deployment" => format!("deployments.apps/{name}"),
            "Service" => format!("services/{name}"),
            other => panic!("a {other} of the segment"),
        })
        .collect();
    assert!(!replaced.is_empty(), "the initial segment exists");

    let text = std::fs::read_to_string(&files.spec)
        .expect("the spec")
        .replace(
            &format!("segment.{segment}"),
            &format!("segment.{replacement}"),
        )
        .replace(
            &format!("segments = [\"{segment}\"]"),
            &format!("segments = [\"{replacement}\"]"),
        );
    std::fs::write(&files.spec, text).expect("the renamed segment");
    let fleet = std::fs::read_to_string(&files.fleet)
        .expect("the fleet")
        .replace(&format!("{segment} = "), &format!("{replacement} = "));
    std::fs::write(&files.fleet, fleet).expect("the renamed quorum");
    log.borrow_mut().clear();

    let result = deploy(&mirror, &files, &mut docker, &mut kube);
    let objects = &kube.objects["kind-euw1"];
    for name in [pool.to_string(), format!("{pool}-{replacement}")] {
        assert!(
            objects.contains_key(&("Deployment".to_string(), name.clone())),
            "the retained hub or replacement segment was not applied: {name}; {result:?}"
        );
    }
    assert!(
        result.is_ok(),
        "pruning a replaced segment must keep its rendered pool serving: {result:?}"
    );
    assert_eq!(
        deletes(&log, "kind-euw1")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        replaced,
    );
    assert!(
        !commands(&log).iter().any(|args| args
            .iter()
            .any(|arg| { arg.ends_with("/drain") || arg.contains("/report/") })),
        "a pool that remains rendered must not be withdrawn"
    );
}

/// Two deploys exercise a segment rename while the root pool stays rendered.
#[test]
fn renaming_a_root_segment_prunes_the_replaced_deployment() {
    rename_segment("front", "entry", "global-eu");
}

/// Two deploys exercise a segment rename while the child pool and its hub stay rendered.
#[test]
fn renaming_a_child_segment_prunes_the_replaced_deployment() {
    rename_segment("engine", "worker", "model-x-eu");
}
