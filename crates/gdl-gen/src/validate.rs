//! A spec checked against its contract's manifest: the placement, the
//! segment and package names, and the bindings, every problem at once.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Error, Spec, dump, emit, paths, spec};

/// The message crates whose library takes a name that the generated
/// contract uses for another crate: `gdl`, or the contract's own.
pub(crate) fn library_clashes(contract: &str, crates: &[spec::ListedCrate]) -> Vec<String> {
    crates
        .iter()
        .filter_map(|c| {
            let whose = if c.lib == "gdl" {
                "the gdl runtime's name in the generated contract"
            } else if c.lib == contract {
                "the generated contract's name"
            } else {
                return None;
            };
            Some(format!(
                "[messages] crate `{}`: its library `{}` is {whose}",
                c.package, c.lib
            ))
        })
        .collect()
}

/// Check the placement and the bindings against the manifest, reporting
/// every problem found.
pub(crate) fn validate(
    spec: &Spec,
    contract: &spec::Contract,
    manifest: &gdl_model::Manifest,
) -> Result<gdl_model::Layout, Error> {
    let mut problems = spec.problems.clone();
    let mut labels = BTreeSet::new();
    for (root, name) in &spec.pipeline_names {
        if manifest
            .node(root)
            .is_none_or(|n| n.role != gdl_model::Role::Source)
        {
            problems.push(format!("pipeline name refers to unknown root `{root}`"));
        }
        if name.trim().is_empty() || !labels.insert(name) {
            problems.push("pipeline names must be nonempty and unique".into());
        }
    }
    let entry_nodes = spec.service.as_ref().map(|s| s.nodes()).unwrap_or_default();
    let entry_names: BTreeSet<_> = entry_nodes.iter().map(|n| n.name.as_str()).collect();
    let bindings: BTreeMap<_, _> = spec
        .bind
        .iter()
        .chain(entry_nodes.iter().map(|n| (&n.name, &n.bind)))
        .collect();
    let source_name = manifest
        .nodes
        .iter()
        .find(|n| n.role == gdl_model::Role::Source)
        .map(|n| n.name.as_str())
        .unwrap_or("");

    if spec.service.is_none()
        && spec.bind.contains_key(source_name)
        && manifest
            .nodes
            .iter()
            .filter(|n| n.role == gdl_model::Role::Source)
            .count()
            > 1
    {
        problems.push("multiple pipeline roots require a service frontend".into());
    }
    if !spec.entry.is_empty() && spec.service.is_none() {
        problems.push("entry adapters require a service frontend".into());
    }
    // Where a node's crate comes from, as the spec writes it: a contract
    // spec binds each node in a table, a graph spec through `[nodes]`.
    let graph_spec = matches!(spec.source, spec::Source::Graph(_));
    let bound_by = |node: &str| {
        if graph_spec {
            format!("node `{node}`'s [nodes] entry")
        } else {
            format!("[bind.{node}]")
        }
    };
    let layout = match gdl_model::layout(manifest, &spec.placement) {
        Ok(layout) => Some(layout),
        Err(errors) => {
            problems.push(errors.to_string());
            None
        }
    };
    if !manifest
        .name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
    {
        problems.push(format!(
            "graph `{}`: its name starts every package name, so it must start with a letter",
            manifest.name
        ));
    }
    if spec.service.is_some() {
        let host = spec
            .placement
            .segments()
            .find(|(_, nodes)| nodes.iter().any(|n| n == source_name))
            .map(|(name, _)| name);
        for root in manifest
            .nodes
            .iter()
            .filter(|n| n.role == gdl_model::Role::Source)
        {
            let placement = spec
                .placement
                .segments()
                .find(|(_, nodes)| nodes.contains(&root.name))
                .map(|(name, _)| name);
            if placement != host {
                problems.push(format!("pipeline entry `{}` must be hosted with its frontend; place its processing nodes independently", root.name));
            }
        }
    }
    // Package names are what Cargo sees: two segments that differ only in
    // case or in `-` against `_` would build one package twice.
    let mut packages: BTreeMap<String, &str> = BTreeMap::new();
    for (segment, nodes) in spec.placement.segments() {
        let mut hosted: BTreeSet<&str> = nodes.iter().map(String::as_str).collect();
        if hosted.contains(source_name) {
            hosted.extend(entry_names.iter().copied());
        }
        let valid = !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !valid {
            problems.push(format!(
                "segment `{segment}`: a segment name is letters, digits, `-`, and `_`"
            ));
            continue;
        }
        // The generated contract is written to `contract/`, whatever its
        // package is called; case is folded for file systems that fold it.
        if graph_spec && segment.eq_ignore_ascii_case("contract") {
            problems.push(format!(
                "segment `{segment}`: the generated contract crate is `contract/`; rename the segment"
            ));
            continue;
        }
        let package = emit::package(manifest, segment);
        // A generated crate depends on the contract and on implementations
        // by package name; Cargo cannot hold two path packages of one name.
        if package == contract.package {
            problems.push(format!(
                "segment `{segment}` becomes package `{package}`, the contract's name"
            ));
        }
        for (node, bind) in &bindings {
            if package == bind.package {
                let whose = if graph_spec {
                    format!("node `{node}`'s [nodes] crate")
                } else {
                    format!("[bind.{node}]'s crate")
                };
                problems.push(format!(
                    "segment `{segment}` becomes package `{package}`, the name of {whose}"
                ));
            }
        }
        // Rust code names crates by library name; each crate the segment's
        // code sees must have its own.
        let mut seen: BTreeMap<String, String> = BTreeMap::new();
        let mut see = |lib: &str, what: String, problems: &mut Vec<String>| {
            if let Some(first) = seen.get(lib) {
                if *first != what {
                    problems.push(format!(
                        "segment `{segment}`: crate name `{lib}` is both {first} and {what}"
                    ));
                }
            } else {
                seen.insert(lib.to_string(), what);
            }
        };
        see(
            &package.replace('-', "_"),
            "the segment's library".into(),
            &mut problems,
        );
        see(&contract.lib, "the contract".into(), &mut problems);
        for fixed in ["gdl", "serde", "tokio", "toml"] {
            see(fixed, format!("the `{fixed}` dependency"), &mut problems);
        }
        // Every crate sees these; a dependency of the same name would
        // shadow the standard library in the generated code.
        for builtin in ["std", "core", "alloc"] {
            see(builtin, "the standard library".into(), &mut problems);
        }
        // One package may implement several nodes: it is one crate.
        for (node, bind) in &bindings {
            if hosted.contains(node.as_str()) {
                see(
                    &bind.lib,
                    format!("package `{}`", bind.package),
                    &mut problems,
                );
            }
        }
        // Cargo names dependencies by package: each key in the generated
        // manifest must be distinct too, whatever the libraries are called.
        let mut keys: BTreeMap<String, String> = BTreeMap::new();
        let mut key = |name: &str, what: String, problems: &mut Vec<String>| match keys.get(name) {
            Some(first) if *first != what => problems.push(format!(
                "segment `{segment}`: dependency `{name}` is both {first} and {what}"
            )),
            Some(_) => {}
            None => {
                keys.insert(name.to_string(), what);
            }
        };
        for fixed in ["gdl", "serde", "tokio", "toml"] {
            key(fixed, format!("the `{fixed}` dependency"), &mut problems);
        }
        key(&contract.package, "the contract".into(), &mut problems);
        for (node, bind) in &bindings {
            if hosted.contains(node.as_str()) {
                let first = bindings
                    .iter()
                    .find(|(n, b)| hosted.contains(n.as_str()) && b.package == bind.package)
                    .map_or(node.as_str(), |(n, _)| n.as_str());
                key(
                    &bind.package,
                    format!("{}'s package", bound_by(first)),
                    &mut problems,
                );
            }
        }
        if let Some(other) = packages.insert(package.clone(), segment) {
            problems.push(format!(
                "segments `{other}` and `{segment}` both become package `{package}`"
            ));
        }
    }
    let mut sources: BTreeMap<&str, (spec::CrateSource, &str)> = BTreeMap::new();
    for (node, bind) in &bindings {
        if manifest.node(node).is_some_and(|n| n.is_identity()) {
            problems.push(format!(
                "identity node `{node}` cannot take an implementation binding"
            ));
        }
        if manifest.node(node).is_none() && !entry_names.contains(node.as_str()) {
            problems.push(format!(
                "{}: the graph has no node `{node}`",
                bound_by(node)
            ));
        }
        if bind.ty.trim().is_empty() {
            problems.push(format!("{}: `type` is empty", bound_by(node)));
        } else {
            match type_crate(&bind.ty) {
                None => problems.push(format!(
                    "{}: type `{}` is not a Rust type path",
                    bound_by(node),
                    bind.ty
                )),
                // The bound crate is the only one of the node's that the
                // segment depends on; a type from anywhere else cannot
                // resolve.
                Some(krate) if krate != bind.lib => problems.push(format!(
                    "{}: type `{}` is not in crate `{}`",
                    bound_by(node),
                    bind.ty,
                    bind.lib
                )),
                Some(_) => {}
            }
        }
        let path = match &bind.source {
            spec::CrateSource::Path(path) => spec::CrateSource::Path(
                dump::absolute(path).unwrap_or_else(|_| paths::normalize(path)),
            ),
            version => version.clone(),
        };
        match sources.get(bind.package.as_str()) {
            Some((first, other)) if *first != path => problems.push(format!(
                "{} and {} give package `{}` two sources",
                bound_by(other),
                bound_by(node),
                bind.package
            )),
            Some(_) => {}
            None => {
                sources.insert(&bind.package, (path, node));
            }
        }
    }
    // A graph spec has reported each node that did not bind already.
    for node in &manifest.nodes {
        if !graph_spec && node.binding.is_some() && !spec.bind.contains_key(&node.name) {
            problems.push(format!("node `{}` has no [bind.{}]", node.name, node.name));
        }
        if node.role == gdl_model::Role::Sink && node.binding.is_none() {
            problems.push(format!(
                "node `{}` has no binding name in the manifest; rebuild the contract with this gdl",
                node.name
            ));
        }
    }
    match (layout, problems.is_empty()) {
        (Some(layout), true) => Ok(layout),
        _ => Err(Error::new(problems.join("\n"))),
    }
}

/// The crate a binding's type path starts in, as cargo names the library
/// (without a raw identifier's `r#`), or `None` if the text is not a Rust
/// path. Parsed rather than split: a library named like a keyword appears
/// in the path as `r#name`, and an unescaped keyword is not a path at all.
pub(crate) fn type_crate(ty: &str) -> Option<String> {
    use syn::ext::IdentExt as _;
    let path: syn::Path = syn::parse_str(ty.trim()).ok()?;
    let first = path.segments.first()?;
    Some(first.ident.unraw().to_string())
}
