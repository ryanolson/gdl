//! A spec checked against its contract's manifest: the placement, the
//! segment and package names, and the bindings, every problem at once.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Error, Spec, dump, paths, spec};

/// `a`, `b`, and `c`: names in backquotes.
fn list(names: &[String]) -> String {
    let quoted: Vec<String> = names.iter().map(|n| format!("`{n}`")).collect();
    quoted.join(", ")
}

/// The message crates whose library takes a name that the generated
/// contract uses for another crate: `gdl_runtime`, or the contract's own.
pub(crate) fn library_clashes(contract: &str, crates: &[spec::ListedCrate]) -> Vec<String> {
    crates
        .iter()
        .filter_map(|c| {
            let whose = if c.lib == "gdl_runtime" {
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

/// Each layout's placement checked against the graph, by layout name:
/// `None` for a layout whose placement does not fit it.
pub(crate) type Layouts = BTreeMap<String, Option<gdl_model::Layout>>;

/// The checks of `spec` that need none of its bindings: each layout's
/// placement against the graph, each problem with the table and key it is
/// about, the pipeline names, and the deployment's segment names. They are
/// made with the bindings, and without them when the bindings cannot be
/// read, so that a node crate that does not read hides none of them. A
/// problem that every layout finds, one of the graph's own, is reported
/// once.
pub(crate) fn unbound(
    spec: &Spec,
    manifest: &gdl_model::Manifest,
) -> (Layouts, Vec<gdl_model::Problem>) {
    let mut problems = Vec::new();
    let mut labels = BTreeSet::new();
    for (root, name) in &spec.pipeline_names {
        if manifest
            .node(root)
            .is_none_or(|n| n.role != gdl_model::Role::Source)
        {
            problems.push(format!(
                "[pipeline_names] {root}: the graph has no source `{root}`"
            ));
        }
        if name.trim().is_empty() || !labels.insert(name) {
            problems.push(format!(
                "[pipeline_names] {root}: pipeline names must be nonempty and unique"
            ));
        }
    }
    problems.extend(spec.deployment.iter().flat_map(|d| d.secret_problems()));
    // One segment's settings serve it in every layout that has it.
    for name in spec.deployment.iter().flat_map(|d| d.segments()) {
        let placed = spec.layouts().any(|laid| {
            laid.placement
                .segments()
                .any(|(segment, _)| segment == name)
        });
        if !placed {
            let why = match spec.placements.tabled() {
                true => format!("no layout has a segment `{name}`"),
                false => format!("the placement has no segment `{name}`"),
            };
            problems.push(format!("[deployment.segment.{name}]: {why}"));
        }
    }
    let mut layouts = Layouts::new();
    let mut placed: Vec<gdl_model::Problem> = Vec::new();
    for laid in spec.layouts() {
        let layout = match gdl_model::layout(manifest, laid.placement) {
            Ok(layout) => Some(layout),
            Err(errors) => {
                for problem in laid.problems(&errors) {
                    if !placed.contains(&problem) {
                        placed.push(problem);
                    }
                }
                None
            }
        };
        layouts.insert(laid.name.to_string(), layout);
    }
    let found = placed
        .into_iter()
        .chain(problems.into_iter().map(Into::into))
        .collect();
    (layouts, found)
}

/// Check each layout's placement, and the bindings, against the manifest,
/// reporting every problem found, and give each layout by its name.
pub(crate) fn validate(
    spec: &Spec,
    contract: &spec::Contract,
    manifest: &gdl_model::Manifest,
) -> Result<BTreeMap<String, gdl_model::Layout>, Error> {
    let mut problems = spec.problems.clone();
    let (layouts, placed) = unbound(spec, manifest);
    problems.extend(crate::policies::problems(spec, manifest, &layouts));
    let entry_nodes = spec.service.as_ref().map(|s| s.nodes()).unwrap_or_default();
    let entry_names: BTreeSet<_> = entry_nodes.iter().map(|n| n.name.as_str()).collect();
    let bindings: BTreeMap<_, _> = spec
        .bind
        .iter()
        .chain(entry_nodes.iter().map(|n| (&n.name, &n.bind)))
        .collect();
    let sources: Vec<&str> = manifest
        .nodes
        .iter()
        .filter(|n| n.role == gdl_model::Role::Source)
        .map(|n| n.name.as_str())
        .collect();
    // The source that hosts the frontend is the one the spec pins, whatever
    // the order of declaration: a pin on any other root would otherwise be
    // dropped without a word.
    let pinned: Vec<&str> = sources
        .iter()
        .copied()
        .filter(|s| spec.bind.contains_key(*s))
        .collect();
    if pinned.len() > 1 {
        problems.push(format!(
            "sources {} each have an implementation; one source hosts the frontend",
            list(&pinned.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        ));
    }
    let source_name = pinned.first().copied().unwrap_or("");
    if spec.service.is_none() && !pinned.is_empty() && sources.len() > 1 {
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
    // Package names are what Cargo sees: two segments that differ only in
    // case or in `-` against `_` would build one package twice, and so
    // would two layouts whose names and segments' run together alike.
    let mut packages: BTreeMap<String, (&str, &str)> = BTreeMap::new();
    for laid in spec.layouts() {
        // Its crates are in a directory named for it, beside the
        // generated contract's.
        if graph_spec && laid.among_several() && laid.name.eq_ignore_ascii_case("contract") {
            problems.push(format!(
                "[layout.{}]: the generated contract crate is `contract/`; rename the layout",
                laid.name
            ));
            continue;
        }
        if spec.service.is_some() {
            let host = laid
                .placement
                .segments()
                .find(|(_, nodes)| nodes.iter().any(|n| n == source_name))
                .map(|(name, _)| name);
            for root in manifest
                .nodes
                .iter()
                .filter(|n| n.role == gdl_model::Role::Source)
            {
                let placement = laid
                    .placement
                    .segments()
                    .find(|(_, nodes)| nodes.contains(&root.name))
                    .map(|(name, _)| name);
                if placement != host {
                    problems.push(laid.named(format!("pipeline entry `{}` must be hosted with its frontend; place its processing nodes independently", root.name)));
                }
            }
        }
        let layout = layouts.get(laid.name).and_then(Option::as_ref);
        for (segment, nodes) in laid.placement.segments() {
            let at = laid.table(&format!("segment.{segment}"));
            let policies = layout
                .map(|layout| crate::policies::registered(spec, layout, segment))
                .unwrap_or_default();
            let mut hosted: BTreeSet<&str> = nodes.iter().map(String::as_str).collect();
            if hosted.contains(source_name) {
                hosted.extend(entry_names.iter().copied());
            }
            // The generated contract is written to `contract/`, whatever its
            // package is called; case is folded for file systems that fold it.
            if graph_spec && !laid.among_several() && segment.eq_ignore_ascii_case("contract") {
                problems.push(format!(
                    "[{at}]: segment `{segment}`: the generated contract crate is `contract/`; rename the segment"
                ));
                continue;
            }
            if let Some(problem) = gdl_model::segment_name_problem(segment) {
                problems.push(format!("[{at}]: {problem}"));
                continue;
            }
            let package = laid.package(&manifest.name, segment);
            // A generated crate depends on the contract and on implementations
            // by package name; Cargo cannot hold two path packages of one name.
            if package == contract.package {
                problems.push(format!(
                    "[{at}]: segment `{segment}` becomes package `{package}`, the contract's name"
                ));
            }
            for (name, bind) in &policies {
                if package == bind.package {
                    problems.push(format!(
                        "[{at}]: segment `{segment}` becomes package `{package}`, the name of [pool_policy.{name}]'s crate"
                    ));
                }
            }
            for (node, bind) in &bindings {
                if package == bind.package {
                    let whose = if graph_spec {
                        format!("node `{node}`'s [nodes] crate")
                    } else {
                        format!("[bind.{node}]'s crate")
                    };
                    problems.push(format!(
                        "[{at}]: segment `{segment}` becomes package `{package}`, the name of {whose}"
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
                            "[{at}]: segment `{segment}`: crate name `{lib}` is both {first} and {what}"
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
            let allocator = spec.allocator.source();
            for fixed in [
                "gdl_runtime",
                "serde",
                "tokio",
                "toml",
                "tracing_subscriber",
            ]
            .into_iter()
            .chain(allocator.map(|allocator| allocator.lib))
            {
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
            for (_, bind) in &policies {
                see(
                    &bind.lib,
                    format!("package `{}`", bind.package),
                    &mut problems,
                );
            }
            // Cargo names dependencies by package: each key in the generated
            // manifest must be distinct too, whatever the libraries are called.
            let mut keys: BTreeMap<String, String> = BTreeMap::new();
            let mut key = |name: &str, what: String, problems: &mut Vec<String>| match keys
                .get(name)
            {
                Some(first) if *first != what => problems.push(format!(
                    "[{at}]: segment `{segment}`: dependency `{name}` is both {first} and {what}"
                )),
                Some(_) => {}
                None => {
                    keys.insert(name.to_string(), what);
                }
            };
            for fixed in [
                "gdl-runtime",
                "serde",
                "tokio",
                "toml",
                "tracing-subscriber",
            ]
            .into_iter()
            .chain(allocator.map(|allocator| allocator.package))
            {
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
            // A policy's crate may implement a hosted node or another policy
            // too: it is one dependency then, whose sources are compared
            // below.
            for (name, bind) in &policies {
                let hosts_it = bindings
                    .iter()
                    .any(|(n, b)| hosted.contains(n.as_str()) && b.package == bind.package);
                let first = policies
                    .iter()
                    .find(|(_, b)| b.package == bind.package)
                    .map_or(*name, |(n, _)| *n);
                if !hosts_it {
                    key(
                        &bind.package,
                        format!("[pool_policy.{first}]'s package"),
                        &mut problems,
                    );
                }
            }
            match packages.insert(package.clone(), (laid.name, segment)) {
                Some((layout, other)) if layout == laid.name => problems.push(format!(
                    "[{at}]: segments `{other}` and `{segment}` both become package `{package}`"
                )),
                Some((layout, other)) => problems.push(format!(
                    "[{at}]: segment `{segment}` becomes package `{package}`, as segment `{other}` of layout `{layout}` does"
                )),
                None => {}
            }
        }
    }
    let mut sources: BTreeMap<String, (spec::CrateSource, String)> = BTreeMap::new();
    let mut give = |table: String, bind: &'_ spec::Bind, problems: &mut Vec<String>| {
        let path = match &bind.source {
            spec::CrateSource::Path(path) => spec::CrateSource::Path(
                dump::absolute(path).unwrap_or_else(|_| paths::normalize(path)),
            ),
            version => version.clone(),
        };
        match sources.get(bind.package.as_str()) {
            Some((first, other)) if *first != path => problems.push(format!(
                "{other} and {table} give package `{}` two sources",
                bind.package
            )),
            Some(_) => {}
            None => {
                sources.insert(bind.package.clone(), (path, table));
            }
        }
    };
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
        give(bound_by(node), bind, &mut problems);
    }
    for (name, bind) in &spec.pool_policies {
        give(format!("[pool_policy.{name}]"), bind, &mut problems);
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
    let laid: Option<BTreeMap<String, gdl_model::Layout>> = layouts
        .into_iter()
        .map(|(name, layout)| Some((name, layout?)))
        .collect();
    match (laid, placed.is_empty() && problems.is_empty()) {
        (Some(layouts), true) => Ok(layouts),
        _ => Err(Error::all(
            placed
                .into_iter()
                .chain(problems.into_iter().map(Into::into)),
        )),
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
