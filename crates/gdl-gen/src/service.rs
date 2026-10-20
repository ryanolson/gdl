use crate::{
    Spec,
    spec::{Bind, ListedCrate},
};
use gdl_extract::{NodeEntry, Offer, PortEntry};
use gdl_model::{ConfigValue, Role};
use gdl_syntax::{Graph, Sig, message_name};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(crate) struct ServiceBinding {
    pub(crate) clients: String,
    pub(crate) entries: Vec<EntryBinding>,
}
impl ServiceBinding {
    pub(crate) fn nodes(&self) -> Vec<&EntryNode> {
        self.entries
            .iter()
            .filter_map(|e| e.pipeline.as_ref())
            .flat_map(EntryHop::nodes)
            .collect()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EntrySpec {
    #[serde(default)]
    pub(crate) forward: Vec<String>,
    pub(crate) operator: Option<String>,
    pub(crate) target: Option<String>,
    pub(crate) nested: Option<Box<EntrySpec>>,
    #[serde(default)]
    pub(crate) backward: Vec<String>,
}
#[derive(Debug, Clone)]
pub(crate) struct EntryBinding {
    pub(crate) field: String,
    pub(crate) path: String,
    pub(crate) pipeline: Option<EntryHop>,
}
#[derive(Debug, Clone)]
pub(crate) struct EntryNode {
    pub(crate) name: String,
    pub(crate) bind: Bind,
}
impl EntryNode {
    pub(crate) fn variable(&self) -> String {
        let encoded: String = self.name.bytes().map(|b| format!("{b:02x}")).collect();
        format!("entry_{encoded}")
    }
}
#[derive(Debug, Clone)]
pub(crate) struct EntryHop {
    pub(crate) forward: Vec<EntryNode>,
    pub(crate) operator: Option<EntryNode>,
    pub(crate) target: Option<String>,
    pub(crate) nested: Option<Box<EntryHop>>,
    pub(crate) backward: Vec<EntryNode>,
}
impl EntryHop {
    /// The roots this arm joins, through its nested arms.
    pub(crate) fn targets(&self) -> Vec<&str> {
        let mut targets: Vec<&str> = self.target.iter().map(String::as_str).collect();
        if let Some(nested) = &self.nested {
            targets.extend(nested.targets());
        }
        targets
    }

    fn nodes(&self) -> Vec<&EntryNode> {
        let mut nodes: Vec<_> = self
            .forward
            .iter()
            .chain(self.operator.iter())
            .chain(&self.backward)
            .collect();
        if let Some(nested) = &self.nested {
            nodes.extend(nested.nodes());
        }
        nodes
    }
    pub(crate) fn expression(&self) -> String {
        let chain = |nodes: &[EntryNode]| {
            nodes.iter().rev().fold("()".into(), |rest, n| {
                format!("(std::sync::Arc::new({}), {rest})", n.variable())
            })
        };
        let inner = match (&self.operator, &self.nested) {
            (Some(op), Some(next)) => format!(
                "gdl::OperatorStage::new(std::sync::Arc::new({}), {})",
                op.variable(),
                next.expression()
            ),
            _ => format!(
                "gdl::Join::new({}.clone())",
                pipeline_variable(self.target.as_deref().expect("bound destination"))
            ),
        };
        format!(
            "gdl::Hop::new({}, {inner}, {})",
            chain(&self.forward),
            chain(&self.backward)
        )
    }
}

struct Binder<'a> {
    spec: &'a mut Spec,
    graph: &'a Graph,
    crates: &'a [ListedCrate],
    offers: &'a [Offer],
    roots: BTreeMap<String, (PortEntry, PortEntry)>,
}
impl Binder<'_> {
    fn node(
        &mut self,
        ty: &str,
        name: String,
        role: Role,
    ) -> Result<(EntryNode, NodeEntry), String> {
        let matches: Vec<_> = self
            .offers
            .iter()
            .enumerate()
            .flat_map(|(i, o)| {
                o.nodes
                    .iter()
                    .filter(move |n| n.ty == ty)
                    .map(move |n| (i, n))
            })
            .collect();
        let [(index, node)] = matches.as_slice() else {
            return Err(format!("`{ty}` must name exactly one catalog node"));
        };
        if node.role != role {
            return Err(format!("`{ty}` must be a {role}"));
        }
        if self.graph.nodes.iter().any(|n| n.ident == name) {
            return Err(format!("entry node `{name}` collides with a pipeline node"));
        }
        let config = gdl_model::resolve_config(
            &node.config,
            &self.spec.config.get(&name).cloned().unwrap_or_default(),
        )
        .map_err(|e| e.to_string())?;
        self.spec.config.insert(name.clone(), config);
        let krate = &self.crates[*index];
        Ok((
            EntryNode {
                name,
                bind: Bind {
                    package: krate.package.clone(),
                    lib: krate.lib.clone(),
                    source: krate.source.clone(),
                    ty: node.ty.clone(),
                },
            },
            (*node).clone(),
        ))
    }
    fn hop(
        &mut self,
        input: &EntrySpec,
        prefix: &str,
        request: &PortEntry,
        response: &PortEntry,
    ) -> Result<EntryHop, String> {
        let mut result = EntryHop {
            forward: Vec::new(),
            operator: None,
            target: None,
            nested: None,
            backward: Vec::new(),
        };
        let mut current = request.clone();
        for (i, ty) in input.forward.iter().enumerate() {
            let (binding, node) = self.node(ty, format!("{prefix}_f{i}"), Role::Forward)?;
            require(&current, port(&node, "In")?, "forward request")?;
            current = port(&node, "Out")?.clone();
            result.forward.push(binding);
        }
        let mut returned = if let Some(ty) = &input.operator {
            if input.target.is_some() {
                return Err("an operator owns a nested arm, not a destination".into());
            }
            let (binding, node) = self.node(ty, format!("{prefix}_op"), Role::Operator)?;
            require(&current, port(&node, "UpReq")?, "operator request")?;
            let nested = input
                .nested
                .as_deref()
                .ok_or("operator requires a nested pipeline")?;
            result.nested = Some(Box::new(self.hop(
                nested,
                &format!("{prefix}_next"),
                port(&node, "DownReq")?,
                port(&node, "DownItem")?,
            )?));
            result.operator = Some(binding);
            port(&node, "UpItem")?.clone()
        } else {
            if input.nested.is_some() {
                return Err("nested pipeline requires an operator".into());
            }
            let target = input
                .target
                .as_ref()
                .ok_or("choose an explicit pipeline destination")?;
            let (request, response) = self
                .roots
                .get(target)
                .ok_or_else(|| format!("unknown pipeline `{target}`"))?;
            require(&current, request, "pipeline request")?;
            result.target = Some(target.clone());
            response.clone()
        };
        for (i, ty) in input.backward.iter().enumerate() {
            let (binding, node) = self.node(ty, format!("{prefix}_b{i}"), Role::Backward)?;
            require(&returned, port(&node, "In")?, "backward response")?;
            returned = port(&node, "Out")?.clone();
            result.backward.push(binding);
        }
        require(&returned, response, "response; add a response converter")?;
        Ok(result)
    }
}
fn port<'a>(node: &'a NodeEntry, name: &str) -> Result<&'a PortEntry, String> {
    node.ports
        .iter()
        .find(|p| p.port == name)
        .ok_or_else(|| format!("{} has no {name} port", node.name))
}
fn require(actual: &PortEntry, expected: &PortEntry, where_: &str) -> Result<(), String> {
    if actual.head == expected.head && actual.item == expected.item {
        Ok(())
    } else {
        Err(format!("{where_} types do not match"))
    }
}

pub(crate) fn bind(
    spec: &mut Spec,
    graph: &Graph,
    crates: &[ListedCrate],
    offers: &[Offer],
    messages: &BTreeMap<String, crate::Resolved>,
) {
    // The frontend's root is the pinned source; validation reports more
    // than one.
    let mut pinned = graph
        .nodes
        .iter()
        .filter(|node| matches!(node.sig, Sig::Source { .. }))
        .map(|node| node.ident.to_string())
        .filter(|name| spec.bind.contains_key(name));
    let (Some(source), None) = (pinned.next(), pinned.next()) else {
        return;
    };
    let Some(frontend) = spec.bind.get(&source) else {
        return;
    };
    let Some(node) = offers
        .iter()
        .flat_map(|o| &o.nodes)
        .find(|n| n.ty == frontend.ty)
    else {
        return;
    };
    let Some(clients) = &node.clients else { return };
    let config = spec.config.get(&source).cloned().unwrap_or_default();
    let origin = |ty: &syn::Type| {
        message_name(ty)
            .ok()
            .flatten()
            .and_then(|n| messages.get(&n))
            .map(|r| r.origin.clone())
    };
    let roots = graph
        .nodes
        .iter()
        .filter_map(|node| {
            let Sig::Source { req, item } = &node.sig else {
                return None;
            };
            Some((
                node.ident.to_string(),
                (
                    PortEntry {
                        port: "Req".into(),
                        head: origin(&req.head),
                        item: req.item.as_ref().and_then(origin),
                    },
                    PortEntry {
                        port: "Item".into(),
                        head: origin(item),
                        item: None,
                    },
                ),
            ))
        })
        .collect();
    let mut binder = Binder {
        spec,
        graph,
        crates,
        offers,
        roots,
    };
    let mut entries = Vec::new();
    for endpoint in &node.entries {
        let enabled = config.get(&endpoint.enabled) == Some(&ConfigValue::Bool(true));
        let selected = binder.spec.entry.get(&endpoint.name).cloned();
        let pipeline = if enabled {
            match selected {
                Some(selected) => match binder.hop(
                    &selected,
                    &format!("{source}_{}", endpoint.name),
                    &endpoint.request,
                    &endpoint.response,
                ) {
                    Ok(hop) => Some(hop),
                    Err(e) => {
                        binder
                            .spec
                            .problems
                            .push(format!("entry `{}`: {e}", endpoint.name));
                        None
                    }
                },
                None => {
                    binder.spec.problems.push(format!(
                        "enabled entry `{}` requires a pipeline",
                        endpoint.name
                    ));
                    None
                }
            }
        } else {
            if selected.is_some() {
                binder
                    .spec
                    .problems
                    .push(format!("disabled entry `{}` has a pipeline", endpoint.name));
            }
            None
        };
        entries.push(EntryBinding {
            field: endpoint.name.clone(),
            path: endpoint.path.clone(),
            pipeline,
        });
    }
    for name in binder.spec.entry.keys() {
        if !node.entries.iter().any(|e| &e.name == name) {
            binder
                .spec
                .problems
                .push(format!("unknown service entry `{name}`"));
        }
    }
    if !entries.iter().any(|e| e.pipeline.is_some()) {
        binder
            .spec
            .problems
            .push("a service must have at least one enabled, wired entry".into());
    }
    binder.spec.service = Some(ServiceBinding {
        clients: clients.clone(),
        entries,
    });
}

pub(crate) fn diagram(spec: &Spec, manifest: &gdl_model::Manifest, mut diagram: String) -> String {
    use std::fmt::Write;
    let Some(service) = &spec.service else {
        return diagram;
    };
    let label = |s: &str| {
        s.replace('#', "#35;")
            .replace('"', "#quot;")
            .replace('<', "#lt;")
            .replace('>', "#gt;")
            .replace(['\n', '\r'], " ")
    };
    fn draw(hop: &EntryHop, outer: &str, manifest: &gdl_model::Manifest, text: &mut String) {
        use std::fmt::Write;
        let mut previous = outer.to_string();
        for node in &hop.forward {
            let _ = writeln!(
                text,
                "    {previous} -->|request| {}[\"{}\"]",
                node.name, node.name
            );
            previous = node.name.clone();
        }
        let inner = if let (Some(op), Some(nested)) = (&hop.operator, &hop.nested) {
            let _ = writeln!(
                text,
                "    {previous} -->|request| {}[\"{}\"]",
                op.name, op.name
            );
            draw(nested, &op.name, manifest, text);
            op.name.clone()
        } else {
            let index = manifest
                .nodes
                .iter()
                .position(|n| Some(&n.name) == hop.target.as_ref())
                .expect("bound pipeline");
            let target = format!("n{index}");
            let _ = writeln!(text, "    {previous} -->|request| {target}");
            target
        };
        previous = inner;
        for node in &hop.backward {
            let _ = writeln!(
                text,
                "    {previous} -.->|response| {}[\"{}\"]",
                node.name, node.name
            );
            previous = node.name.clone();
        }
        let _ = writeln!(text, "    {previous} -.->|response| {outer}");
    }
    for (i, entry) in service.entries.iter().enumerate() {
        if let Some(pipeline) = &entry.pipeline {
            let _ = writeln!(diagram, "    entry{i}[\"{}\"]", label(&entry.path));
            draw(pipeline, &format!("entry{i}"), manifest, &mut diagram);
        }
    }
    diagram
}

pub(crate) fn pipeline_variable(name: &str) -> String {
    let encoded: String = name.bytes().map(|b| format!("{b:02x}")).collect();
    format!("pipeline_{encoded}")
}
