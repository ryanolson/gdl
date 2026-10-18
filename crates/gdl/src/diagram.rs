//! A manifest drawn as a Graphviz DOT graph or a Mermaid flowchart.
//!
//! Request edges are solid and response edges dashed, each labeled with its
//! message type. With a layout, each segment is a cluster and every edge
//! that crosses segments is drawn thick and red. An edge whose type has no
//! codec says so in its label: no placement can cut it.
//!
//! Node and segment names go into labels only. Identifiers are generated
//! (`n0`, `s0`), because a node may be named like a keyword of either
//! format (`end` in Mermaid) and a segment name may hold any character.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use crate::decl::Role;
use crate::error::{Error, ErrorKind};
use crate::manifest::{Manifest, TypeInfo, role_tag};
use crate::placement::{Layout, PlacementErrors, check_manifest};

/// The color of an edge that crosses segments.
const CROSSING: &str = "#d62728";

impl Manifest {
    /// The graph in Graphviz DOT. With `layout`, each segment is a cluster
    /// and edges that cross segments are drawn thick and red. Fails if
    /// `layout` was checked against another contract.
    pub fn to_dot(&self, layout: Option<&Layout>) -> Result<String, Error> {
        self.check(layout)?;
        let mut out = String::new();
        let _ = writeln!(out, "digraph {} {{", dot_string(&self.name));
        let _ = writeln!(out, "    rankdir=LR;");
        let node = |out: &mut String, indent: &str, index: usize| {
            let info = &self.nodes[index];
            let label = format!("{}\n{}", info.name, role_tag(info.role));
            let _ = writeln!(
                out,
                "{indent}n{index} [label={}, {}];",
                dot_string(&label),
                dot_shape(info.role)
            );
        };
        match layout {
            None => {
                for index in 0..self.nodes.len() {
                    node(&mut out, "    ", index);
                }
            }
            Some(layout) => {
                for (s, (segment, members)) in self.segments(layout).into_iter().enumerate() {
                    let _ = writeln!(out, "    subgraph cluster_s{s} {{");
                    let _ = writeln!(out, "        label={};", dot_string(segment));
                    for index in members {
                        node(&mut out, "        ", index);
                    }
                    let _ = writeln!(out, "    }}");
                }
            }
        }
        for edge in self.edges(layout) {
            let mut attrs = vec![format!("label={}", dot_string(&type_label(edge.ty)))];
            if edge.backward {
                attrs.push("style=dashed".to_string());
            }
            // An edge into a hop's outer end closes the loop back to the
            // left; letting it rank the nodes would pull the response path
            // under the request path in a tangle.
            if edge.into_outer {
                attrs.push("constraint=false".to_string());
            }
            if edge.crosses {
                attrs.push(format!("color={}", dot_string(CROSSING)));
                attrs.push("penwidth=2".to_string());
            }
            let _ = writeln!(
                out,
                "    n{} -> n{} [{}];",
                edge.from,
                edge.to,
                attrs.join(", ")
            );
        }
        out.push_str("}\n");
        Ok(out)
    }

    /// The graph as a Mermaid flowchart. With `layout`, each segment is a
    /// subgraph and edges that cross segments are drawn thick and red. Fails
    /// if `layout` was checked against another contract.
    pub fn to_mermaid(&self, layout: Option<&Layout>) -> Result<String, Error> {
        self.check(layout)?;
        let mut out = String::new();
        let _ = writeln!(out, "---");
        // Front matter is YAML, where `: ` and ` #` mean something:
        // quoted as a YAML string, not escaped as a label.
        let _ = writeln!(out, "title: {}", yaml_string(&self.name));
        let _ = writeln!(out, "---");
        let _ = writeln!(out, "flowchart LR");
        let node = |out: &mut String, indent: &str, index: usize| {
            let info = &self.nodes[index];
            let label = format!(
                "\"{}<br/>{}\"",
                mermaid_text(&info.name),
                role_tag(info.role)
            );
            let (open, close) = mermaid_shape(info.role);
            let _ = writeln!(out, "{indent}n{index}{open}{label}{close}");
        };
        match layout {
            None => {
                for index in 0..self.nodes.len() {
                    node(&mut out, "    ", index);
                }
            }
            Some(layout) => {
                for (s, (segment, members)) in self.segments(layout).into_iter().enumerate() {
                    let _ = writeln!(out, "    subgraph s{s}[\"{}\"]", mermaid_text(segment));
                    for index in members {
                        node(&mut out, "        ", index);
                    }
                    let _ = writeln!(out, "    end");
                }
            }
        }
        // Mermaid styles a link by its position among all links, so the
        // crossings are collected by position as the links are written.
        let mut crossing = Vec::new();
        for (position, edge) in self.edges(layout).into_iter().enumerate() {
            let arrow = if edge.backward { "-.->" } else { "-->" };
            let _ = writeln!(
                out,
                "    n{} {arrow}|\"{}\"| n{}",
                edge.from,
                mermaid_text(&type_label(edge.ty)),
                edge.to
            );
            if edge.crosses {
                crossing.push(position.to_string());
            }
        }
        if !crossing.is_empty() {
            let _ = writeln!(
                out,
                "    linkStyle {} stroke:{CROSSING},stroke-width:3px",
                crossing.join(",")
            );
        }
        Ok(out)
    }

    /// The manifest is checked as `layout` checks it: a manifest is plain
    /// data, and one that names an undeclared node or lacks an edge type
    /// cannot be drawn. A layout of another contract would place none of
    /// this graph's nodes: every node would fall outside the clusters, and
    /// every edge would look as if it stayed in one segment.
    fn check(&self, layout: Option<&Layout>) -> Result<(), Error> {
        let errors = check_manifest(self);
        if !errors.is_empty() {
            return Err(Error::new(
                ErrorKind::Placement,
                PlacementErrors(errors).to_string(),
            ));
        }
        match layout {
            Some(layout) if layout.contract_hash() != self.contract_hash() => Err(Error::new(
                ErrorKind::Placement,
                "the layout was checked against another contract",
            )),
            _ => Ok(()),
        }
    }

    /// Each segment of `layout`, in name order, with the indexes of its
    /// nodes in declaration order.
    fn segments<'a>(&self, layout: &'a Layout) -> Vec<(&'a str, Vec<usize>)> {
        let names: BTreeSet<&str> = self
            .nodes
            .iter()
            .filter_map(|node| layout.segment_of(&node.name))
            .collect();
        names
            .into_iter()
            .map(|segment| {
                let members = (0..self.nodes.len())
                    .filter(|&i| layout.segment_of(&self.nodes[i].name) == Some(segment))
                    .collect();
                (segment, members)
            })
            .collect()
    }

    /// Every edge, hop by hop from the outermost: the request path, then the
    /// response path.
    fn edges(&self, layout: Option<&Layout>) -> Vec<Edge<'_>> {
        let index = |name: &str| {
            self.nodes
                .iter()
                .position(|node| node.name == name)
                .expect("a hop names only declared nodes")
        };
        let crosses = |from: &str, to: &str| {
            layout.is_some_and(|layout| layout.segment_of(from) != layout.segment_of(to))
        };
        let mut edges = Vec::new();
        for hop in &self.hops {
            let paths = [
                (
                    false,
                    hop.forward_path().collect::<Vec<_>>(),
                    &hop.forward_types,
                ),
                (
                    true,
                    hop.backward_path().collect::<Vec<_>>(),
                    &hop.backward_types,
                ),
            ];
            for (backward, path, types) in paths {
                for (pair, ty) in path.windows(2).zip(types) {
                    edges.push(Edge {
                        from: index(pair[0]),
                        to: index(pair[1]),
                        ty,
                        backward,
                        into_outer: backward && pair[1] == hop.outer,
                        crosses: crosses(pair[0], pair[1]),
                    });
                }
            }
        }
        edges
    }
}

struct Edge<'a> {
    from: usize,
    to: usize,
    ty: &'a TypeInfo,
    backward: bool,
    /// The last edge of a response path, which returns to the hop's outer
    /// end.
    into_outer: bool,
    crosses: bool,
}

/// The type as the graph definition writes it, with each part that has no
/// codec marked: a streamed request crosses only when both parts have one.
/// A type named by a plain path is labeled by its last segment: a contract
/// generated from a graph file writes each message's full path, which the
/// contract hash needs and the reader does not.
fn type_label(ty: &TypeInfo) -> String {
    let part = |name: &str, wire: bool| {
        let name = short(name);
        if wire {
            name.to_string()
        } else {
            format!("{name} (no codec)")
        }
    };
    match &ty.stream {
        None => part(&ty.name, ty.wire),
        Some(item) => format!(
            "{} + stream {}",
            part(&ty.name, ty.wire),
            part(&item.name, item.wire)
        ),
    }
}

/// The last segment of a plain path (`:: a :: b :: C` is `C`); any other
/// type as it is.
fn short(name: &str) -> &str {
    let segments: Vec<&str> = name
        .trim()
        .trim_start_matches("::")
        .split("::")
        .map(str::trim)
        .collect();
    let plain = segments.iter().all(|s| {
        let s = s.strip_prefix("r#").unwrap_or(s);
        s.chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_')
            && s.chars().all(|c| c.is_alphanumeric() || c == '_')
    });
    match segments.last() {
        Some(last) if plain && segments.len() > 1 => last.strip_prefix("r#").unwrap_or(last),
        _ => name,
    }
}

fn dot_shape(role: Role) -> &'static str {
    match role {
        Role::Source => "shape=oval",
        Role::Operator => "shape=box, peripheries=2",
        Role::Forward => "shape=box",
        Role::Sink => "shape=cylinder",
        Role::Backward => "shape=box, style=rounded",
    }
}

fn mermaid_shape(role: Role) -> (&'static str, &'static str) {
    match role {
        Role::Source => ("([", "])"),
        Role::Operator => ("[[", "]]"),
        Role::Forward => ("[", "]"),
        Role::Sink => ("[(", ")]"),
        Role::Backward => ("(", ")"),
    }
}

/// A DOT quoted string.
fn dot_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// A YAML double-quoted string.
fn yaml_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Text for inside a quoted Mermaid label: the characters Mermaid would
/// read as markup become entity codes.
fn mermaid_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '#' => out.push_str("#35;"),
            '"' => out.push_str("#quot;"),
            '<' => out.push_str("#lt;"),
            '>' => out.push_str("#gt;"),
            _ => out.push(ch),
        }
    }
    out
}
