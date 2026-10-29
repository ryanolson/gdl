//! Property declarations in a graph's text: `property <kind>: <Type>;`
//! declares a kind and the message type of its keys, and a node says
//! which kinds it provides and which it needs, and from which class, after
//! its signature.

use gdl_model::{Encoding, PropertyInfo, PropertyNeed, TypeInfo};
use gdl_syntax::Graph;

const GRAPH: &str = r#"
    name = "llm", schema = 1;
    property model: Model;
    source   Http:   Req -> stream Chunk;
    operator Route:  (Req -> stream Chunk) over (Req -> stream Chunk) needs model from sets;
    property region: Region;
    sink     Engine: Req -> stream Chunk provides model, region needs region from sets;
    topology: Http => Route { Engine };
"#;

fn parse(text: &str) -> syn::Result<Graph> {
    Graph::parse(text.parse().expect("tokens"))
}

fn wire(name: &str) -> Option<TypeInfo> {
    Some(TypeInfo::new(format!("p::{name}"), true, Encoding::Json))
}

#[test]
fn a_graph_declares_kinds_and_who_provides_and_needs_them() {
    let graph = parse(GRAPH).expect("a graph");
    let manifest = graph.manifest(&wire).expect("a manifest");
    let need = |node: &str| PropertyNeed {
        node: node.into(),
        class: "sets".into(),
    };
    assert_eq!(
        manifest.properties,
        [
            PropertyInfo {
                kind: "model".into(),
                ty: TypeInfo::new(":: p :: Model", true, Encoding::Json),
                providers: vec!["Engine".into()],
                needs: vec![need("Route")],
            },
            PropertyInfo {
                kind: "region".into(),
                ty: TypeInfo::new(":: p :: Region", true, Encoding::Json),
                providers: vec!["Engine".into()],
                needs: vec![need("Engine")],
            },
        ]
    );
    // Declared kinds are not nodes and add none.
    assert_eq!(manifest.nodes.len(), 3);
}

#[test]
fn a_graph_without_properties_declares_none() {
    let text = GRAPH
        .replace("property model: Model;", "")
        .replace("property region: Region;", "")
        .replace(" needs model from sets", "")
        .replace(" provides model, region needs region from sets", "");
    let manifest = parse(&text)
        .expect("a graph")
        .manifest(&wire)
        .expect("a manifest");
    assert!(manifest.properties.is_empty());
}

/// Each mistake in a declaration is refused, with the reason.
#[test]
fn a_declaration_that_cannot_hold_is_refused() {
    let cases = [
        (
            GRAPH.replace("property region: Region;", "property model: Region;"),
            "property kind `model` is declared twice",
        ),
        (
            GRAPH.replace("property region: Region;", "property Region: Region;"),
            "property kind name",
        ),
        (
            GRAPH.replace("provides model, region", "provides model, zone"),
            "property kind `zone` is not declared: declare it with `property zone: <Type>;`",
        ),
        (
            GRAPH.replace("needs model from sets", "needs zone from sets"),
            "property kind `zone` is not declared: declare it with `property zone: <Type>;`",
        ),
        (
            GRAPH.replace("provides model, region", "provides model, model"),
            "node `Engine` provides property kind `model` twice",
        ),
        (
            GRAPH.replace(
                "needs region from sets",
                "needs region from sets, region from sets",
            ),
            "node `Engine` needs property kind `region` from class `sets` twice",
        ),
        (
            GRAPH.replace("needs model from sets", "needs model from Sets"),
            "class name",
        ),
        (
            GRAPH.replace(
                "operator Route:  (Req -> stream Chunk) over (Req -> stream Chunk)",
                "identity Route: Req -> stream Chunk",
            ),
            "a built-in identity has no implementation, so it provides and needs no property",
        ),
        (
            GRAPH.replace("property model: Model;", "property model: stream Model;"),
            "expected `;`",
        ),
    ];
    for (text, why) in cases {
        let error = match parse(&text) {
            Ok(_) => panic!("accepted {text}"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains(why), "{why}: {error}");
    }
}

/// A property's type is a wire message, as the type of an edge that may
/// be cut is: the hub carries its keys, and a reader decodes them.
#[test]
fn a_propertys_type_is_a_wire_message() {
    let graph = parse(GRAPH).expect("a graph");
    let local = graph
        .manifest(&|name| Some(TypeInfo::new(name, name != "Region", Encoding::Msgpack)))
        .expect_err("a local property type");
    assert!(
        local.contains("property `region`: `Region` is not a wire type"),
        "{local}"
    );
    let unit = parse(&GRAPH.replace("property model: Model;", "property model: ();"))
        .expect("a graph")
        .manifest(&wire)
        .expect_err("a unit property type");
    assert!(
        unit.contains("property `model`: its type is a message, not `()`"),
        "{unit}"
    );
}
