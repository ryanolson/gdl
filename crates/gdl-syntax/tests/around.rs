//! An around node's braces hold the rest of its own hop: its forward half
//! comes before them and its backward half after.

use gdl_syntax::Graph;

fn parse(text: &str) -> syn::Result<Graph> {
    Graph::parse(text.parse().expect("tokens"))
}

fn names(idents: &[proc_macro2::Ident]) -> Vec<String> {
    idents.iter().map(ToString::to_string).collect()
}

#[test]
fn an_around_node_puts_one_half_on_each_side_of_its_braces() {
    let graph = parse(
        r#"
        name = "around", schema = 1;
        source   Http: Req -> stream Resp;
        forward  Trim: Req -> Req;
        around   Pre: (Req -> stream Resp) over (Pre -> stream Out);
        around   Back: (Pre -> stream Out) over (Pre -> stream Tok);
        sink     Engine: Pre -> stream Tok;
        backward Mark: Out -> Out;
        backward Last: Resp -> Resp;
        topology: Http => Trim => Pre { Back { Engine } => Mark } => Last;
    "#,
    )
    .expect("a graph");
    assert_eq!(graph.hops.len(), 1, "an around node opens no hop");
    let hop = &graph.hops[0];
    assert_eq!(names(&hop.forward), ["Trim", "Pre", "Back"]);
    assert_eq!(hop.inner, "Engine");
    assert_eq!(names(&hop.backward), ["Back", "Mark", "Pre", "Last"]);
    let manifest = graph
        .manifest(&|name| {
            Some(gdl_model::TypeInfo::new(
                format!("protocol::{name}"),
                true,
                Default::default(),
            ))
        })
        .expect("manifest");
    let placement = gdl_model::Placement::new()
        .segment("front", ["Http", "Trim", "Pre", "Back", "Mark", "Last"])
        .segment("engine", ["Engine"]);
    gdl_model::layout(&manifest, &placement).expect("both pairs in the outer end's task");
}

#[test]
fn an_around_node_without_braces_is_refused() {
    let error = parse(
        r#"
        name = "bare", schema = 1;
        source Http: Req -> stream Resp;
        around Pre: (Req -> stream Resp) over (Req -> stream Resp);
        sink   Engine: Req -> stream Resp;
        topology: Http => Pre => Engine;
    "#,
    )
    .map(drop)
    .expect_err("no braces");
    assert!(error.to_string().contains("in braces"), "{error}");
}

#[test]
fn an_around_node_whose_braces_hold_no_inner_end_is_refused() {
    let error = parse(
        r#"
        name = "empty", schema = 1;
        source  Http: Req -> stream Resp;
        forward Trim: Req -> Req;
        around  Pre: (Req -> stream Resp) over (Req -> stream Resp);
        sink    Engine: Req -> stream Resp;
        topology: Http => Pre { Trim } => Engine;
    "#,
    )
    .map(drop)
    .expect_err("no inner end in the braces");
    assert!(
        error.to_string().contains("needs a sink or an operator"),
        "{error}"
    );
}

#[test]
fn an_around_node_under_an_operator_is_in_the_operators_hop() {
    let graph = parse(
        r#"
        name = "nested", schema = 1;
        source   Http: Req -> stream Resp;
        operator Retry: (Req -> stream Resp) over (Req -> stream Resp);
        around   Back: (Req -> stream Resp) over (Req -> stream Resp);
        sink     Engine: Req -> stream Resp;
        topology: Http => Retry { Back { Engine } };
    "#,
    )
    .expect("a graph");
    assert_eq!(graph.hops.len(), 2);
    assert!(graph.hops[0].forward.is_empty());
    assert_eq!(names(&graph.hops[1].forward), ["Back"]);
    assert_eq!(names(&graph.hops[1].backward), ["Back"]);
}
