//! A nodes file: node declarations in graph syntax, without a header or a
//! topology, as a node crate offers them.

use gdl_syntax::{Role, declarations, message_name};
use proc_macro2::TokenStream;

fn parse(text: &str) -> Result<Vec<(String, Role)>, String> {
    declarations(text.parse::<TokenStream>().expect("tokens"))
        .map(|nodes| {
            nodes
                .iter()
                .map(|n| (n.ident.to_string(), n.role()))
                .collect()
        })
        .map_err(|e| e.to_string())
}

#[test]
fn a_nodes_file_declares_nodes_of_every_role_but_the_source() {
    let nodes = parse(
        "// The engine's nodes.
         sink     Engine: Words -> stream Token;
         forward  Resample: Session + stream Audio -> stream Pcm;
         backward Detokenize: Token -> Piece;
         operator Gate: (Words -> stream Piece) over (Words -> stream Token);",
    )
    .expect("declarations");
    assert_eq!(
        nodes,
        [
            ("Engine".to_string(), Role::Sink),
            ("Resample".to_string(), Role::Forward),
            ("Detokenize".to_string(), Role::Backward),
            ("Gate".to_string(), Role::Operator),
        ]
    );
    assert_eq!(parse(""), Ok(vec![]));
}

#[test]
fn a_nodes_file_declares_no_source_and_no_name_twice() {
    let error = parse("source A: Req -> stream Item;").expect_err("a source");
    assert!(
        error.contains("a source takes no implementation"),
        "{error}"
    );
    let error =
        parse("sink A: Req -> stream Item; sink A: Req -> stream Item;").expect_err("twice");
    assert!(error.contains("node declared twice"), "{error}");
    let error = parse("sink A: Req -> stream Item; topology: A;").expect_err("a topology");
    assert!(!error.is_empty());
}

/// The rule both a graph file and a nodes file follow: a message position
/// holds a bare name, `r#` dropped, or `()`.
#[test]
fn a_message_position_holds_a_name_or_unit() {
    let ty = |text: &str| syn::parse_str::<syn::Type>(text).expect("type");
    assert_eq!(message_name(&ty("Words")), Ok(Some("Words".to_string())));
    assert_eq!(message_name(&ty("r#Words")), Ok(Some("Words".to_string())));
    assert_eq!(message_name(&ty("()")), Ok(None));
    assert_eq!(message_name(&ty("a::Words")), Err(()));
    assert_eq!(message_name(&ty("Vec<Words>")), Err(()));
}
