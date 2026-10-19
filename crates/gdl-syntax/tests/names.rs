//! The message names a graph file's message positions hold.

use gdl_syntax::message_name;

/// The rule a graph file follows: a message position
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
