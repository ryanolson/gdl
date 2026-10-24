use std::path::Path;

/// A spec text of `layouts`, each placing the graph in one segment.
fn spec(layouts: &[&str]) -> String {
    let mut text = String::from(
        "[graph]\npath = \"graph.gdl\"\n[messages]\ncrates = [\"protocols@0.1.0\"]\n\
         [gdl]\nversion = \"0.1\"\n",
    );
    for name in layouts {
        text.push_str(&format!(
            "[layout.{name}.segment.all]\nnodes = [\"Source\", \"Engine\"]\n"
        ));
    }
    text
}

fn read(text: &str) -> gdl_gen::Spec {
    gdl_gen::Spec::from_text(text, "", Path::new("/work")).expect("a spec")
}

/// The composer keeps what it edits: it saves a spec of one layout, in a
/// `[layout]` table or not, and refuses one of several rather than drop
/// all but one.
#[test]
fn the_composer_refuses_a_spec_of_several_layouts() {
    super::one_layout(&read(&spec(&["only"]))).expect("one layout");
    let plain = spec(&[]) + "[segment.all]\nnodes = [\"Source\", \"Engine\"]\n";
    super::one_layout(&read(&plain)).expect("one layout");
    let refused = super::one_layout(&read(&spec(&["east", "west"]))).expect_err("two layouts");
    assert!(
        refused.starts_with(
            "the composer saves a spec of one layout; this one has layouts `east`, `west`"
        ),
        "{refused}"
    );
}
