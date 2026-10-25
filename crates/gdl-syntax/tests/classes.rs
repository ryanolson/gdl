//! The graph of a model served in cells (docs/src/concepts/cells.md), parsed
//! from its text, under a placement of one segment per stage in two classes
//! (A to C, then D to F): one class boundary, at
//! the chooser's hop, and one WAN round trip when a request spills.

use gdl_syntax::Graph;

const CELLS: &str = r#"
    name = "llm", schema = 1;
    source   Http:     ChatRequest -> stream ChatChunk;
    operator Pre:      (ChatRequest -> stream ChatChunk) over (Prompt -> stream Text);
    operator Route:    (Prompt -> stream Text) over (Tokens -> stream Text);
    operator Model:    (Tokens -> stream Text) over (Tokens -> stream Text);
    sink     Engine:   Tokens -> stream Token;
    backward Detok:    Token -> Text;
    topology: Http => Pre { Route { Model { Engine => Detok } } };
"#;

const PLACEMENT: &str = r#"
    [segment.A]
    nodes = ["Http"]
    [segment.B]
    nodes = ["Pre"]
    [segment.C]
    nodes = ["Route"]
    [segment.D]
    nodes = ["Model"]
    [segment.E]
    nodes = ["Engine"]
    [segment.F]
    nodes = ["Detok"]

    [class.global]
    segments = ["A", "B", "C"]
    [class.model_x]
    segments = ["D", "E", "F"]
"#;

#[test]
fn the_cells_graph_crosses_once_at_the_chooser() {
    let graph = Graph::parse(CELLS.parse().expect("tokens")).expect("the cells graph");
    let manifest = graph
        .manifest(&|name| Some(gdl_model::TypeInfo::new(name, true, Default::default())))
        .expect("manifest");
    let placement = gdl_model::Placement::from_toml(PLACEMENT).expect("placement");
    let layout = gdl_model::layout(&manifest, &placement).expect("a layout");
    assert_eq!(
        layout.boundaries(),
        [gdl_model::ClassBoundary {
            hop: 2,
            from: "global".into(),
            into: "model_x".into(),
        }]
    );
    assert_eq!(
        layout.report().to_string(),
        "hop 0: 2 crossings, 1 anchors, 0 WAN round trips\n\
         hop 1: 2 crossings, 1 anchors, 0 WAN round trips\n\
         hop 2: 2 crossings, 1 anchors, 1 WAN round trips (global -> model_x)\n\
         hop 3: 3 crossings, 2 anchors, 0 WAN round trips\n\
         per request: 9 crossings, 5 anchors, 1 WAN round trips"
    );
}
