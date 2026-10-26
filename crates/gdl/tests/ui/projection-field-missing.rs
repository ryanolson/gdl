// A projection reads a field the edge type does not have: here a
// misspelling, which would fail every request's read.
#[derive(serde::Deserialize)]
pub struct Routed {
    pub request: String,
    pub hint: u64,
}

#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hnit: u64,
}

fn main() {}
