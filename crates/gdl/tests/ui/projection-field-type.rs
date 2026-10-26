// A projection reads a field of the edge type as another type.
#[derive(serde::Deserialize)]
pub struct Routed {
    pub request: String,
    pub hint: u64,
}

#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: String,
}

fn main() {}
