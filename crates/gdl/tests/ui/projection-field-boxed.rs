// A field's type must be the edge type's own: `Box<u64>` reads as `u64`
// would coerce, but the projection names its fields' types exactly.
#[derive(serde::Deserialize)]
pub struct Routed {
    pub hint: Box<u64>,
}

#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: u64,
}

fn main() {}
