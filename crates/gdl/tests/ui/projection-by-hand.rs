// A projection's fields are checked against its edge type only by
// `derive(gdl::Projection)`, so the build refuses a hand-written impl.
#[derive(serde::Deserialize)]
struct Routed {
    hint: u64,
}

#[derive(serde::Deserialize)]
struct Hinted {
    hint: u64,
}

impl gdl::Projection for Hinted {
    type Of = Routed;
}

fn main() {}
