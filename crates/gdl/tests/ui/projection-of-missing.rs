// A projection names the edge type it reads a part of.
#[derive(serde::Deserialize, gdl::Projection)]
struct Hinted {
    hint: u64,
}

fn main() {}
