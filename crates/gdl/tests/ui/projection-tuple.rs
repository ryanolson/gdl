// A projection reads fields by name, so it has named fields.
#[derive(serde::Deserialize)]
pub struct Routed {
    pub hint: u64,
}

#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Routed)]
struct Hinted(u64);

fn main() {}
