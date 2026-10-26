// A projection skips the fields it does not read, so it cannot deny them:
// it would refuse every request that has the rest of the edge type.
#[derive(serde::Deserialize)]
pub struct Routed {
    pub hint: String,
    pub body: String,
}

#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Routed)]
#[serde(deny_unknown_fields)]
struct Hinted {
    hint: String,
}

fn main() {}
