// A projection reads the name its edge type writes, which is one name, so
// an alias on its field is refused.
#[derive(serde::Deserialize)]
pub struct Routed {
    pub hint: String,
    pub body: Vec<String>,
}

#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Routed)]
struct Hinted {
    #[serde(alias = "routing_hint")]
    hint: String,
}

fn main() {}
