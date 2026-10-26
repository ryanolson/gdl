// A packed edge type is still checked: a misspelled field is named.
#[derive(serde::Deserialize)]
#[repr(C, packed)]
pub struct Routed {
    pub hint: u64,
    pub body: u8,
}

#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hnit: u64,
}

fn main() {}
