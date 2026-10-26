// A packed edge type is still checked: a field of another type is a
// mismatch.
#[derive(serde::Deserialize)]
#[repr(C, packed)]
pub struct Routed {
    pub hint: u64,
    pub body: u8,
}

#[derive(serde::Deserialize, gdl::Projection)]
#[gdl(of = Routed)]
struct Hinted {
    hint: u32,
}

fn main() {}
