// A generic node takes type parameters only: an alias names each
// instantiation by its types, and a node must be `'static`.

#[derive(gdl::Node)]
pub struct Engine<'a>(&'a str);

#[derive(gdl::Node)]
pub struct Fixed<const N: usize>([u8; N]);

fn main() {}
