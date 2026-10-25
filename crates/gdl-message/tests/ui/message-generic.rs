// A generic message takes type parameters only: an alias names each
// instantiation by its types, and a message must be `'static`, which a
// lifetime parameter is not. A const parameter has no source form the
// reader can compare.

#[derive(gdl_message::Message)]
struct Borrowed<'a> {
    text: &'a str,
}

#[derive(gdl_message::Message)]
struct Fixed<const N: usize> {
    bytes: [u8; N],
}

fn main() {}
