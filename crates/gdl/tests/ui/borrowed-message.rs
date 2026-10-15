// A message type that deserializes only by borrowing from its input cannot
// be decoded from a received frame, so the wire probe rejects it outright.

#[derive(serde::Serialize, serde::Deserialize)]
struct Borrowed<'a> {
    s: &'a str,
}

fn main() {
    let _ = gdl::wire_codec!(Borrowed<'static>);
}
