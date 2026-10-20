// Only a message can be exported: a type that does not derive `Message`
// fails where it is exported.

pub mod inner {
    pub struct Words;
}

#[gdl_message::export]
pub use inner::Words;

fn main() {}
