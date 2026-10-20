// An export names each message it offers: a glob names none.

pub mod inner {
    #[derive(gdl_message::Message)]
    pub struct Words;
}

#[gdl_message::export]
pub use inner::*;

fn main() {}
