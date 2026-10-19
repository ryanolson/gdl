// An export offers a public path: a private `use` offers none.

mod inner {
    #[derive(gdl_message::Message)]
    pub struct Words;
}

#[gdl_message::export]
use inner::Words;

fn main() {}
