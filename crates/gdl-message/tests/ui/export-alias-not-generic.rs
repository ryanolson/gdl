// An alias offers an instantiation of a generic message: a message that
// is not generic is offered under another path with `pub use`.

#[derive(gdl_message::Message)]
pub struct Prompt;

#[gdl_message::export]
pub type Other = Prompt;

fn main() {}
