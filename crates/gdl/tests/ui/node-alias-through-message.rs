// `gdl_message` knows messages, not nodes: an alias of a node
// instantiation is exported through gdl.

#[derive(gdl::Node)]
pub struct Relay<B>(std::marker::PhantomData<fn() -> B>);

#[gdl_message::export]
pub type U32Relay = Relay<u32>;

fn main() {}
