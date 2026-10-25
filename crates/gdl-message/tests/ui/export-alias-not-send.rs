// Every message crosses tasks: an instantiation that is not `Send` is
// refused where its alias names it.

#[derive(gdl_message::Message)]
pub struct Carried<P> {
    pub payload: P,
}

#[gdl_message::export]
pub type Shared = Carried<std::rc::Rc<str>>;

fn main() {}
