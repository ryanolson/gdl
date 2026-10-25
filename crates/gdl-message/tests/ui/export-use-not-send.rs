// Every message crosses tasks: an instantiation that is not `Send` is
// refused where a `pub use` exports it, through an alias that does not.

#[derive(gdl_message::Message)]
pub struct Carried<P> {
    pub payload: P,
}

pub type Unmarked = Carried<std::rc::Rc<str>>;

#[gdl_message::export]
pub use Unmarked as Shared;

fn main() {}
