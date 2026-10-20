// Every message crosses tasks, so it must be `Send + 'static`. The derive
// says so where the type is defined, not later in a contract that uses it.

#[derive(gdl_message::Message)]
struct Shared {
    text: std::rc::Rc<str>,
}

fn main() {}
