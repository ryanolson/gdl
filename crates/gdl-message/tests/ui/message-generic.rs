// A generic type has no one concrete type to register, so it cannot be a
// message: a graph names `Wrapper`, and nothing says which `T` that is.

#[derive(gdl_message::Message)]
struct Wrapper<T> {
    value: T,
}

#[derive(gdl_message::Message)]
struct Borrowed<'a> {
    text: &'a str,
}

fn main() {}
