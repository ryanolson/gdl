// An alias names one instantiation: an alias with parameters of its own
// names none.

#[derive(gdl_message::Message)]
pub struct Carried<P, B> {
    pub payload: P,
    pub builder: B,
}

#[gdl_message::export]
pub type Carry<T> = Carried<T, u8>;

fn main() {}
