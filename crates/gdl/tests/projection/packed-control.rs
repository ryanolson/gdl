use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[repr(C, packed)]
pub struct Routed {
    pub hint: u64,
    pub body: u8,
}

#[derive(Deserialize)]
pub struct Hinted {
    pub hint: u64,
}

fn main() {
    let request = Routed { hint: 42, body: 7 };
    for encoding in [gdl::Encoding::Json, gdl::Encoding::Msgpack] {
        let bytes = gdl::__runtime::encoder::<Routed>(encoding)(&request).expect("encode");
        let whole = gdl::__runtime::decoder::<Routed>(encoding)(&bytes).expect("round trip");
        let hint = whole.hint;
        assert_eq!(hint, 42);
        let part = gdl::__runtime::decoder::<Hinted>(encoding)(&bytes).expect("projection");
        assert_eq!(part.hint, hint);
    }
}
