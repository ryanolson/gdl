#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, gdl::Message)]
#[gdl(codec = "msgpack_array")]
enum Event {
    Token {
        id: u32,
        text: String,
    },
    #[serde(skip)]
    Internal {
        #[serde(skip_serializing_if = "Option::is_none")]
        cache: Option<u32>,
    },
}

fn main() {
    let value = Event::Token {
        id: 7,
        text: "token".into(),
    };
    let bytes = rmp_serde::to_vec(&value).expect("encode a carried variant");
    assert_eq!(
        rmp_serde::from_slice::<Event>(&bytes).expect("decode a carried variant"),
        value
    );
    assert!(rmp_serde::to_vec(&Event::Internal { cache: None }).is_err());
}
