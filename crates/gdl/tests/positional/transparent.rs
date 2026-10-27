#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, gdl::Message)]
#[gdl(codec = "msgpack_array")]
#[serde(transparent)]
struct Optional {
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<u32>,
}

fn main() {
    for value in [Optional { value: None }, Optional { value: Some(7) }] {
        let bytes = rmp_serde::to_vec(&value).expect("encode a transparent struct");
        assert_eq!(
            rmp_serde::from_slice::<Optional>(&bytes).expect("decode a transparent struct"),
            value
        );
    }
}
