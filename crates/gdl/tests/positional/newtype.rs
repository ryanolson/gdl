#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, gdl::Message)]
#[gdl(codec = "msgpack_array")]
struct Optional(#[serde(skip_serializing_if = "Option::is_none")] Option<u32>);

fn main() {
    for (value, expected) in [(Optional(None), vec![0xc0]), (Optional(Some(7)), vec![7])] {
        let bytes = rmp_serde::to_vec(&value).expect("encode a newtype");
        assert_eq!(bytes, expected);
        assert_eq!(
            rmp_serde::from_slice::<Optional>(&bytes).expect("decode a newtype"),
            value
        );
    }
}
