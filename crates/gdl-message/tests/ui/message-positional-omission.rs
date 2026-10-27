// Positional MessagePack has no field names: a field that one direction
// writes and the other does not read shifts every field after it.

#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub struct Hinted {
    pub id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<u32>,
    pub text: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub struct Carried<P> {
    #[serde(skip_serializing)]
    pub payload: P,
}

#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub enum Event {
    Done,
    Token {
        id: u32,
        #[serde(skip_deserializing)]
        logprob: f32,
    },
}

#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub struct Pair(u32, #[serde(skip_serializing_if = "Option::is_none")] Option<u32>);

fn main() {}
