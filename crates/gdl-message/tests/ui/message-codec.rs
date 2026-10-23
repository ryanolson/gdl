#[derive(gdl_message::Message)]
#[gdl(codec = "bincode")]
struct Unknown;

#[derive(gdl_message::Message)]
#[gdl(codec = "json", codec = "msgpack")]
struct Duplicate;

#[derive(gdl_message::Message)]
#[gdl(codex = "json")]
struct Misspelled;

fn main() {}
