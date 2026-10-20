//! A message a macro makes: the build registers it, and the source reader
//! cannot see it. `cargo gdl extract --verify` reports the difference.

macro_rules! message {
    ($name:ident) => {
        /// Made by a macro.
        #[derive(gdl_message::Message)]
        pub struct $name;
    };
}

message!(Hidden);

/// Written out.
#[derive(gdl_message::Message)]
pub struct Seen;
