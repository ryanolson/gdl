use serde::{Deserialize, Serialize};

/// The encoding of one complete message at a segment boundary.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    /// MessagePack maps with field names, including in nested structs.
    #[default]
    Msgpack,
    /// Positional MessagePack arrays. Fields must not be conditionally omitted.
    MsgpackArray,
    /// JSON, using the message's serde representation.
    Json,
}

impl Encoding {
    /// The encodings accepted by message declarations.
    pub const ALL: [Self; 3] = [Self::Msgpack, Self::MsgpackArray, Self::Json];

    /// The name used by `#[gdl(codec = "...")]` and the catalogue.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Msgpack => "msgpack",
            Self::MsgpackArray => "msgpack_array",
            Self::Json => "json",
        }
    }

    /// The wire revision of this encoding, included in the contract hash.
    pub const fn version(self) -> u32 {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Declaration {
        encoding: Encoding,
    }

    #[test]
    fn catalogue_spelling_matches_the_contract_identity() {
        for encoding in Encoding::ALL {
            let declaration = Declaration { encoding };
            let text = format!("encoding = {:?}\n", encoding.name());
            assert_eq!(toml::to_string(&declaration).expect("serialize"), text);
            assert_eq!(
                toml::from_str::<Declaration>(&text).expect("deserialize"),
                declaration
            );
        }
    }
}
