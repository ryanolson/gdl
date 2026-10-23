use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Encoding, Error, ErrorKind, Result};

/// Select once when an edge is built, then call the encoder per value.
pub fn encoder<T: Serialize>(encoding: Encoding) -> fn(&T) -> Result<Vec<u8>> {
    match encoding {
        Encoding::Msgpack => encode_msgpack::<T>,
        Encoding::MsgpackArray => encode_array::<T>,
        Encoding::Json => encode_json::<T>,
    }
}

/// Runtime edges and typed selectors must use the same decoding policy.
pub fn decoder<T: DeserializeOwned>(encoding: Encoding) -> fn(&[u8]) -> Result<T> {
    match encoding {
        Encoding::Msgpack | Encoding::MsgpackArray => decode_msgpack::<T>,
        Encoding::Json => decode_json::<T>,
    }
}

fn encode_msgpack<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    // Reserve as serde_json does: token-sized maps then need no buffer growth.
    let mut bytes = Vec::with_capacity(128);
    rmp_serde::encode::write_named(&mut bytes, value)
        .map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))?;
    Ok(bytes)
}

fn encode_array<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(128);
    rmp_serde::encode::write(&mut bytes, value)
        .map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))?;
    Ok(bytes)
}

fn decode_msgpack<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}
