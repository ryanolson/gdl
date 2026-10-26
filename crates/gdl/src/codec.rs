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

/// [`encoder`] that appends the value to a buffer the caller owns, such as
/// the frame a transport sends, so the value is not copied after it is
/// encoded. After an error the buffer may hold part of the value.
pub fn encoder_into<T: Serialize>(encoding: Encoding) -> fn(&T, &mut Vec<u8>) -> Result<()> {
    match encoding {
        Encoding::Msgpack => write_msgpack::<T>,
        Encoding::MsgpackArray => write_array::<T>,
        Encoding::Json => write_json::<T>,
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
    write_msgpack(value, &mut bytes)?;
    Ok(bytes)
}

fn encode_array<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(128);
    write_array(value, &mut bytes)?;
    Ok(bytes)
}

fn write_msgpack<T: Serialize>(value: &T, bytes: &mut Vec<u8>) -> Result<()> {
    rmp_serde::encode::write_named(bytes, value)
        .map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn write_array<T: Serialize>(value: &T, bytes: &mut Vec<u8>) -> Result<()> {
    rmp_serde::encode::write(bytes, value).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn decode_msgpack<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn write_json<T: Serialize>(value: &T, bytes: &mut Vec<u8>) -> Result<()> {
    serde_json::to_writer(bytes, value).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Token {
        id: u32,
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        logprob: Option<f32>,
    }

    /// Writing into a buffer gives the bytes the owned encode gives, for
    /// every encoding, after whatever the buffer already holds: a transport
    /// frame's header sits in front of the value.
    #[test]
    fn encoding_into_a_buffer_appends_the_owned_encoding() {
        let token = Token {
            id: 7,
            text: "hi".into(),
            logprob: None,
        };
        for encoding in [Encoding::Msgpack, Encoding::MsgpackArray, Encoding::Json] {
            let owned = encoder::<Token>(encoding)(&token).expect("encode");
            let mut buffer = b"header".to_vec();
            encoder_into::<Token>(encoding)(&token, &mut buffer).expect("encode into");
            assert_eq!(&buffer[..6], b"header", "{encoding:?}");
            assert_eq!(buffer[6..], owned, "{encoding:?}");
        }
    }
}
