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

/// [`encoder_into`] that writes the value into the buffer's spare room when
/// it fits, so a value that fits is serialized once. A value that does not
/// fit is serialized to count its length, and then into a buffer grown by
/// exactly that length: the buffer never grows past the value, and the
/// value is not moved after it is written. A `Serialize` that gives another
/// length on its next call only makes the buffer grow, as [`encoder_into`]
/// does.
pub fn encoder_sized<T: Serialize>(encoding: Encoding) -> fn(&T, &mut Vec<u8>) -> Result<()> {
    match encoding {
        Encoding::Msgpack => |value, bytes| write_sized(Encoding::Msgpack, value, bytes),
        Encoding::MsgpackArray => |value, bytes| write_sized(Encoding::MsgpackArray, value, bytes),
        Encoding::Json => |value, bytes| write_sized(Encoding::Json, value, bytes),
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
    write_msgpack_to(bytes, value)
}

fn write_array<T: Serialize>(value: &T, bytes: &mut Vec<u8>) -> Result<()> {
    write_array_to(bytes, value)
}

fn write_msgpack_to<T: Serialize>(out: &mut impl std::io::Write, value: &T) -> Result<()> {
    rmp_serde::encode::write_named(out, value)
        .map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn write_array_to<T: Serialize>(out: &mut impl std::io::Write, value: &T) -> Result<()> {
    rmp_serde::encode::write(out, value).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

fn write_to<T: Serialize>(
    encoding: Encoding,
    out: &mut impl std::io::Write,
    value: &T,
) -> Result<()> {
    match encoding {
        Encoding::Msgpack => write_msgpack_to(out, value),
        Encoding::MsgpackArray => write_array_to(out, value),
        Encoding::Json => serde_json::to_writer(out, value)
            .map_err(|e| Error::new(ErrorKind::Codec, e.to_string())),
    }
}

fn write_sized<T: Serialize>(encoding: Encoding, value: &T, bytes: &mut Vec<u8>) -> Result<()> {
    let start = bytes.len();
    let mut within = Within {
        bytes,
        overflowed: false,
    };
    match write_to(encoding, &mut within, value) {
        Ok(()) => return Ok(()),
        // The flag, not the error: each format wraps a writer's error its own way.
        Err(_) if within.overflowed => {}
        Err(error) => return Err(error),
    }
    bytes.truncate(start);
    let mut counter = Counter(0);
    write_to(encoding, &mut counter, value)?;
    bytes.reserve_exact(counter.0);
    write_to(encoding, bytes, value)
}

/// Appends to a buffer without growing it, and fails when the buffer is
/// full.
struct Within<'a> {
    bytes: &'a mut Vec<u8>,
    overflowed: bool,
}

impl std::io::Write for Within<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.bytes.capacity() - self.bytes.len() < bytes.len() {
            self.overflowed = true;
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Counts the bytes written to it, and drops them.
struct Counter(usize);

impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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

    /// Counts how often it is serialized.
    struct Walked<'a>(&'a Token, &'a std::cell::Cell<u32>);

    impl Serialize for Walked<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            self.1.set(self.1.get() + 1);
            self.0.serialize(serializer)
        }
    }

    /// A value that fits the buffer's spare room is serialized once, into
    /// it. One a byte over is serialized to count it, then into a buffer
    /// grown by exactly its length. Either way the bytes are the owned
    /// encoding's, after what the buffer held, in every encoding.
    #[test]
    fn a_sized_encode_fits_the_spare_room_or_grows_by_exactly_the_value() {
        let walks = std::cell::Cell::new(0);
        for encoding in [Encoding::Msgpack, Encoding::MsgpackArray, Encoding::Json] {
            for text in [0, 1, 200, 70_000].map(|len| "t".repeat(len)) {
                let token = Token {
                    id: 300,
                    text,
                    logprob: Some(-0.5),
                };
                let owned = encoder::<Token>(encoding)(&token).expect("encode");
                for spare in [owned.len() - 1, owned.len(), owned.len() + 1] {
                    let mut buffer = Vec::with_capacity(6 + spare);
                    buffer.extend_from_slice(b"header");
                    walks.set(0);
                    let walked = Walked(&token, &walks);
                    encoder_sized::<Walked>(encoding)(&walked, &mut buffer).expect("encode");
                    let case = format!("{encoding:?}, {} B in {spare} B", owned.len());
                    assert_eq!(&buffer[..6], b"header", "{case}");
                    assert_eq!(buffer[6..], owned, "{case}");
                    if spare < owned.len() {
                        assert_eq!(walks.get(), 3, "{case}: a partial write, a count, a write");
                        assert_eq!(buffer.capacity(), buffer.len(), "{case}: spare room");
                    } else {
                        assert_eq!(walks.get(), 1, "{case}: serialized again");
                        assert_eq!(buffer.capacity(), 6 + spare, "{case}: the buffer grew");
                    }
                }
            }
        }
    }
}
