//! A reply to a call: what the `gdl.call` handler answers, and the one place
//! its bytes are written. A reply is made as a [`Reply`], whatever it says,
//! and only [`Reply::write`] turns one into bytes, holding every kind to
//! `call_reply_max`. The tags are private here, so no other code can write
//! a reply of its own. Under a call codec an answer is compressed here,
//! before it is written, so that the limit holds the bytes on the wire; the
//! caller decodes it within its own `decompressed_max`.

use gdl::{Error, ErrorKind, Result};

use super::super::envelope::{decode, decoded, write_msgpack};
use crate::zstd::{self, Compression};

/// What a reply to a call says, before it is written.
pub(crate) struct Reply(Kind);

enum Kind {
    /// The service answered: the tag, then its response in its message's
    /// encoding, written in place.
    Answered(Vec<u8>),
    /// The service ran and failed.
    Failed(Error),
    /// The service never ran.
    Refused(Error),
}

/// A reply's first byte.
#[derive(Clone, Copy, Debug)]
#[repr(u8)]
enum Tag {
    Answered = 0,
    Failed = 1,
    Refused = 2,
}

impl Reply {
    /// The service answered with what `encode` writes: its response in its
    /// message's encoding. A response that does not encode is a failure.
    /// Under a call codec, `compression`, a response from its floor on is
    /// one zstd frame after the tag, when that is shorter.
    pub(crate) fn answered(
        encode: impl FnOnce(&mut Vec<u8>) -> Result<()>,
        compression: Option<Compression>,
    ) -> Self {
        let mut bytes = vec![Tag::Answered as u8];
        if let Err(error) = encode(&mut bytes) {
            return Self::failed(error);
        }
        match compression {
            Some(compression) if compression.applies(bytes.len() - 1) => {
                match compressed(&bytes[1..]) {
                    Ok(Some(framed)) => Self(Kind::Answered(framed)),
                    Ok(None) => Self(Kind::Answered(bytes)),
                    Err(error) => Self::failed(error),
                }
            }
            _ => Self(Kind::Answered(bytes)),
        }
    }

    /// The service ran and failed with `error`.
    pub(crate) fn failed(error: Error) -> Self {
        Self(Kind::Failed(error))
    }

    /// The service never ran, for `error`.
    pub(crate) fn refused(error: Error) -> Self {
        Self(Kind::Refused(error))
    }

    /// Whether the service never ran: a refusal holds no call permit, and
    /// its caller may try another instance.
    pub(crate) fn is_refused(&self) -> bool {
        matches!(self.0, Kind::Refused(_))
    }

    /// The reply's bytes, at most `max` of them, its tag included, for any
    /// reply and any `max`: the one invariant of a reply's size. A reply past
    /// `max`, of any kind, is replaced by a [fallback](fallback) of at most
    /// [`FALLBACK_MAX`] bytes, which keeps its kind: a refusal stays a
    /// refusal, so the caller still knows that the service never ran; an
    /// answer becomes a failure. The settings refuse a `call_reply_max` below
    /// [`FALLBACK_MAX`], so the fallback always fits; below it, the tag
    /// alone is written, which the caller reads as a `Codec` error.
    pub(crate) fn write(self, service: Option<&str>, max: usize) -> Vec<u8> {
        // The tag of what replaces a reply past `max`.
        let (instead, bytes) = match self.0 {
            Kind::Answered(bytes) => (Tag::Failed, bytes),
            Kind::Failed(error) => (Tag::Failed, error_bytes(Tag::Failed, &error)),
            Kind::Refused(error) => (Tag::Refused, error_bytes(Tag::Refused, &error)),
        };
        if bytes.len() <= max {
            return bytes;
        }
        let fallback = fallback(instead, service, bytes.len());
        if fallback.len() <= max {
            return fallback;
        }
        vec![instead as u8]
    }
}

/// `plain` as the answer's tag and one frame, or `None` when the frame is
/// not shorter, and the answer crosses plain.
fn compressed(plain: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut framed = Vec::with_capacity(1 + zstd::bound(plain.len()));
    framed.push(Tag::Answered as u8);
    zstd::compress_into(plain, &mut framed)?;
    Ok((framed.len() <= plain.len()).then_some(framed))
}

/// The most bytes of a service's name that a fallback carries: nothing in
/// one grows with its input.
const NAME_CUT: usize = 64;

/// The largest a [fallback](fallback) is, its tag included: one with a name
/// of [`NAME_CUT`] bytes and a length of `u64::MAX`.
pub(crate) const FALLBACK_MAX: usize = 153;

// The settings' least `call_reply_max` holds the largest fallback.
const _: () = assert!(FALLBACK_MAX <= crate::dist::runtime::RuntimeConfig::CALL_REPLY_MIN);

/// What replaces a reply of `len` bytes that is past its limit: a `Codec`
/// error after `tag`, which names the service, cut to [`NAME_CUT`] bytes,
/// and the reply's length. Its size has a fixed bound, [`FALLBACK_MAX`],
/// whatever the reply and the name. Written only for a reply past its
/// limit: a reply within it costs no allocation for this.
fn fallback(tag: Tag, service: Option<&str>, len: usize) -> Vec<u8> {
    let message = match service {
        Some(name) => format!(
            "the reply of unary node `{}` is {len} bytes, over `call_reply_max`",
            cut(name)
        ),
        None => format!("the reply to a call is {len} bytes, over `call_reply_max`"),
    };
    error_bytes(tag, &Error::new(ErrorKind::Codec, message))
}

/// `name`, cut to at most [`NAME_CUT`] bytes, at a character's boundary.
fn cut(name: &str) -> &str {
    let mut end = name.len().min(NAME_CUT);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    &name[..end]
}

/// A reply that carries `error`, after `tag`.
fn error_bytes(tag: Tag, error: &Error) -> Vec<u8> {
    let mut bytes = vec![tag as u8];
    // An error always encodes; if it did not, the caller reads a codec
    // error from the short reply.
    let _ = write_msgpack(&mut bytes, error);
    bytes
}

/// The error of a reply whose bytes say that the service never ran, so
/// that the caller may try another instance; `None` for any other reply.
/// A reply replaced for its size keeps its kind ([`Reply::write`]), so
/// this holds for every reply written.
pub(crate) fn refusal(bytes: &[u8]) -> Option<Error> {
    match bytes.split_first() {
        Some((&tag, rest)) if tag == Tag::Refused as u8 => {
            Some(decode::<Error>(rest).unwrap_or_else(|error| error))
        }
        _ => None,
    }
}

/// A reply's response, decoded by `decode`, or the reply's error. Under a
/// call codec, `compression`, a response that is a frame is decoded first,
/// within `compression`'s limit, the caller's `decompressed_max`: a frame
/// that states more fails the call with `Codec` before anything is
/// allocated for it.
pub(crate) fn response<T>(
    bytes: &[u8],
    compression: Option<Compression>,
    decode: impl FnOnce(&[u8]) -> Result<T>,
) -> Result<T> {
    let body = read(bytes)?;
    match compression {
        Some(compression) if zstd::is_frame(body) => {
            decode(&decoded(body, compression.limit(), &[])?)
        }
        _ => decode(body),
    }
}

/// A reply, read: the response's bytes, borrowed, or the error.
pub(crate) fn read(bytes: &[u8]) -> Result<&[u8]> {
    let Some((&tag, rest)) = bytes.split_first() else {
        return Err(Error::new(ErrorKind::Codec, "an empty reply to a call"));
    };
    match tag {
        t if t == Tag::Answered as u8 => Ok(rest),
        t if t == Tag::Failed as u8 || t == Tag::Refused as u8 => Err(decode::<Error>(rest)?),
        other => Err(Error::new(
            ErrorKind::Codec,
            format!("a reply to a call with tag {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dist::runtime::RuntimeConfig;

    /// The fallback's bound is exact: its largest case, a name of
    /// [`NAME_CUT`] bytes and a length of `u64::MAX`, is that size, and a
    /// longer name is cut to the same.
    #[test]
    fn the_fallbacks_bound_is_its_largest_case() {
        let name = "n".repeat(NAME_CUT);
        let largest = fallback(Tag::Refused, Some(&name), u64::MAX as usize);
        assert_eq!(largest.len(), FALLBACK_MAX);
        let longer = fallback(Tag::Failed, Some(&"n".repeat(4096)), u64::MAX as usize);
        assert_eq!(longer.len(), FALLBACK_MAX);
        assert!(fallback(Tag::Failed, None, u64::MAX as usize).len() <= FALLBACK_MAX);
        // A name cut inside a character is cut before it.
        let wide = format!("{}é", "n".repeat(NAME_CUT - 1));
        assert_eq!(cut(&wide).len(), NAME_CUT - 1);
    }

    /// A reply of `kind` whose content is about `size` bytes.
    fn reply(kind: Tag, size: usize) -> Reply {
        let error = || Error::new(ErrorKind::Node, "x".repeat(size));
        match kind {
            // Through `calloc` and `memcpy`: a byte loop is slow in a test build.
            Tag::Answered => Reply::answered(
                |out| {
                    out.extend_from_slice(&vec![0; size]);
                    Ok(())
                },
                None,
            ),
            Tag::Failed => Reply::failed(error()),
            Tag::Refused => Reply::refused(error()),
        }
    }

    /// Every kind of reply, every length of name, contents around each
    /// limit, limits from the least the settings allow: what is written is
    /// at most the limit, and reads back as the kind it should. A reply
    /// within its limit is written as it is; a reply past it is a `Codec`
    /// error that keeps whether the service ran.
    #[test]
    fn every_reply_is_held_to_its_limit_whatever_its_kind_and_name() {
        let names: Vec<Option<String>> = std::iter::once(None)
            .chain(
                [0, 1, 63, 64, 65, 2048, 4096]
                    .into_iter()
                    .map(|n| Some("s".repeat(n))),
            )
            .collect();
        let min = RuntimeConfig::CALL_REPLY_MIN;
        let limits = [min, min + 1, 1024, 2048, 4096, 8192, 65536];
        let mut cases = 0;
        for kind in [Tag::Answered, Tag::Failed, Tag::Refused] {
            for name in &names {
                for max in limits {
                    for size in [0, max - 2, max - 1, max, max + 1, max + 2, 4 * max] {
                        let bytes = reply(kind, size).write(name.as_deref(), max);
                        assert!(
                            bytes.len() <= max,
                            "{} bytes over {max}, size {size}",
                            bytes.len()
                        );
                        assert_eq!(refusal(&bytes).is_some(), matches!(kind, Tag::Refused));
                        let read = read(&bytes);
                        match (kind, read) {
                            (Tag::Answered, Ok(body)) => {
                                assert_eq!(bytes[0], Tag::Answered as u8);
                                assert_eq!(body.len(), size);
                            }
                            (Tag::Answered, Err(error)) => {
                                assert_eq!(bytes[0], Tag::Failed as u8);
                                assert_eq!(error.kind(), ErrorKind::Codec);
                                assert!(1 + size > max, "replaced within its limit");
                            }
                            (_, Ok(_)) => panic!("an error read back as an answer"),
                            (_, Err(error)) => {
                                assert_eq!(bytes[0], kind as u8, "the kind changed");
                                let kept = error.kind() == ErrorKind::Node
                                    && error.message().len() == size;
                                let replaced = error.kind() == ErrorKind::Codec;
                                assert!(kept || replaced, "{error}");
                            }
                        }
                        cases += 1;
                    }
                }
            }
        }
        assert_eq!(cases, 3 * 8 * 7 * 7);
    }

    /// A limit below the fallback, which the settings refuse, still bounds
    /// what is written: the tag alone, which reads as a `Codec` error.
    #[test]
    fn a_limit_below_the_fallback_writes_the_tag_alone() {
        let bytes = reply(Tag::Refused, 4096).write(Some("Tok"), 32);
        assert_eq!(bytes, [Tag::Refused as u8]);
        assert_eq!(read(&bytes).expect_err("an error").kind(), ErrorKind::Codec);
    }

    /// An answer of `content`, compressed from `floor` on.
    fn coded(content: &[u8], floor: u32) -> Reply {
        Reply::answered(
            |out| {
                out.extend_from_slice(content);
                Ok(())
            },
            Some(Compression::new(floor, usize::MAX)),
        )
    }

    /// Under a call codec, `call_reply_max` holds the bytes on the wire, its
    /// tag included: an answer whose plain form is past the limit crosses
    /// when its frame fits, and reads back whole; one whose frame does not
    /// fit is the fallback, as a plain answer past the limit is. Below its
    /// floor, or where the frame is not shorter, an answer is written
    /// plain. Errors are never compressed.
    #[test]
    fn a_compressed_answer_is_held_to_its_limit_on_its_wire_bytes() {
        let max = 1024;
        let zeros = vec![0u8; 100 * max];
        let bytes = coded(&zeros, 512).write(Some("Tok"), max);
        assert!(bytes.len() <= max, "{} bytes", bytes.len());
        assert_eq!(bytes[0], Tag::Answered as u8);
        assert!(zstd::is_frame(&bytes[1..]));
        let back = response(&bytes, Some(Compression::new(512, zeros.len())), |b| {
            Ok(b.to_vec())
        })
        .expect("answered");
        assert_eq!(back, zeros);
        // A frame past the limit: the fallback, which names the service.
        let mut rng = Seeded(0xc0de_c0de);
        let dense: Vec<u8> = (0..4 * max).map(|_| rng.next() as u8).collect();
        let bytes = coded(&dense, 512).write(Some("Tok"), max);
        assert!(bytes.len() <= max);
        let error = read(&bytes).expect_err("past the limit");
        assert_eq!(error.kind(), ErrorKind::Codec);
        assert!(error.message().contains("`Tok`"), "{error}");
        // Below the floor, plain.
        let bytes = coded(&zeros[..500], 512).write(Some("Tok"), max);
        assert_eq!(read(&bytes).expect("answered"), &zeros[..500]);
        // A frame that is not shorter: plain.
        let bytes = coded(&dense[..900], 512).write(Some("Tok"), max);
        assert_eq!(read(&bytes).expect("answered"), &dense[..900]);
        // An error stays an error, plain, under a codec too.
        let bytes = Reply::failed(Error::new(ErrorKind::Node, "x".repeat(800))).write(None, max);
        assert_eq!(read(&bytes).expect_err("failed").message().len(), 800);
    }

    /// The caller decodes a compressed answer within its own limit: a
    /// frame under the service's `call_reply_max` that states more than the
    /// caller's `decompressed_max` fails with `Codec`. A plain answer is
    /// read as it is, and a frame is read only under a codec.
    #[test]
    fn a_compressed_answer_decodes_within_the_callers_limit() {
        let zeros = vec![0u8; 100_000];
        let bytes = coded(&zeros, 512).write(Some("Tok"), 1024);
        assert!(bytes.len() <= 1024);
        let limit = Compression::new(512, 8192);
        let error = response(&bytes, Some(limit), |b| Ok(b.len())).expect_err("past the limit");
        assert_eq!(error.kind(), ErrorKind::Codec);
        assert!(
            error.message().contains("over the limit of 8192"),
            "{error}"
        );
        let within = Compression::new(512, zeros.len());
        assert_eq!(response(&bytes, Some(within), |b| Ok(b.len())), Ok(100_000));
        // Without a codec the frame is the response's bytes, as they are.
        assert_eq!(response(&bytes, None, |b| Ok(b.len())), Ok(bytes.len() - 1));
    }

    /// A seeded generator, splitmix64: the workspace has no property-test
    /// crate, and a fixed seed makes a failing case one to rerun.
    struct Seeded(u64);

    impl Seeded {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        /// A number in `0..n`.
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// The invariant of `Reply::write`, over 10,000 cases from one seed:
    /// random kinds, service names of 0 to 8 KiB, of one-byte or two-byte
    /// characters, or none; limits from the least the settings take to the
    /// most, spread evenly over their powers of two; contents around the
    /// limit, small, or anywhere up to twice it. What is written is at most
    /// the limit, and reads back as its kind. About a second: the large
    /// limits make contents of megabytes.
    #[test]
    fn a_written_reply_is_within_its_limit_for_random_replies_names_and_limits() {
        const SEED: u64 = 0x5eed_0267_2026_1104;
        const CASES: usize = 10_000;
        let least = RuntimeConfig::CALL_REPLY_MIN;
        let most = RuntimeConfig::FRAME_MAX - RuntimeConfig::FRAME_ROOM;
        let mut rng = Seeded(SEED);
        let (low, high) = (least.ilog2(), most.ilog2());
        for case in 0..CASES {
            let kind = [Tag::Answered, Tag::Failed, Tag::Refused][rng.below(3)];
            let name = match rng.below(8) {
                0 => None,
                pick => {
                    let len = rng.below(8 * 1024 + 1);
                    let ch = if pick % 2 == 0 { "é" } else { "s" };
                    Some(ch.repeat(len / ch.len()))
                }
            };
            let power = low + rng.below((high - low + 1) as usize) as u32;
            let max = ((1usize << power) + rng.below(1usize << power)).clamp(least, most);
            let size = match rng.below(4) {
                0 => rng.below(4096),
                1 | 2 => (max + rng.below(129)).saturating_sub(64),
                _ => rng.below(2 * max),
            };
            let bytes = reply(kind, size).write(name.as_deref(), max);
            let at = || {
                format!(
                    "seed {SEED:#x}, case {case}: {kind:?}, name of {:?} bytes, limit {max}, content {size}, written {}",
                    name.as_ref().map(String::len),
                    bytes.len()
                )
            };
            assert!(bytes.len() <= max, "past its limit: {}", at());
            assert_eq!(
                refusal(&bytes).is_some(),
                matches!(kind, Tag::Refused),
                "read as refused or not wrongly: {}",
                at()
            );
            let right = match (kind, read(&bytes)) {
                (Tag::Answered, Ok(body)) => bytes[0] == Tag::Answered as u8 && body.len() == size,
                (Tag::Answered, Err(error)) => {
                    bytes[0] == Tag::Failed as u8 && error.kind() == ErrorKind::Codec
                }
                (_, Ok(_)) => false,
                (_, Err(error)) => {
                    bytes[0] == kind as u8
                        && matches!(error.kind(), ErrorKind::Node | ErrorKind::Codec)
                }
            };
            assert!(right, "read back as the wrong kind: {}", at());
        }
    }
}
