//! An edge codec's compression: one zstd frame for a value at least an
//! edge's floor long (docs/src/concepts/crossings.md#edge-codecs). Built
//! with the `zstd` feature; without it nothing can be compressed, and a
//! segment whose layout compresses on one of its edges refuses to start.

use gdl::{Error, ErrorKind, Result};

/// A zstd frame starts with these bytes. No named MessagePack value that
/// gdl writes does: `0x28` is the positive fixint 40, a whole value in one
/// byte, and gdl writes one value with nothing after it.
const MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// How an edge compresses: values of at least `floor` encoded bytes are
/// sent as a zstd frame when that is smaller, and a frame is read back
/// into at most `limit` bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct Compression {
    floor: usize,
    limit: usize,
}

impl Compression {
    /// Compression above `floor`, reading frames of up to `limit` bytes.
    pub(crate) fn new(floor: u32, limit: usize) -> Self {
        Self {
            floor: usize::try_from(floor).unwrap_or(usize::MAX),
            limit,
        }
    }

    /// Whether a value of `len` encoded bytes is compressed.
    pub(crate) fn applies(&self, len: usize) -> bool {
        len >= self.floor
    }

    /// The most bytes a frame may decompress into.
    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
}

/// How a forward edge compresses a request's head: `compression`, and
/// whether its sender keeps the plain head once the frame is made. It is
/// kept only where routing reads the head again on a later choice: a
/// selector, `affinity` among them, or a class boundary's pool policy.
/// `round_robin` and `random` read nothing, so the plain head goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeadCompression {
    pub(crate) compression: Compression,
    pub(crate) keeps_plain: bool,
}

/// Whether `bytes` are a compressed value rather than a plain one.
pub(crate) fn is_frame(bytes: &[u8]) -> bool {
    bytes.get(..MAGIC.len()) == Some(&MAGIC)
}

/// The size `frame` decompresses into, after the receiver's three rules:
/// `frame` is exactly one complete frame, it states its size, and the size
/// is at most `limit`. Checked before anything is allocated. libzstd
/// decodes concatenated frames into one output and reads a size from the
/// first frame only, so without the first rule two frames could write past
/// the limit.
pub(crate) fn content_size(frame: &[u8], limit: usize) -> Result<usize> {
    imp::content_size(frame, limit)
}

/// The most bytes a frame of `len` bytes takes, which
/// [`compress_into`] reserves.
pub(crate) fn bound(len: usize) -> usize {
    imp::bound(len)
}

/// Append `src` to `out` as one frame, after what `out` holds.
pub(crate) fn compress_into(src: &[u8], out: &mut Vec<u8>) -> Result<()> {
    #[cfg(feature = "fault-injection")]
    counted::WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    imp::compress_into(src, out)
}

/// How many frames this process has written and read, for the tests that
/// show which values crossed compressed.
#[cfg(feature = "fault-injection")]
pub fn frames() -> (u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (counted::WRITTEN.load(Relaxed), counted::READ.load(Relaxed))
}

#[cfg(feature = "fault-injection")]
mod counted {
    use std::sync::atomic::AtomicU64;

    pub(super) static WRITTEN: AtomicU64 = AtomicU64::new(0);
    pub(super) static READ: AtomicU64 = AtomicU64::new(0);
}

/// Replace what `out` holds with `frame` decompressed: one complete frame
/// of at most `limit` bytes ([`content_size`]). Written into exactly that
/// many bytes, never into `out`'s spare capacity.
pub(crate) fn decompress_into(frame: &[u8], limit: usize, out: &mut Vec<u8>) -> Result<()> {
    let size = content_size(frame, limit)?;
    #[cfg(feature = "fault-injection")]
    counted::READ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    out.clear();
    out.resize(size, 0);
    imp::decompress_exact(frame, &mut out[..size])
}

fn refused(why: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Codec, format!("a zstd frame: {why}"))
}

#[cfg(feature = "zstd")]
mod imp {
    use std::cell::RefCell;

    use gdl::Result;
    use zstd_safe::{CCtx, CParameter, DCtx, InBuffer, OutBuffer, zstd_sys::ZSTD_EndDirective};

    use super::refused;

    thread_local! {
        // One of each per thread, made at first use: a context made per
        // value costs two allocations in libzstd, and a small value's frame
        // costs little else.
        static COMPRESS: RefCell<CCtx<'static>> = RefCell::new(level_one());
        static DECOMPRESS: RefCell<DCtx<'static>> = RefCell::new(DCtx::create());
    }

    fn level_one() -> CCtx<'static> {
        let mut context = CCtx::create();
        // Level 1 is in range, so libzstd takes it.
        let _ = context.set_parameter(CParameter::CompressionLevel(1));
        context
    }

    fn named(code: usize) -> gdl::Error {
        refused(zstd_safe::get_error_name(code))
    }

    pub(super) fn content_size(frame: &[u8], limit: usize) -> Result<usize> {
        match zstd_safe::find_frame_compressed_size(frame) {
            Ok(len) if len == frame.len() => {}
            Ok(_) => return Err(refused("bytes follow the frame")),
            Err(code) => return Err(named(code)),
        }
        match zstd_safe::get_frame_content_size(frame) {
            Ok(Some(size)) => match usize::try_from(size) {
                Ok(size) if size <= limit => Ok(size),
                _ => Err(refused(format!(
                    "it decompresses into {size} bytes, over the limit of {limit}"
                ))),
            },
            Ok(None) => Err(refused("it states no content size")),
            Err(_) => Err(refused("its header does not read")),
        }
    }

    pub(super) fn bound(len: usize) -> usize {
        zstd_safe::compress_bound(len)
    }

    pub(super) fn compress_into(src: &[u8], out: &mut Vec<u8>) -> Result<()> {
        out.reserve(zstd_safe::compress_bound(src.len()));
        let pos = out.len();
        COMPRESS.with(|context| {
            let mut context = context.borrow_mut();
            // The size goes into the frame's header, which the receiver
            // checks before it allocates.
            context
                .set_pledged_src_size(Some(src.len() as u64))
                .map_err(named)?;
            let mut output = OutBuffer::around_pos(out, pos);
            let mut input = InBuffer::around(src);
            let left = context
                .compress_stream2(&mut output, &mut input, ZSTD_EndDirective::ZSTD_e_end)
                .map_err(named)?;
            if left != 0 {
                return Err(refused("the frame did not fit its bound"));
            }
            Ok(())
        })
    }

    pub(super) fn decompress_exact(frame: &[u8], out: &mut [u8]) -> Result<()> {
        let size = out.len();
        let written = DECOMPRESS.with(|context| context.borrow_mut().decompress(out, frame));
        match written {
            Ok(written) if written == size => Ok(()),
            Ok(written) => Err(refused(format!(
                "it decompressed into {written} bytes, not the {size} it states"
            ))),
            Err(code) => Err(named(code)),
        }
    }
}

#[cfg(not(feature = "zstd"))]
mod imp {
    use gdl::Result;

    use super::refused;

    fn unbuilt() -> gdl::Error {
        refused("this segment was built without gdl-runtime's `zstd` feature")
    }

    pub(super) fn bound(len: usize) -> usize {
        len
    }

    pub(super) fn content_size(_: &[u8], _: usize) -> Result<usize> {
        Err(unbuilt())
    }

    pub(super) fn compress_into(_: &[u8], _: &mut Vec<u8>) -> Result<()> {
        Err(unbuilt())
    }

    pub(super) fn decompress_exact(_: &[u8], _: &mut [u8]) -> Result<()> {
        Err(unbuilt())
    }
}

/// `len` bytes of text that compress as a request's messages do: words
/// in an order a seed decides, not one byte over and over.
#[cfg(test)]
pub(crate) fn words(len: usize, seed: u64) -> String {
    const WORDS: [&str; 16] = [
        "the",
        "model",
        "returned",
        "a",
        "tool",
        "call",
        "with",
        "its",
        "arguments",
        "and",
        "user",
        "asked",
        "for",
        "more",
        "context",
        "again",
    ];
    let mut state = seed | 1;
    let mut text = String::with_capacity(len + 16);
    while text.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        text.push_str(WORDS[(state % 16) as usize]);
        text.push(if state.is_multiple_of(7) { '\n' } else { ' ' });
    }
    text.truncate(len);
    text
}

/// `len` bytes that do not compress.
#[cfg(test)]
pub(crate) fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

#[cfg(all(test, feature = "zstd"))]
mod tests {
    use super::*;

    fn frame(src: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        compress_into(src, &mut out).expect("compressed");
        out
    }

    /// A value round-trips, and a frame is told from any plain value gdl
    /// writes: a MessagePack value that starts with `0x28` is one byte.
    #[test]
    fn a_frame_round_trips_and_is_told_from_a_plain_value() {
        let src = "the same words again and again ".repeat(100).into_bytes();
        let compressed = frame(&src);
        assert!(is_frame(&compressed));
        assert!(compressed.len() < src.len());
        let mut back = Vec::new();
        decompress_into(&compressed, src.len(), &mut back).expect("decompressed");
        assert_eq!(back, src);
        let forty = rmp_serde::to_vec_named(&40u8).expect("encoded");
        assert_eq!(forty, [0x28]);
        assert!(!is_frame(&forty));
        let mut rest = &[0x28, 0xb5, 0x2f, 0xfd][..];
        let _: serde::de::IgnoredAny = rmp_serde::decode::from_read(&mut rest).expect("one value");
        assert_eq!(rest.len(), 3, "a value starting 0x28 ends after one byte");
    }

    /// A 1,024-character string is 1,027 bytes of MessagePack. As two
    /// frames, its first 1,024 bytes and its last 3, each frame obeys its
    /// own size, and the two together pass a check of the first frame's
    /// size against a limit of 1,024. The receiver refuses them, also into
    /// a reused buffer whose capacity is over the limit.
    #[test]
    fn two_frames_are_refused_whatever_the_first_one_says() {
        let value = rmp_serde::to_vec_named(&"x".repeat(1024)).expect("encoded");
        assert_eq!(value.len(), 1027);
        let mut two = frame(&value[..1024]);
        two.extend_from_slice(&frame(&value[1024..]));
        assert_eq!(
            content_size(&two[..frame(&value[..1024]).len()], 1024).ok(),
            Some(1024)
        );
        let mut scratch = Vec::with_capacity(4096);
        let refused = decompress_into(&two, 1024, &mut scratch).expect_err("two frames");
        assert_eq!(refused.kind(), ErrorKind::Codec);
        assert!(refused.to_string().contains("bytes follow"), "{refused}");
        // Controls: one frame at the limit reads; one with a byte after it,
        // or one over the limit, does not.
        decompress_into(&frame(&value[..1024]), 1024, &mut scratch).expect("at the limit");
        assert_eq!(scratch, value[..1024]);
        let mut trailing = frame(&value[..1024]);
        trailing.push(0);
        assert!(decompress_into(&trailing, 1024, &mut scratch).is_err());
        assert!(decompress_into(&frame(&value), 1024, &mut scratch).is_err());
    }

    /// A frame that states no size is refused before anything is
    /// allocated, as is one that does not read.
    #[test]
    fn a_frame_without_a_size_is_refused() {
        let mut context = zstd_safe::CCtx::create();
        let mut sizeless = Vec::with_capacity(256);
        context
            .set_parameter(zstd_safe::CParameter::ContentSizeFlag(false))
            .expect("a flag");
        context
            .compress2(&mut sizeless, b"a value")
            .expect("compressed");
        let error = content_size(&sizeless, 1 << 20).expect_err("no size");
        assert!(error.to_string().contains("no content size"), "{error}");
        assert!(content_size(&MAGIC, 1 << 20).is_err());
    }
}
