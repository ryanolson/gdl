//! A call's envelope: its request is a [`CallRequest`], a plain
//! [`CallHead`] or the [`CallFrame`] only a call codec makes of one, each
//! behind the envelope's room, which [`Call::wire`] fills, or copies after
//! a header too large for it. A retry copies the request back out as the
//! state it was sent in, never by reading its bytes.

use super::*;
use crate::wire::Codec;
use crate::zstd::{self, Compression};
use gdl_model::Encoding;

fn identity() -> Identity {
    Identity {
        contract: 7,
        placement: 9,
    }
}

fn call(cx: &Cx, payload: impl Into<CallRequest>) -> Call {
    Call {
        identity: identity(),
        service: 3,
        cx: WireCx::bounded(cx, Deadline::after(std::time::Duration::from_secs(30))),
        payload: payload.into(),
    }
}

const FLOOR: u32 = 1024;
const LIMIT: usize = 1 << 20;

fn compression() -> Compression {
    Compression::new(FLOOR, LIMIT)
}

/// Text that compresses as a conversation does, about `len` bytes.
fn words(len: usize) -> String {
    "the model called a tool with its arguments "
        .repeat(len / 43 + 1)
        .chars()
        .take(len)
        .collect()
}

/// `len` bytes that do not compress, from a seeded splitmix64.
fn noise(len: usize) -> Vec<u8> {
    let mut state = 0x5eed_c0de_u64;
    (0..len)
        .map(|_| {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            (z ^ (z >> 31)) as u8
        })
        .collect()
}

/// A codec that writes its value's bytes as they are, as no encoding gdl
/// has does: a test's way to send any bytes as a request.
fn raw() -> Codec<Vec<u8>> {
    Codec::from_fns(
        |value: &Vec<u8>, out: &mut Vec<u8>| {
            out.extend_from_slice(value);
            Ok(())
        },
        |bytes: &[u8]| Ok(bytes.to_vec()),
    )
}

/// The request crosses in the buffer its codec wrote it into: the
/// envelope's other fields are written in front of it, and it is read
/// back where it is, with every field.
#[test]
fn a_calls_request_crosses_in_the_buffer_it_was_encoded_into() {
    let codec = Codec::<String>::new(Encoding::default());
    let head = codec.encode_call(&"hello".to_string()).expect("encoded");
    let written = head.0[HEADER_ROOM..].as_ptr();
    let bytes = call(&Cx::new(), head).wire().expect("an envelope");
    let arrived = CallArrived::read(&bytes).expect("read back");
    assert_eq!(arrived.version, VERSION);
    assert_eq!(arrived.identity, identity());
    assert_eq!(arrived.service, 3);
    assert_eq!(arrived.payload.as_ptr(), written, "the request was copied");
    assert_eq!(codec.decode(arrived.payload).expect("decoded"), "hello");
}

/// A header past the room, as large meta makes, is written with a copy of
/// the request after it, and reads back the same: plain, or a frame.
#[test]
fn a_call_whose_header_passes_the_room_copies_its_request() {
    let codec = Codec::<String>::new(Encoding::default());
    let mut cx = Cx::new();
    cx.insert_meta("large", "m".repeat(HEADER_ROOM));
    let bytes = call(
        &cx,
        codec.encode_call(&"hello".to_string()).expect("encoded"),
    )
    .wire()
    .expect("an envelope");
    let arrived = CallArrived::read(&bytes).expect("read back");
    assert_eq!(
        arrived.cx.meta.get("large").map(String::len),
        Some(HEADER_ROOM)
    );
    assert_eq!(codec.decode(arrived.payload).expect("decoded"), "hello");
    let large = words(8 * FLOOR as usize);
    let head = codec.encode_call(&large).expect("encoded");
    let frame = head
        .compressed(compression())
        .expect("ok")
        .expect("a frame");
    let bytes = call(&cx, frame).wire().expect("an envelope");
    let arrived = CallArrived::read(&bytes).expect("read back");
    let plain = decoded(arrived.payload, LIMIT, &[]).expect("decoded");
    assert_eq!(codec.decode(&plain).expect("decoded"), large);
}

/// A plain request, past any floor, crosses as its message's encoding,
/// byte for byte, as the service's decode reads it: only a call codec
/// makes a frame, and only of a request it is given.
#[test]
fn a_plain_calls_request_is_never_compressed() {
    let codec = Codec::<String>::new(Encoding::default());
    let large = words(4 * gdl_model::EdgeCodec::DEFAULT_FLOOR as usize);
    let plain = codec.encode(&large).expect("encoded");
    let bytes = call(&Cx::new(), codec.encode_call(&large).expect("encoded"))
        .wire()
        .expect("an envelope");
    let arrived = CallArrived::read(&bytes).expect("read back");
    assert_eq!(arrived.payload, &plain[..]);
}

/// From its floor, a request that shrinks becomes one frame, which the
/// envelope carries in the buffer it was compressed into, and which
/// decodes, within the receiver's limit, into the plain request.
#[test]
fn a_request_at_its_floor_crosses_as_one_frame() {
    let codec = Codec::<String>::new(Encoding::default());
    let large = words(4 * FLOOR as usize);
    let head = codec.encode_call(&large).expect("encoded");
    let plain = head.plain().to_vec();
    let frame = head
        .compressed(compression())
        .expect("ok")
        .expect("a frame");
    let written = frame.0[HEADER_ROOM..].as_ptr();
    let bytes = call(&Cx::new(), frame).wire().expect("an envelope");
    let arrived = CallArrived::read(&bytes).expect("read back");
    assert_eq!(arrived.payload.as_ptr(), written, "the frame was copied");
    assert!(zstd::is_frame(arrived.payload));
    assert!(arrived.payload.len() < plain.len() / 4, "it shrank");
    assert_eq!(
        decoded(arrived.payload, LIMIT, &[]).expect("decoded"),
        plain
    );
    let error = decoded(arrived.payload, plain.len() - 1, &[]).expect_err("past the limit");
    assert_eq!(error.kind(), ErrorKind::Codec);
}

/// Below its floor a request stays plain, and so does one whose frame is
/// not shorter: the call codec gives no frame for either.
#[test]
fn a_request_below_its_floor_or_that_does_not_shrink_stays_plain() {
    let codec = Codec::<String>::new(Encoding::default());
    let small = codec
        .encode_call(&words(FLOOR as usize - 16))
        .expect("encoded");
    assert!(small.compressed(compression()).expect("ok").is_none());
    let dense = raw()
        .encode_call(&noise(4 * FLOOR as usize))
        .expect("encoded");
    assert!(dense.compressed(compression()).expect("ok").is_none());
    let at = codec.encode_call(&words(FLOOR as usize)).expect("encoded");
    assert!(
        at.compressed(compression()).expect("ok").is_some(),
        "at the floor"
    );
}

/// A retry copies its request out of the envelope it was sent in, as the
/// state the caller holds says: a frame as a frame, after it checks that
/// the bytes are one complete frame, and a plain request as it is,
/// whatever its bytes, even ones that begin as a frame does.
#[test]
fn a_retry_copies_each_request_as_the_state_it_was_sent_in() {
    let codec = Codec::<String>::new(Encoding::default());
    let head = codec
        .encode_call(&words(4 * FLOOR as usize))
        .expect("encoded");
    let plain = head.plain().to_vec();
    let frame = head
        .compressed(compression())
        .expect("ok")
        .expect("a frame");
    let sent = frame.0[HEADER_ROOM..].to_vec();
    let bytes = call(&Cx::new(), frame).wire().expect("an envelope");
    let again = CallFrame::sent_in(&bytes).expect("a frame again");
    assert_eq!(&again.0[HEADER_ROOM..], &sent[..]);
    let resent = call(&Cx::new(), again).wire().expect("an envelope");
    let arrived = CallArrived::read(&resent).expect("read back");
    assert_eq!(
        decoded(arrived.payload, LIMIT, &[]).expect("decoded"),
        plain
    );
    // A plain envelope holds no frame.
    let plain_bytes = call(
        &Cx::new(),
        codec.encode_call(&"hi".to_string()).expect("encoded"),
    )
    .wire()
    .expect("an envelope");
    assert!(CallFrame::sent_in(&plain_bytes).is_err());
    // A plain request that begins with the zstd magic, as a hand-made codec
    // with no call codec may write, is copied as it is, and arrives intact.
    let mut magic = vec![0x28, 0xb5, 0x2f, 0xfd];
    magic.extend_from_slice(&noise(64));
    let bytes = call(&Cx::new(), raw().encode_call(&magic).expect("encoded"))
        .wire()
        .expect("an envelope");
    let again = CallHead::sent_in(&bytes).expect("copied");
    assert_eq!(again.plain(), &magic[..]);
    let resent = call(&Cx::new(), again).wire().expect("an envelope");
    assert_eq!(
        CallArrived::read(&resent).expect("read back").payload,
        &magic[..]
    );
}
