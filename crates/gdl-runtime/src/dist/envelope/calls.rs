//! A call's envelope: its request is a [`CallHead`], encoded plain behind
//! the envelope's room, which [`Call::wire`] fills, or copies after a
//! header too large for it. No compressed or sent head can reach it.

use super::*;
use crate::wire::Codec;
use gdl_model::Encoding;

fn identity() -> Identity {
    Identity {
        contract: 7,
        placement: 9,
    }
}

fn call(cx: &Cx, payload: CallHead) -> Call {
    Call {
        identity: identity(),
        service: 3,
        cx: WireCx::bounded(cx, Deadline::after(std::time::Duration::from_secs(30))),
        payload,
    }
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
/// the request after it, and reads back the same.
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
}

/// A request past a `[codec]` entry's floor still crosses plain: its
/// bytes are its message's encoding, as the service's decode reads them.
#[test]
fn a_calls_request_is_never_compressed() {
    let codec = Codec::<String>::new(Encoding::default());
    let large = "x".repeat(4 * gdl_model::EdgeCodec::DEFAULT_FLOOR as usize);
    let plain = codec.encode(&large).expect("encoded");
    let bytes = call(&Cx::new(), codec.encode_call(&large).expect("encoded"))
        .wire()
        .expect("an envelope");
    let arrived = CallArrived::read(&bytes).expect("read back");
    assert_eq!(arrived.payload, &plain[..]);
}
