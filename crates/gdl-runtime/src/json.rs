//! A graph's source served as JSON lines: the front end of a generated
//! segment binary, kept here so that it is tested once rather than
//! generated into every crate.

use std::future::Future;

use futures::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use gdl::Items;
use gdl::Result;

/// The source's request, as a segment's binary reads it from JSON lines:
/// `DeserializeOwned`, reported by name and once where it is not, rather
/// than as each serde trait it lacks with a list of the types that have
/// them. The source reader took it for a wire type, or the generator
/// would not have written the binary that reads it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is the source's request, which this segment's binary reads as JSON lines, but is not a wire type",
    label = "the binary reads `{Self}` here",
    note = "the source reader took it for a wire type; check its serde impls with `cargo gdl extract --verify`"
)]
pub trait JsonRequest: DeserializeOwned {}

#[diagnostic::do_not_recommend]
impl<T: DeserializeOwned> JsonRequest for T {}

/// The source's response item, as a segment's binary writes it as JSON
/// lines: `Serialize`, reported as [`JsonRequest`] is.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is the source's response item, which this segment's binary writes as JSON lines, but is not a wire type",
    label = "the binary writes `{Self}` here",
    note = "the source reader took it for a wire type; check its serde impls with `cargo gdl extract --verify`"
)]
pub trait JsonItem: Serialize {}

#[diagnostic::do_not_recommend]
impl<T: Serialize> JsonItem for T {}

/// How [`serve_json_lines`] reads a request and writes an item. Made by
/// [`Json::new`], whose bounds are the only place the feeder needs serde:
/// a source's types that are not wire types are reported there, once,
/// rather than at the call and again where its future is awaited. A
/// generated segment makes it with [`json_codec!`](crate::json_codec),
/// which needs no bound at all: the segment asserts its types on its own,
/// each once, with the crossings it asserts.
pub struct Json<Req, Item> {
    decode: fn(&str) -> serde_json::Result<Req>,
    encode: fn(&Item) -> serde_json::Result<String>,
}

impl<Req, Item> Json<Req, Item> {
    /// The JSON form of a source's request and response items.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self
    where
        Req: JsonRequest,
        Item: JsonItem,
    {
        Self {
            decode: |text| serde_json::from_str(text),
            // Serialized straight to text: going through
            // `serde_json::Value` would refuse integers beyond 64 bits,
            // which JSON text holds.
            encode: |item| serde_json::to_string(&ItemLine { item }),
        }
    }

    /// Of these functions: what [`json_codec!`](crate::json_codec) makes.
    #[doc(hidden)]
    pub fn from_fns(
        decode: fn(&str) -> serde_json::Result<Req>,
        encode: fn(&Item) -> serde_json::Result<String>,
    ) -> Self {
        Self { decode, encode }
    }
}

/// Compiles only for a [`JsonRequest`]: what a generated segment asserts of
/// the source's request it reads, where no crossing check asserts more.
pub const fn reads_json<T: JsonRequest>() {}

/// Compiles only for a [`JsonItem`]: what a generated segment asserts of
/// the source's response item it writes, where no crossing check asserts
/// more.
pub const fn writes_json<T: JsonItem>() {}

/// The JSON codec of a source's request and item, with no bound on either:
/// where the type is concrete, an autoref probe takes serde's functions
/// where it has them, and else ones that fail. A generated segment asserts
/// the types apart, so that a type that is not a wire type is reported
/// once, by the assertion that names it, and never by this.
#[macro_export]
macro_rules! json_codec {
    ($req:ty, $item:ty) => {{
        #[allow(unused_imports)]
        use $crate::__json::{DecodeNo as _, DecodeYes as _, EncodeNo as _, EncodeYes as _};
        $crate::Json::<$req, $item>::from_fns(
            (&$crate::__json::Probe::<$req>::new()).__gdl_decode(),
            (&$crate::__json::Probe::<$item>::new()).__gdl_encode(),
        )
    }};
}

/// The probes [`json_codec!`](crate::json_codec) calls: the bounded impl on
/// `Probe<T>`, the fallback on `&Probe<T>`, so that method lookup on
/// `&Probe<T>` finds the bounded one where `T` has serde's traits.
#[doc(hidden)]
pub mod probe {
    use std::marker::PhantomData;

    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use super::ItemLine;

    pub struct Probe<T>(PhantomData<fn() -> T>);

    impl<T> Probe<T> {
        #[allow(clippy::new_without_default)]
        pub const fn new() -> Self {
            Self(PhantomData)
        }
    }

    pub trait DecodeYes<T> {
        fn __gdl_decode(&self) -> fn(&str) -> serde_json::Result<T>;
    }

    impl<T: DeserializeOwned> DecodeYes<T> for Probe<T> {
        fn __gdl_decode(&self) -> fn(&str) -> serde_json::Result<T> {
            |text| serde_json::from_str(text)
        }
    }

    pub trait DecodeNo<T> {
        fn __gdl_decode(&self) -> fn(&str) -> serde_json::Result<T>;
    }

    impl<T> DecodeNo<T> for &Probe<T> {
        fn __gdl_decode(&self) -> fn(&str) -> serde_json::Result<T> {
            |_| Err(serde::de::Error::custom("the request is not a wire type"))
        }
    }

    pub trait EncodeYes<T> {
        fn __gdl_encode(&self) -> fn(&T) -> serde_json::Result<String>;
    }

    impl<T: Serialize> EncodeYes<T> for Probe<T> {
        fn __gdl_encode(&self) -> fn(&T) -> serde_json::Result<String> {
            |item| serde_json::to_string(&ItemLine { item })
        }
    }

    pub trait EncodeNo<T> {
        fn __gdl_encode(&self) -> fn(&T) -> serde_json::Result<String>;
    }

    impl<T> EncodeNo<T> for &Probe<T> {
        fn __gdl_encode(&self) -> fn(&T) -> serde_json::Result<String> {
            |_| Err(serde::ser::Error::custom("the item is not a wire type"))
        }
    }
}

/// Answer one JSON request per line of `input`, one request at a time.
///
/// Each item is written as `{"item": ...}` on its own line. The answer ends
/// with `{"done": null}`, or with `{"error": "..."}` when the request is
/// malformed, the call fails, the stream ends with an error, or an item has
/// no JSON form (a map with non-string keys, for example). In that last
/// case the stream is dropped, which cancels it. Blank lines are skipped.
///
/// Returns when `input` ends, or once `stop` has resolved. A stop ends
/// reading, never an answer: one that comes while an answer is being
/// written lets it finish, and no further line is read. A process that must
/// end an answer at once stops another way (a second signal ends a segment
/// binary).
pub async fn serve_json_lines<Req, Item, R, W, S, F, Fut>(
    input: R,
    mut output: W,
    stop: S,
    json: Json<Req, Item>,
    mut call: F,
) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    S: Future<Output = ()>,
    F: FnMut(Req) -> Fut,
    Fut: Future<Output = Result<Items<Item>>>,
{
    let mut input = input;
    let mut bytes = Vec::new();
    let stop = futures::FutureExt::fuse(stop);
    tokio::pin!(stop);
    loop {
        bytes.clear();
        // A line cut short by the stop is dropped with the rest of the
        // input: it was never a request.
        let read = tokio::select! {
            biased;
            () = &mut stop => return Ok(()),
            read = input.read_until(b'\n', &mut bytes) => read?,
        };
        if read == 0 {
            return Ok(());
        }
        // A line that is not text is a bad request, not the end of serving:
        // only a failure to read ends it.
        let line = String::from_utf8_lossy(&bytes);
        if line.trim().is_empty() {
            continue;
        }
        let parsed = std::str::from_utf8(&bytes)
            .map_err(|e| format!("bad request: {e}"))
            .and_then(|text| (json.decode)(text).map_err(|e| format!("bad request: {e}")));
        let end = match parsed {
            Err(error) => failure(error),
            Ok(request) => match call(request).await {
                Err(error) => failure(error.to_string()),
                Ok(items) => answer(items, json.encode, &mut output).await?,
            },
        };
        write_line(&mut output, &end).await?;
        output.flush().await?;
    }
}

/// Write each item, each as `encode` writes it; return the line that ends
/// the answer.
async fn answer<Item, W: AsyncWrite + Unpin>(
    mut items: Items<Item>,
    encode: fn(&Item) -> serde_json::Result<String>,
    output: &mut W,
) -> std::io::Result<serde_json::Value> {
    while let Some(item) = items.next().await {
        let item = match item {
            Ok(item) => item,
            Err(error) => return Ok(failure(error.to_string())),
        };
        match encode(&item) {
            // Flushed per item: a stream that stays open after an item must
            // not keep that item in a buffered writer.
            Ok(mut line) => {
                line.push('\n');
                output.write_all(line.as_bytes()).await?;
                output.flush().await?;
            }
            Err(error) => return Ok(failure(format!("an item has no JSON form: {error}"))),
        }
    }
    Ok(serde_json::json!({ "done": null }))
}

#[derive(Serialize)]
pub(crate) struct ItemLine<'a, T> {
    item: &'a T,
}

fn failure(message: String) -> serde_json::Value {
    serde_json::json!({ "error": message })
}

async fn write_line<W: AsyncWrite + Unpin>(
    output: &mut W,
    value: &serde_json::Value,
) -> std::io::Result<()> {
    let mut line = value.to_string();
    line.push('\n');
    output.write_all(line.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use gdl::Error;
    use gdl::items;

    /// Answers `n` with `n` items; `0` fails the call; `99` answers with an
    /// item JSON cannot hold.
    async fn call(n: u32) -> Result<Items<BTreeMap<(u32, u32), u32>>> {
        match n {
            0 => Err(Error::node("no items")),
            99 => Ok(items(futures::stream::iter([
                Ok(BTreeMap::from([((1, 2), 3)])),
                Ok(BTreeMap::new()),
            ]))),
            n => Ok(items(futures::stream::iter(
                (0..n).map(|_| Ok(BTreeMap::new())),
            ))),
        }
    }

    /// The codec a generated segment makes takes serde's functions for a
    /// wire type, and for any other type builds all the same, with
    /// functions that fail: the segment's own assertion reports that type.
    #[test]
    fn the_codec_of_a_type_that_is_not_a_wire_type_builds_and_fails() {
        struct Opaque;
        let wire = crate::json_codec!(u32, u32);
        assert_eq!((wire.decode)("7").expect("decodes"), 7);
        assert_eq!((wire.encode)(&7).expect("encodes"), r#"{"item":7}"#);
        let opaque = crate::json_codec!(Opaque, Opaque);
        let error = (opaque.decode)("7").err().expect("refused");
        assert!(error.to_string().contains("the request is not a wire type"));
        let error = (opaque.encode)(&Opaque).expect_err("refused");
        assert!(error.to_string().contains("the item is not a wire type"));
    }

    #[tokio::test]
    async fn each_item_reaches_the_reader_as_it_is_produced() {
        // The second item waits until the reader has the first: through a
        // buffered writer, an item written but not flushed would never come.
        let (reader, writer) = tokio::io::duplex(64 * 1024);
        let (seen, first_read) = tokio::sync::oneshot::channel::<()>();
        let mut first_read = Some(first_read);
        let call = move |_: u32| {
            let first_read = first_read.take();
            async move {
                let rest = futures::stream::once(async move {
                    if let Some(first_read) = first_read {
                        let _ = first_read.await;
                    }
                    Ok(2u32)
                });
                Ok(items(futures::stream::iter([Ok(1u32)]).chain(rest)))
            }
        };
        let serving = tokio::spawn(async move {
            let output = tokio::io::BufWriter::new(writer);
            serve_json_lines(
                &b"1\n"[..],
                output,
                std::future::pending(),
                Json::new(),
                call,
            )
            .await
        });
        let mut lines = tokio::io::BufReader::new(reader).lines();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), lines.next_line())
            .await
            .expect("the first item arrives while the stream is still open")
            .expect("read");
        assert_eq!(first.as_deref(), Some(r#"{"item":1}"#));
        let _ = seen.send(());
        assert_eq!(
            lines.next_line().await.expect("read").as_deref(),
            Some(r#"{"item":2}"#)
        );
        assert_eq!(
            lines.next_line().await.expect("read").as_deref(),
            Some(r#"{"done":null}"#)
        );
        serving.await.expect("join").expect("serve");
    }

    /// A stop that comes while an answer is being written ends reading, not
    /// the answer: the answer is written to its end, and the request sent
    /// after the stop is never read.
    #[tokio::test]
    async fn a_stop_ends_reading_not_the_answer_being_written() {
        let (mut input, reader) = tokio::io::duplex(1024);
        let (writer, output) = tokio::io::duplex(64 * 1024);
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let mut released = Some(released);
        let call = move |_: u32| {
            let released = released.take();
            async move {
                let rest = futures::stream::once(async move {
                    if let Some(released) = released {
                        let _ = released.await;
                    }
                    Ok(2u32)
                });
                Ok(items(futures::stream::iter([Ok(1u32)]).chain(rest)))
            }
        };
        let serving = tokio::spawn(async move {
            let stop = async move {
                let _ = stopped.await;
            };
            serve_json_lines(
                tokio::io::BufReader::new(reader),
                writer,
                stop,
                Json::new(),
                call,
            )
            .await
        });
        input.write_all(b"1\n").await.expect("write");
        let mut lines = tokio::io::BufReader::new(output).lines();
        let wait = std::time::Duration::from_secs(5);
        let first = tokio::time::timeout(wait, lines.next_line()).await;
        assert_eq!(
            first.expect("in time").expect("read").as_deref(),
            Some(r#"{"item":1}"#)
        );
        // The stop comes mid-answer, and another request after it.
        let _ = stop.send(());
        input.write_all(b"1\n").await.expect("write");
        let _ = release.send(());
        let mut rest = Vec::new();
        while let Some(line) = tokio::time::timeout(wait, lines.next_line())
            .await
            .expect("in time")
            .expect("read")
        {
            rest.push(line);
        }
        assert_eq!(rest, [r#"{"item":2}"#, r#"{"done":null}"#]);
        tokio::time::timeout(wait, serving)
            .await
            .expect("serving ended after the answer")
            .expect("join")
            .expect("serve");
    }

    /// A stop while no request is being answered ends serving at once,
    /// though the input stays open.
    #[tokio::test]
    async fn a_stop_while_waiting_for_a_request_ends_serving() {
        let (_input, reader) = tokio::io::duplex(1024);
        let call = |_: u32| async { Ok(items(futures::stream::iter([Ok(1u32)]))) };
        let served = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            serve_json_lines(
                tokio::io::BufReader::new(reader),
                Vec::new(),
                std::future::ready(()),
                Json::new(),
                call,
            ),
        )
        .await;
        served.expect("ended in time").expect("serve");
    }

    #[tokio::test]
    async fn integers_beyond_64_bits_are_served() {
        let call =
            |_: u32| async { Ok(items(futures::stream::iter([Ok((u128::MAX, i128::MIN))]))) };
        let mut output = Vec::new();
        serve_json_lines(
            &b"1\n"[..],
            &mut output,
            std::future::pending(),
            Json::new(),
            call,
        )
        .await
        .expect("serve");
        assert_eq!(
            std::str::from_utf8(&output).expect("utf8"),
            format!(
                "{{\"item\":[{},{}]}}\n{{\"done\":null}}\n",
                u128::MAX,
                i128::MIN
            )
        );
    }

    #[tokio::test]
    async fn every_request_gets_one_answer_and_serving_goes_on() {
        let input: &[u8] = b"2\n\n0\nnot json\n99\n\xff\n1\n";
        let mut output = Vec::new();
        serve_json_lines(
            input,
            &mut output,
            std::future::pending(),
            Json::new(),
            call,
        )
        .await
        .expect("serve");
        let lines: Vec<&str> = std::str::from_utf8(&output)
            .expect("utf8")
            .lines()
            .collect();
        assert_eq!(lines.len(), 9, "{lines:#?}");
        assert_eq!(
            &lines[..3],
            [r#"{"item":{}}"#, r#"{"item":{}}"#, r#"{"done":null}"#]
        );
        assert_eq!(lines[3], r#"{"error":"node error: no items"}"#);
        assert!(
            lines[4].starts_with(r#"{"error":"bad request: "#),
            "{}",
            lines[4]
        );
        assert!(
            lines[5].starts_with(r#"{"error":"an item has no JSON form: "#),
            "{}",
            lines[5]
        );
        assert!(
            lines[6].starts_with(r#"{"error":"bad request: "#),
            "{}",
            lines[6]
        );
        assert_eq!(&lines[7..], [r#"{"item":{}}"#, r#"{"done":null}"#]);
    }
}
