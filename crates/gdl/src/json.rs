//! A graph's source served as JSON lines: the front end of a generated
//! segment binary, kept here so that it is tested once rather than
//! generated into every crate.

use std::future::Future;

use futures::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::Result;
use crate::items::Items;

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
    mut call: F,
) -> std::io::Result<()>
where
    Req: DeserializeOwned,
    Item: Serialize,
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
            .and_then(|text| {
                serde_json::from_str::<Req>(text).map_err(|e| format!("bad request: {e}"))
            });
        let end = match parsed {
            Err(error) => failure(error),
            Ok(request) => match call(request).await {
                Err(error) => failure(error.to_string()),
                Ok(items) => answer(items, &mut output).await?,
            },
        };
        write_line(&mut output, &end).await?;
        output.flush().await?;
    }
}

/// Write each item; return the line that ends the answer.
async fn answer<Item: Serialize, W: AsyncWrite + Unpin>(
    mut items: Items<Item>,
    output: &mut W,
) -> std::io::Result<serde_json::Value> {
    while let Some(item) = items.next().await {
        let item = match item {
            Ok(item) => item,
            Err(error) => return Ok(failure(error.to_string())),
        };
        // Serialized straight to text: going through `serde_json::Value`
        // would refuse integers beyond 64 bits, which JSON text holds.
        match serde_json::to_string(&ItemLine { item: &item }) {
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
struct ItemLine<'a, T> {
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
    use crate::error::Error;
    use crate::items::items;

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
            serve_json_lines(&b"1\n"[..], output, std::future::pending(), call).await
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
            serve_json_lines(tokio::io::BufReader::new(reader), writer, stop, call).await
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
        serve_json_lines(&b"1\n"[..], &mut output, std::future::pending(), call)
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
        serve_json_lines(input, &mut output, std::future::pending(), call)
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
