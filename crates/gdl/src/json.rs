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
/// Returns when `input` ends.
pub async fn serve_json_lines<Req, Item, R, W, F, Fut>(
    input: R,
    mut output: W,
    mut call: F,
) -> std::io::Result<()>
where
    Req: DeserializeOwned,
    Item: Serialize,
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnMut(Req) -> Fut,
    Fut: Future<Output = Result<Items<Item>>>,
{
    let mut input = input;
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        if input.read_until(b'\n', &mut bytes).await? == 0 {
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
            Ok(mut line) => {
                line.push('\n');
                output.write_all(line.as_bytes()).await?;
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
    async fn integers_beyond_64_bits_are_served() {
        let call =
            |_: u32| async { Ok(items(futures::stream::iter([Ok((u128::MAX, i128::MIN))]))) };
        let mut output = Vec::new();
        serve_json_lines(&b"1\n"[..], &mut output, call)
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
        serve_json_lines(input, &mut output, call)
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
