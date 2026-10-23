//! The client of a cell's watch: the hub's server-sent events, parsed into
//! [`CellWatchEvent`]s.

use std::time::Duration;

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};

use super::{CellWatchEvent, EXTENSION, ROUTE};

/// How long a connection to a cell's hub may take to open.
const CONNECT: Duration = Duration::from_secs(5);

/// How long a watch may go without a byte before it ends: three of the
/// hub's keep-alives, so that a stalled connection is told from a quiet
/// cell.
const IDLE: Duration = Duration::from_secs(30);

/// The most of a refusal's body kept for its error.
const REFUSAL: usize = 1024;

/// Watches cells' hubs, over one pool of connections.
#[derive(Debug, Clone)]
pub struct CellWatcher {
    http: reqwest::Client,
}

impl CellWatcher {
    /// A watcher with a connect timeout and an idle timeout longer than
    /// the hub's keep-alive.
    pub fn new() -> Result<Self, CellWatchError> {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT)
            .read_timeout(IDLE)
            .build()
            .map_err(|e| CellWatchError(format!("the cell watch's HTTP client: {e}")))?;
        Ok(Self { http })
    }

    /// Watch the cell whose hub is at `hub`, `host:port` as the cell's
    /// record names it. The events end when the hub ends the watch (it
    /// stopped, or this watcher fell behind), when the connection fails or
    /// goes idle, or at an event that does not parse; a follower then
    /// watches again from a snapshot. A hub that has not yet published its
    /// cell's record holds the watch until it has, since the epoch it would
    /// send is not settled before, and refuses it after 5 s.
    pub async fn watch(
        &self,
        hub: &str,
    ) -> Result<BoxStream<'static, CellWatchEvent>, CellWatchError> {
        gdl_model::check_hub_address(hub).map_err(CellWatchError)?;
        let url = format!("http://{hub}/v1/ext/{EXTENSION}{ROUTE}");
        let mut response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| CellWatchError(format!("GET {url}: {e}")))?;
        let status = response.status();
        if !status.is_success() {
            let mut body = Vec::new();
            while body.len() < REFUSAL
                && let Ok(Some(chunk)) = response.chunk().await
            {
                body.extend_from_slice(&chunk);
            }
            body.truncate(REFUSAL);
            return Err(CellWatchError(format!(
                "GET {url}: {status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }
        Ok(events(response.bytes_stream()).boxed())
    }
}

/// Why a cell's watch could not begin.
#[derive(Clone, PartialEq, Eq)]
pub struct CellWatchError(String);

impl CellWatchError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for CellWatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Debug for CellWatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CellWatchError {}

/// The events in a watch's bytes, ending with the bytes, at the first
/// error, or at the first event that does not parse.
fn events<S, B, E>(bytes: S) -> impl Stream<Item = CellWatchEvent> + Send + 'static
where
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: AsRef<[u8]>,
{
    let state = (Box::pin(bytes), Vec::new());
    futures::stream::unfold(state, |(mut bytes, mut buffer)| async move {
        loop {
            if let Some(end) = event_end(&buffer) {
                let raw: Vec<u8> = buffer.drain(..end).collect();
                match parse(&raw) {
                    Ok(Some(event)) => return Some((event, (bytes, buffer))),
                    Ok(None) => continue,
                    Err(()) => return None,
                }
            }
            match bytes.next().await {
                Some(Ok(chunk)) => buffer.extend_from_slice(chunk.as_ref()),
                Some(Err(_)) | None => return None,
            }
        }
    })
}

/// The end of the first whole event in `buffer`, past its blank line.
fn event_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(2)
        .position(|pair| pair == b"\n\n")
        .map(|at| at + 2)
}

/// One server-sent event: `None` for one without data (a keep-alive
/// comment), `Err` for one whose data is not an event.
fn parse(raw: &[u8]) -> Result<Option<CellWatchEvent>, ()> {
    let text = std::str::from_utf8(raw).map_err(|_| ())?;
    let mut data: Option<String> = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("data:") {
            let value = value.strip_prefix(' ').unwrap_or(value);
            match &mut data {
                Some(data) => {
                    data.push('\n');
                    data.push_str(value);
                }
                None => data = Some(value.to_string()),
            }
        }
    }
    data.map(|data| serde_json::from_str(&data).map_err(|_| ()))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::{CellWatchEvent, events};
    use futures::StreamExt;
    use gdl_cells::CellState;
    use velo_hub::velo::InstanceId;

    fn parsed(chunks: &[&str]) -> Vec<CellWatchEvent> {
        let chunks: Vec<Result<Vec<u8>, ()>> = chunks
            .iter()
            .map(|chunk| Ok(chunk.as_bytes().to_vec()))
            .collect();
        futures::executor::block_on(events(futures::stream::iter(chunks)).collect())
    }

    fn left(instance: InstanceId) -> CellWatchEvent {
        CellWatchEvent::Left {
            segment: "f".into(),
            instance,
        }
    }

    fn sse(event: &CellWatchEvent) -> String {
        format!(
            "data: {}\n\n",
            serde_json::to_string(event).expect("an event serializes")
        )
    }

    /// The wire form is JSON tagged by `event`, so that a watcher in
    /// another build reads it without the hub's types.
    #[test]
    fn an_event_is_json_tagged_by_its_kind() {
        let instance = InstanceId::new_v4();
        assert_eq!(
            serde_json::to_value(left(instance)).expect("an event serializes"),
            serde_json::json!({ "event": "left", "segment": "f", "instance": instance }),
        );
        assert_eq!(
            serde_json::to_value(CellWatchEvent::State {
                state: CellState::Degraded
            })
            .expect("an event serializes"),
            serde_json::json!({ "event": "state", "state": "degraded" }),
        );
        assert_eq!(
            serde_json::to_value(CellWatchEvent::Synced).expect("an event serializes"),
            serde_json::json!({ "event": "synced" }),
        );
    }

    /// An event split across chunks is read whole, and a keep-alive
    /// comment is no event.
    #[test]
    fn events_split_across_chunks_and_keep_alives_parse() {
        let (a, b) = (InstanceId::new_v4(), InstanceId::new_v4());
        let first = sse(&left(a));
        let second = sse(&left(b));
        let (head, tail) = first.split_at(11);
        let got = parsed(&[head, tail, ":\n\n", &second[..4], &second[4..]]);
        assert_eq!(got, [left(a), left(b)]);
    }

    /// Data split over several `data:` lines is one value, joined by
    /// newlines, as server-sent events define it.
    #[test]
    fn data_over_several_lines_is_one_event() {
        let got = parsed(&["data: {\"event\":\ndata: \"synced\"}\n\n"]);
        assert_eq!(got, [CellWatchEvent::Synced]);
    }

    /// An event that does not parse ends the stream: what follows it
    /// cannot be trusted to follow on.
    #[test]
    fn an_event_that_does_not_parse_ends_the_stream() {
        let a = InstanceId::new_v4();
        let got = parsed(&[
            &sse(&left(a)),
            "data: {\"event\":\"later\"}\n\n",
            &sse(&left(a)),
        ]);
        assert_eq!(got, [left(a)]);
    }

    /// A connection that fails ends the stream, and an event cut short by
    /// it is not delivered.
    #[test]
    fn a_failed_connection_ends_the_stream() {
        let a = InstanceId::new_v4();
        let whole = sse(&left(a));
        let chunks: Vec<Result<Vec<u8>, ()>> = vec![
            Ok(whole.clone().into_bytes()),
            Ok(whole.as_bytes()[..10].to_vec()),
            Err(()),
            Ok(whole.into_bytes()),
        ];
        let got: Vec<CellWatchEvent> =
            futures::executor::block_on(events(futures::stream::iter(chunks)).collect());
        assert_eq!(got, [left(a)]);
    }

    /// A hub address that is not `host:port` is refused before anything
    /// is dialled.
    #[tokio::test]
    async fn an_address_that_is_not_host_and_port_is_refused() {
        let watcher = super::CellWatcher::new().expect("a client");
        for address in ["http://hub:1337", "hub", "hub:0", "hub:1337/v1"] {
            let Err(error) = watcher.watch(address).await else {
                panic!("watched {address}");
            };
            assert!(error.to_string().contains("host:port"), "{error}");
        }
    }
}
