//! The client of a cell hub's watches: its server-sent events, parsed into
//! [`CellWatchEvent`]s or [`FollowEvent`]s, or read as events of a kind
//! this build does not know.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};

use super::watched::Classify;
use super::{
    CellWatchEvent, EXTENSION, FOLLOWS_ROUTE, FollowEvent, Follower, RELEASE_ROUTE, UnknownEvent,
    UnknownFollowEvent, WATCH_ROUTE, Watched,
};

/// How long a connection to a cell's hub may take to open.
const CONNECT: Duration = Duration::from_secs(5);

/// How long a watch may go without a byte before it ends: three of the
/// hub's keep-alives, so that a stalled connection is told from a quiet
/// cell.
const IDLE: Duration = Duration::from_secs(30);

/// The most of a refusal's body kept for its error.
const REFUSAL: usize = 1024;

/// Watches cells' hubs, over one pool of connections: their cells, and
/// what they follow. Its clones share the pool, and the count of the
/// events of a kind this build does not know that they have read.
#[derive(Debug, Clone)]
pub struct CellWatcher {
    http: reqwest::Client,
    unknown: Arc<AtomicU64>,
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
        Ok(Self {
            http,
            unknown: Arc::default(),
        })
    }

    /// How many events of a kind this build does not know this watcher
    /// and its clones have read, in every watch and snapshot.
    pub fn unknown_events(&self) -> u64 {
        self.unknown.load(Ordering::Relaxed)
    }

    /// Watch the cell whose hub is at `hub`, `host:port` as the cell's
    /// record names it. The events end when the hub ends the watch (it
    /// stopped, this watcher fell behind, or the cell's drain ended), when
    /// the connection fails or goes idle, or at an event that is not JSON
    /// or is of a known kind whose fields do not parse; a follower then
    /// watches again from a snapshot. An event of a kind this build does
    /// not know is read as [`Watched::Unknown`], and the watch goes on: it
    /// concerns the watched cell, which the reader fails closed for until
    /// it has read the cell again. A hub that has not yet published its
    /// cell's record holds the watch until it has, since the epoch it
    /// would send is not settled before, and refuses it after 5 s.
    pub async fn watch(
        &self,
        hub: &str,
    ) -> Result<BoxStream<'static, Watched<CellWatchEvent, UnknownEvent>>, CellWatchError> {
        self.open(hub, WATCH_ROUTE, None).await
    }

    /// [`CellWatcher::watch`], as `follower`: the cell's hub counts the
    /// watch among the cell's followers until it is released
    /// ([`CellWatcher::release`]).
    pub async fn follow(
        &self,
        hub: &str,
        follower: &Follower,
    ) -> Result<BoxStream<'static, Watched<CellWatchEvent, UnknownEvent>>, CellWatchError> {
        self.open(hub, WATCH_ROUTE, Some(follower)).await
    }

    /// Watch what the hub at `hub`, `host:port`, follows: its relay, each
    /// [`FollowEvent`] as it is sent. The events end as a cell's watch's
    /// do, and also at an event of a kind this build does not know that
    /// names no follow; one that names its follow is read as
    /// [`Watched::Unknown`], for the reader to fail closed for that cell
    /// alone until it has read it again ([`CellWatcher::follows_of`]). A
    /// consumer whose watch ended watches again from a snapshot.
    pub async fn follows(
        &self,
        hub: &str,
    ) -> Result<BoxStream<'static, Watched<FollowEvent, UnknownFollowEvent>>, CellWatchError> {
        self.open(hub, FOLLOWS_ROUTE, None).await
    }
    /// Let go, as `follower`, of the cell whose hub is at `hub`: its watch
    /// no longer counts among the cell's followers.
    pub async fn release(&self, hub: &str, follower: &Follower) -> Result<(), CellWatchError> {
        gdl_model::check_hub_address(hub).map_err(CellWatchError)?;
        let url = format!("http://{hub}/v1/ext/{EXTENSION}{RELEASE_ROUTE}");
        let response = self
            .http
            .post(&url)
            .json(follower)
            .send()
            .await
            .map_err(|e| CellWatchError(format!("POST {url}: {e}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(CellWatchError(format!("POST {url}: {status}")));
        }
        Ok(())
    }

    /// Watch one follow of the relay of the hub at `hub`, `host:port`: the
    /// follow of the cell `cell` of `class` as a watch of the relay begins
    /// with it, then `synced`, then each event of the relay that names the
    /// cell, as [`CellWatcher::follows`] reads them. A reader that met an
    /// event of a kind it does not know for the cell reads the cell from
    /// here: this watch's snapshot and the events after it are ordered as
    /// the relay's are, which a reading beside another watch is not.
    pub async fn follows_of(
        &self,
        hub: &str,
        class: &str,
        cell: &str,
    ) -> Result<BoxStream<'static, Watched<FollowEvent, UnknownFollowEvent>>, CellWatchError> {
        gdl_model::check_class_name(class).map_err(CellWatchError)?;
        gdl_model::check_cell_name(cell).map_err(CellWatchError)?;
        self.open(hub, &super::followed_path(class, cell), None)
            .await
    }

    async fn open<T: Classify>(
        &self,
        hub: &str,
        route: &str,
        follower: Option<&Follower>,
    ) -> Result<BoxStream<'static, Watched<T, T::Unknown>>, CellWatchError> {
        gdl_model::check_hub_address(hub).map_err(CellWatchError)?;
        let url = format!("http://{hub}/v1/ext/{EXTENSION}{route}");
        let mut request = self.http.get(&url);
        if let Some(follower) = follower {
            request = request.query(follower);
        }
        let mut response = request
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
        Ok(events(response.bytes_stream(), Arc::clone(&self.unknown)).boxed())
    }
}

/// Why a cell's watch could not begin.
#[derive(Clone, PartialEq, Eq)]
pub struct CellWatchError(String);

impl CellWatchError {
    /// The error `message`: also the hub's, for a watch it refuses in
    /// process.
    pub fn new(message: impl Into<String>) -> Self {
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
/// error, or at the first event that is neither an event this build knows
/// nor one of a kind it does not ([`Classify::unknown`]). Each of the
/// latter is counted in `unknown`.
fn events<T, S, B, E>(
    bytes: S,
    unknown: Arc<AtomicU64>,
) -> impl Stream<Item = Watched<T, T::Unknown>> + Send + 'static
where
    T: Classify,
    S: Stream<Item = Result<B, E>> + Send + 'static,
    B: AsRef<[u8]>,
{
    let state = (Box::pin(bytes), Vec::new());
    futures::stream::unfold(state, move |(mut bytes, mut buffer)| {
        let unknown = Arc::clone(&unknown);
        async move {
            loop {
                if let Some(end) = event_end(&buffer) {
                    let raw: Vec<u8> = buffer.drain(..end).collect();
                    match parse::<T>(&raw) {
                        Ok(Some(item)) => {
                            if matches!(item, Watched::Unknown(_)) {
                                unknown.fetch_add(1, Ordering::Relaxed);
                            }
                            return Some((item, (bytes, buffer)));
                        }
                        Ok(None) => continue,
                        Err(()) => return None,
                    }
                }
                match bytes.next().await {
                    Some(Ok(chunk)) => buffer.extend_from_slice(chunk.as_ref()),
                    Some(Err(_)) | None => return None,
                }
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
/// comment), `Err` for one whose data is no event. An event of a known
/// kind costs the one parse; only one that does not parse is read again,
/// for its kind and the cell it names.
fn parse<T: Classify>(raw: &[u8]) -> Result<Option<Watched<T, T::Unknown>>, ()> {
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
    let Some(data) = data else {
        return Ok(None);
    };
    match serde_json::from_str(&data) {
        Ok(event) => Ok(Some(Watched::Event(event))),
        Err(_) => T::unknown(&data)
            .map(|event| Some(Watched::Unknown(event)))
            .ok_or(()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{CellWatchEvent, events};
    use crate::watched::Classify;
    use crate::{FollowEvent, UnknownEvent, UnknownFollowEvent, Watched};
    use futures::StreamExt;
    use gdl_cells::CellState;
    use velo::InstanceId;

    /// What a watch's bytes read as, and how many of them were of a kind
    /// this build does not know.
    fn read<T: Classify>(chunks: &[&str]) -> (Vec<Watched<T, T::Unknown>>, u64) {
        let chunks: Vec<Result<Vec<u8>, ()>> = chunks
            .iter()
            .map(|chunk| Ok(chunk.as_bytes().to_vec()))
            .collect();
        let unknown = Arc::new(AtomicU64::new(0));
        let read = futures::executor::block_on(
            events::<T, _, _, _>(futures::stream::iter(chunks), Arc::clone(&unknown)).collect(),
        );
        (read, unknown.load(Ordering::Relaxed))
    }

    /// The events of a cell's watch, when all are of known kinds.
    fn parsed(chunks: &[&str]) -> Vec<CellWatchEvent> {
        read::<CellWatchEvent>(chunks)
            .0
            .into_iter()
            .map(|item| item.event().expect("a known event"))
            .collect()
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
                state: CellState::Degraded,
                unfollowed: false,
            })
            .expect("an event serializes"),
            serde_json::json!({ "event": "state", "state": "degraded" }),
        );
        assert_eq!(
            serde_json::to_value(CellWatchEvent::Load {
                load: Some(gdl_cells::LoadSummary {
                    admitted: 3,
                    limit: 8,
                    instances: 2,
                    refusals: 1,
                    period_ms: 1000,
                })
            })
            .expect("an event serializes"),
            serde_json::json!({
                "event": "load",
                "load": {
                    "admitted": 3,
                    "limit": 8,
                    "instances": 2,
                    "refusals": 1,
                    "period_ms": 1000
                },
            }),
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

    /// An event that is not JSON, or of a known kind whose fields do not
    /// parse, ends the stream: what follows it cannot be trusted to follow
    /// on.
    #[test]
    fn an_event_that_does_not_parse_ends_the_stream() {
        let a = InstanceId::new_v4();
        for bad in [
            "data: {\"event\":\n\n",
            "data: {\"event\":\"left\",\"segment\":7}\n\n",
            "data: {\"event\":\"state\",\"state\":\"paused\"}\n\n",
            "data: {\"kind\":\"left\"}\n\n",
            "data: [1]\n\n",
        ] {
            let got = parsed(&[&sse(&left(a)), bad, &sse(&left(a))]);
            assert_eq!(got, [left(a)], "{bad:?}");
        }
    }

    /// A field a later build adds to a known event is ignored: the event
    /// is read as this build knows it.
    #[test]
    fn a_known_event_with_a_field_it_does_not_know_parses() {
        let a = InstanceId::new_v4();
        let later = format!(
            "data: {{\"event\":\"left\",\"segment\":\"f\",\"instance\":\"{a}\",\"why\":{{\"code\":3}}}}\n\n"
        );
        assert_eq!(parsed(&[&later]), [left(a)]);
        let relayed = format!(
            "data: {{\"event\":\"left\",\"class\":\"model_x\",\"cell\":\"m\",\"generation\":2,\"segment\":\"f\",\"instance\":\"{a}\",\"why\":1}}\n\n"
        );
        let (got, unknown) = read::<FollowEvent>(&[&relayed]);
        assert_eq!(
            got,
            [Watched::Event(FollowEvent::Left {
                class: "model_x".into(),
                cell: "m".into(),
                generation: 2,
                segment: "f".into(),
                instance: a,
            })]
        );
        assert_eq!(unknown, 0);
    }

    /// An event of a kind this build does not know, between two known
    /// ones, is read as unknown and counted, and the watch goes on: on a
    /// cell's watch it concerns the watched cell, whatever it names.
    #[test]
    fn an_unknown_kind_on_a_cell_watch_is_read_and_the_watch_goes_on() {
        let (a, b) = (InstanceId::new_v4(), InstanceId::new_v4());
        let (got, unknown) = read::<CellWatchEvent>(&[
            &sse(&left(a)),
            "data: {\"event\":\"paused\",\"cell\":7,\"until\":{\"ms\":10}}\n\n",
            &sse(&left(b)),
        ]);
        assert_eq!(
            got,
            [
                Watched::Event(left(a)),
                Watched::Unknown(UnknownEvent {
                    kind: "paused".into()
                }),
                Watched::Event(left(b)),
            ]
        );
        assert_eq!(unknown, 1);
    }

    fn relay_state(cell: &str, state: CellState) -> FollowEvent {
        FollowEvent::State {
            class: "model_x".into(),
            cell: cell.into(),
            generation: 1,
            state,
        }
    }

    fn relay_sse(event: &FollowEvent) -> String {
        format!(
            "data: {}\n\n",
            serde_json::to_string(event).expect("an event serializes")
        )
    }

    /// On the relay, an event of a kind this build does not know names the
    /// follow it concerns, and the watch goes on past it; one that names no
    /// follow ends the watch, since no cell can be failed closed for it.
    #[test]
    fn an_unknown_kind_on_the_relay_names_its_follow() {
        let before = relay_state("n", CellState::Ready);
        let after = relay_state("n", CellState::Degraded);
        let (got, unknown) = read::<FollowEvent>(&[
            &relay_sse(&before),
            "data: {\"event\":\"paused\",\"class\":\"model_x\",\"cell\":\"m\",\"generation\":3,\"for_ms\":5}\n\n",
            &relay_sse(&after),
        ]);
        assert_eq!(
            got,
            [
                Watched::Event(before.clone()),
                Watched::Unknown(UnknownFollowEvent {
                    kind: "paused".into(),
                    class: "model_x".into(),
                    cell: "m".into(),
                    generation: 3,
                }),
                Watched::Event(after.clone()),
            ]
        );
        assert_eq!(unknown, 1);
        for nameless in [
            "data: {\"event\":\"paused\"}\n\n",
            "data: {\"event\":\"paused\",\"class\":\"model_x\",\"cell\":\"m\"}\n\n",
            "data: {\"event\":\"paused\",\"class\":\"model_x\",\"cell\":\"m\",\"generation\":\"3\"}\n\n",
        ] {
            let (got, _) =
                read::<FollowEvent>(&[&relay_sse(&before), nameless, &relay_sse(&after)]);
            assert_eq!(got, [Watched::Event(before.clone())], "{nameless:?}");
        }
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
        let got: Vec<Watched<CellWatchEvent, UnknownEvent>> = futures::executor::block_on(
            events(futures::stream::iter(chunks), Arc::default()).collect(),
        );
        assert_eq!(got, [Watched::Event(left(a))]);
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
