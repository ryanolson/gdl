//! A segment instance's link to its cell's hub's record service: each
//! request from a [`gdl::CellRecords`] goes to the hub's `gdl-keyed`
//! route, with the instance's own credential. `gdl` keeps its own types
//! for the operations and answers, since the wire types live in
//! `gdl-watch`, which links velo, and a node crate must build without it;
//! they are converted here, one to one.

use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use gdl::{Error, ErrorKind, Expected, Record, RecordAnswer, RecordLink, RecordOp, RecordReply};
use gdl_watch::{KeyedAnswer, KeyedAsk, KeyedOp, KeyedRecord, KeyedReply};
use velo::InstanceId;
use velo_hub::client::{ClientError, ExtClient};

use super::HubSession;

impl HubSession {
    /// The link through which the instance `instance` this session
    /// registered reaches its hub's record service. It does not keep the
    /// registration alive: once the session is dropped, each request fails
    /// without reaching the hub.
    pub(crate) fn records_link(&self, instance: InstanceId) -> Arc<dyn RecordLink> {
        Arc::new(KeyedLink {
            ext: self.client.ext(gdl_watch::KEYED_EXTENSION),
            path: gdl_watch::keyed_path(instance),
        })
    }
}

struct KeyedLink {
    ext: ExtClient,
    path: String,
}

impl RecordLink for KeyedLink {
    fn ask(
        &self,
        ops: &mut dyn Iterator<Item = RecordOp<'_>>,
    ) -> BoxFuture<'static, gdl::Result<RecordReply>> {
        let ask = KeyedAsk {
            ops: ops.map(op).collect(),
        };
        let (ext, path) = (self.ext.clone(), self.path.clone());
        async move {
            let reply: KeyedReply =
                ext.control_post(&path, &ask)
                    .await
                    .map_err(|error: ClientError| {
                        Error::new(
                            ErrorKind::Transport,
                            format!("the cell's hub did not take a record request: {error}"),
                        )
                    })?;
            Ok(RecordReply {
                answers: reply.answers.into_iter().map(answer).collect(),
                eviction_age: reply.eviction_age_ms.map(Duration::from_millis),
            })
        }
        .boxed()
    }
}

fn op(op: RecordOp<'_>) -> KeyedOp {
    match op {
        RecordOp::Get { key } => KeyedOp::Get { key: key.to_vec() },
        RecordOp::Set {
            key,
            value,
            expected,
        } => KeyedOp::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            expected: match expected {
                Expected::Replace => gdl_watch::Expected::Replace,
                Expected::Absent => gdl_watch::Expected::Absent,
                Expected::Version(version) => gdl_watch::Expected::Version(version),
            },
        },
        RecordOp::Check { key, version } => KeyedOp::Check {
            key: key.to_vec(),
            version,
        },
    }
}

fn answer(answer: KeyedAnswer) -> RecordAnswer {
    match answer {
        KeyedAnswer::Found { record: r } => RecordAnswer::Found(record(r)),
        KeyedAnswer::Missing => RecordAnswer::Missing,
        KeyedAnswer::Applied { version } => RecordAnswer::Applied(version),
        KeyedAnswer::Refused { current } => RecordAnswer::Refused(current.map(record)),
        KeyedAnswer::Unchanged => RecordAnswer::Unchanged,
        KeyedAnswer::Newer { record: r } => RecordAnswer::Newer(record(r)),
        KeyedAnswer::Gone => RecordAnswer::Gone,
    }
}

fn record(record: KeyedRecord) -> Record {
    Record {
        value: record.value,
        version: record.version,
    }
}
