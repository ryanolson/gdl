//! What crosses a segment boundary besides stream items: the request
//! envelopes and their replies. Every message carries the deployment
//! identity, so an instance built from another contract or placement refuses
//! it instead of misreading it.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::fabric::ReplyTo;
use gdl::{Cx, Deadline, RequestId};
use gdl::{Error, ErrorKind, Result};

/// The envelope format. A receiver rejects any other version. Version 4
/// carries each stream item as velo's own byte item, which version 3
/// segments cannot read, so the two refuse each other at the envelope that
/// starts the stream.
pub(crate) const VERSION: u8 = 4;

/// Refuse an envelope of a version other than [`VERSION`].
pub(crate) fn check_version(version: u8) -> Result<()> {
    if version == VERSION {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::Placement,
        format!("envelope version {version} is not supported"),
    ))
}

/// Which deployment a message belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    pub(crate) contract: u64,
    pub(crate) placement: u64,
}

/// The part of a [`Cx`] that crosses segments. Its default is what an
/// entry leaves in an envelope whose context it took.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct WireCx {
    id: u128,
    meta: BTreeMap<String, String>,
    deadline: Option<u64>,
}

impl WireCx {
    pub(crate) fn of(cx: &Cx) -> Self {
        Self {
            id: cx.id().as_u128(),
            meta: cx.meta().clone(),
            deadline: cx.deadline().map(Deadline::unix_nanos),
        }
    }

    /// Rebuild the context on arrival, or fail if its deadline has passed.
    /// `wall_now` reads this instance's wall clock, in Unix nanoseconds.
    pub(crate) fn arrive(
        self,
        skew: std::time::Duration,
        wall_now: impl FnOnce() -> u64,
    ) -> Result<Cx> {
        let deadline = match self.deadline {
            None => None,
            Some(unix_nanos) => Some(
                Deadline::from_wall(unix_nanos, skew, wall_now)
                    .ok_or_else(Error::deadline_exceeded)?,
            ),
        };
        Ok(Cx::arrived(
            RequestId::from_u128(self.id),
            self.meta,
            deadline,
        ))
    }

    pub(crate) fn root(&self) -> u128 {
        self.id
    }

    /// Add what a routing selector chose to tell the receiver.
    pub(crate) fn add_meta(&mut self, meta: Vec<(String, String)>) {
        self.meta.extend(meta);
    }
}

/// A request handed to the next segment on a hop's forward path.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Forward {
    pub(crate) version: u8,
    pub(crate) identity: Identity,
    pub(crate) hop: u16,
    /// Position on the hop's forward path of the first stage to run.
    pub(crate) pos: u16,
    pub(crate) cx: WireCx,
    pub(crate) reply: ReplyTo,
    #[serde(with = "serde_bytes")]
    pub(crate) payload: Vec<u8>,
}

/// What a stage that accepted a forward hand-off answers: for a streamed
/// request, where to send its items. The stage that reads the items mints
/// their anchor, because a velo anchor is read only in the process that
/// minted it.
pub(crate) type Accepted = Option<ReplyTo>;

/// A request to prepare one backward run of a hop.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Open {
    pub(crate) version: u8,
    pub(crate) identity: Identity,
    pub(crate) hop: u16,
    /// Position on the hop's backward path of the run's first stage.
    pub(crate) pos: u16,
    pub(crate) cx: WireCx,
    /// Where the run delivers its output.
    pub(crate) reply: ReplyTo,
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec(value).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(|e| Error::new(ErrorKind::Codec, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelopes_round_trip_with_full_width_ids() {
        let cx = Cx::new().with_timeout(std::time::Duration::from_secs(30));
        let envelope = Forward {
            version: VERSION,
            identity: Identity {
                contract: u64::MAX,
                placement: 7,
            },
            hop: 1,
            pos: 2,
            cx: WireCx::of(&cx),
            reply: ReplyTo::test_value(u128::MAX - 5),
            payload: vec![1, 2, 3],
        };
        let back: Forward = decode(&encode(&envelope).expect("encode")).expect("decode");
        assert_eq!(back.identity, envelope.identity);
        assert_eq!(back.cx.root(), cx.id().as_u128());
        assert_eq!(back.reply.handle_u128(), u128::MAX - 5);
        assert_eq!(back.payload, vec![1, 2, 3]);
        let arrived = back
            .cx
            .arrive(
                std::time::Duration::from_millis(50),
                gdl::__runtime::wall_now_nanos,
            )
            .expect("live");
        assert_eq!(arrived.id(), cx.id());
        assert!(arrived.deadline().is_some());
    }

    #[test]
    fn an_acknowledgement_carries_its_input_reply() {
        let accepted: Result<Accepted> = Ok(Some(ReplyTo::test_value(u128::MAX - 9)));
        let back: Result<Accepted> = decode(&encode(&accepted).expect("encode")).expect("decode");
        let reply = back.expect("accepted").expect("an input reply");
        assert_eq!(reply.handle_u128(), u128::MAX - 9);
        let plain: Result<Accepted> = Ok(None);
        let back: Result<Accepted> = decode(&encode(&plain).expect("encode")).expect("decode");
        assert!(back.expect("accepted").is_none());
    }

    #[test]
    fn an_envelope_of_version_3_is_refused() {
        let envelope = Open {
            version: 3,
            identity: Identity {
                contract: 1,
                placement: 2,
            },
            hop: 0,
            pos: 1,
            cx: WireCx::default(),
            reply: ReplyTo::test_value(7),
        };
        let back: Open = decode(&encode(&envelope).expect("encode")).expect("decode");
        let refused = check_version(back.version).expect_err("version 3 was accepted");
        assert_eq!(refused.kind(), ErrorKind::Placement);
        assert!(refused.to_string().contains("version 3"), "{refused}");
        assert!(check_version(VERSION).is_ok());
    }
}
