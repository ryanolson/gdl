//! Rust that the reader must read as the compiler does, one case to an
//! item. Unlike the other crates here, this one builds: `cargo gdl extract
//! --verify` compares what it registers with what the reader finds.

#![allow(dead_code, unused_imports, non_camel_case_types)]

use gdl::{Cx, Result};

// A file whose own `#![cfg]` is false holds nothing.
pub mod gated;

// A `#[path]` in an inline module is relative to the inline module's
// directory: `src/inline/payload.rs`, not `src/payload.rs`.
pub mod inline {
    #[path = "payload.rs"]
    pub mod nested;
}

// An inline module's own `#[path]` in a file that is not `mod.rs` is
// relative to the file's directory: `src/alternate/leaf.rs`.
pub mod paths;

// A glob imports only what the importing module can see: `output`
// cannot see `hidden::Payload`, so `output::Payload` is `visible`'s.
mod hidden {
    #[derive(gdl::Message)]
    struct Payload;

    pub mod bridge {
        pub use super::*;
    }
}
mod visible {
    #[derive(gdl::Message)]
    pub struct Payload;
}
pub mod output {
    pub use crate::hidden::*;
    pub use crate::visible::*;
}
#[gdl::export]
pub use output::Payload as Chosen;

// Through two globs: `hidden::bridge` sees `hidden`'s private `Payload`,
// but a glob of `bridge` does not, so `passed::Payload` is `visible`'s.
pub mod passed {
    pub use crate::hidden::bridge::*;
    pub use crate::visible::*;
}
#[gdl::export]
pub use passed::Payload as Passed;

// The export attribute, imported under another name.
mod defs {
    #[derive(gdl::Message)]
    pub struct Aliased;
}
use gdl::export as offer;
#[offer]
pub use defs::Aliased;

// Serde impls that borrow: not `DeserializeOwned`, so no wire type.
#[derive(gdl::Message, serde::Serialize, serde::Deserialize)]
pub struct Borrowed {
    pub text: &'static str,
}

// A skipped field is not deserialized, so its reference borrows nothing.
#[derive(gdl::Message, serde::Serialize, serde::Deserialize)]
pub struct Skipping {
    #[serde(skip)]
    pub note: &'static str,
    pub text: String,
}

// Fields under `cfg`: a skip that `cfg_attr` applies, and a reference
// that `cfg` removes, borrow nothing.
#[derive(gdl::Message, serde::Serialize, serde::Deserialize)]
pub struct Configured {
    #[cfg_attr(all(), serde(skip))]
    pub note: &'static str,
    #[cfg(any())]
    pub gone: &'static str,
    pub text: String,
}

// A variant under `cfg`: one that `cfg` removes borrows nothing.
#[derive(gdl::Message, serde::Serialize, serde::Deserialize)]
pub enum Gated {
    Owned(String),
    #[cfg(any())]
    Removed(&'static str),
}

// Raw identifiers: the catalogue and the reader both write `type::match`.
pub mod r#type {
    #[derive(gdl::Message)]
    pub struct r#match;
}
#[gdl::export]
pub use r#type::r#match as Matched;

// `extern crate self` names this crate.
extern crate self as local;
#[gdl::export]
pub use local::visible::Payload as SelfAlias;

// Globs that import each other: a name they do not hold is looked up in
// each once, not along every path through the cycle.
pub mod cycle {
    pub mod a {
        pub use super::*;
    }
    pub mod b {
        pub use super::*;
    }
    pub use a::*;
    pub use b::*;
    #[gdl::export]
    pub use read_protocols::Words as Cycled;
}

// Eleven modules, each importing every other by a glob: each module is
// searched once for a name, whatever path through the globs leads there.
pub mod many {
    pub mod c0 {
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c1 {
        pub use super::c0::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c2 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c3 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c4 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c5 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c6 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c7 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c8::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c8 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c9::*;
        pub use super::c10::*;
    }
    pub mod c9 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c10::*;
    }
    pub mod c10 {
        pub use super::c0::*;
        pub use super::c1::*;
        pub use super::c2::*;
        pub use super::c3::*;
        pub use super::c4::*;
        pub use super::c5::*;
        pub use super::c6::*;
        pub use super::c7::*;
        pub use super::c8::*;
        pub use super::c9::*;
    }
    pub use c0::*;
    #[gdl::export]
    pub use read_protocols::Words as Many;
}

#[derive(gdl::Message)]
pub struct Actual;
#[derive(gdl::Message)]
pub struct Wrong;

// Associated types under `cfg`: the one that holds is the port.
#[derive(gdl::Node)]
pub struct Selected;

impl gdl::Forward for Selected {
    #[cfg(any())]
    type In = Wrong;
    #[cfg(not(any()))]
    type In = Actual;
    type Out = Actual;

    async fn forward(&self, _cx: &Cx, req: Self::In) -> Result<Self::Out> {
        Ok(req)
    }
}

// A role trait and `Streamed`, imported under other names.
use gdl::Forward as Stage;
use gdl::Streamed as Input;

#[derive(gdl::Node)]
pub struct Renamed;

impl Stage for Renamed {
    type In = Input<Actual, Actual>;
    type Out = Actual;

    async fn forward(&self, _cx: &Cx, _req: Self::In) -> Result<Self::Out> {
        Ok(Actual)
    }
}

// Metrics with a raw field name and a doc comment over two lines: the
// build registers what the reader reads.
#[derive(gdl::Node)]
#[gdl(metrics = MeasuredMetrics)]
pub struct Measured;

/// What `Measured` reports.
#[derive(gdl::Metrics)]
pub struct MeasuredMetrics {
    /// Items passed,
    /// by kind.
    #[gdl(labels = ["kind"])]
    r#passed_total: gdl::prometheus::IntCounterVec,
    /// Time in the stage.
    #[gdl(buckets = [0.5, 1, 2.5])]
    stage_seconds: gdl::prometheus::Histogram,
}

impl gdl::Build for Measured {
    type Config = ();
    type Metrics = MeasuredMetrics;

    async fn build((): (), _metrics: MeasuredMetrics) -> Result<Self> {
        Ok(Self)
    }
}

impl gdl::Forward for Measured {
    type In = Actual;
    type Out = Actual;

    async fn forward(&self, _cx: &Cx, req: Self::In) -> Result<Self::Out> {
        Ok(req)
    }
}
