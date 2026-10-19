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

// A glob imports only what the importing module can see: `output`
// cannot see `hidden::Payload`, so `output::Payload` is `visible`'s.
mod hidden {
    #[derive(gdl::Message)]
    struct Payload;
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
