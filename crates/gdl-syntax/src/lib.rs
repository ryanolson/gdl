//! The gdl graph syntax: the body of `gdl::graph!`, and of a graph file.
//!
//! `graph!` expands a checked [`Graph`] into Rust; `gdl-gen` reads a graph
//! file into the same [`Graph`] and resolves its message names. One parser
//! serves both, so the two cannot accept different graphs.

mod model;
mod parse;

pub use model::{Edge, Graph, Hop, Node, Role, identifier, message_name};
pub use parse::{Request, Sig};
