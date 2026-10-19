//! The gdl graph syntax: the body of `gdl::graph!`, of a graph file, and
//! the node declarations of a nodes file.
//!
//! `graph!` expands a checked [`Graph`] into Rust; `gdl-gen` reads a graph
//! file into the same [`Graph`] and resolves its message names. One parser
//! serves both, so the two cannot accept different graphs.

mod model;
mod parse;

pub use model::{Edge, Graph, Hop, Node, Role, declarations, identifier, message_name};
pub use parse::{Request, Sig};
