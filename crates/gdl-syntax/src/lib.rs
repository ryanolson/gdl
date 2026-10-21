//! The gdl graph syntax: the body of `gdl::graph!`, and of a graph file.
//!
//! `graph!` expands a checked [`Graph`] into Rust; `gdl-gen` reads a graph
//! file into the same [`Graph`] and resolves its message names. One parser
//! serves both, so the two cannot accept different graphs.

mod config;
mod endpoint;
mod manifest;
mod metrics;
mod model;
mod parse;

pub use config::{NodeTypes, graph_fields, node_types};
pub use endpoint::{Endpoint, endpoints};
pub use gdl_model::Role;
pub use metrics::metric_fields;
pub use model::{Edge, Graph, Hop, Node, identifier, message_name};
pub use parse::{Request, Sig};
