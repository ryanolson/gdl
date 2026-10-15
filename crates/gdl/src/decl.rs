use std::fmt;
use std::marker::PhantomData;

use crate::cx::Cx;
use crate::error::{Error, Result};
use crate::items::Items;
use crate::role::{Backward, Forward, Next, Operator, Sink};

/// The role a node plays in a graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// The entry point. It has no implementation.
    Source,
    /// Sees both the request and the response.
    Operator,
    /// Transforms the request only.
    Forward,
    /// The innermost node.
    Sink,
    /// Transforms the response stream only.
    Backward,
}

/// A node declaration: the marker type a graph definition creates for each
/// node. It fixes the node's name and role; the role-specific declaration
/// traits below fix its port types.
pub trait NodeDecl: Send + Sync + 'static {
    /// The node's name in the graph.
    const NAME: &'static str;
    /// The node's role.
    const ROLE: Role;
}

/// Port types of a source.
pub trait SourceDecl: NodeDecl {
    /// The request a caller sends.
    type Req: Send + 'static;
    /// The response item a caller receives.
    type Item: Send + 'static;
}

/// Port types of an operator.
pub trait OperatorDecl: NodeDecl {
    /// See [`Operator::UpReq`].
    type UpReq: Send + 'static;
    /// See [`Operator::UpItem`].
    type UpItem: Send + 'static;
    /// See [`Operator::DownReq`].
    type DownReq: Send + 'static;
    /// See [`Operator::DownItem`].
    type DownItem: Send + 'static;
}

/// Port types of a forward node.
pub trait ForwardDecl: NodeDecl {
    /// See [`Forward::In`].
    type In: Send + 'static;
    /// See [`Forward::Out`].
    type Out: Send + 'static;
}

/// Port types of a sink.
pub trait SinkDecl: NodeDecl {
    /// See [`Sink::Req`].
    type Req: Send + 'static;
    /// See [`Sink::Item`].
    type Item: Send + 'static;
}

/// Port types of a backward node.
pub trait BackwardDecl: NodeDecl {
    /// See [`Backward::In`].
    type In: Send + 'static;
    /// See [`Backward::Out`].
    type Out: Send + 'static;
}

/// The implementation slot of a node that this segment does not host.
///
/// It is parameterized by the node's declaration so that it has exactly that
/// node's port types: a single shared placeholder could not be both an
/// `Raw -> Tok` backward node and a `Tok -> Out` one. Every call fails with
/// `NotHosted`; placement validation guarantees that none is made. A segment
/// binary fills the other slots with `Absent` and so never names (or links)
/// their implementations.
pub struct Absent<D>(PhantomData<fn() -> D>);

impl<D> Absent<D> {
    /// The placeholder for node `D`.
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<D> Default for Absent<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D> Clone for Absent<D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D> Copy for Absent<D> {}

impl<D: NodeDecl> fmt::Debug for Absent<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Absent<{}>", D::NAME)
    }
}

impl<D: SinkDecl> Sink for Absent<D> {
    type Req = D::Req;
    type Item = D::Item;

    async fn generate(&self, _cx: Cx, _req: Self::Req) -> Result<Items<Self::Item>> {
        Err(Error::not_hosted(D::NAME))
    }
}

impl<D: ForwardDecl> Forward for Absent<D> {
    type In = D::In;
    type Out = D::Out;

    async fn forward(&self, _cx: &Cx, _req: Self::In) -> Result<Self::Out> {
        Err(Error::not_hosted(D::NAME))
    }
}

impl<D: BackwardDecl> Backward for Absent<D> {
    type In = D::In;
    type Out = D::Out;

    fn backward(&self, _cx: &Cx, _input: Items<Self::In>) -> Items<Self::Out> {
        Box::pin(futures::stream::once(async {
            Err(Error::not_hosted(D::NAME))
        }))
    }
}

impl<D: OperatorDecl> Operator for Absent<D> {
    type UpReq = D::UpReq;
    type UpItem = D::UpItem;
    type DownReq = D::DownReq;
    type DownItem = D::DownItem;

    async fn generate<N>(&self, _cx: Cx, _req: Self::UpReq, _next: N) -> Result<Items<Self::UpItem>>
    where
        N: Next<Self::DownReq, Self::DownItem>,
    {
        Err(Error::not_hosted(D::NAME))
    }
}
