use crate::{Backward, Cx, Forward, Items, Next, Operator, Result};
use std::marker::PhantomData;

/// A stateless operator that passes requests and response streams unchanged.
pub struct IdentityOperator<Req, Item>(PhantomData<fn(Req) -> Item>);
impl<Req, Item> IdentityOperator<Req, Item> {
    /// Construct a stateless identity.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
impl<Req, Item> Default for IdentityOperator<Req, Item> {
    fn default() -> Self {
        Self::new()
    }
}
impl<Req: Send + 'static, Item: Send + 'static> Operator for IdentityOperator<Req, Item> {
    type UpReq = Req;
    type DownReq = Req;
    type UpItem = Item;
    type DownItem = Item;
    async fn generate<N: Next<Req, Item>>(&self, cx: Cx, req: Req, next: N) -> Result<Items<Item>> {
        next.generate(cx, req).await
    }
}

/// A stateless identity node for either the request or response path.
pub struct IdentityNode<Req>(PhantomData<fn(Req)>);
impl<Req> IdentityNode<Req> {
    /// Construct a stateless identity.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
impl<Req> Default for IdentityNode<Req> {
    fn default() -> Self {
        Self::new()
    }
}
impl<Req: Send + 'static> Forward for IdentityNode<Req> {
    type In = Req;
    type Out = Req;
    async fn forward(&self, cx: &Cx, req: Req) -> Result<Req> {
        cx.check()?;
        Ok(req)
    }
}

impl<Item: Send + 'static> Backward for IdentityNode<Item> {
    type In = Item;
    type Out = Item;
    fn backward(&self, _: &Cx, input: Items<Item>) -> Items<Item> {
        input
    }
}
