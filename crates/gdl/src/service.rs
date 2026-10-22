use crate::{Build, Cx, Items, Next, Result};
use futures::future::BoxFuture;
use std::{future::Future, sync::Arc};

/// A typed client at a hosted protocol boundary.
///
/// Erasure keeps the service independent of generated pipeline types. It adds
/// one boxed future per external call; pipeline stages remain statically typed.
pub struct Client<Req, Item> {
    next: Arc<dyn Call<Req, Item>>,
}

impl<Req: Send + 'static, Item: Send + 'static> Client<Req, Item> {
    /// Bind a typed pipeline without changing its cancellation scope.
    pub fn new<N: Next<Req, Item>>(next: N) -> Self {
        Self {
            next: Arc::new(next),
        }
    }
}

impl<Req, Item> Clone for Client<Req, Item> {
    fn clone(&self) -> Self {
        Self {
            next: Arc::clone(&self.next),
        }
    }
}

impl<Req: Send + 'static, Item: Send + 'static> Next<Req, Item> for Client<Req, Item> {
    async fn generate(&self, cx: Cx, request: Req) -> Result<Items<Item>> {
        self.next.call(cx, request).await
    }
}

trait Call<Req, Item>: Send + Sync + 'static {
    fn call(&self, cx: Cx, request: Req) -> BoxFuture<'_, Result<Items<Item>>>;
}
impl<Req: Send + 'static, Item: Send + 'static, N: Next<Req, Item>> Call<Req, Item> for N {
    fn call(&self, cx: Cx, request: Req) -> BoxFuture<'_, Result<Items<Item>>> {
        Box::pin(self.generate(cx, request))
    }
}

/// A hosted service with a bundle of independently typed endpoint clients.
pub trait Service: Build {
    /// The public entry contracts. Disabled entries have no client.
    type Clients: Endpoints + Send + 'static;
    /// Host all enabled entries with one listener and one shutdown lifecycle.
    fn serve(
        self,
        clients: Self::Clients,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Catalog metadata for a service's typed client bundle.
pub trait Endpoints {
    /// The client fields, endpoint paths, enabling flags, and message types.
    #[doc(hidden)]
    fn entries() -> Vec<crate::__private::Endpoint>;
}
