use std::fmt;

use crate::items::{Items, until_error};

/// A streaming request: a head value and a stream of items, sent together.
/// A pure stream is `Streamed<(), T>`.
///
/// The items are fused when the request is built: they end after their
/// first `Err`. A stream that crossed a segment boundary is fused there, so
/// fusing wherever a request is built gives a local run the outcome a
/// remote run has, whatever the placement.
///
/// A streamed request is not `Clone`: its items can be read once. An
/// operator that retries, hedges, or migrates a streamed request keeps its
/// own copy of the items it needs.
pub struct Streamed<H, T> {
    /// The value that describes the stream, such as a session's settings.
    pub head: H,
    items: Items<T>,
}

impl<H, T: Send + 'static> Streamed<H, T> {
    /// A request with this head and these items.
    pub fn new(head: H, items: Items<T>) -> Self {
        Self {
            head,
            items: until_error(items),
        }
    }

    /// The head and the items.
    pub fn into_parts(self) -> (H, Items<T>) {
        (self.head, self.items)
    }

    /// The same items with another head. The items are already fused, so
    /// a node that changes only the head adds nothing to their path.
    pub fn map_head<G>(self, f: impl FnOnce(H) -> G) -> Streamed<G, T> {
        Streamed {
            head: f(self.head),
            items: self.items,
        }
    }
}

impl<H: fmt::Debug, T> fmt::Debug for Streamed<H, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Streamed")
            .field("head", &self.head)
            .finish_non_exhaustive()
    }
}

/// A streamed request's message names: its head's, and its item's. A port
/// written `stream Item` has a head of `()`.
impl<H: gdl_message::Shape, T: gdl_message::Shape> gdl_message::Shape for Streamed<H, T> {
    const HEAD: &'static str = H::HEAD;
    const ITEM: Option<&'static str> = Some(T::HEAD);
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::error::Error;
    use crate::items::items;

    #[tokio::test]
    async fn a_streamed_request_ends_after_its_first_error() {
        let input = items(futures::stream::iter([
            Ok(1),
            Err(Error::node("bad input")),
            Ok(2),
        ]));
        let (head, mut items) = Streamed::new("h", input).into_parts();
        assert_eq!(head, "h");
        assert_eq!(items.next().await.map(|r| r.ok()), Some(Some(1)));
        assert!(matches!(items.next().await, Some(Err(_))));
        assert!(items.next().await.is_none(), "an item followed the error");
    }

    #[tokio::test]
    async fn mapping_the_head_keeps_the_items() {
        let request = Streamed::new(2, items(futures::stream::iter([Ok("a"), Ok("b")])));
        let (head, items) = request.map_head(|n| n * 10).into_parts();
        assert_eq!(head, 20);
        let got: Vec<_> = items.map(|r| r.ok()).collect().await;
        assert_eq!(got, [Some("a"), Some("b")]);
    }
}
