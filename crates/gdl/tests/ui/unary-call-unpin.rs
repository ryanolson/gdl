//! A unary call's future is not `Unpin`: once polled, the service's future
//! lives inside it and must not move.

use std::sync::Arc;

use gdl::{Cx, Result, Unary, UnaryClient};

struct Echo;

impl Unary for Echo {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, req: u64) -> Result<u64> {
        Ok(req)
    }
}

fn unpin<T: Unpin>(_: &T) {}

fn main() {
    let cx = Cx::new();
    let client = UnaryClient::local(Arc::new(Echo));
    unpin(&client.call(&cx, 1));
}
