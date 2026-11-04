//! A unary service's future must be `Send`: a call's future holds it, and
//! moves between threads with the caller's.

use std::rc::Rc;

use gdl::{Cx, Result, Unary};

struct Shared;

impl Unary for Shared {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, req: u64) -> Result<u64> {
        let held = Rc::new(req);
        std::future::ready(()).await;
        Ok(*held)
    }
}

fn main() {}
