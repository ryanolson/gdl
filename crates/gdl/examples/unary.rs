//! `unary <shape> <calls>` makes `calls` calls of a unary service in one
//! task, for callgrind to count a call's instructions: run it at two
//! counts and divide the difference by the difference in calls, so that the
//! setup cancels. Each shape runs a direct call beside it, so a call's own
//! cost is the shape less its direct row.
//!
//! - `direct`, `placed`, `hosted`, `boxed`: a service that adds one to its
//!   request, called on its `Arc` and awaited; through a `UnaryClient`, its
//!   future built in the call's own; through the client a node takes in its
//!   build of a service its segment hosts, which reads the service's
//!   readiness first; and through a boxed future behind a trait object, as
//!   an erased call costs without the in-place buffer.
//! - `<shape>-pending`: the same service, which waits once before it
//!   answers.
//! - `<shape>-locked`: a service that reads a map behind a tokio mutex, a
//!   future of about 160 bytes.
//! - `sizes`: print the size of each service's future and of the call's.
//!
//! ```text
//! RUSTFLAGS="--cfg rustix_use_libc" cargo build --release -p gdl --example unary
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/unary placed 10000
//! valgrind --tool=callgrind --callgrind-out-file=/dev/null target/release/examples/unary placed 20000
//! ```

#![allow(missing_docs)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use futures::future::BoxFuture;
use gdl::{Build, Cx, Result, Unary, UnaryClient, UnaryServices};

/// Adds one to its request, after one wait when `pending`.
struct AddOne {
    pending: bool,
}

impl Unary for AddOne {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, n: u64) -> Result<u64> {
        if self.pending {
            tokio::task::yield_now().await;
        }
        Ok(n + 1)
    }
}

impl Build for AddOne {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self { pending: false })
    }
}

/// Reads a map behind a tokio mutex.
struct Locked(tokio::sync::Mutex<HashMap<u64, u64>>);

impl Unary for Locked {
    type Req = u64;
    type Resp = u64;

    async fn call(&self, _cx: &Cx, n: u64) -> Result<u64> {
        Ok(self.0.lock().await.get(&n).copied().unwrap_or(n) + 1)
    }
}

impl Build for Locked {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> Result<Self> {
        Ok(Self(tokio::sync::Mutex::new(HashMap::new())))
    }
}

/// An erased call that boxes the service's future.
trait Boxed: Send + Sync {
    fn call<'a>(&'a self, cx: &'a Cx, n: u64) -> BoxFuture<'a, Result<u64>>;
}

impl<S: Unary<Req = u64, Resp = u64>> Boxed for S {
    fn call<'a>(&'a self, cx: &'a Cx, n: u64) -> BoxFuture<'a, Result<u64>> {
        Box::pin(Unary::call(self, cx, n))
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let shape = args.next().expect("a shape");
    if shape == "sizes" {
        sizes();
        return;
    }
    let calls: u64 = args
        .next()
        .expect("a count")
        .parse()
        .expect("a count of calls");
    let (how, service) = match shape.split_once('-') {
        None => (shape.as_str(), "ready"),
        Some((how, service)) => (how, service),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let sum = match service {
        "ready" => run(how, AddOne { pending: false }, calls, &runtime),
        "pending" => run(how, AddOne { pending: true }, calls, &runtime),
        "locked" => {
            let map = (0..64).map(|k| (k, k)).collect();
            run(how, Locked(tokio::sync::Mutex::new(map)), calls, &runtime)
        }
        other => panic!("no service `{other}`"),
    };
    black_box(sum);
}

fn run<S: Unary<Req = u64, Resp = u64> + Build>(
    how: &str,
    service: S,
    calls: u64,
    runtime: &tokio::runtime::Runtime,
) -> u64 {
    let service = Arc::new(service);
    let cx = Cx::new();
    runtime.block_on(async {
        let mut sum = 0u64;
        match how {
            "direct" => {
                let service = black_box(&service);
                for n in 0..calls {
                    sum = sum.wrapping_add(service.call(&cx, black_box(n)).await.unwrap_or(0));
                }
            }
            "placed" => {
                let client = UnaryClient::local(Arc::clone(&service));
                let client = black_box(&client);
                for n in 0..calls {
                    sum = sum.wrapping_add(client.call(&cx, black_box(n)).await.unwrap_or(0));
                }
            }
            "hosted" => {
                let services =
                    UnaryServices::new([("Caller".to_string(), vec!["Service".to_string()])]);
                services
                    .serve("Service", Arc::clone(&service))
                    .expect("served");
                let client = services
                    .scope("Caller", async {
                        UnaryClient::<u64, u64>::current("Service")
                    })
                    .await
                    .expect("a client");
                services.built();
                services.ready().await.expect("ready");
                let client = black_box(&client);
                for n in 0..calls {
                    sum = sum.wrapping_add(client.call(&cx, black_box(n)).await.unwrap_or(0));
                }
            }
            "boxed" => {
                let erased: Arc<dyn Boxed> = service.clone();
                let erased = black_box(&erased);
                for n in 0..calls {
                    sum = sum.wrapping_add(erased.call(&cx, black_box(n)).await.unwrap_or(0));
                }
            }
            other => panic!("no call `{other}`"),
        }
        sum
    })
}

/// Print the size of each service's future and of the call's own.
fn sizes() {
    let cx = Cx::new();
    let ready = AddOne { pending: false };
    let locked = Locked(tokio::sync::Mutex::new(HashMap::new()));
    let client = UnaryClient::local(Arc::new(AddOne { pending: false }));
    println!(
        "ready service future: {} B",
        size_of_val(&Unary::call(&ready, &cx, 1))
    );
    println!(
        "locked service future: {} B",
        size_of_val(&Unary::call(&locked, &cx, 1))
    );
    println!("call future: {} B", size_of_val(&client.call(&cx, 1)));
}
