//! The closing example in `docs/src/guides/write-nodes.md` is the body of
//! `guide` below, verbatim: this file compiles it, runs it, and checks that
//! the guide's text matches.
//!
//! Rejected: compiling the guide's text with `rustc` against the rlibs in
//! the target directory. A target directory that holds both a workspace
//! build and a `-p gdl` build has two `gdl` and two `futures` rlibs, and a
//! mismatched pair fails with the same error as a missing `StreamExt`
//! import.

use std::sync::Arc;

use futures::StreamExt;
use gdl::{Cx, Hop, Items, Next, Operator, OperatorStage, Result, Sink, SinkStage, Source};

fn reply(chunk: u32) -> u32 {
    chunk
}

fn closing() -> u32 {
    99
}

async fn guide<N: Next<u32, u32>>(cx: Cx, req: u32, next: N) -> Result<Items<u32>> {
    // The example begins.
    use futures::StreamExt;

    let below = next.generate(cx.clone(), req).await?;
    let out = futures::stream::unfold(Some((below, cx)), |state| async move {
        let (mut below, cx) = state?;
        match below.next().await {
            Some(Ok(chunk)) => Some((Ok(reply(chunk)), Some((below, cx)))),
            Some(Err(error)) => Some((Err(error), None)),
            // A cancel or a passed deadline also ends here: drop, then check.
            None => {
                drop(below);
                Some((cx.check().map(|()| closing()), None))
            }
        }
    });
    Ok(gdl::items(out))
    // The example ends.
}

/// The example's text, as this file holds it, at the guide's indent.
fn compiled() -> String {
    let source = include_str!("closingguide.rs");
    let body = source
        .split_once("    // The example begins.\n")
        .expect("the start marker")
        .1
        .split_once("    // The example ends.\n")
        .expect("the end marker")
        .0;
    body.lines()
        .map(|line| line.strip_prefix("    ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The example's text, as the guide holds it.
fn guided() -> String {
    let guide = include_str!("../../../docs/src/guides/write-nodes.md");
    guide
        .split_once("So check the context on the clean end,")
        .expect("the closing advice")
        .1
        .split_once("```rust\n")
        .expect("a Rust block")
        .1
        .split_once("\n```")
        .expect("the end of the block")
        .0
        .to_owned()
}

#[test]
fn the_guides_closing_example_is_the_compiled_one() {
    let compiled = compiled();
    assert!(compiled.contains("cx.check()"), "{compiled}");
    assert_eq!(guided(), compiled);
}

struct Example;

impl Operator for Example {
    type UpReq = u32;
    type UpItem = u32;
    type DownReq = u32;
    type DownItem = u32;

    async fn generate<N: Next<u32, u32>>(&self, cx: Cx, req: u32, next: N) -> Result<Items<u32>> {
        guide(cx, req, next).await
    }
}

struct Ready;

impl Sink for Ready {
    type Req = u32;
    type Item = u32;

    async fn generate(&self, _cx: Cx, req: u32) -> Result<Items<u32>> {
        Ok(gdl::items(futures::stream::iter([Ok(req)])))
    }
}

/// The example closes a request that ends cleanly.
#[tokio::test]
async fn the_closing_example_closes_a_clean_end() {
    let graph = Source::new(Hop::new(
        (),
        OperatorStage::new(
            Arc::new(Example),
            Hop::new((), SinkStage::new(Arc::new(Ready)), ()),
        ),
        (),
    ));
    let out: Vec<_> = graph
        .generate(Cx::new(), 7)
        .await
        .expect("a stream")
        .map(|item| item.map_err(|error| error.kind()))
        .collect()
        .await;
    assert_eq!(out, [Ok(7), Ok(99)]);
}
