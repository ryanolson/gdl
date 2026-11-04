//! A router that calls a tokenizer on each request, and a sink. The router
//! takes its client of the tokenizer in its build, and calls it the same
//! way whether the tokenizer is in its segment or in another.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use calls_protocols::{Text, Tokens};
use gdl::{Build, Cx, ErrorKind, Items, Next, Operator, Result, Sink, Unary, UnaryClient, items};
use serde::Deserialize;

pub mod counting;

/// The tokenizer and the sink take no settings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

/// Counts the words of a text. It answers only once its `ready` has run,
/// so that a call made before then fails.
#[derive(gdl::Node)]
pub struct Tokenize {
    loaded: AtomicBool,
}

impl Build for Tokenize {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self {
            loaded: AtomicBool::new(false),
        })
    }

    async fn ready(&self) -> Result<()> {
        self.loaded.store(true, Ordering::Release);
        Ok(())
    }
}

impl Unary for Tokenize {
    type Req = Text;
    type Resp = Tokens;

    async fn call(&self, _cx: &Cx, text: Text) -> Result<Tokens> {
        if !self.loaded.load(Ordering::Acquire) {
            return Err(gdl::Error::new(ErrorKind::Node, "called before ready"));
        }
        let count = text.text.split_whitespace().count();
        Ok(Tokens {
            count: u32::try_from(count).unwrap_or(u32::MAX),
        })
    }
}

/// What the router does besides routing, for the tests.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RouterSettings {
    /// Call the tokenizer in the build, which fails.
    pub call_in_build: bool,
    /// Call the tokenizer in `ready`, which waits for the tokenizer's own.
    pub call_in_ready: bool,
}

/// Asks the tokenizer how many words a request has, and sends the request
/// on with the count.
#[derive(gdl::Node)]
pub struct Router {
    tokenize: UnaryClient<Text, Tokens>,
    call_in_ready: bool,
}

impl Build for Router {
    type Config = RouterSettings;
    type Metrics = ();

    async fn build(settings: RouterSettings, (): ()) -> Result<Self> {
        let tokenize = UnaryClient::current("Tokenize")?;
        if settings.call_in_build {
            tokenize.call(&Cx::new(), text("in the build")).await?;
        }
        Ok(Self {
            tokenize,
            call_in_ready: settings.call_in_ready,
        })
    }

    async fn ready(&self) -> Result<()> {
        if self.call_in_ready {
            let tokens = self.tokenize.call(&Cx::new(), text("in ready")).await?;
            if tokens.count != 2 {
                return Err(gdl::Error::new(ErrorKind::Node, "a wrong count in ready"));
            }
        }
        Ok(())
    }
}

/// Calls made through the router, and what this thread allocated in them,
/// under the [`counting::Counting`] allocator.
pub static CALLS: AtomicU64 = AtomicU64::new(0);
/// See [`CALLS`].
pub static ALLOCATED: AtomicU64 = AtomicU64::new(0);

impl Operator for Router {
    type UpReq = Text;
    type UpItem = Text;
    type DownReq = Text;
    type DownItem = Text;

    async fn generate<N>(&self, cx: Cx, req: Text, next: N) -> Result<Items<Text>>
    where
        N: Next<Text, Text>,
    {
        // The request is copied before the count: what the call itself
        // allocates is counted, not what the router gives it.
        let asked = req.clone();
        let counted = counting::Count::start();
        let tokens = self.tokenize.call(&cx, asked).await;
        ALLOCATED.fetch_add(counted.stop(), Ordering::Relaxed);
        CALLS.fetch_add(1, Ordering::Relaxed);
        let tokens = tokens?;
        let routed = text(&format!("{} [{} words]", req.text, tokens.count));
        next.generate(cx, routed).await
    }
}

/// Answers with the request's text, once.
#[derive(gdl::Node)]
pub struct Echo;

impl Build for Echo {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self)
    }
}

impl Sink for Echo {
    type Req = Text;
    type Item = Text;

    async fn generate(&self, _cx: Cx, req: Text) -> Result<Items<Text>> {
        Ok(items(futures::stream::iter([Ok(text(&format!(
            "echo {}",
            req.text
        )))])))
    }
}

fn text(text: &str) -> Text {
    Text {
        text: text.to_string(),
    }
}
