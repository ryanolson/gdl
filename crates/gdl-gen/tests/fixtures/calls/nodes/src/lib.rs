//! A router that calls a tokenizer on each request, and a sink that calls
//! it too. Each takes its client of the tokenizer in its build, and calls
//! it the same way whether the tokenizer is in its segment or in another.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use calls_protocols::{Text, Tokens};
use gdl::{Build, Cx, ErrorKind, Items, Next, Operator, Result, Sink, Unary, UnaryClient, items};
use serde::Deserialize;

pub mod counting;

/// The tokenizer and the sink take no settings.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoSettings {}

/// Counts the words of a text. It answers only once its `ready` has run,
/// so that a call made before then fails. Each copy counts the calls it
/// answered on a counter of its own.
#[derive(gdl::Node)]
pub struct Tokenize {
    loaded: AtomicBool,
    answered: Arc<AtomicU64>,
}

/// The counter of each copy of the tokenizer this process built, in the
/// order it built them, so that a program can print them at its end. Only
/// read there: the copies share nothing through it.
pub static COPIES: Mutex<Vec<Arc<AtomicU64>>> = Mutex::new(Vec::new());

impl Build for Tokenize {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        let answered = Arc::new(AtomicU64::new(0));
        COPIES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::clone(&answered));
        Ok(Self {
            loaded: AtomicBool::new(false),
            answered,
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
        self.answered.fetch_add(1, Ordering::Relaxed);
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

/// Calls made through the router and the sink, and what their threads
/// allocated in them, under the [`counting::Counting`] allocator.
pub static CALLS: AtomicU64 = AtomicU64::new(0);
/// See [`CALLS`].
pub static ALLOCATED: AtomicU64 = AtomicU64::new(0);

/// Call `client` with `text`, counting the call and what it allocated.
async fn counted(client: &UnaryClient<Text, Tokens>, cx: &Cx, text: Text) -> Result<Tokens> {
    let counted = counting::Count::start();
    let tokens = client.call(cx, text).await;
    ALLOCATED.fetch_add(counted.stop(), Ordering::Relaxed);
    CALLS.fetch_add(1, Ordering::Relaxed);
    tokens
}

/// What this process's callers did, and what each copy of the tokenizer
/// it built answered: `calls <n> allocated <n> copies [<n>, ...]`.
pub fn report() -> String {
    let copies: Vec<u64> = COPIES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .map(|answered| answered.load(Ordering::Relaxed))
        .collect();
    format!(
        "calls {} allocated {} copies {copies:?}",
        CALLS.load(Ordering::Relaxed),
        ALLOCATED.load(Ordering::Relaxed)
    )
}

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
        let tokens = counted(&self.tokenize, &cx, asked).await?;
        let routed = text(&format!("{} [{} words]", req.text, tokens.count));
        next.generate(cx, routed).await
    }
}

/// Asks the tokenizer about the request, then answers with its text, once.
#[derive(gdl::Node)]
pub struct Echo {
    tokenize: UnaryClient<Text, Tokens>,
}

impl Build for Echo {
    type Config = NoSettings;
    type Metrics = ();

    async fn build(_: NoSettings, (): ()) -> Result<Self> {
        Ok(Self {
            tokenize: UnaryClient::current("Tokenize")?,
        })
    }
}

impl Sink for Echo {
    type Req = Text;
    type Item = Text;

    async fn generate(&self, cx: Cx, req: Text) -> Result<Items<Text>> {
        counted(&self.tokenize, &cx, req.clone()).await?;
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
