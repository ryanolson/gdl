//! The streamed reference graph: the reference topology with a streamed
//! request on both hops.
//!
//! ```text
//! A ==(Ask + stream Word)==> B ==(Brief + stream Word)==> C ====> D
//! A <======== G <========== B <======= E <======= F <=========== D
//! ```
//!
//! Every node marks the head and each word it passes on, so the output names
//! the path each input item took. D answers each input word with a note and
//! ends with a count, so a run that loses, repeats, or reorders input gives
//! a different output. As in the reference graph, the request carries a
//! `Script` that tells the nodes how to behave, and nodes record what
//! happens to them in an `Events` log.

#![allow(dead_code)]

use std::sync::Arc;

use futures::{Stream, StreamExt};
use gdl::{Backward, Cx, Error, Forward, Items, Next, Operator, Result, Sink, Streamed, items};
use serde::{Deserialize, Serialize};

use super::{Events, OnDrop};

// ----- messages -----

/// What the nodes do for one request.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Script {
    /// D stops reading after this many words, then ends.
    pub take: Option<u32>,
    /// D reads its first word inside `generate`, before it returns its
    /// stream.
    pub read_first: bool,
    /// D answers an input error with a note and ends cleanly.
    pub recover: bool,
    /// D answers an input error with a note and keeps reading.
    pub read_past_error: bool,
    /// D ignores its input and ends at once with one note.
    pub ignore_input: bool,
    /// Attempts numbered below this fail before their first item.
    pub fail_attempts: u32,
    /// B reads its whole input, then retries attempts that fail before
    /// their first item from what it read.
    pub buffered_retry: bool,
    /// C ends the words it passes on when its context is asked to stop.
    pub stop_ends_input: bool,
    /// When its input ends, D waits for a stop or the end of its context
    /// before its count.
    pub hold_at_end: bool,
    /// D returns a stream that never reads its input and never ends.
    pub stall: bool,
    /// D sends this many notes before it reads any input.
    pub emit_first: u32,
    /// C's words wait for a stop before C reads its input; the stop then
    /// ends them.
    pub wait_for_stop: bool,
    /// After its input ends, C waits for a stop, then adds the word `tail`.
    pub tail_on_stop: bool,
    /// D hands its input to a task of its own that reads it to the end, and
    /// answers with one note at once.
    pub detach_input: bool,
    /// D reads one word and hands the rest of its input, unread, to a task
    /// of its own that drops it only once the test logs `test:drop:<key>`.
    /// D answers with one note once the test logs `test:release:<key>`.
    pub hold_input_unread: bool,
}

/// A's request head.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ask {
    pub text: String,
    pub script: Script,
}

/// B's request head, to C and D.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Brief {
    pub text: String,
    pub attempt: u32,
    pub script: Script,
}

/// One input item.
pub type Word = String;

/// D to F.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Note(pub String);

/// F to E.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mark(pub String);

/// E to B.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Line(pub String);

/// B to G to A.
pub type Reply = String;

pub mod contract {
    use super::{Ask, Brief, Line, Mark, Note, Reply, Word};

    gdl::graph! {
        name = "streamed", schema = 1;
        source   A: Ask + stream Word -> stream Reply;
        operator B: (Ask + stream Word -> stream Reply) over (Brief + stream Word -> stream Line);
        forward  C: Brief + stream Word -> Brief + stream Word;
        sink     D: Brief + stream Word -> stream Note;
        backward F: Note -> Mark;
        backward E: Mark -> Line;
        backward G: Reply -> Reply;
        topology: A => B { C => D => F => E } => G;
    }
}

/// A forward node turns a value into a stream: X splits a brief's text into
/// words for D.
pub mod expanding {
    use super::{Brief, Note, Word};

    gdl::graph! {
        name = "expanding", schema = 1;
        source A: Brief -> stream Note;
        forward X: Brief -> Brief + stream Word;
        sink   D: Brief + stream Word -> stream Note;
        topology: A => X => D;
    }
}

/// A forward node turns a stream into a value: Y joins the words into the
/// brief's text for S.
pub mod collapsing {
    use super::{Brief, Note, Word};

    gdl::graph! {
        name = "collapsing", schema = 1;
        source A: Brief + stream Word -> stream Note;
        forward Y: Brief + stream Word -> Brief;
        sink   S: Brief -> stream Note;
        topology: A => Y => S;
    }
}

// ----- nodes -----

pub struct NodeB {
    pub events: Arc<Events>,
}

impl Operator for NodeB {
    type UpReq = Streamed<Ask, Word>;
    type UpItem = Reply;
    type DownReq = Streamed<Brief, Word>;
    type DownItem = Line;

    async fn generate<N>(&self, cx: Cx, req: Self::UpReq, next: N) -> Result<Items<Reply>>
    where
        N: Next<Self::DownReq, Line>,
    {
        let (ask, input) = req.into_parts();
        self.events.record(format!("B:start:{}", ask.text));
        let text = format!("b({})", ask.text);
        let brief = |attempt| Brief {
            text: text.clone(),
            attempt,
            script: ask.script.clone(),
        };
        let words = items(input.map(|word| word.map(|w| format!("b({w})"))));
        let out = if ask.script.buffered_retry {
            let read: Vec<Result<Word>> = words.collect().await;
            retry_from(&cx, &next, brief, read, &self.events).await?
        } else {
            next.generate(cx, Streamed::new(brief(0), words)).await?
        };
        Ok(items(out.map(|line| line.map(|Line(s)| format!("b<{s}>")))))
    }
}

/// Retry while an attempt fails before its first item, each time from the
/// words already read: a streamed request can be read only once, so an
/// operator that retries keeps its own copy.
async fn retry_from<N>(
    cx: &Cx,
    next: &N,
    brief: impl Fn(u32) -> Brief,
    read: Vec<Result<Word>>,
    events: &Events,
) -> Result<Items<Line>>
where
    N: Next<Streamed<Brief, Word>, Line>,
{
    const ATTEMPTS: u32 = 3;
    let mut attempt = 0;
    loop {
        let input = items(futures::stream::iter(read.clone()));
        let mut out = next
            .generate(cx.clone(), Streamed::new(brief(attempt), input))
            .await?;
        match out.next().await {
            Some(Err(e)) if attempt + 1 < ATTEMPTS => {
                events.record(format!("B:retry:{attempt}:{}", e.kind()));
                attempt += 1;
            }
            Some(first) => return Ok(items(futures::stream::iter([first]).chain(out))),
            None => return Ok(out),
        }
    }
}

pub struct NodeC {
    pub events: Arc<Events>,
}

impl Forward for NodeC {
    type In = Streamed<Brief, Word>;
    type Out = Streamed<Brief, Word>;

    async fn forward(&self, cx: &Cx, req: Self::In) -> Result<Self::Out> {
        let (mut brief, input) = req.into_parts();
        self.events.record(format!("C:{}", brief.text));
        brief.text = format!("c({})", brief.text);
        let words = items(input.map(|word| word.map(|w| format!("c({w})"))));
        let script = &brief.script;
        let words = if script.stop_ends_input {
            items(words.take_until(stopping(cx)))
        } else {
            words
        };
        let words = if script.wait_for_stop {
            // Nothing is read before the stop, and the stop ends the words.
            let first = futures::stream::once(stopping(cx)).filter_map(|()| async { None });
            items(first.chain(words.take_until(stopping(cx))))
        } else {
            words
        };
        let words = if script.tail_on_stop {
            let cx = cx.clone();
            let tail = futures::stream::once(async move {
                cx.stopping().await;
                Ok("tail".to_string())
            });
            items(words.chain(tail))
        } else {
            words
        };
        Ok(Streamed::new(brief, words))
    }
}

pub struct NodeD {
    pub events: Arc<Events>,
}

impl Sink for NodeD {
    type Req = Streamed<Brief, Word>;
    type Item = Note;

    async fn generate(&self, cx: Cx, req: Self::Req) -> Result<Items<Note>> {
        let (brief, mut input) = req.into_parts();
        let key = format!("{}#{}", brief.text, brief.attempt);
        self.events.record(format!("D:start:{key}"));
        if brief.attempt < brief.script.fail_attempts {
            return Ok(items(futures::stream::iter([Err(Error::node(
                "attempt failed",
            ))])));
        }
        if brief.script.ignore_input {
            let note = Note(format!("{}:ignored", brief.text));
            return Ok(items(futures::stream::iter([Ok(note)])));
        }
        if brief.script.hold_input_unread {
            // Reading moves the anchor's frames from velo's transport into
            // the anchor's own buffer, which only retiring the anchor frees.
            let _ = input.next().await;
            let holder = Arc::clone(&self.events);
            let dropped = key.clone();
            tokio::spawn(async move {
                holder
                    .wait_for(&format!("test:drop:{dropped}"), super::WAIT)
                    .await;
                drop(input);
                holder.record(format!("D:held-dropped:{dropped}"));
            });
            let events = Arc::clone(&self.events);
            let note = Note(format!("{}:held", brief.text));
            let answer = async move {
                events
                    .wait_for(&format!("test:release:{key}"), super::WAIT)
                    .await;
                Ok(note)
            };
            return Ok(items(futures::stream::once(answer)));
        }
        if brief.script.detach_input {
            let events = Arc::clone(&self.events);
            let key = key.clone();
            tokio::spawn(async move {
                while input.next().await.is_some() {}
                events.record(format!("D:detached-input-ended:{key}"));
            });
            let note = Note(format!("{}:detached", brief.text));
            return Ok(items(futures::stream::iter([Ok(note)])));
        }
        if brief.script.stall {
            let held = input;
            return Ok(items(futures::stream::pending().map(move |note| {
                let _held = &held;
                note
            })));
        }
        let first = if brief.script.read_first {
            let first = input.next().await;
            self.events.record(format!("D:read-first:{key}"));
            Some(first)
        } else {
            None
        };
        let reading = Reading {
            input: Some(input),
            first,
            count: 0,
            sent_first: 0,
            brief,
            key,
            cx,
            events: Arc::clone(&self.events),
        };
        Ok(items(futures::stream::unfold(reading, Reading::step)))
    }
}

/// D's state while it answers its input.
struct Reading {
    /// `None` once D has stopped reading.
    input: Option<Items<Word>>,
    /// The first word, when D read it before returning its stream.
    first: Option<Option<Result<Word>>>,
    count: u32,
    /// Notes sent before any input was read.
    sent_first: u32,
    brief: Brief,
    key: String,
    cx: Cx,
    events: Arc<Events>,
}

impl Reading {
    async fn step(mut self) -> Option<(Result<Note>, Self)> {
        let input = self.input.as_mut()?;
        if self.sent_first < self.brief.script.emit_first {
            self.sent_first += 1;
            let note = Note(format!("{}:first:{}", self.brief.text, self.sent_first));
            return Some((Ok(note), self));
        }
        if self.brief.script.take == Some(self.count) {
            self.input = None;
            self.events.record(format!("D:took:{}", self.key));
            return Some((Ok(self.end()), self));
        }
        let next = match self.first.take() {
            Some(first) => first,
            None => input.next().await,
        };
        let text = &self.brief.text;
        match next {
            Some(Ok(word)) => {
                self.count += 1;
                let note = Note(format!("{text}:{word}"));
                Some((Ok(note), self))
            }
            Some(Err(error)) if self.brief.script.read_past_error => {
                let note = Note(format!("{text}:input-error:{}", error.kind()));
                Some((Ok(note), self))
            }
            Some(Err(error)) => {
                self.input = None;
                let item = if self.brief.script.recover {
                    Ok(Note(format!("{text}:input-error:{}", error.kind())))
                } else {
                    Err(error)
                };
                Some((item, self))
            }
            None => {
                self.input = None;
                if self.brief.script.hold_at_end {
                    tokio::select! {
                        () = self.cx.stopping() => {}
                        _ = self.cx.done() => {}
                    }
                }
                Some((Ok(self.end()), self))
            }
        }
    }

    fn end(&self) -> Note {
        self.events.record(format!("D:done:{}", self.key));
        Note(format!("{}:end:{}", self.brief.text, self.count))
    }
}

pub struct NodeF;

impl Backward for NodeF {
    type In = Note;
    type Out = Mark;

    fn backward(&self, _cx: &Cx, input: Items<Note>) -> Items<Mark> {
        items(input.map(|note| note.map(|Note(s)| Mark(format!("f({s})")))))
    }
}

pub struct NodeE;

impl Backward for NodeE {
    type In = Mark;
    type Out = Line;

    fn backward(&self, _cx: &Cx, input: Items<Mark>) -> Items<Line> {
        items(input.map(|mark| mark.map(|Mark(s)| Line(format!("e({s})")))))
    }
}

pub struct NodeG;

impl Backward for NodeG {
    type In = Reply;
    type Out = Reply;

    fn backward(&self, _cx: &Cx, input: Items<Reply>) -> Items<Reply> {
        items(input.map(|reply| reply.map(|s| format!("g({s})"))))
    }
}

/// X: the brief's text, split into words. With `stop_ends_input` the words
/// go on until a stop.
pub struct NodeX;

impl Forward for NodeX {
    type In = Brief;
    type Out = Streamed<Brief, Word>;

    async fn forward(&self, cx: &Cx, brief: Brief) -> Result<Self::Out> {
        let words: Vec<Result<Word>> = brief.text.split(' ').map(|w| Ok(w.to_string())).collect();
        let words = futures::stream::iter(words);
        if brief.script.stop_ends_input {
            let endless = words.chain(futures::stream::pending());
            let words = items(endless.take_until(stopping(cx)));
            return Ok(Streamed::new(brief, words));
        }
        Ok(Streamed::new(brief, items(words)))
    }
}

/// A future that resolves when `cx` is asked to stop, owning what it needs.
fn stopping(cx: &Cx) -> impl std::future::Future<Output = ()> + Send + 'static {
    let cx = cx.clone();
    async move { cx.stopping().await }
}

/// Y: the words, joined into the brief's text.
pub struct NodeY;

impl Forward for NodeY {
    type In = Streamed<Brief, Word>;
    type Out = Brief;

    async fn forward(&self, _cx: &Cx, req: Self::In) -> Result<Brief> {
        let (mut brief, input) = req.into_parts();
        let words: Vec<Word> = input
            .map(|word| word)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_>>()?;
        brief.text = format!("{}:{}", brief.text, words.join("+"));
        Ok(brief)
    }
}

/// S: one note with the brief's text.
pub struct NodeS;

impl Sink for NodeS {
    type Req = Brief;
    type Item = Note;

    async fn generate(&self, _cx: Cx, brief: Brief) -> Result<Items<Note>> {
        Ok(items(futures::stream::iter([Ok(Note(brief.text))])))
    }
}

// ----- assembly -----

/// Every node of the streamed reference graph bound.
pub fn bindings(
    events: &Arc<Events>,
) -> contract::Bindings<NodeB, NodeC, NodeD, NodeF, NodeE, NodeG> {
    contract::Bindings::new()
        .b(NodeB {
            events: Arc::clone(events),
        })
        .c(NodeC {
            events: Arc::clone(events),
        })
        .d(NodeD {
            events: Arc::clone(events),
        })
        .f(NodeF)
        .e(NodeE)
        .g(NodeG)
}

/// A's request.
pub fn ask(text: &str, script: Script, words: Items<Word>) -> Streamed<Ask, Word> {
    Streamed::new(
        Ask {
            text: text.to_string(),
            script,
        },
        words,
    )
}

/// What A receives for `text` and `words` when every word reaches D and D
/// counts them all.
pub fn replies(text: &str, words: &[&str]) -> Vec<Result<Reply>> {
    let head = format!("c(b({text}))");
    words
        .iter()
        .map(|w| format!("{head}:c(b({w}))"))
        .chain([format!("{head}:end:{}", words.len())])
        .map(|note| Ok(reply(&note)))
        .collect()
}

/// A note from D as A receives it.
pub fn reply(note: &str) -> Reply {
    format!("g(b<e(f({note}))>)")
}

/// An input stream of `words`. It records each word it yields and its own
/// drop, and with `then_pending` it never ends after the last word.
pub fn words(events: &Arc<Events>, words: &[&str], then_pending: bool) -> Items<Word> {
    let dropped = OnDrop::new(Arc::clone(events), "input:dropped".to_string());
    let log = Arc::clone(events);
    let listed: Vec<Result<Word>> = words.iter().map(|w| Ok(w.to_string())).collect();
    let tail: std::pin::Pin<Box<dyn Stream<Item = Result<Word>> + Send>> = if then_pending {
        Box::pin(futures::stream::pending())
    } else {
        Box::pin(futures::stream::empty())
    };
    items(futures::stream::iter(listed).chain(tail).map(move |word| {
        let _held = &dropped;
        if let Ok(w) = &word {
            log.record(format!("input:sent:{w}"));
        }
        word
    }))
}

/// An input stream that yields `before`, then `error`, then `after`.
pub fn words_with_error(
    events: &Arc<Events>,
    before: &[&str],
    error: Error,
    after: &[&str],
) -> Items<Word> {
    let listed: Vec<Result<Word>> = before
        .iter()
        .map(|w| Ok(w.to_string()))
        .chain([Err(error)])
        .chain(after.iter().map(|w| Ok(w.to_string())))
        .collect();
    let dropped = OnDrop::new(Arc::clone(events), "input:dropped".to_string());
    let log = Arc::clone(events);
    items(futures::stream::iter(listed).map(move |word| {
        let _held = &dropped;
        if let Ok(w) = &word {
            log.record(format!("input:sent:{w}"));
        }
        word
    }))
}

// ----- distributed -----

use gdl::{Deployment, Register, Roster, RuntimeConfig};

use super::Cluster;

/// Deploy the streamed reference graph with explicit instances, each bound
/// with every node: a fat build that can serve any segment.
pub fn deploy(
    instances: &[(String, Arc<velo::Velo>)],
    placement: &gdl::Placement,
    events: &Arc<Events>,
    config: RuntimeConfig,
) -> Cluster<impl Next<Streamed<Ask, Word>, Reply> + Register + use<>> {
    deploy_configured(instances, placement, events, &|_| config.clone())
}

/// Deploy the streamed reference graph with each segment's runtime
/// settings chosen by `config`.
pub fn deploy_configured(
    instances: &[(String, Arc<velo::Velo>)],
    placement: &gdl::Placement,
    events: &Arc<Events>,
    config: &dyn Fn(&str) -> RuntimeConfig,
) -> Cluster<impl Next<Streamed<Ask, Word>, Reply> + Register + use<>> {
    let events = Arc::clone(events);
    deploy_graph(
        instances,
        &contract::manifest(),
        placement,
        config,
        move |deployment| contract::assemble(deployment, bindings(&events)),
    )
}

/// Deploy any graph: `assemble` builds each instance's root from its
/// deployment.
pub fn deploy_graph<H: Register>(
    instances: &[(String, Arc<velo::Velo>)],
    manifest: &gdl::Manifest,
    placement: &gdl::Placement,
    config: &dyn Fn(&str) -> RuntimeConfig,
    assemble: impl Fn(&Deployment) -> gdl::Result<H>,
) -> Cluster<H> {
    let mut roster = Roster::new();
    for (name, node) in instances {
        roster.add(name.clone(), node.peer_info());
    }
    let segments = instances
        .iter()
        .map(|(name, node)| {
            let deployment = Deployment::new(
                Arc::clone(node),
                manifest,
                placement,
                name,
                &roster,
                config(name),
            )
            .expect("deployment");
            let root = assemble(&deployment).expect("graph");
            (name.clone(), deployment.start(root).expect("start"))
        })
        .collect();
    Cluster { segments }
}

/// One velo node per segment of `placement`, in its segment order.
pub fn instances(
    nodes: &[Arc<velo::Velo>],
    placement: &gdl::Placement,
) -> Vec<(String, Arc<velo::Velo>)> {
    placement
        .segments()
        .map(|(name, _)| name.to_string())
        .zip(nodes.iter().cloned())
        .collect()
}
