//! Routing selectors: a crossing's router that names a selector the
//! sending segment registers, or one of gdl's own.

mod common;

use std::time::Duration;

use common::selecting::Counting;
use common::*;
use futures::StreamExt;
use gdl::{
    Choice, Cx, Encoded, Error, ErrorKind, Instances, Placement, Request, RequestRouter, Roster,
    RuntimeConfig, Selector,
};

fn routed(router: RequestRouter) -> Placement {
    Placement::new()
        .segment("front", ["A", "B", "C", "G"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"])
        .route("D", router)
}

fn selector(name: &str) -> RequestRouter {
    RequestRouter::Selector(name.into())
}

/// A front, two engine instances, and a post segment, each on a velo node
/// of its own; the engines' instance ids in the order given.
async fn two_engines() -> (Vec<(String, velo::Velo)>, [velo::InstanceId; 2]) {
    let mut nodes = Vec::new();
    for _ in 0..4 {
        nodes.push(velo_node().await);
    }
    let engines = [nodes[1].instance_id(), nodes[2].instance_id()];
    let instances = vec![
        ("front".into(), nodes[0].clone()),
        ("engine".into(), nodes[1].clone()),
        ("engine".into(), nodes[2].clone()),
        ("post".into(), nodes[3].clone()),
    ];
    (instances, engines)
}

/// How many hand-offs each engine instance took, in `two_engines`' order.
fn engine_forwards<H>(cluster: &Cluster<H>) -> Vec<usize> {
    cluster
        .segments
        .iter()
        .filter(|(name, _)| name == "engine")
        .map(|(_, segment)| segment.served().forwards)
        .collect()
}

/// A configuration that registers `selector` as `name`.
fn with<S: Selector + Clone>(name: &str, selector: &S) -> RuntimeConfig {
    let mut config = RuntimeConfig::default();
    config
        .selectors
        .add(name, selector.clone())
        .expect("a selector");
    config
}

/// Wait until `done` holds, for at most `WAIT`.
async fn until(what: &str, done: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A sending segment whose crossing names a selector that nothing
/// registers, and that gdl does not have, refuses to start; the segments
/// that do not send through it start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_selector_nothing_provides_fails_the_senders_start() {
    let node = velo_node().await;
    let placement = routed(selector("nowhere"));
    let manifest = reference_manifest(|_| true);
    let mut roster = Roster::new();
    roster.add("front", node.peer_info());
    let deployment = |segment: &str| {
        gdl::Deployment::new(
            node.clone(),
            &manifest,
            &placement,
            segment,
            &roster,
            RuntimeConfig::default(),
        )
    };
    let Err(error) = deployment("front") else {
        panic!("the front started without its selector");
    };
    assert_eq!(error.kind(), gdl::ErrorKind::Placement);
    assert!(error.message().contains("`nowhere`"), "{error}");
    assert!(
        deployment("engine").is_ok(),
        "the engine sends through nothing"
    );
}

/// Reads the request's head as the edge's own type: an odd token count
/// goes to the instance with the lower id, an even one to the higher.
#[derive(Clone)]
struct ByTokens;

impl Selector for ByTokens {
    type Head = Pre;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Pre>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        let mut sorted: Vec<_> = instances.iter().collect();
        sorted.sort_by_key(|instance| instance.id());
        let chosen = if request.head().plan.tokens % 2 == 1 {
            sorted[0]
        } else {
            sorted[sorted.len() - 1]
        };
        Ok(Choice::new(chosen))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_typed_selector_routes_each_request_where_it_says() {
    let (instances, engines) = two_engines().await;
    let events = Events::new();
    let cluster = deploy_configured(&instances, &routed(selector("tokens")), &events, &|_| {
        with("tokens", &ByTokens)
    });
    for tokens in [1, 3, 5, 2] {
        let plan = Plan {
            tokens,
            ..Plan::default()
        };
        let out = cluster
            .segment("front")
            .call(Cx::new(), prompt(&format!("t{tokens}"), plan))
            .await
            .expect("routed");
        assert_eq!(collect(out).await, expected(0..tokens));
    }
    let lower = usize::from(engines[1].as_u128() < engines[0].as_u128());
    let mut want = vec![0, 0];
    want[lower] = 3;
    want[1 - lower] = 1;
    assert_eq!(engine_forwards(&cluster), want);
    assert!(settles(&cluster).await);
}

/// Reads the head as the source's request, which is not what crosses
/// into D.
#[derive(Clone)]
struct WrongHead;

impl Selector for WrongHead {
    type Head = Prompt;

    async fn select<'a>(
        &'a self,
        _: Request<'a, Prompt>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        Ok(Choice::new(instances.get(0).expect("one")))
    }
}

/// Reads a streamed edge's head.
#[derive(Clone)]
struct BriefHead;

impl Selector for BriefHead {
    type Head = common::streamed::Brief;

    async fn select<'a>(
        &'a self,
        _: Request<'a, common::streamed::Brief>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        Ok(Choice::new(instances.get(0).expect("one")))
    }
}

/// Reads a streamed edge's head as its items' type.
#[derive(Clone)]
struct WordHead;

impl Selector for WordHead {
    type Head = common::streamed::Word;

    async fn select<'a>(
        &'a self,
        _: Request<'a, common::streamed::Word>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        Ok(Choice::new(instances.get(0).expect("one")))
    }
}

/// The check is by the edge's codec, not by what the segment hosts: the
/// front does not host D, so D's slot there is `Absent`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_selector_that_reads_another_head_type_fails_the_start() {
    let node = velo_node().await;
    let mut roster = Roster::new();
    roster.add("front", node.peer_info());
    let events = Events::new();

    let placement = routed(selector("wrong"));
    let manifest = reference_manifest(|_| true);
    let deployment = gdl::Deployment::new(
        node.clone(),
        &manifest,
        &placement,
        "front",
        &roster,
        with("wrong", &WrongHead),
    )
    .expect("the selector resolves");
    let Err(error) = reference_graph(&deployment, &events) else {
        panic!("a selector that reads `Prompt` routed a `Pre` crossing");
    };
    assert_eq!(error.kind(), ErrorKind::Placement);
    for part in ["`wrong`", "Prompt", "Pre"] {
        assert!(error.message().contains(part), "{part}: {error}");
    }

    // A streamed edge's head is its head type, not its items' type.
    let streamed = common::streamed::contract::manifest();
    let assemble = |config: RuntimeConfig| {
        let deployment = gdl::Deployment::new(
            node.clone(),
            &streamed,
            &placement,
            "front",
            &roster,
            config,
        )
        .expect("the selector resolves");
        common::streamed::contract::assemble(&deployment, common::streamed::bindings(&events))
            .map(|_| ())
    };
    assemble(with("wrong", &BriefHead)).expect("a `Brief` head on a `Brief + stream Word` edge");
    let Err(error) = assemble(with("wrong", &WordHead)) else {
        panic!("a selector that reads the items' type routed a streamed edge");
    };
    assert!(error.message().contains("Brief"), "{error}");
}

/// Fails with the error it is given.
#[derive(Clone)]
struct Refusing(ErrorKind);

impl Selector for Refusing {
    type Head = Encoded;

    async fn select<'a>(
        &'a self,
        _: Request<'a, Encoded>,
        _: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        Err(match self.0 {
            ErrorKind::Routing => Error::routing("no instance has the prefix"),
            kind => Error::new(kind, "no room"),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selectors_error_fails_the_request_with_its_kind() {
    let (instances, _) = two_engines().await;
    for kind in [ErrorKind::Routing, ErrorKind::Overloaded] {
        let events = Events::new();
        let cluster =
            deploy_configured(&instances, &routed(selector("refusing")), &events, &|_| {
                with("refusing", &Refusing(kind))
            });
        let result = cluster
            .segment("front")
            .call(Cx::new(), prompt("r", Plan::default()))
            .await;
        let error = match result {
            Err(error) => error,
            Ok(out) => match collect(out).await.pop() {
                Some(Err(error)) => error,
                other => panic!("the request was not refused: {other:?}"),
            },
        };
        assert_eq!(error.kind(), kind, "{error}");
        assert_eq!(engine_forwards(&cluster), [0, 0]);
        assert!(settles(&cluster).await);
    }
}

/// Adds the instance's position as metadata.
#[derive(Clone)]
struct Ranked;

impl Selector for Ranked {
    type Head = Encoded;

    async fn select<'a>(
        &'a self,
        _: Request<'a, Encoded>,
        instances: Instances<'a>,
    ) -> gdl::Result<Choice<'a>> {
        let chosen = instances.get(instances.len() - 1).expect("one");
        Ok(Choice::new(chosen).with_meta("rank", "3"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_choices_metadata_reaches_the_receiving_context() {
    let (instances, _) = two_engines().await;
    let events = Events::new();
    let cluster = deploy_configured(&instances, &routed(selector("ranked")), &events, &|_| {
        with("ranked", &Ranked)
    });
    let mut cx = Cx::new();
    cx.insert_meta("caller", "kept");
    let out = cluster
        .segment("front")
        .call(cx, prompt("m", Plan::default()))
        .await
        .expect("routed");
    collect(out).await;
    let seen = events.cx_of("D:c(m)#0");
    assert_eq!(seen.meta().get("rank").map(String::as_str), Some("3"));
    assert_eq!(seen.meta().get("caller").map(String::as_str), Some("kept"));
}

/// The outer end holds its lease while the response it returns is open,
/// and lets it go when the stream is dropped; a failure before the open
/// signal lets it go at once; a hedging operator's two attempts each take
/// one and give it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_outer_end_holds_a_lease_while_its_response_is_open() {
    let (instances, _) = two_engines().await;
    let events = Events::new();
    let counting = Counting::default();
    let cluster = deploy_configured(&instances, &routed(selector("counting")), &events, &|_| {
        counting.config()
    });
    let front = cluster.segment("front");

    let held = Plan {
        tokens: 2,
        hold_at_end: true,
        ..Plan::default()
    };
    let mut out = front
        .call(Cx::new(), prompt("held", held))
        .await
        .expect("routed");
    for _ in 0..2 {
        out.next().await.expect("an item").expect("ok");
    }
    events.wait_for("D:start:c(held)#0", WAIT).await;
    assert_eq!((counting.taken(), counting.live()), (1, 1));
    drop(out);
    until("the lease is given back", || counting.live() == 0).await;

    let failing = Plan {
        fail_generate: true,
        ..Plan::default()
    };
    let result = front.call(Cx::new(), prompt("fails", failing)).await;
    if let Ok(out) = result {
        assert!(collect(out).await.iter().any(Result::is_err));
    }
    until("the failed request's lease is given back", || {
        counting.live() == 0
    })
    .await;
    assert_eq!(counting.taken(), 2);

    let hedged = Plan {
        tokens: 2,
        mode: Mode::Hedge,
        ..Plan::default()
    };
    let out = front
        .call(Cx::new(), prompt("hedged", hedged))
        .await
        .expect("routed");
    assert_eq!(collect(out).await, expected(0..2));
    until("both attempts' leases are given back", || {
        counting.live() == 0
    })
    .await;
    assert_eq!(counting.taken(), 4, "each attempt chooses once");
    assert!(settles(&cluster).await);
}

/// A forward run that hands on, with nothing of the response passing it,
/// gives its lease back at the acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forward_run_that_hands_on_gives_its_lease_back_at_the_acknowledgement() {
    let mut nodes = Vec::new();
    for _ in 0..4 {
        nodes.push(velo_node().await);
    }
    let placement = Placement::new()
        .segment("front", ["A", "B", "G"])
        .segment("mid", ["C"])
        .segment("engine", ["D"])
        .segment("post", ["F", "E"])
        .route("D", selector("counting"));
    let events = Events::new();
    let counting = Counting::default();
    let instances: Vec<(String, velo::Velo)> = ["front", "mid", "engine", "post"]
        .into_iter()
        .map(String::from)
        .zip(nodes.iter().cloned())
        .collect();
    let cluster = deploy_configured(&instances, &placement, &events, &|_| counting.config());
    let held = Plan {
        tokens: 1,
        hold_at_end: true,
        ..Plan::default()
    };
    let mut out = cluster
        .segment("front")
        .call(Cx::new(), prompt("on", held))
        .await
        .expect("routed");
    out.next().await.expect("an item").expect("ok");
    assert_eq!(counting.taken(), 1);
    until("the hand-on's lease is given back", || counting.live() == 0).await;
    drop(out);
    assert!(settles(&cluster).await);
}

/// A relay holds its lease while it still sends its input on, and gives
/// it back once the input ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relay_holds_its_lease_while_its_input_is_open() {
    use common::streamed::{Script, ask, replies};
    let mut nodes = Vec::new();
    for _ in 0..3 {
        nodes.push(velo_node().await);
    }
    let placement = Placement::new()
        .segment("front", ["A", "B", "G"])
        .segment("mid", ["C"])
        .segment("engine", ["D", "F", "E"])
        .route("D", selector("counting"));
    let events = Events::new();
    let counting = Counting::default();
    let cluster = common::streamed::deploy_configured(
        &common::streamed::instances(&nodes, &placement),
        &placement,
        &events,
        &|_| counting.config(),
    );
    let (input, words) = futures::channel::mpsc::unbounded::<gdl::Result<String>>();
    input.unbounded_send(Ok("x".into())).expect("sent");
    let mut out = cluster
        .segment("front")
        .call(
            Cx::new(),
            ask("relay", Script::default(), gdl::items(words)),
        )
        .await
        .expect("routed");
    // The answer to the first word has come back, and the input is open.
    let first = tokio::time::timeout(WAIT, out.next())
        .await
        .expect("an answer to the first word");
    assert_eq!((counting.taken(), counting.live()), (1, 1));
    drop(input);
    let mut answer = vec![first.expect("an item")];
    answer.extend(collect(out).await);
    assert_eq!(answer, replies("relay", &["x"]));
    until("the relay's lease is given back", || counting.live() == 0).await;
    assert!(settles(&cluster).await);
}

/// `least_loaded` sends the next request where fewer of this sender's
/// requests are held, and counts a stream again once it is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn least_loaded_sends_where_fewer_requests_are_held() {
    let (instances, _) = two_engines().await;
    let events = Events::new();
    let cluster = deploy_configured(
        &instances,
        &routed(selector("least_loaded")),
        &events,
        &|_| RuntimeConfig::default(),
    );
    let front = cluster.segment("front");
    let held = |text: &str| {
        let plan = Plan {
            tokens: 1,
            hold_at_end: true,
            ..Plan::default()
        };
        front.call(Cx::new(), prompt(text, plan))
    };
    let mut first = held("one").await.expect("routed");
    first.next().await.expect("an item").expect("ok");
    let mut second = held("two").await.expect("routed");
    second.next().await.expect("an item").expect("ok");
    assert_eq!(engine_forwards(&cluster), [1, 1], "both went to one engine");
    let busy_first = events.cx_of("D:c(one)#0");
    drop(first);
    busy_first.cancelled().await;
    // Its lease is given back once its stream is dropped at the front.
    let mut third = held("three").await.expect("routed");
    third.next().await.expect("an item").expect("ok");
    let forwards = engine_forwards(&cluster);
    assert_eq!(forwards.iter().sum::<usize>(), 3);
    assert!(forwards.contains(&2), "{forwards:?}");
    drop((second, third));
    assert!(settles(&cluster).await);
}

/// gdl's own selectors route by name with nothing registered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gdls_own_selectors_need_no_registration() {
    let (instances, _) = two_engines().await;
    for name in ["affinity", "least_loaded"] {
        let events = Events::new();
        let cluster = deploy_configured(&instances, &routed(selector(name)), &events, &|_| {
            RuntimeConfig::default()
        });
        for i in 0..4u32 {
            let plan = Plan {
                tokens: i + 1,
                ..Plan::default()
            };
            let out = cluster
                .segment("front")
                .call(Cx::new(), prompt(&format!("{name}{i}"), plan))
                .await
                .expect("routed");
            assert_eq!(collect(out).await, expected(0..i + 1));
        }
        assert_eq!(engine_forwards(&cluster).iter().sum::<usize>(), 4);
        assert!(settles(&cluster).await);
    }
}
