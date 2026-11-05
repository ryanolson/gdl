//! `graph!` with unary nodes: the reference graph, with two unary nodes and
//! `calls` clauses. Each unary node has a declaration marker with its
//! types, a manifest entry, and a slot in `Bindings` for its shared
//! service; and a service's readiness, which a call from another node's
//! `ready` waits for.

mod common;
#[path = "unary/lifetime.rs"]
mod lifetime;

use std::marker::PhantomData;
use std::sync::Arc;

use common::*;
use gdl_runtime::{NodeDecl, Role, TypeInfo, UnaryDecl, UnaryInfo};

mod contract {
    use super::common::{Chunk, Out, Pre, Prompt, Raw, Tok};

    gdl_runtime::graph! {
        name = "reference", schema = 1;
        unary    Lookup: Prompt -> Pre idempotent;
        source   A: Prompt -> stream Chunk;
        operator B: (Prompt -> stream Chunk) over (Pre -> stream Out) calls Lookup, Score;
        forward  C: Pre -> Pre;
        sink     D: Pre -> stream Raw calls Score;
        backward E: Tok -> Out;
        backward F: Raw -> Tok;
        backward G: Chunk -> Chunk;
        unary    Score: Pre -> Tok calls Lookup;
        topology: A => B { C => D => F => E } => G;
    }
}

#[test]
fn the_manifest_has_the_unary_nodes_and_every_calls_clause() {
    let mut expected = reference_manifest(|_| true);
    let ty = |name: &str| TypeInfo::new(name, true, Default::default());
    let unary = |name: &str, req: &str, resp: &str, idempotent: bool| gdl_runtime::NodeInfo {
        name: name.into(),
        role: Role::Unary,
        binding: Some(name.to_lowercase()),
        calls: vec![],
        unary: Some(UnaryInfo {
            req: ty(req),
            resp: ty(resp),
            idempotent,
        }),
    };
    expected
        .nodes
        .insert(0, unary("Lookup", "Prompt", "Pre", true));
    expected.nodes.push(gdl_runtime::NodeInfo {
        calls: vec!["Lookup".into()],
        ..unary("Score", "Pre", "Tok", false)
    });
    let mut calls = |name: &str, services: &[&str]| {
        let node = expected
            .nodes
            .iter_mut()
            .find(|n| n.name == name)
            .expect("present");
        node.calls = services.iter().map(|s| s.to_string()).collect();
    };
    calls("B", &["Lookup", "Score"]);
    calls("D", &["Score"]);
    let generated = contract::manifest();
    assert_eq!(generated, expected);
    assert_ne!(
        generated.contract_hash(),
        reference_manifest(|_| true).contract_hash()
    );
}

#[test]
fn a_unary_node_declares_its_role_and_types() {
    fn same<T>(_: PhantomData<T>, _: PhantomData<T>) {}
    same(
        PhantomData::<<contract::nodes::Lookup as UnaryDecl>::Req>,
        PhantomData::<Prompt>,
    );
    same(
        PhantomData::<<contract::nodes::Lookup as UnaryDecl>::Resp>,
        PhantomData::<Pre>,
    );
    assert_eq!(<contract::nodes::Score as NodeDecl>::ROLE, Role::Unary);
    assert_eq!(<contract::nodes::Score as NodeDecl>::NAME, "Score");
}

/// Answers nothing: a unary node's slot is typed, not called, here.
struct LookupNode;

impl gdl::Unary for LookupNode {
    type Req = Prompt;
    type Resp = Pre;

    async fn call(&self, _cx: &gdl::Cx, _req: Prompt) -> gdl::Result<Pre> {
        Err(gdl::Error::new(gdl::ErrorKind::Node, "not called"))
    }
}

/// The bindings have a slot per node that takes an implementation, the
/// unary nodes' included, in declaration order. A unary node's setter
/// takes the service's `Arc`, which a segment shares with the clients its
/// callers took; a slot left unbound is `Absent`.
#[test]
fn a_unary_node_takes_its_shared_service_in_the_bindings() {
    let events = Arc::new(Events::default());
    let lookup = Arc::new(LookupNode);
    let _bound: contract::Bindings<
        LookupNode,
        NodeB,
        NodeC,
        NodeD,
        NodeE,
        NodeF,
        NodeG,
        gdl_runtime::Absent<contract::nodes::Score>,
    > = contract::Bindings::new()
        .lookup(Arc::clone(&lookup))
        .b(NodeB {
            events: Arc::clone(&events),
        })
        .c(NodeC {
            events: Arc::clone(&events),
        })
        .d(NodeD {
            events: Arc::clone(&events),
        })
        .e(NodeE)
        .f(NodeF)
        .g(NodeG);
}

/// A service whose `ready` reads its pool hub's records: they are bound
/// only once the instance has registered with the hub, so a `ready` run in
/// the build would fail.
struct Loaded {
    records: gdl::PoolRecords,
    loaded: std::sync::atomic::AtomicBool,
}

impl gdl::Build for Loaded {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> gdl::Result<Self> {
        Ok(Self {
            records: gdl::PoolRecords::current("loads")?,
            loaded: std::sync::atomic::AtomicBool::new(false),
        })
    }

    async fn ready(&self) -> gdl::Result<()> {
        self.records.get(b"model").await?;
        self.loaded
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

impl gdl::Unary for Loaded {
    type Req = ();
    type Resp = bool;

    async fn call(&self, _cx: &gdl::Cx, (): ()) -> gdl::Result<bool> {
        Ok(self.loaded.load(std::sync::atomic::Ordering::Acquire))
    }
}

/// Calls `Loaded` in its `ready`, and keeps the answer.
struct Caller {
    loaded: gdl::UnaryClient<(), bool>,
    answered: std::sync::OnceLock<bool>,
}

impl gdl::Build for Caller {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> gdl::Result<Self> {
        Ok(Self {
            loaded: gdl::UnaryClient::current("Loaded")?,
            answered: std::sync::OnceLock::new(),
        })
    }

    async fn ready(&self) -> gdl::Result<()> {
        let loaded = self.loaded.call(&gdl::Cx::new(), ()).await?;
        let _ = self.answered.set(loaded);
        Ok(())
    }
}

/// A node's `ready` calls a service whose `ready` reads the pool hub's
/// records: the service's `ready` runs once, in the readiness phase, after
/// the instance has registered with its pool's hub, and the call waits for
/// it. The services and the readiness are joined as a segment binary joins
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_from_ready_reaches_a_service_whose_ready_reads_pool_records() {
    use common::pools::{
        Resolving, SEGMENTS, global_hub, global_pool, pool_hub_of, settings, spec,
    };
    use gdl::Build;

    let directory = Resolving::new();
    let (hub, _) = pool_hub_of(&spec(), global_hub(), &directory).await;
    let config = settings(
        hub.public_addr().port(),
        hub.control_addr().port(),
        5_000,
        &global_pool(),
    );
    let records = &config.runtime.records;
    let services = gdl::UnaryServices::new(
        "front",
        [("Caller".to_string(), vec!["Loaded".to_string()])],
    );
    let loaded = records
        .scope(services.scope("Loaded", Loaded::build((), ())))
        .await
        .expect("built");
    assert!(
        loaded.records.get(b"model").await.is_err(),
        "the records are not bound before the launch"
    );
    services.serve("Loaded", Arc::new(loaded)).expect("served");
    let caller = records
        .scope(services.scope("Caller", Caller::build((), ())))
        .await
        .expect("built");
    services.built();
    let process = gdl_runtime::Process::start(&config)
        .await
        .expect("the process starts");
    let ready = async {
        tokio::try_join!(caller.ready(), services.ready())?;
        Ok(())
    };
    let _launch =
        tokio::time::timeout(WAIT, config.launch(&process, "a", SEGMENTS, &spec(), ready))
            .await
            .expect("the launch in time")
            .expect("the instance launches");
    assert_eq!(caller.answered.get(), Some(&true), "called once loaded");
}
