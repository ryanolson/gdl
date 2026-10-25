//! Feasibility spike for the typed graph structure.
//!
//! One generic dispatcher, written once, must:
//! - accept a bindings struct whose slots are differently typed `Absent<N>`
//!   placeholders or real implementations, so a segment names only the
//!   implementations it hosts;
//! - choose at run time, from a placement, whether an edge is a local call or
//!   a crossing;
//! - send a crossing through the edge's codec as bytes, without the generic
//!   code ever needing a serde bound;
//! - reject a crossing over an edge whose type has no codec.
//!
//! The crossing here is a loopback (encode, then decode); the velo runtime
//! replaces it, and nothing else.

mod common;

use std::sync::Arc;

use common::*;
use futures::StreamExt;
use gdl::{Backward, Cx, Error, ErrorKind, Forward, Items, Result, Sink, items};
use gdl_runtime::{Absent, BackwardDecl, Codec, ForwardDecl, NodeDecl, Role, SinkDecl, wire_codec};

// ----- declarations: what a graph definition generates -----

struct D;
impl NodeDecl for D {
    const NAME: &'static str = "D";
    const ROLE: Role = Role::Sink;
}
impl SinkDecl for D {
    type Req = Pre;
    type Item = Raw;
}

struct F;
impl NodeDecl for F {
    const NAME: &'static str = "F";
    const ROLE: Role = Role::Backward;
}
impl BackwardDecl for F {
    type In = Raw;
    type Out = Tok;
}

struct E;
impl NodeDecl for E {
    const NAME: &'static str = "E";
    const ROLE: Role = Role::Backward;
}
impl BackwardDecl for E {
    type In = Tok;
    type Out = Out;
}

/// A forward node whose output is not serializable.
struct L;
impl NodeDecl for L {
    const NAME: &'static str = "L";
    const ROLE: Role = Role::Forward;
}
impl ForwardDecl for L {
    type In = Pre;
    type Out = Local;
}

/// A message with no serde impls: it may only travel on a local edge.
struct Local(String);

// ----- bindings and codecs -----

/// The bindings typestate: each slot holds an implementation or `Absent`.
struct Slots<DS, FS, ES, LS> {
    d: Arc<DS>,
    f: Arc<FS>,
    e: Arc<ES>,
    l: Arc<LS>,
}

/// Per-edge codecs, created where every type is concrete.
struct Codecs {
    d_to_f: Option<Codec<Raw>>,
    f_to_e: Option<Codec<Tok>>,
    l_to_d: Option<Codec<Local>>,
}

fn codecs() -> Codecs {
    Codecs {
        d_to_f: wire_codec!(Raw),
        f_to_e: wire_codec!(Tok),
        l_to_d: wire_codec!(Local),
    }
}

/// Which edges this placement cuts.
#[derive(Clone, Copy, Default)]
struct Cuts {
    d_to_f: bool,
    f_to_e: bool,
    l_to_d: bool,
}

// ----- the generic dispatcher -----

/// Run L, D, F, E with each edge local or crossed according to `cuts`. The
/// function is generic over every slot and has no serde bound anywhere.
async fn dispatch<DS, FS, ES, LS>(
    slots: &Slots<DS, FS, ES, LS>,
    codecs: &Codecs,
    cuts: Cuts,
    cx: Cx,
    req: Pre,
) -> Result<Items<Out>>
where
    DS: Sink<Req = Pre, Item = Raw>,
    FS: Backward<In = Raw, Out = Tok>,
    ES: Backward<In = Tok, Out = Out>,
    LS: Forward<In = Pre, Out = Local>,
{
    let local = slots.l.forward(&cx, req).await?;
    let Local(text) = cross_value(cuts.l_to_d, codecs.l_to_d, local)?;
    let raw = slots
        .d
        .generate(
            cx.clone(),
            Pre {
                text,
                attempt: 0,
                plan: Plan {
                    tokens: 3,
                    ..Plan::default()
                },
            },
        )
        .await?;
    let raw = cross_stream(cuts.d_to_f, codecs.d_to_f, raw)?;
    let tok = cross_stream(cuts.f_to_e, codecs.f_to_e, slots.f.backward(&cx, raw))?;
    Ok(slots.e.backward(&cx, tok))
}

/// One branch per boundary: local passes through, a cut goes through bytes.
fn cross_value<T>(cut: bool, codec: Option<Codec<T>>, value: T) -> Result<T> {
    if !cut {
        return Ok(value);
    }
    let codec = codec.ok_or_else(not_wire)?;
    codec.decode(&codec.encode(&value)?)
}

fn cross_stream<T: Send + 'static>(
    cut: bool,
    codec: Option<Codec<T>>,
    input: Items<T>,
) -> Result<Items<T>> {
    if !cut {
        return Ok(input);
    }
    let codec = codec.ok_or_else(not_wire)?;
    Ok(items(input.map(move |item| {
        let item = item?;
        let bytes: Vec<u8> = codec.encode(&item)?;
        codec.decode(&bytes)
    })))
}

fn not_wire() -> Error {
    Error::new(ErrorKind::Placement, "edge has no codec")
}

// ----- implementations -----

struct ToLocal;

impl Forward for ToLocal {
    type In = Pre;
    type Out = Local;

    async fn forward(&self, _cx: &Cx, req: Pre) -> Result<Local> {
        Ok(Local(req.text))
    }
}

fn full_slots(events: &Arc<Events>) -> Slots<NodeD, NodeF, NodeE, ToLocal> {
    Slots {
        d: Arc::new(NodeD {
            events: Arc::clone(events),
        }),
        f: Arc::new(NodeF),
        e: Arc::new(NodeE),
        l: Arc::new(ToLocal),
    }
}

fn pre(text: &str) -> Pre {
    Pre {
        text: text.to_string(),
        attempt: 0,
        plan: Plan::default(),
    }
}

fn expected_out() -> Vec<Result<Out>> {
    (0..3).map(|i| Ok(Out(format!("e(f{i})")))).collect()
}

#[tokio::test]
async fn local_and_crossed_edges_give_the_same_items() {
    let events = Events::new();
    let slots = full_slots(&events);
    let codecs = codecs();
    for cuts in [
        Cuts::default(),
        Cuts {
            d_to_f: true,
            ..Cuts::default()
        },
        Cuts {
            d_to_f: true,
            f_to_e: true,
            ..Cuts::default()
        },
    ] {
        let out = dispatch(&slots, &codecs, cuts, Cx::new(), pre("s"))
            .await
            .expect("dispatch");
        assert_eq!(collect(out).await, expected_out());
    }
}

#[tokio::test]
async fn absent_slots_of_different_port_types_compile_and_report_not_hosted() {
    // This segment hosts D only; F and E are absent with different port
    // types (`Raw -> Tok` and `Tok -> Out`), L is absent too.
    let events = Events::new();
    let slots: Slots<NodeD, Absent<F>, Absent<E>, Absent<L>> = Slots {
        d: Arc::new(NodeD {
            events: Arc::clone(&events),
        }),
        f: Arc::new(Absent::new()),
        e: Arc::new(Absent::new()),
        l: Arc::new(Absent::new()),
    };
    let error = match dispatch(&slots, &codecs(), Cuts::default(), Cx::new(), pre("a")).await {
        Ok(_) => panic!("L is not hosted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::NotHosted);
    assert!(error.message().contains("`L`"), "{error}");
}

#[tokio::test]
async fn a_segment_without_the_sink_names_only_its_own_implementations() {
    let slots: Slots<Absent<D>, NodeF, NodeE, ToLocal> = Slots {
        d: Arc::new(Absent::new()),
        f: Arc::new(NodeF),
        e: Arc::new(NodeE),
        l: Arc::new(ToLocal),
    };
    let error = match dispatch(&slots, &codecs(), Cuts::default(), Cx::new(), pre("b")).await {
        Ok(_) => panic!("D is not hosted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::NotHosted);
    assert!(error.message().contains("`D`"), "{error}");
}

#[tokio::test]
async fn a_local_non_serde_edge_works_and_cutting_it_is_rejected() {
    let events = Events::new();
    let slots = full_slots(&events);
    let codecs = codecs();
    assert!(codecs.l_to_d.is_none());

    let out = dispatch(&slots, &codecs, Cuts::default(), Cx::new(), pre("n"))
        .await
        .expect("local edge");
    assert_eq!(collect(out).await, expected_out());

    let cut = Cuts {
        l_to_d: true,
        ..Cuts::default()
    };
    let error = match dispatch(&slots, &codecs, cut, Cx::new(), pre("n")).await {
        Ok(_) => panic!("cutting a non-wire edge must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Placement);
}
