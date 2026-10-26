//! What the projection checks can and cannot see. The build checks each
//! field's Rust name and type, and registration compares the names that
//! each type's `Deserialize` reads. Neither sees which names the edge type
//! writes, so the ignored tests below fail: see "A projection is not
//! checked against what its edge type writes" in
//! `docs/src/operations/limitations.md`.

#![deny(warnings)]

use std::marker::PhantomData;

use gdl::{Cell, CellPolicy, CellRequest, Projected, Projection, Selector};
use serde::{Deserialize, Serialize};

struct Route<P>(PhantomData<fn() -> P>);

impl<P: Projection> CellPolicy for Route<P> {
    type Head = Projected<P>;

    fn choose(&self, _: CellRequest<'_, Self::Head>, cells: &[Cell]) -> Option<gdl::CellChoice> {
        (!cells.is_empty()).then(|| gdl::CellChoice::new(0))
    }
}

impl<P: Projection> Selector for Route<P> {
    type Head = Projected<P>;

    async fn select<'a>(
        &'a self,
        _: gdl::Request<'a, Self::Head>,
        instances: gdl::Instances<'a>,
    ) -> gdl::Result<gdl::Choice<'a>> {
        Ok(gdl::Choice::new(instances.get(0).expect("one instance")))
    }
}

fn registrations<P: Projection>() -> [(&'static str, gdl::Result<()>); 2] {
    [
        (
            "policy",
            gdl::CellPolicies::new().add("by_hint", Route::<P>(PhantomData)),
        ),
        (
            "selector",
            gdl::Selectors::new().add("by_hint", Route::<P>(PhantomData)),
        ),
    ]
}

mod renamed {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Routed {
        #[serde(rename = "payload")]
        hint: String,
        #[serde(rename = "hint")]
        body: u64,
    }

    #[derive(Debug, Deserialize, Projection)]
    #[gdl(of = Routed)]
    struct Hinted {
        hint: String,
    }

    #[test]
    fn the_edge_round_trips_but_the_projection_reads_another_field() {
        let request = Routed {
            hint: "llama".to_string(),
            body: 42,
        };
        let direct = Hinted {
            hint: request.hint.clone(),
        };
        assert_eq!(direct.hint, request.hint);
        for encoding in [gdl::Encoding::Json, gdl::Encoding::Msgpack] {
            let bytes = gdl::__runtime::encoder::<Routed>(encoding)(&request).expect("encode");
            let whole = gdl::__runtime::decoder::<Routed>(encoding)(&bytes).expect("round trip");
            assert_eq!(whole, request);
            let error = gdl::__runtime::decoder::<Hinted>(encoding)(&bytes)
                .expect_err("the wire's `hint` is a u64, not Routed::hint's String");
            assert_eq!(error.kind(), gdl::ErrorKind::Codec);
            println!("{encoding:?}: {error}");
        }
    }

    #[test]
    #[ignore = "a name that serde gives another field is not refused: see the limitation"]
    fn another_field_cannot_supply_the_projection_wire_name() {
        the_edge_round_trips_but_the_projection_reads_another_field();
        let results = registrations::<Hinted>();
        assert!(
            results.iter().all(|(_, result)| result.is_err()),
            "the unreadable projection must be refused: {results:?}"
        );
    }
}

mod serialized {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Routed {
        #[serde(rename(serialize = "routing_hint"), alias = "routing_hint")]
        hint: String,
    }

    #[derive(Debug, Deserialize, Projection)]
    #[gdl(of = Routed)]
    struct Hinted {
        hint: String,
    }

    #[derive(Debug, Deserialize, Projection)]
    #[gdl(of = Routed)]
    struct Matched {
        #[serde(rename = "routing_hint")]
        hint: String,
    }

    #[test]
    fn the_deserialize_name_is_absent_from_the_edge_bytes() {
        let request = Routed {
            hint: "llama".to_string(),
        };
        let direct = Hinted {
            hint: request.hint.clone(),
        };
        assert_eq!(direct.hint, request.hint);
        for encoding in [gdl::Encoding::Json, gdl::Encoding::Msgpack] {
            let bytes = gdl::__runtime::encoder::<Routed>(encoding)(&request).expect("encode");
            let whole = gdl::__runtime::decoder::<Routed>(encoding)(&bytes).expect("round trip");
            assert_eq!(whole, request);
            let matched = gdl::__runtime::decoder::<Matched>(encoding)(&bytes)
                .expect("the projection that uses the serialization name");
            assert_eq!(matched.hint, request.hint);
            let error = gdl::__runtime::decoder::<Hinted>(encoding)(&bytes)
                .expect_err("the edge serializes routing_hint, never hint");
            assert_eq!(error.kind(), gdl::ErrorKind::Codec);
            println!("{encoding:?}: {error}");
        }
        let matched = registrations::<Matched>();
        assert!(
            matched.iter().all(|(_, result)| result.is_ok()),
            "{matched:?}"
        );
    }

    #[test]
    #[ignore = "a name the edge type reads but never writes is not refused: see the limitation"]
    fn an_input_only_name_cannot_validate_an_unreadable_projection() {
        the_deserialize_name_is_absent_from_the_edge_bytes();
        let results = registrations::<Hinted>();
        assert!(
            results.iter().all(|(_, result)| result.is_err()),
            "Deserialize names cannot validate a Serialize name: {results:?}"
        );
    }
}

/// A field's type may name the projection itself.
mod recursive {
    use super::*;

    #[derive(Debug, Deserialize)]
    #[allow(dead_code, reason = "the projection reads it")]
    pub struct Routed {
        pub children: Vec<Part>,
        pub body: String,
    }

    #[derive(Debug, PartialEq, Deserialize, Projection)]
    #[gdl(of = Routed)]
    pub struct Part {
        pub children: Vec<Self>,
    }

    #[test]
    fn a_field_of_the_projection_type_builds_and_registers() {
        let bytes = br#"{"children":[{"children":[],"body":"x"}],"body":"y"}"#;
        let part = gdl::__runtime::decoder::<Part>(gdl::Encoding::Json)(bytes).expect("read");
        assert_eq!(part.children, [Part { children: vec![] }]);
        let results = registrations::<Part>();
        assert!(
            results.iter().all(|(_, result)| result.is_ok()),
            "{results:?}"
        );
    }
}

#[test]
fn a_packed_edge_can_be_read_as_a_subset() {
    trybuild::TestCases::new().pass("tests/projection/packed-control.rs");
}

#[test]
fn a_projection_of_a_packed_edge_builds() {
    trybuild::TestCases::new().pass("tests/projection/packed.rs");
}
