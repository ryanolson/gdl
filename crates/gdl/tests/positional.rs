//! Positional codecs and serde representations that do not omit positions.

#[test]
fn a_newtype_does_not_apply_its_fields_conditional_skip() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Optional(#[serde(skip_serializing_if = "Option::is_none")] Option<u32>);

    for (value, expected) in [(Optional(None), vec![0xc0]), (Optional(Some(7)), vec![7])] {
        let bytes = rmp_serde::to_vec(&value).expect("encode a newtype");
        assert_eq!(bytes, expected);
        assert_eq!(
            rmp_serde::from_slice::<Optional>(&bytes).expect("decode a newtype"),
            value
        );
    }
}

#[test]
fn a_newtype_with_no_positional_omission_can_be_a_message() {
    trybuild::TestCases::new().pass("tests/positional/newtype.rs");
}

#[test]
fn a_skipped_variant_cannot_shift_a_carried_variants_fields() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    enum Event {
        Token {
            id: u32,
            text: String,
        },
        #[serde(skip)]
        Internal {
            #[serde(skip_serializing_if = "Option::is_none")]
            cache: Option<u32>,
        },
    }

    let value = Event::Token {
        id: 7,
        text: "token".into(),
    };
    let bytes = rmp_serde::to_vec(&value).expect("encode a carried variant");
    assert_eq!(
        rmp_serde::from_slice::<Event>(&bytes).expect("decode a carried variant"),
        value
    );
    assert!(rmp_serde::to_vec(&Event::Internal { cache: None }).is_err());
}

#[test]
fn a_skipped_variants_fields_do_not_refuse_a_positional_message() {
    trybuild::TestCases::new().pass("tests/positional/skipped.rs");
}

#[test]
fn a_transparent_message_does_not_apply_its_fields_conditional_skip() {
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(transparent)]
    struct Optional {
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<u32>,
    }

    for (value, expected) in [
        (Optional { value: None }, vec![0xc0]),
        (Optional { value: Some(7) }, vec![7]),
    ] {
        let bytes = rmp_serde::to_vec(&value).expect("encode a transparent struct");
        assert_eq!(bytes, expected);
        assert_eq!(
            rmp_serde::from_slice::<Optional>(&bytes).expect("decode a transparent struct"),
            value
        );
    }

    trybuild::TestCases::new().pass("tests/positional/transparent.rs");
}
