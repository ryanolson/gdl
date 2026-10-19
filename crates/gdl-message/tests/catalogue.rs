//! The message catalogue: `#[derive(gdl_message::Message)]` registers a type with
//! its package, name, definition path, and wire capability, and
//! `Catalogue::linked` lists every registered type in the program.

use gdl_message::{Catalogue, Message, MessageInfo, Shape};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, gdl_message::Message)]
struct Prompt {
    #[allow(dead_code)]
    text: String,
}

/// Not a wire type: it has no serde impls.
#[derive(gdl_message::Message)]
struct Local;

mod inner {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, gdl_message::Message)]
    pub enum Words {
        #[allow(dead_code)]
        One(String),
    }
}

/// A re-export is the same type, not another message.
#[allow(unused_imports)]
use inner::Words as Reexported;

/// Spelled raw, named plainly: a graph names it `Raw`.
#[derive(gdl_message::Message)]
struct r#Raw;

/// Not derived: not in the catalogue.
#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
struct Unregistered;

/// Another registry in the same program, with the name a registry would
/// naturally take. linkme names a slice's linker section by the static's
/// name alone, so the catalogue's must not be a common one.
// linkme places the slice in a linker section; nothing here is unsafe.
#[allow(unsafe_code)]
#[linkme::distributed_slice]
static MESSAGES: [&'static str];

#[allow(unsafe_code)]
#[linkme::distributed_slice(MESSAGES)]
static GREETING: &str = "hello";

fn find<'a>(catalogue: &'a Catalogue, name: &str) -> Vec<&'a MessageInfo> {
    catalogue
        .messages
        .iter()
        .filter(|m| m.name == name)
        .collect()
}

#[test]
fn a_derived_message_is_registered_with_its_definition_path() {
    let catalogue = Catalogue::linked();
    let prompt = find(&catalogue, "Prompt");
    assert_eq!(
        prompt,
        [&MessageInfo {
            package: "gdl-message".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            dir: env!("CARGO_MANIFEST_DIR").into(),
            name: "Prompt".into(),
            path: "catalogue::Prompt".into(),
            wire: true,
        }]
    );
    let words = find(&catalogue, "Words");
    assert_eq!(words.len(), 1, "a re-export registers nothing: {words:?}");
    assert_eq!(words[0].path, "catalogue::inner::Words");
    assert!(find(&catalogue, "Reexported").is_empty());
    let raw = find(&catalogue, "Raw");
    assert_eq!(raw.len(), 1, "{:?}", catalogue.messages);
    assert_eq!(raw[0].path, "catalogue::Raw");
    assert!(find(&catalogue, "Unregistered").is_empty());
}

#[test]
fn another_registry_named_like_the_catalogue_is_independent() {
    assert_eq!(MESSAGES[..], ["hello"]);
    assert_eq!(find(&Catalogue::linked(), "Prompt").len(), 1);
}

/// The derive's `Message` constants are the catalogue entry's identity, and
/// its `Shape` is one plain message.
#[test]
fn a_message_states_its_identity_and_shape() {
    assert_eq!(<Prompt as Message>::NAME, "Prompt");
    assert_eq!(<Prompt as Message>::PACKAGE, "gdl-message");
    assert_eq!(<inner::Words as Message>::PATH, "catalogue::inner::Words");
    assert_eq!(<Raw as Message>::NAME, "Raw");
    assert_eq!(
        (<Prompt as Shape>::HEAD, <Prompt as Shape>::ITEM),
        ("Prompt", None)
    );
    assert_eq!((<() as Shape>::HEAD, <() as Shape>::ITEM), ("()", None));
}

#[test]
fn the_catalogue_records_wire_capability() {
    let catalogue = Catalogue::linked();
    assert!(find(&catalogue, "Prompt")[0].wire);
    assert!(!find(&catalogue, "Local")[0].wire);
}

/// The generator reads the catalogue from a program's output, as TOML, and
/// gets it back exactly; entries come in a fixed order whatever the link
/// order was.
#[test]
fn the_catalogue_round_trips_through_toml_in_a_fixed_order() {
    let catalogue = Catalogue::linked();
    let mut sorted = catalogue.messages.clone();
    sorted.sort();
    assert_eq!(catalogue.messages, sorted);
    let back = Catalogue::from_toml(&catalogue.to_toml()).expect("catalogue");
    assert_eq!(back, catalogue);
}
