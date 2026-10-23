//! The message catalogue: `#[derive(gdl_message::Message)]` registers a type with
//! its package, name, definition path, and wire capability;
//! `#[gdl_message::export]` registers another public path to a message; and
//! `Catalogue::linked` lists every registration in the program.

use gdl_message::{Catalogue, Identity, Message, MessageInfo, Shape};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, gdl_message::Message)]
struct Prompt {
    #[allow(dead_code)]
    text: String,
}

/// Not a wire type: it has no serde impls.
#[derive(gdl_message::Message)]
struct Local;

#[derive(gdl_message::Message)]
#[gdl(codec = "json")]
struct JsonMessage;

#[derive(gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
struct ArrayMessage;

#[test]
fn a_messages_encoding_is_registered_without_requiring_serde() {
    let catalogue = Catalogue::linked();
    assert_eq!(JsonMessage::ENCODING, gdl_model::Encoding::Json);
    assert_eq!(ArrayMessage::ENCODING, gdl_model::Encoding::MsgpackArray);
    assert_eq!(Prompt::ENCODING, gdl_model::Encoding::Msgpack);
    for (name, encoding) in [
        ("JsonMessage", JsonMessage::ENCODING),
        ("ArrayMessage", ArrayMessage::ENCODING),
    ] {
        let message = find(&catalogue, name)[0];
        assert_eq!(message.encoding, encoding);
        assert!(!message.wire);
    }
}

mod inner {
    use serde::{Deserialize, Serialize};

    /// Words, in a module, exported at the root under another name.
    #[derive(Serialize, Deserialize, gdl_message::Message)]
    pub enum Words {
        /// One word.
        #[allow(dead_code)]
        One(String),
    }
}

mod hidden {
    /// Defined in a private module, offered by an export.
    #[derive(gdl_message::Message)]
    pub struct Piece;
}

/// An export: the root path is public, though the definition is not.
#[gdl_message::export]
pub use hidden::Piece;

/// An export under another name.
#[gdl_message::export]
pub use inner::Words as Prompted;

/// A re-export is the same type, not another message.
#[allow(unused_imports)]
use inner::Words as Reexported;

/// Spelled raw, named plainly: a graph names it `Raw`.
#[derive(gdl_message::Message)]
struct r#Raw;

/// A module spelled raw: `module_path!` keeps the `r#`, and the
/// catalogue drops it, as the source reader does.
pub mod r#type {
    /// In a keyword module.
    #[derive(gdl_message::Message)]
    pub struct Kept;
}

/// An export of a message in a keyword module.
#[gdl_message::export]
pub use r#type::Kept as Typed;

mod selfish {
    /// Exported as `{self}`.
    #[derive(gdl_message::Message)]
    pub struct Selfish;
}

/// `{self}` binds the name before it.
#[gdl_message::export]
pub use selfish::Selfish::{self};

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
            encoding: Default::default(),
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
    let prompt = Identity {
        package: "gdl-message",
        version: env!("CARGO_PKG_VERSION"),
        dir: env!("CARGO_MANIFEST_DIR"),
        path: "catalogue::Prompt",
    };
    assert_eq!(<Prompt as Message>::NAME, "Prompt");
    assert_eq!(<Prompt as Message>::IDENTITY, prompt);
    assert_eq!(
        <inner::Words as Message>::IDENTITY.path,
        "catalogue::inner::Words"
    );
    assert_eq!(<Raw as Message>::NAME, "Raw");
    const { assert!(!<Prompt as Shape>::STREAMED) };
    assert_eq!(
        (<Prompt as Shape>::HEAD, <Prompt as Shape>::ITEM),
        (Some(prompt), None)
    );
    assert_eq!((<() as Shape>::HEAD, <() as Shape>::ITEM), (None, None));
}

/// An export is registered under its public path, with the identity of the
/// message it offers, whatever name it is offered under.
#[test]
fn an_export_registers_its_public_path_and_origin() {
    let catalogue = Catalogue::linked();
    let exports: Vec<(&str, &str, &str, &str)> = catalogue
        .exports
        .iter()
        .map(|e| {
            (
                e.name.as_str(),
                e.path.as_str(),
                e.package.as_str(),
                e.origin.path.as_str(),
            )
        })
        .collect();
    assert_eq!(
        exports,
        [
            (
                "Piece",
                "catalogue::Piece",
                "gdl-message",
                "catalogue::hidden::Piece"
            ),
            (
                "Prompted",
                "catalogue::Prompted",
                "gdl-message",
                "catalogue::inner::Words"
            ),
            (
                "Selfish",
                "catalogue::Selfish",
                "gdl-message",
                "catalogue::selfish::Selfish"
            ),
            (
                "Typed",
                "catalogue::Typed",
                "gdl-message",
                "catalogue::type::Kept"
            ),
        ]
    );
    assert_eq!(catalogue.exports[0].origin.package, "gdl-message");
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

/// Paths are written without `r#`, whatever `module_path!` gives: the
/// source reader writes them so, and the generator escapes each keyword
/// where it writes a path in code.
#[test]
fn a_raw_module_is_registered_by_its_plain_name() {
    let catalogue = Catalogue::linked();
    let kept = find(&catalogue, "Kept");
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].path, "catalogue::type::Kept");
    let typed = catalogue
        .exports
        .iter()
        .find(|e| e.name == "Typed")
        .expect("exported");
    assert_eq!(typed.origin.path, "catalogue::type::Kept");
    assert_eq!(
        <r#type::Kept as Message>::IDENTITY.path,
        "catalogue::r#type::Kept",
        "the identity keeps the compiler's spelling; the catalogue drops it"
    );
}
