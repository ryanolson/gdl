//! The message catalogue: `#[derive(gdl_message::Message)]` registers a type with
//! its package, name, definition path, and wire capability;
//! `#[gdl_message::export]` registers another public path to a message, or
//! a message for each instantiation of a generic one that a `pub type`
//! names; and `Catalogue::linked` lists every registration in the program.

use gdl_message::{Catalogue, Identity, Message, MessageInfo, Shape};
use serde::{Deserialize, Serialize};

/// A prompt: a wire type.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Prompt {
    #[allow(dead_code)]
    text: String,
}

/// Not a wire type: it has no serde impls.
#[derive(gdl_message::Message)]
pub struct Local;

#[derive(gdl_message::Message)]
#[gdl(codec = "json")]
struct JsonMessage;

#[derive(gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
struct ArrayMessage;

/// A field both serde directions skip keeps every position, so
/// `msgpack_array` takes it; the refused omissions are in
/// `tests/ui/message-positional-omission.rs`.
#[derive(Serialize, Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
#[allow(dead_code)]
struct ArraySkips {
    id: u32,
    #[serde(skip)]
    cache: Option<u32>,
    #[serde(skip_serializing, skip_deserializing)]
    scratch: Option<u32>,
    #[cfg_attr(any(), serde(skip_serializing_if = "Option::is_none"))]
    hint: Option<u32>,
}

/// A named codec writes each field's name, so an omitted field shifts
/// nothing.
#[derive(Serialize, Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack")]
#[allow(dead_code)]
struct NamedOmits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hint: Option<u32>,
    #[serde(skip_serializing)]
    secret: u32,
}

#[derive(Serialize, Deserialize, gdl_message::Message)]
#[gdl(codec = "json")]
#[allow(dead_code)]
enum JsonOmits {
    Token {
        #[serde(skip_deserializing)]
        logprob: f32,
    },
}

#[test]
fn a_messages_encoding_is_registered_without_requiring_serde() {
    let catalogue = Catalogue::linked();
    assert_eq!(JsonMessage::ENCODING, gdl_model::Encoding::Json);
    assert_eq!(ArrayMessage::ENCODING, gdl_model::Encoding::MsgpackArray);
    assert_eq!(Prompt::ENCODING, gdl_model::Encoding::Msgpack);
    assert_eq!(ArraySkips::ENCODING, gdl_model::Encoding::MsgpackArray);
    assert_eq!(NamedOmits::ENCODING, gdl_model::Encoding::Msgpack);
    assert_eq!(JsonOmits::ENCODING, gdl_model::Encoding::Json);
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

/// A generic carrier: one definition, and a message for each
/// instantiation that an exported alias names. Its codec is every
/// instantiation's.
#[derive(Serialize, Deserialize, gdl_message::Message)]
#[gdl(codec = "json")]
pub struct Carried<P, B> {
    /// What it carries.
    pub payload: P,
    /// How it is built.
    pub builder: B,
}

/// An instantiation whose arguments are wire types: a wire type.
#[gdl_message::export]
pub type WiredCarry = Carried<Prompt, inner::Words>;

/// An instantiation with an argument that has no serde impls: local.
#[gdl_message::export]
pub type LocalCarry = Carried<Prompt, Local>;

/// The same instantiation named again, by an alias of an alias: one
/// message offered under two names, as two exports of one message are.
#[gdl_message::export]
pub type WiredAgain = WiredCarry;

/// An instantiation exported by `pub use` of its alias.
#[gdl_message::export]
pub use LocalCarry as LocalAgain;

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
        (<Prompt as Shape>::head(), <Prompt as Shape>::item()),
        (Some(prompt), None)
    );
    assert_eq!((<() as Shape>::head(), <() as Shape>::item()), (None, None));
}

/// An instantiation of a generic message is identified by its generic
/// type's package and its type, each argument by its definition path,
/// however it is spelled: an alias and the type it names are one message.
#[test]
fn an_instantiation_states_its_identity_by_its_type() {
    let wired = Identity {
        package: "gdl-message",
        version: env!("CARGO_PKG_VERSION"),
        dir: env!("CARGO_MANIFEST_DIR"),
        path: "catalogue::Carried<catalogue::Prompt, catalogue::inner::Words>",
    };
    assert_eq!(<WiredCarry as Shape>::head(), Some(wired));
    assert_eq!(<Carried<Prompt, Reexported> as Shape>::head(), Some(wired));
    assert_eq!(<WiredAgain as Shape>::item(), None);
    const { assert!(!<WiredCarry as Shape>::STREAMED) };
    assert_eq!(
        gdl_message::message_encoding!(WiredCarry),
        gdl_model::Encoding::Json,
        "an instantiation takes its generic type's codec"
    );
}

/// The generic type is not in the catalogue: only an instantiation that
/// an alias names is, as an export of that instantiation under the
/// alias's path, with its own wire capability and its generic type's
/// codec.
#[test]
fn an_alias_registers_an_instantiation_under_its_path() {
    let catalogue = Catalogue::linked();
    assert!(
        find(&catalogue, "Carried").is_empty(),
        "{:?}",
        catalogue.messages
    );
    assert!(
        catalogue.messages.iter().all(|m| !m.path.contains('<')),
        "{:?}",
        catalogue.messages
    );
    let export = |name: &str| {
        catalogue
            .exports
            .iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("{name} in {:?}", catalogue.exports))
    };
    let wired = "catalogue::Carried<catalogue::Prompt, catalogue::inner::Words>";
    let local = "catalogue::Carried<catalogue::Prompt, catalogue::Local>";
    for (name, path, origin, wire) in [
        ("WiredCarry", "catalogue::WiredCarry", wired, true),
        ("WiredAgain", "catalogue::WiredAgain", wired, true),
        ("LocalCarry", "catalogue::LocalCarry", local, false),
        ("LocalAgain", "catalogue::LocalAgain", local, false),
    ] {
        let e = export(name);
        assert_eq!(
            (e.path.as_str(), e.origin.path.as_str(), e.wire, e.encoding),
            (path, origin, wire, gdl_model::Encoding::Json),
            "{name}"
        );
        assert_eq!(e.origin.package, "gdl-message");
        assert_eq!(e.origin.dir, env!("CARGO_MANIFEST_DIR"));
    }
}

/// An export is registered under its public path, with the identity of the
/// message it offers, whatever name it is offered under.
#[test]
fn an_export_registers_its_public_path_and_origin() {
    let catalogue = Catalogue::linked();
    // The instantiations an alias names are below.
    let exports: Vec<(&str, &str, &str, &str)> = catalogue
        .exports
        .iter()
        .filter(|e| !e.origin.path.contains('<'))
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
    // An export carries its message's wire capability and codec.
    let wire: Vec<(&str, bool)> = catalogue
        .exports
        .iter()
        .filter(|e| !e.origin.path.contains('<'))
        .map(|e| (e.name.as_str(), e.wire))
        .collect();
    assert_eq!(
        wire,
        [
            ("Piece", false),
            ("Prompted", true),
            ("Selfish", false),
            ("Typed", false)
        ]
    );
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
