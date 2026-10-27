//! Positional omissions depend on the serde representation and every variant.

use gdl_extract::{Cargo, Extraction, Workspace};

fn read(source: &str) -> Extraction {
    let temp = tempfile::Builder::new()
        .prefix("gdl-positional-")
        .tempdir()
        .expect("a scratch crate");
    let dir = temp.path();
    std::fs::create_dir(dir.join("src")).expect("source directory");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"positional\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .expect("manifest");
    std::fs::write(dir.join("src/lib.rs"), source).expect("source");
    Workspace::load(&dir.join("Cargo.toml"), &Cargo::trusted())
        .expect("metadata")
        .extract("positional")
        .expect("the package")
}

#[test]
fn the_reader_accepts_a_newtype_without_a_skip_attribute() {
    let found = read(
        r#"
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub struct Optional(Option<u32>);
"#,
    );
    assert!(found.problems.is_empty(), "{found:?}");
    assert_eq!(found.offer.messages.len(), 1);
}

#[test]
fn the_reader_accepts_a_newtype_whose_serde_representation_has_no_omission() {
    let found = read(
        r#"
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub struct Optional(#[serde(skip_serializing_if = "Option::is_none")] Option<u32>);
"#,
    );
    assert!(found.problems.is_empty(), "{found:?}");
    assert_eq!(found.offer.messages.len(), 1);
}

#[test]
fn the_reader_does_not_check_fields_of_a_serde_skipped_variant() {
    let found = read(
        r#"
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub enum Event {
    Token { id: u32, text: String },
    #[serde(skip)]
    Internal {
        #[serde(skip_serializing_if = "Option::is_none")]
        cache: Option<u32>,
    },
}
"#,
    );
    assert!(found.problems.is_empty(), "{found:?}");
    assert_eq!(found.offer.messages.len(), 1);
}

const BORROWED: &str = r#"
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
pub enum Event {
    Text {
        #[serde(borrow)]
        text: std::borrow::Cow<'static, str>,
    },
    Token {
        id: u32,
        #[serde(skip_serializing)]
        score: u32,
    },
}
#[gdl_message::export]
pub use Event as Exported;
"#;

#[test]
fn an_ordinary_omission_is_refused_with_its_export() {
    let control = read(&BORROWED.replace("#[serde(borrow)]", ""));
    assert!(control.problems.iter().any(|p| p.contains("Token::score")));
    assert!(control.offer.messages.is_empty());
    assert!(control.offer.exports.is_empty());
}

#[test]
fn an_omission_after_a_borrowed_variant_is_still_refused() {
    let borrowed = read(BORROWED);
    assert!(
        borrowed.problems.iter().any(|p| p.contains("Token::score")),
        "a borrowed variant hid the omission: {borrowed:?}"
    );
    assert!(borrowed.offer.messages.is_empty());
    assert!(borrowed.offer.exports.is_empty());
}

#[test]
fn the_reader_accepts_a_transparent_message() {
    let found = read(
        r#"
#[derive(serde::Serialize, serde::Deserialize, gdl_message::Message)]
#[gdl(codec = "msgpack_array")]
#[serde(transparent)]
pub struct Optional {
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<u32>,
}
"#,
    );
    assert!(found.problems.is_empty(), "{found:?}");
    assert_eq!(found.offer.messages.len(), 1);
}
