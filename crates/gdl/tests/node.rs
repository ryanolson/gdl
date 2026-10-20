//! `#[derive(gdl::Node)]`: a node type registers each role trait it
//! implements, with the messages at its ports, read through the role
//! trait's associated types; and `gdl::Message` works in a crate that names
//! only gdl.

use gdl::{
    Backward, Catalogue, Cx, Error, Forward, Items, Next, Operator, Result, Sink, Streamed, items,
};

/// Words.
#[derive(gdl::Message)]
pub struct Words;

/// A token.
#[derive(gdl::Message)]
pub struct Token;

/// A piece of text.
#[derive(gdl::Message)]
pub struct Piece;

mod engine {
    use super::*;

    /// A sink, in a module, re-exported at the root.
    #[derive(gdl::Node)]
    pub struct Engine;

    impl Sink for Engine {
        type Req = Words;
        type Item = Token;

        async fn generate(&self, _cx: Cx, _req: Words) -> Result<Items<Token>> {
            Err(Error::node("a fixture"))
        }
    }
}

pub use engine::Engine;

/// A forward node with streamed requests, with a head and without one.
#[derive(gdl::Node)]
pub struct Resample;

impl Forward for Resample {
    type In = Streamed<Words, Token>;
    type Out = Streamed<(), Token>;

    async fn forward(&self, _cx: &Cx, _req: Self::In) -> Result<Self::Out> {
        Err(Error::node("a fixture"))
    }
}

/// A backward node.
#[derive(gdl::Node)]
pub struct Detokenize;

impl Backward for Detokenize {
    type In = Token;
    type Out = Piece;

    fn backward(&self, _cx: &Cx, _input: Items<Token>) -> Items<Piece> {
        items(futures::stream::empty())
    }
}

/// An operator.
#[derive(gdl::Node)]
pub struct Gate;

impl Operator for Gate {
    type UpReq = Words;
    type UpItem = Piece;
    type DownReq = Words;
    type DownItem = Token;

    async fn generate<N>(&self, _cx: Cx, _req: Words, _next: N) -> Result<Items<Piece>>
    where
        N: Next<Words, Token>,
    {
        Err(Error::node("a fixture"))
    }
}

/// A type that implements two roles: registered with both, for the reader
/// and `--verify` to refuse.
#[derive(gdl::Node)]
pub struct Both;

impl Sink for Both {
    type Req = Words;
    type Item = Token;

    async fn generate(&self, _cx: Cx, _req: Words) -> Result<Items<Token>> {
        Err(Error::node("a fixture"))
    }
}

impl Backward for Both {
    type In = Token;
    type Out = Token;

    fn backward(&self, _cx: &Cx, input: Items<Token>) -> Items<Token> {
        input
    }
}

/// A sink whose request is not a message: no role is found.
#[derive(gdl::Node)]
pub struct Plain;

impl Sink for Plain {
    type Req = String;
    type Item = Token;

    async fn generate(&self, _cx: Cx, _req: String) -> Result<Items<Token>> {
        Err(Error::node("a fixture"))
    }
}

/// A stream nested in a streamed request's head.
#[derive(gdl::Node)]
pub struct Nested;

impl Sink for Nested {
    type Req = Streamed<Streamed<Words, Token>, Token>;
    type Item = Token;

    async fn generate(&self, _cx: Cx, _req: Self::Req) -> Result<Items<Token>> {
        Err(Error::node("a fixture"))
    }
}

fn summary(catalogue: &Catalogue) -> Vec<String> {
    // The type's last path segment, `()` for none.
    let short = |path: Option<&str>| {
        path.map_or("()".to_string(), |p| {
            p.rsplit("::").next().unwrap_or("").to_string()
        })
    };
    catalogue
        .nodes
        .iter()
        .map(|n| {
            let roles: Vec<String> = n
                .roles
                .iter()
                .map(|r| {
                    let ports: Vec<String> = r
                        .ports
                        .iter()
                        .map(|p| {
                            let nested = if p.nested { " nested" } else { "" };
                            if p.streamed {
                                format!(
                                    "{}={} + stream {}{nested}",
                                    p.port,
                                    short(p.head.as_ref().map(|o| o.path.as_str())),
                                    short(p.item.as_ref().map(|o| o.path.as_str()))
                                )
                            } else {
                                format!(
                                    "{}={}",
                                    p.port,
                                    short(p.head.as_ref().map(|o| o.path.as_str()))
                                )
                            }
                        })
                        .collect();
                    format!("{}({})", r.role, ports.join(", "))
                })
                .collect();
            format!("{} {}: {}", n.name, n.path, roles.join(" "))
        })
        .collect()
}

#[test]
fn a_node_registers_its_roles_and_ports() {
    let catalogue = Catalogue::linked();
    assert_eq!(
        summary(&catalogue),
        [
            "Both node::Both: backward(In=Token, Out=Token) sink(Req=Words, Item=Token)",
            "Detokenize node::Detokenize: backward(In=Token, Out=Piece)",
            "Engine node::Engine: sink(Req=Words, Item=Token)",
            "Gate node::Gate: operator(UpReq=Words, UpItem=Piece, DownReq=Words, DownItem=Token)",
            "Nested node::Nested: sink(Req=Words + stream Token nested, Item=Token)",
            "Plain node::Plain: ",
            "Resample node::Resample: forward(In=Words + stream Token, Out=() + stream Token)",
        ]
    );
    let engine = catalogue
        .nodes
        .iter()
        .find(|n| n.name == "Engine")
        .expect("Engine");
    let words = engine.roles[0].ports[0].head.as_ref().expect("Words");
    assert_eq!(
        (words.package.as_str(), words.path.as_str()),
        ("gdl", "node::Words")
    );
}

/// `gdl::Message` registers through gdl: this crate names no other.
#[test]
fn a_message_derived_through_gdl_is_registered() {
    let catalogue = Catalogue::linked();
    let words = catalogue
        .messages
        .iter()
        .find(|m| m.path == "node::Words")
        .expect("Words");
    assert_eq!(words.package, "gdl");
}
