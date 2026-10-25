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

/// A node whose build registers metrics, though its derive does not name
/// them: the catalogue still shows what the build registers, so
/// `extract --verify` sees the source reader miss them.
#[derive(gdl::Node)]
pub struct Measured;

/// What `Measured` reports.
#[derive(gdl::Metrics)]
#[expect(
    dead_code,
    reason = "registered, never counted: only the catalogue is read"
)]
pub struct MeasuredMetrics {
    /// Words passed.
    passed_total: gdl::prometheus::IntCounter,
}

impl gdl::Build for Measured {
    type Config = ();
    type Metrics = MeasuredMetrics;
    async fn build((): (), _metrics: MeasuredMetrics) -> Result<Self> {
        Ok(Self)
    }
}

impl Sink for Measured {
    type Req = Words;
    type Item = Token;
    async fn generate(&self, _cx: Cx, _req: Words) -> Result<Items<Token>> {
        Err(Error::node("unused"))
    }
}

/// A generic carrier: an instantiation is a message where an alias names
/// it.
#[derive(serde::Serialize, serde::Deserialize, gdl::Message)]
#[gdl(codec = "json")]
pub struct Carried<P, B> {
    /// What it carries.
    pub payload: P,
    /// How it is built.
    pub builder: B,
}

/// The settings of every `Relay`.
#[derive(serde::Deserialize, gdl::Config)]
pub struct RelaySettings {
    /// Whether it relays at all.
    #[gdl(graph, default = true, label = "Enabled", subtext = "Relays")]
    #[allow(dead_code)]
    pub enabled: bool,
}

/// What every `Relay` reports.
#[derive(gdl::Metrics)]
#[expect(
    dead_code,
    reason = "registered, never counted: only the catalogue is read"
)]
pub struct RelayMetrics {
    /// Requests relayed.
    relayed_total: gdl::prometheus::IntCounter,
}

/// A generic forward node, written once: each instantiation that an alias
/// names is a node of the alias's name.
#[derive(gdl::Node)]
#[gdl(config = RelaySettings, metrics = RelayMetrics)]
pub struct Relay<B>(std::marker::PhantomData<fn() -> B>);

impl<B: Send + Sync + 'static> gdl::Build for Relay<B> {
    type Config = RelaySettings;
    type Metrics = RelayMetrics;
    async fn build(_config: RelaySettings, _metrics: RelayMetrics) -> Result<Self> {
        Ok(Self(std::marker::PhantomData))
    }
}

impl<B: Send + Sync + 'static> Forward for Relay<B> {
    type In = Carried<Words, B>;
    type Out = Carried<Words, B>;

    async fn forward(&self, _cx: &Cx, req: Self::In) -> Result<Self::Out> {
        Ok(req)
    }
}

/// One instantiation of the node.
#[gdl::export]
pub type TokenRelay = Relay<Token>;

/// Another, of the same implementation.
#[gdl::export]
pub type PieceRelay = Relay<Piece>;

/// The carrier its ports carry, named for the graph.
#[gdl::export]
pub type TokenCarried = Carried<Words, Token>;

fn summary(catalogue: &Catalogue) -> Vec<String> {
    // The type without this crate's paths, `()` for none.
    let short = |path: Option<&str>| path.map_or("()".to_string(), |p| p.replace("node::", ""));
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
            "Measured node::Measured: sink(Req=Words, Item=Token)",
            "Nested node::Nested: sink(Req=Words + stream Token nested, Item=Token)",
            "Plain node::Plain: ",
            "Resample node::Resample: forward(In=Words + stream Token, Out=() + stream Token)",
            // A node with graph configuration sorts after those without.
            "PieceRelay node::PieceRelay: forward(In=Carried<Words, Piece>, Out=Carried<Words, Piece>)",
            "TokenRelay node::TokenRelay: forward(In=Carried<Words, Token>, Out=Carried<Words, Token>)",
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

/// A node's metrics are registered from its `Build`, named or not.
#[test]
fn a_node_registers_the_metrics_its_build_registers() {
    let catalogue = Catalogue::linked();
    let metrics = |name: &str| -> Vec<String> {
        catalogue
            .nodes
            .iter()
            .find(|n| n.name == name)
            .expect(name)
            .metrics
            .iter()
            .map(|m| m.name.clone())
            .collect()
    };
    assert_eq!(metrics("Measured"), ["passed_total"]);
    assert!(metrics("Engine").is_empty(), "no Build, no metrics");
}

/// An instantiation of a generic node is registered under its alias, with
/// its generic type's configuration and the metrics its build registers,
/// and its ports carry the instantiation of the message, by type: the
/// carrier alias offers the same message.
#[test]
fn an_alias_registers_an_instantiation_of_a_generic_node() {
    let catalogue = Catalogue::linked();
    assert!(catalogue.nodes.iter().all(|n| n.name != "Relay"));
    let relay = catalogue
        .nodes
        .iter()
        .find(|n| n.name == "TokenRelay")
        .expect("TokenRelay");
    let fields: Vec<&str> = relay.config.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(fields, ["enabled"]);
    let metrics: Vec<&str> = relay.metrics.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(metrics, ["relayed_total"]);
    let carried = catalogue
        .exports
        .iter()
        .find(|e| e.name == "TokenCarried")
        .expect("TokenCarried");
    assert_eq!(relay.roles[0].ports[0].head.as_ref(), Some(&carried.origin));
    assert_eq!(
        carried.origin.path,
        "node::Carried<node::Words, node::Token>"
    );
    assert_eq!(carried.encoding, gdl::Encoding::Json);
    assert!(!carried.wire, "Words and Token have no serde impls");
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
