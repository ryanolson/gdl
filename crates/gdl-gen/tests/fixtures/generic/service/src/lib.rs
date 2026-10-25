//! A generic service whose clients are its parameter: each alias's
//! instantiation hosts its own argument's endpoints.

use std::future::Future;
use std::marker::PhantomData;

use gdl::{Build, Result};
use serde::{Deserialize, Serialize};

/// A question.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Ask {
    /// The text.
    pub text: String,
}

/// An answer.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Answer {
    /// The text.
    pub text: String,
}

/// A note.
#[derive(Serialize, Deserialize, gdl::Message)]
pub struct Note {
    /// The text.
    pub text: String,
}

/// Which endpoints a server hosts.
#[derive(Deserialize, gdl::Config)]
pub struct Hosted {
    /// The question endpoint.
    #[gdl(graph, default = true)]
    pub enabled_ask: bool,
    /// The note endpoint.
    #[gdl(graph, default = true)]
    pub enabled_note: bool,
}

/// The clients of a server that answers questions.
#[derive(gdl::Endpoints)]
pub struct AskClients {
    /// Questions.
    #[gdl(endpoint = "/ask", enabled = "enabled_ask")]
    pub ask: Option<gdl::Client<Ask, Answer>>,
}

/// The clients of a server that takes notes.
#[derive(gdl::Endpoints)]
pub struct NoteClients {
    /// Notes.
    #[gdl(endpoint = "/note", enabled = "enabled_note")]
    pub note: Option<gdl::Client<Note, Answer>>,
}

/// A server written once, whose clients are its parameter.
#[derive(gdl::Node)]
#[gdl(config = Hosted)]
pub struct Server<C>(PhantomData<fn() -> C>);

impl<C: 'static> Build for Server<C> {
    type Config = Hosted;
    type Metrics = ();

    async fn build(_config: Hosted, (): ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

impl<C: gdl::Endpoints + Send + 'static> gdl::Service for Server<C> {
    type Clients = C;

    async fn serve(
        self,
        _clients: C,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        shutdown.await;
        Ok(())
    }
}

/// The server of questions.
#[gdl::export]
pub type AskServer = Server<AskClients>;

/// The server of notes, of the same implementation.
#[gdl::export]
pub type NoteServer = Server<NoteClients>;
