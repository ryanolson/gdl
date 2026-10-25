// The configuration a generic node names is its build's: an instantiation
// whose build takes another fails where its alias names it.

#[derive(serde::Deserialize, gdl::Config)]
pub struct Settings {
    #[gdl(graph, default = true)]
    pub enabled: bool,
}

#[derive(gdl::Node)]
#[gdl(config = Settings)]
pub struct Relay<B>(std::marker::PhantomData<fn() -> B>);

impl<B: Send + Sync + 'static> gdl::Build for Relay<B> {
    type Config = ();
    type Metrics = ();

    async fn build((): (), (): ()) -> gdl::Result<Self> {
        Ok(Self(std::marker::PhantomData))
    }
}

#[gdl::export]
pub type U32Relay = Relay<u32>;

fn main() {}
