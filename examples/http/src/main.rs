use gdl::{Build, Frontend};
use http_frontend::{Config, Echo, HttpFrontend, graph};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8081".into())
        .parse()?;
    let frontend = HttpFrontend::build(Config { listen }, ()).await?;
    println!("HTTP frontend: http://{}/generate", frontend.local_addr()?);
    let next = graph::local(graph::Bindings::new().engine(Echo));
    let shutdown = gdl::Cx::new();
    let stop = shutdown.clone();
    let serving = frontend.serve(next, async move { stop.cancelled().await });
    tokio::pin!(serving);
    tokio::select! {
        result = &mut serving => result?,
        signal = tokio::signal::ctrl_c() => {signal?;shutdown.cancel();serving.await?;}
    }
    Ok(())
}
