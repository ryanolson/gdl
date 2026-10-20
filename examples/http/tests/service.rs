use gdl::{Build, Frontend};
use http_frontend::{Config, Echo, HttpFrontend, graph};
use std::time::Duration;

#[tokio::test]
async fn a_frontend_serves_streams_and_collected_responses_and_shuts_down() {
    let catalog = gdl::Catalogue::linked();
    let source = catalog
        .nodes
        .iter()
        .find(|node| node.name == "HttpFrontend")
        .expect("registered frontend");
    assert_eq!(source.roles[0].role, "source");
    assert_eq!(
        source.roles[0]
            .ports
            .iter()
            .map(|port| port.port.as_str())
            .collect::<Vec<_>>(),
        ["Req", "Item"]
    );
    let frontend = HttpFrontend::build(Config {
        listen: "127.0.0.1:0".parse().expect("address"),
    })
    .await
    .expect("frontend");
    let address = frontend.local_addr().expect("bound address");
    assert!(
        HttpFrontend::build(Config { listen: address })
            .await
            .is_err(),
        "bind errors must reach the caller"
    );
    let stop = gdl::Cx::new();
    let shutdown = stop.clone();
    let serving = tokio::spawn(frontend.serve(
        graph::local(graph::Bindings::new().engine(Echo)),
        async move { shutdown.cancelled().await },
    ));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("client");
    let url = format!("http://{address}/generate");
    let response = client
        .post(&url)
        .json(&serde_json::json!({"text":"hello source"}))
        .send()
        .await
        .expect("stream response");
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "application/x-ndjson");
    assert_eq!(
        response.text().await.expect("stream"),
        "{\"item\":{\"text\":\"hello\"}}\n{\"item\":{\"text\":\"source\"}}\n"
    );
    let response = client
        .post(&url)
        .json(&serde_json::json!({"text":"hello source","stream":false}))
        .send()
        .await
        .expect("collected response");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<serde_json::Value>().await.expect("json"),
        serde_json::json!({"text":"hello source"})
    );
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("shutdown deadline")
        .expect("task")
        .expect("service");
}
