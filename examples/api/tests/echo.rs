//! `Echo` with a pace streams the same chunks, and refuses a pace that is
//! not a whole number of milliseconds.

use api_frontend::{Chunk, Echo, Prompt};
use futures::StreamExt;
use gdl::{Build, Sink};

async fn built(pace: Option<&str>) -> gdl::Result<Echo> {
    let config = pace
        .map(|ms| ("pace_ms".to_string(), ms.to_string()))
        .into_iter()
        .collect();
    Echo::build(config, ()).await
}

async fn words(echo: &Echo) -> Vec<String> {
    let items = echo
        .generate(
            gdl::Cx::default(),
            Prompt {
                text: "one two three".into(),
            },
        )
        .await
        .expect("a stream");
    items
        .map(|chunk: gdl::Result<Chunk>| chunk.expect("a chunk").text)
        .collect()
        .await
}

#[tokio::test]
async fn a_paced_echo_streams_the_same_words() {
    let plain = built(None).await.expect("built");
    let paced = built(Some("1")).await.expect("built");
    assert_eq!(words(&plain).await, ["one", "two", "three"]);
    assert_eq!(words(&paced).await, ["one", "two", "three"]);
    assert!(built(Some("fast")).await.is_err());
}
