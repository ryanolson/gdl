use api_frontend::*;
use gdl::{Build, Next, Operator, Service};
use std::{sync::Arc, time::Duration};

fn adapt<O, N>(op: O, next: N) -> gdl::Client<O::UpReq, O::UpItem>
where
    O: Operator,
    N: Next<O::DownReq, O::DownItem>,
{
    gdl::Client::new(gdl::Source::new(gdl::Hop::new(
        (),
        gdl::OperatorStage::new(Arc::new(op), next),
        (),
    )))
}

#[tokio::test]
async fn enabled_endpoints_use_distinct_adapters_and_disabled_endpoints_are_absent() {
    let catalog = gdl::Catalogue::linked();
    let source = catalog
        .nodes
        .iter()
        .find(|n| n.name == "ApiFrontend")
        .expect("source registration");
    assert_eq!(source.config.len(), 3);
    assert_eq!(source.roles[0].entries.len(), 3);
    assert!(source.config.iter().all(|f| f.name.starts_with("enabled_")));
    for all in [true, false] {
        let frontend = ApiFrontend::build(
            ApiConfig {
                enabled_chat: true,
                enabled_responses: all,
                enabled_anthropic: all,
                listen: "127.0.0.1:0".parse().unwrap(),
            },
            (),
        )
        .await
        .unwrap();
        let address = frontend.local_addr().unwrap();
        let pipeline = gdl::Source::new(gdl::Hop::new((), gdl::SinkStage::new(Arc::new(Echo)), ()));
        let clients = ApiClients {
            chat: Some(if all {
                adapt(ChatAdapter, pipeline.clone())
            } else {
                gdl::Client::new(gdl::Hop::new(
                    (Arc::new(ExtractChat), ()),
                    gdl::Join::new(pipeline.clone()),
                    (Arc::new(FormatChat), ()),
                ))
            }),
            responses: all.then(|| adapt(ResponsesAdapter, pipeline.clone())),
            anthropic: all.then(|| adapt(AnthropicAdapter, pipeline.clone())),
        };
        let (stop, wait) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(frontend.serve(clients, async {
            let _ = wait.await;
        }));
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let (chat, responses, messages) = tokio::join!(
            http.post(format!("http://{address}/v1/chat/completions"))
                .json(&serde_json::json!({"messages":["chat words"]}))
                .send(),
            http.post(format!("http://{address}/v1/responses"))
                .json(&serde_json::json!({"input":"response words"}))
                .send(),
            http.post(format!("http://{address}/v1/messages"))
                .json(&serde_json::json!({"messages":["message words"],"max_tokens":1}))
                .send()
        );
        assert_eq!(
            chat.unwrap().text().await.unwrap(),
            "{\"delta\":\"chat\"}\n{\"delta\":\"words\"}\n"
        );
        if all {
            assert_eq!(
                responses.unwrap().text().await.unwrap(),
                "{\"output_text\":\"response\"}\n{\"output_text\":\"words\"}\n"
            );
            assert_eq!(
                messages.unwrap().text().await.unwrap(),
                "{\"text\":\"message\"}\n"
            );
        } else {
            assert_eq!(responses.unwrap().status(), 404);
            assert_eq!(messages.unwrap().status(), 404);
        }
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
