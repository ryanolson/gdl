//! Audio streams in through the generated segments while transcripts stream
//! back: the client's audio crosses to the resampler and on to the
//! recognizer, which answers after each chunk.

use asr_contract::{Session, Transcript};
use futures::StreamExt;

fn session(language: &str, sample_rate: u32) -> Session {
    Session {
        language: language.to_string(),
        sample_rate,
    }
}

async fn collect(out: gdl::Items<Transcript>) -> Vec<gdl::Result<Transcript>> {
    out.collect().await
}

fn heard(text: &str, done: bool) -> gdl::Result<Transcript> {
    Ok(Transcript {
        text: text.to_string(),
        done,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audio_streams_in_and_transcripts_stream_back() {
    let demo = asr_demo::start("en").await.expect("start");
    let audio = asr_demo::speak(&["hello", "big", "world"], 48_000);
    let out = demo
        .transcribe(session("en", 48_000), audio)
        .await
        .expect("transcribe");
    assert_eq!(
        collect(out).await,
        vec![
            heard("hello", false),
            heard("hello big", false),
            heard("hello big world", false),
            heard("Hello big world.", true),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transcripts_arrive_before_the_audio_ends() {
    let demo = asr_demo::start("en").await.expect("start");
    // The client holds the audio stream open after the first word.
    let (tx, rx) = futures::channel::mpsc::unbounded();
    let audio = gdl::items(rx);
    let mut first = asr_demo::speak(&["first"], 16_000);
    tx.unbounded_send(first.next().await.expect("a chunk"))
        .expect("send");
    let mut out = demo
        .transcribe(session("en", 16_000), audio)
        .await
        .expect("transcribe");
    assert_eq!(out.next().await, Some(heard("first", false)));
    let mut second = asr_demo::speak(&["second"], 16_000);
    tx.unbounded_send(second.next().await.expect("a chunk"))
        .expect("send");
    drop(tx);
    assert_eq!(
        collect(out).await,
        vec![heard("first second", false), heard("First second.", true)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_the_recognizer_cannot_serve_is_refused() {
    let demo = asr_demo::start("en").await.expect("start");
    let audio = asr_demo::speak(&["bonjour"], 16_000);
    let refused = demo.transcribe(session("fr", 16_000), audio).await;
    let error = refused.err().expect("refused");
    assert!(error.to_string().contains("not served"), "{error}");
}
