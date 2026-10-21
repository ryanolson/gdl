//! The chat example's segment binaries, run as separate processes, through
//! their lives: started in any order, probed while they start, serve, and
//! drain, and stopped, alone or all together, without cutting an answer.
#![cfg(unix)]

mod common;

use std::time::Duration;

use common::{Chat, ask, get, listener};

/// The longest a case waits for a process to say something it expects.
/// Generous: a cold start of three processes on a loaded machine.
const WAIT: Duration = Duration::from_secs(60);

/// How long a process may take to exit once it should.
const EXIT: Duration = Duration::from_secs(15);

const REQUEST: &str = r#"{"text": "one two three", "max_words": 3}"#;

/// A request written to the front before its peers exist waits with the
/// front at the roster's rendezvous, and is answered once they serve.
#[test]
fn a_request_written_before_the_peers_start_is_answered() {
    let chat = Chat::new("gdl-life-early-");
    let mut front = chat.spawn(
        "front",
        &chat.config("front", "[system]\nhost = \"127.0.0.1\"\nport = 0\n"),
        true,
    );
    let errors = front.stderr();
    // The front has started: its listener is bound, and it is meeting.
    errors.find("system listener at", WAIT);
    ask(&mut front, REQUEST);
    let _engine = chat.spawn("engine", &chat.config("engine", ""), false);
    let _post = chat.spawn("post", &chat.config("post", ""), false);
    let out = front.stdout();
    let answer: Vec<String> = (0..4).map(|_| out.next(WAIT)).collect();
    assert!(
        answer[..3]
            .iter()
            .all(|line| line.starts_with(r#"{"item":"#)),
        "{answer:?}"
    );
    assert_eq!(answer[3], r#"{"done":null}"#);
    // End of input: the front drains and exits.
    drop(front.0.stdin.take());
    assert!(front.exits_within(EXIT).success());
}

/// A front whose engine never starts gives up at the rendezvous's bound,
/// with an error that names what is missing.
#[test]
fn a_front_whose_engine_never_starts_ends_at_the_rendezvous() {
    let chat = Chat::new("gdl-life-alone-");
    let mut front = chat.spawn(
        "front",
        &chat.config("front", "rendezvous_ms = 1500\n"),
        true,
    );
    let _post = chat.spawn(
        "post",
        &chat.config("post", "rendezvous_ms = 30000\n"),
        false,
    );
    let errors = front.stderr();
    let status = front.exits_within(EXIT);
    assert!(!status.success(), "{status}");
    let said = errors.rest(EXIT).join("\n");
    assert!(said.contains("engine (0 of 1)"), "{said}");
}

/// Every process is stopped at once while an answer streams through all
/// three: the answer arrives in full, and each exits cleanly.
#[test]
fn every_process_stopped_mid_answer_finishes_it() {
    let chat = Chat::new("gdl-life-all-");
    let mut engine = chat.spawn(
        "engine",
        &chat.config("engine", "[node.Engine]\ndelay_ms = 400\n"),
        false,
    );
    let mut post = chat.spawn("post", &chat.config("post", ""), false);
    let mut front = chat.spawn("front", &chat.config("front", ""), true);
    let errors = front.stderr();
    errors.find("front: ready", WAIT);
    ask(&mut front, REQUEST);
    let out = front.stdout();
    let first = out.next(WAIT);
    assert!(first.starts_with(r#"{"item":"#), "{first}");
    for process in [&front, &engine, &post] {
        process.signal("-TERM");
    }
    let rest: Vec<String> = (0..3).map(|_| out.next(WAIT)).collect();
    assert!(
        rest[..2].iter().all(|line| line.starts_with(r#"{"item":"#)),
        "the answer was cut short: {first} {rest:?}"
    );
    assert_eq!(rest[2], r#"{"done":null}"#);
    for (name, process) in [
        ("front", &mut front),
        ("engine", &mut engine),
        ("post", &mut post),
    ] {
        let status = process.exits_within(EXIT);
        assert!(status.success(), "{name}: {status}");
    }
}

/// An engine is live from its start and healthy only while it serves:
/// `starting` while its node loads, `ready` once every instance serves, and
/// `draining` once stopped, while it waits for the front to drain.
/// `--healthcheck` agrees with the health path throughout.
#[test]
fn the_probes_follow_an_engine_through_its_life() {
    let chat = Chat::new("gdl-life-probes-");
    let settings = "[node.Engine]\nload_ms = 2000\n[system]\nhost = \"127.0.0.1\"\nport = 0\n";
    let mut engine = chat.spawn("engine", &chat.config("engine", settings), false);
    let errors = engine.stderr();
    let address = listener(&errors.find("system listener at", WAIT));
    // The health check reads the same settings, with the port fixed.
    let check = chat.config(
        "engine-check",
        &format!("[system]\nhost = \"0.0.0.0\"\nport = {}\n", address.port()),
    );
    let healthy = || {
        std::process::Command::new(common::built().join("chat-engine"))
            .arg("--healthcheck")
            .arg(&check)
            .output()
            .expect("health check")
            .status
            .success()
    };
    let phase = |body: &str| {
        let at = body.find("\"phase\":\"").expect("a phase") + 9;
        body[at..at + body[at..].find('"').expect("its end")].to_string()
    };

    let (code, body) = get(address, "/live");
    assert_eq!((code, phase(&body).as_str()), (200, "starting"), "{body}");
    let (code, body) = get(address, "/health");
    assert_eq!((code, phase(&body).as_str()), (503, "starting"), "{body}");
    assert!(!healthy(), "healthy while starting");

    let _post = chat.spawn("post", &chat.config("post", ""), false);
    let mut front = chat.spawn("front", &chat.config("front", ""), true);
    let front_errors = front.stderr();
    errors.find("engine: ready", WAIT);
    front_errors.find("front: ready", WAIT);
    let (code, body) = get(address, "/health");
    assert_eq!((code, phase(&body).as_str()), (200, "ready"), "{body}");
    assert!(healthy(), "unhealthy while serving");

    // Stopped, the engine waits for the front, which still serves.
    engine.signal("-TERM");
    let started = std::time::Instant::now();
    loop {
        let (code, body) = get(address, "/health");
        if phase(&body) == "draining" {
            assert_eq!(code, 503, "{body}");
            break;
        }
        assert!(started.elapsed() < WAIT, "never draining: {body}");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(get(address, "/live").0, 200);
    assert!(!healthy(), "healthy while draining");
    assert!(
        engine.0.try_wait().expect("state").is_none(),
        "the engine left before the front drained"
    );

    // The front stops with its input open and idle; once it has drained,
    // the engine leaves.
    front.signal("-TERM");
    let status = front.exits_within(EXIT);
    assert!(status.success(), "{status}: {:?}", front_errors.rest(EXIT));
    assert!(engine.exits_within(EXIT).success());
}

/// A JSON-lines front stops on a signal while its input is open and idle:
/// the graph stops, and the process does not wait on the read of stdin
/// that never returns.
#[test]
fn a_json_lines_front_stops_on_a_signal_with_idle_input() {
    let chat = Chat::new("gdl-life-idle-");
    let _engine = chat.spawn("engine", &chat.config("engine", ""), false);
    let _post = chat.spawn("post", &chat.config("post", ""), false);
    let mut front = chat.spawn("front", &chat.config("front", ""), true);
    let errors = front.stderr();
    errors.find("front: ready", WAIT);
    front.signal("-INT");
    assert!(front.exits_within(EXIT).success());
}

/// A signal that comes while the JSON-lines source is writing an answer
/// ends reading, not the answer: the rest of the answer arrives, and the
/// process exits cleanly. The engine waits before each word, and the
/// signal is sent once the first word is read, so the other two are
/// produced after it.
#[test]
fn a_signal_does_not_cut_an_answer_short() {
    let chat = Chat::new("gdl-life-answer-");
    let _engine = chat.spawn(
        "engine",
        &chat.config("engine", "[node.Engine]\ndelay_ms = 500\n"),
        false,
    );
    let _post = chat.spawn("post", &chat.config("post", ""), false);
    let mut front = chat.spawn("front", &chat.config("front", ""), true);
    // The input stays open: only the signal ends reading.
    ask(&mut front, REQUEST);
    let out = front.stdout();
    let first = out.next(WAIT);
    assert!(first.starts_with(r#"{"item":"#), "{first}");
    front.signal("-INT");
    let rest: Vec<String> = (0..3).map(|_| out.next(WAIT)).collect();
    assert!(
        rest[..2].iter().all(|line| line.starts_with(r#"{"item":"#)),
        "the answer was cut short: {first} {rest:?}"
    );
    assert_eq!(rest[2], r#"{"done":null}"#);
    assert!(front.exits_within(EXIT).success());
}

/// A running engine serves its metrics on its system listener: the
/// engine's own, which its node defines, and the runtime's for every node
/// it hosts, each with dynamo's prefix and identity labels.
#[test]
fn a_running_engine_serves_its_metrics() {
    let chat = Chat::new("gdl-life-metrics-");
    let settings = "[system]\nhost = \"127.0.0.1\"\nport = 0\n";
    let mut engine = chat.spawn("engine", &chat.config("engine", settings), false);
    let errors = engine.stderr();
    let address = listener(&errors.find("system listener at", WAIT));
    let mut post = chat.spawn("post", &chat.config("post", ""), false);
    let mut front = chat.spawn("front", &chat.config("front", ""), true);
    ask(&mut front, REQUEST);
    let out = front.stdout();
    let answer: Vec<String> = (0..4).map(|_| out.next(WAIT)).collect();
    assert_eq!(answer[3], r#"{"done":null}"#, "{answer:?}");

    let (code, scrape) = get(address, "/metrics");
    assert_eq!(code, 200);
    // The engine's request ends when its stream does, which the front has
    // read by now; the runtime records it there.
    let series = |name: &str| -> Vec<&str> {
        scrape
            .lines()
            .filter(|line| line.starts_with(&format!("{name}{{")))
            .collect()
    };
    let words = series("dynamo_component_words_total");
    assert_eq!(words.len(), 1, "{scrape}");
    for label in [
        "dynamo_endpoint=\"Engine\"",
        "dynamo_component=\"engine\"",
        "dynamo_namespace=\"chat-",
        "worker_id=\"",
    ] {
        assert!(words[0].contains(label), "{label} missing: {}", words[0]);
    }
    assert!(words[0].ends_with(" 3"), "{}", words[0]);
    let requests = series("dynamo_component_requests_total");
    assert!(
        requests
            .iter()
            .any(|line| line.contains("dynamo_endpoint=\"Engine\"") && line.ends_with(" 1")),
        "{scrape}"
    );
    assert_eq!(
        series("dynamo_component_uptime_seconds").len(),
        1,
        "{scrape}"
    );

    drop(front.0.stdin.take());
    assert!(front.exits_within(EXIT).success());
    for process in [&engine, &post] {
        process.signal("-TERM");
    }
    assert!(engine.exits_within(EXIT).success());
    assert!(post.exits_within(EXIT).success());
}
