use super::*;

/// A segment's `config` table adds to the settings `run` writes, and may
/// not say where the segment finds its peers.
#[test]
fn the_spec_adds_to_each_segments_settings() {
    let roster = Path::new("/tmp/gdl-run-x/roster");
    let bare = settings(roster, None).expect("settings");
    assert_eq!(
        toml::to_string(&bare).expect("toml"),
        "listen = \"127.0.0.1:0\"\nroster = \"/tmp/gdl-run-x/roster\"\n"
    );
    let config: toml::Table = toml::from_str("[node.Engine]\nuppercase = true\n").expect("config");
    let table = settings(roster, Some(&config)).expect("settings");
    assert_eq!(table["node"]["Engine"]["uppercase"].as_bool(), Some(true));
    assert_eq!(table["listen"].as_str(), Some("127.0.0.1:0"));
    for key in ["roster", "hub", "instances", "pool"] {
        let config: toml::Table = toml::from_str(&format!("{key} = \"x\"\n")).expect("config");
        let error = settings(roster, Some(&config)).expect_err(key);
        assert!(error.to_string().contains(&format!("`{key}`")), "{error}");
    }
}

mod processes {
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use super::super::unix::{Running, Stops, watch};

    const GRACE: Duration = Duration::from_secs(5);

    /// Segments played by `sh` scripts in a scratch directory. A segment
    /// that traps SIGTERM creates `<name>.ready` once its trap is set, so
    /// that no stop reaches it first.
    struct Play {
        dir: tempfile::TempDir,
    }

    impl Play {
        fn new() -> Self {
            let dir = tempfile::Builder::new()
                .prefix("gdl-run-")
                .tempdir()
                .expect("scratch");
            Self { dir }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.dir.path().join(name)
        }

        /// Segment `name`, running `script` once each of `after` is
        /// ready. In `script`, `$1` is the segment's log file.
        fn segment(&self, name: &str, after: &[&str], script: &str, grace: Duration) -> Running {
            let mut wait = String::new();
            for other in after {
                let ready = self.path(&format!("{other}.ready"));
                wait.push_str(&format!(
                    "while [ ! -e '{}' ]; do sleep 0.02; done; ",
                    ready.display()
                ));
            }
            let ready = self.path(&format!("{name}.ready"));
            let script = script.replace("READY", &format!(": > '{}'", ready.display()));
            let mut command = Command::new("sh");
            command
                .arg("-c")
                .arg(format!("{wait}{script}"))
                .arg("sh")
                .arg(self.path(name));
            Running::spawn(name, &mut command, grace).expect(name)
        }

        /// The SIGTERMs segment `name` noted.
        fn stops(&self, name: &str) -> usize {
            std::fs::read_to_string(self.path(name)).map_or(0, |text| text.lines().count())
        }

        /// Wait until segment `name` is ready.
        fn ready(&self, name: &str) {
            let ready = self.path(&format!("{name}.ready"));
            let deadline = Instant::now() + Duration::from_secs(5);
            while !ready.exists() {
                assert!(Instant::now() < deadline, "{name} is not ready");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    /// Notes each SIGTERM in `$1`, and exits on its own once it has seen
    /// one and waited a little, so that a second would be seen too.
    const COUNTS_STOPS: &str = "trap 'echo term >> \"$1\"' TERM; READY; \
        while [ ! -s \"$1\" ]; do sleep 0.02; done; sleep 0.3; exit 0";

    /// The source's segment ends at the end of its input; each other
    /// segment gets exactly one SIGTERM, and the run succeeds.
    #[test]
    fn the_end_of_the_input_stops_each_other_segment_once() {
        let play = Play::new();
        let front = play.segment("front", &["engine", "post"], "exit 0", GRACE);
        let others = vec![
            play.segment("engine", &[], COUNTS_STOPS, GRACE),
            play.segment("post", &[], COUNTS_STOPS, GRACE),
        ];
        watch(front, others, &Stops::default()).expect("a clean run");
        assert_eq!((play.stops("engine"), play.stops("post")), (1, 1));
    }

    /// A segment that fails stops the others at once, and is named first.
    #[test]
    fn a_failed_segment_stops_the_rest_and_is_named() {
        let play = Play::new();
        let front = play.segment("front", &[], COUNTS_STOPS, GRACE);
        let others = vec![play.segment("engine", &["front"], "exit 3", GRACE)];
        let started = Instant::now();
        let error = watch(front, others, &Stops::default()).expect_err("a failure");
        assert!(started.elapsed() < Duration::from_secs(3), "not at once");
        assert_eq!(error.to_string(), "segment `engine` failed: exit status: 3");
        assert_eq!(play.stops("front"), 1);
    }

    /// A segment other than the source's that exits cleanly while the
    /// graph runs leaves it unable to answer: the run fails.
    #[test]
    fn a_segment_that_leaves_early_fails_the_run() {
        let play = Play::new();
        let front = play.segment("front", &[], COUNTS_STOPS, GRACE);
        let others = vec![play.segment("post", &["front"], "exit 0", GRACE)];
        let error = watch(front, others, &Stops::default()).expect_err("a failure");
        assert_eq!(
            error.to_string(),
            "segment `post` exited before segment `front` ended"
        );
    }

    /// After a SIGINT, which the terminal sent every segment, `run` sends
    /// none of its own and waits: whichever segment exits first is no
    /// failure.
    #[test]
    fn an_interrupt_is_not_sent_again() {
        let play = Play::new();
        let stops = Stops::default();
        stops.interrupted.store(true, Ordering::Relaxed);
        let drains = "trap 'echo term >> \"$1\"' TERM; READY; sleep 0.3; exit 0";
        let front = play.segment("front", &[], drains, GRACE);
        let others = vec![play.segment("post", &["front"], "exit 0", GRACE)];
        watch(front, others, &stops).expect("a clean stop");
        assert_eq!((play.stops("front"), play.stops("post")), (0, 0));
    }

    /// A SIGTERM to `run` goes once to every segment.
    #[test]
    fn a_termination_is_sent_once_to_each_segment() {
        let play = Play::new();
        let stops = Stops::default();
        let front = play.segment("front", &[], COUNTS_STOPS, GRACE);
        let others = vec![play.segment("post", &[], COUNTS_STOPS, GRACE)];
        std::thread::scope(|scope| {
            scope.spawn(|| {
                play.ready("front");
                play.ready("post");
                stops.terminated.store(true, Ordering::Relaxed);
            });
            watch(front, others, &stops).expect("a clean stop");
        });
        assert_eq!((play.stops("front"), play.stops("post")), (1, 1));
    }

    /// A segment that outlives its grace after the stop is killed, and the
    /// run fails.
    #[test]
    fn a_segment_that_does_not_stop_is_killed() {
        let play = Play::new();
        let front = play.segment("front", &["engine"], "exit 0", GRACE);
        let stubborn = "trap '' TERM; READY; while :; do sleep 0.02; done";
        let others = vec![play.segment("engine", &[], stubborn, Duration::ZERO)];
        let started = Instant::now();
        let error = watch(front, others, &Stops::default()).expect_err("a kill");
        assert!(started.elapsed() < Duration::from_secs(3), "not killed");
        assert_eq!(
            error.to_string(),
            "segment `engine` did not stop within 0 s, and was killed"
        );
    }
}
