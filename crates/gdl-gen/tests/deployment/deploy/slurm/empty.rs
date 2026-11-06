//! `down` of a pool with no parent when its hub lists no instance while a
//! joiner, which its launcher started, has not registered yet: the empty
//! list does not end the wait while the joiner runs. It gets one `TERM`,
//! and has ended before the pool's job is cancelled; another `join` that
//! runs meanwhile is refused, and changes nothing.

use rustix::process::Signal;

use super::join::{launch_of, until};
use super::*;

/// A joiner that registers after `down`'s first read of the hub, which
/// listed no instance, while its launcher has not yet passed on its
/// `TERM`, must get that `TERM` and drain before the pool's job is
/// cancelled.
#[test]
fn down_signals_a_joiner_before_cancelling_even_when_the_hub_is_empty() {
    down_with_late_joiner(Late::AfterEmptyRead, false);
}

/// The control: a joiner that never registers, so the hub lists no
/// instance at all, still gets its one `TERM`, and ends before the cancel.
#[test]
fn down_signals_a_visible_joiner_with_an_empty_hub() {
    down_with_late_joiner(Late::Never, false);
}

/// Another `join` that runs while `down` stops the pool is refused: the
/// pool's job is marked stopping in the registry. The first joiner keeps
/// its request, gets its one `TERM`, and ends before the cancel.
#[test]
fn down_signals_a_joiner_after_another_join_clears_its_hold() {
    down_with_late_joiner(Late::Never, true);
}

/// A joiner that registers after `down`'s last read of the steps it
/// signals, while another `join` runs and is refused, must still get its
/// signal and drain before the pool's job is cancelled: `down` waits on
/// each request in the registry until its launcher says it ended, not on
/// what it saw in a listing.
#[test]
fn down_signals_a_joiner_whose_hold_is_cleared_after_its_final_step_read() {
    down_with_late_joiner(Late::AfterEmptyRead, true);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Late {
    /// It registers once the hub has listed no instance, before its
    /// launcher passes on its `TERM`.
    AfterEmptyRead,
    /// It never registers.
    Never,
}

fn down_with_late_joiner(late: Late, another_join: bool) {
    struct Racing<'a> {
        fake: &'a mut Fake,
        site: &'a Site,
        dir: PathBuf,
        late: Late,
        another_join: bool,
        /// What the other `join` said.
        refused: Option<String>,
        /// Whether the hub listed the joiner after it listed none.
        listed_late: bool,
        empty_read: bool,
    }

    impl gdl_gen::Slurm for Racing<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let hub_read = command.program == "curl"
                && command
                    .args
                    .last()
                    .is_some_and(|url| url == "http://node-1002:8337/v1/ext/gdl-pool/instances");
            let reply = gdl_gen::Slurm::run(self.fake, command)?;
            if hub_read {
                let listed = reply.stdout.contains("joiner");
                if self.empty_read && listed {
                    self.listed_late = true;
                }
                if !self.empty_read && !reply.stdout.contains("segment") {
                    self.empty_read = true;
                    if self.late == Late::AfterEmptyRead {
                        // The joiner registers now, and its launcher, slow
                        // until then, reads its `TERM` and its stop after.
                        launchers::go(&self.dir);
                        until("the joiner registers", || {
                            launchers::registered(&self.dir).len() == 1
                        });
                    }
                    if self.another_join {
                        self.refused = Some(
                            super::join::join(
                                self.site,
                                self.fake,
                                "global-us",
                                "front",
                                gdl_gen::JoinBy::Step,
                            )
                            .expect_err("the pool stops")
                            .to_string(),
                        );
                    }
                    self.fake.signal_launchers(1002, Signal::CONT);
                }
            }
            Ok(reply)
        }
    }

    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(&site.files.fleet, fleet.replace(
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n",
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\nslurm = { spare_nodes = 1 }\n",
    )).expect("a spare node");
    let mut fake = Fake::new(&site);
    // Each joiner registers only once the test lets it.
    fake.gate = true;
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.launchers_up(1002);
    let dir = site.fleet_dir.join("global-us");
    std::fs::write(
        dir.join("front.toml"),
        "[system]\nport = 9090\n[runtime]\ndrain_ms = 0\n",
    )
    .expect("the shortest stop grace");
    let joiner = super::join::join(
        &site,
        &mut fake,
        "global-us",
        "front",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    let launch = launch_of(&joiner, "1002.4").to_string();
    let [pid] = launchers::joiners(&dir)[..] else {
        panic!("one joiner");
    };
    // Its launcher is slow to read its signal and its mailbox.
    fake.signal_launchers(1002, Signal::STOP);

    let mut racing = Racing {
        fake: &mut fake,
        site: &site,
        dir: dir.clone(),
        late,
        another_join,
        refused: None,
        listed_late: false,
        empty_read: false,
    };
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&Log::default()),
        &mut racing,
        &mut |_| {},
    );
    let (refused, listed_late, empty_read) = (
        racing.refused.clone(),
        racing.listed_late,
        racing.empty_read,
    );
    assert!(empty_read, "the hub listed no instance once");
    if late == Late::AfterEmptyRead {
        assert!(listed_late, "the joiner registered after the empty list");
    }
    if another_join {
        let refused = refused.expect("another join ran");
        assert!(refused.contains("is being stopped"), "{refused}");
    }
    assert!(result.is_ok(), "down must wait for the joiner: {result:?}");
    assert_eq!(
        fake.killed,
        Vec::<(i32, bool, usize)>::new(),
        "the pool's job was cancelled while its joiner ran"
    );
    assert_eq!(
        launchers::terms(&dir, pid),
        1,
        "one TERM for launch {launch}"
    );
    assert!(!launchers::running(pid));
}

/// A request whose launcher's step is gone, its joiner with it, is no
/// joiner for `down` to wait on: `down` forgets it and completes,
/// signalling nothing it would not have.
#[test]
fn down_forgets_a_request_whose_launcher_is_gone() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(
        &site.files.fleet,
        fleet.replace(
            "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n",
            "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\nslurm = { spare_nodes = 1 }\n",
        ),
    )
    .expect("a spare node");
    let mut slurm = Fake::new(&site);
    site.deploy(&mirror, &mut slurm, false).expect("deployed");
    slurm.launchers_up(1002);
    let dir = site.fleet_dir.join("global-us");
    launchers::seed(&dir, 1002, "spare-1002-0", "front", "1002.4", "l9", false);
    // Its launcher's step ends, as one whose launcher died does.
    slurm.launchers.retain(|launcher| launcher.job != 1002);
    slurm.calls.borrow_mut().clear();
    site.down(&mut slurm).expect("down completes");
    assert!(
        !requests(&dir).iter().any(|line| line.contains("l9")),
        "{:?}",
        requests(&dir)
    );
    assert_eq!(slurm.listed(), Vec::<String>::new());
}

/// On a site that signals every process of a step, `down` of a pool with
/// no parent still gives a joiner one `TERM`: it stops a joiner through
/// its launcher's mailbox alone, and sends a launcher's step no `TERM`
/// that would reach the joiner a second time.
#[test]
fn down_where_slurm_signals_children_sends_a_joiner_one_term() {
    struct Ordered<'a> {
        fake: &'a mut Fake,
        dir: PathBuf,
    }

    impl gdl_gen::Slurm for Ordered<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            // Any signal to the launcher's step comes after its launcher
            // passed on the stop.
            if command.program == "scancel" && command.args.iter().any(|arg| arg == "1002.4") {
                until("the launcher passed on its stop", || {
                    launchers::joiners(&self.dir)
                        .into_iter()
                        .any(|pid| launchers::terms(&self.dir, pid) == 1)
                });
            }
            gdl_gen::Slurm::run(self.fake, command)
        }
    }

    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(&site.files.fleet, fleet.replace(
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\n",
        "[pool.global-us]\nclass = \"global\"\ncluster = \"lab\"\nslurm = { spare_nodes = 1 }\n",
    )).expect("a spare node");
    let mut fake = Fake::new(&site);
    fake.signal_children = true;
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.launchers_up(1002);
    let dir = site.fleet_dir.join("global-us");
    std::fs::write(
        dir.join("front.toml"),
        "[system]\nport = 9090\n[runtime]\ndrain_ms = 0\n",
    )
    .expect("the shortest stop grace");
    super::join::join(
        &site,
        &mut fake,
        "global-us",
        "front",
        gdl_gen::JoinBy::Step,
    )
    .expect("joined");
    let [pid] = launchers::joiners(&dir)[..] else {
        panic!("one joiner");
    };
    let mut ordered = Ordered {
        fake: &mut fake,
        dir: dir.clone(),
    };
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&Log::default()),
        &mut ordered,
        &mut |_| {},
    );
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(launchers::terms(&dir, pid), 1);
    assert_eq!(fake.killed, Vec::<(i32, bool, usize)>::new());
}
