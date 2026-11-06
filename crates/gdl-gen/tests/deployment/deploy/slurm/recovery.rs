//! `down` of a pool with no parent when a request, from a `cargo gdl
//! join` that ended before it saw its joiner start, waits in a launcher's
//! mailbox: the launcher reads it after `down` has started, or before.

use rustix::process::Signal;

use super::join::until;
use super::*;

/// A launcher too slow to read its mailbox reads a request only after
/// `down` has read the pool's steps once: by then `down` has withdrawn it,
/// so it never starts. Were it started and missed, the joiner would be
/// cancelled while it runs.
#[test]
fn down_signals_a_reserved_joiner_that_appears_after_the_first_step_listing() {
    down_with_reserved_joiner(false);
}

/// The control: a request the launcher started before `down` gets one
/// `TERM`, and its joiner ends before the pool's job is cancelled.
#[test]
fn down_signals_a_reserved_joiner_visible_before_the_first_step_listing() {
    down_with_reserved_joiner(true);
}

fn down_with_reserved_joiner(started_before_down: bool) {
    struct Slow<'a> {
        fake: &'a mut Fake,
        resumed: bool,
    }

    impl gdl_gen::Slurm for Slow<'_> {
        fn run(
            &mut self,
            command: &gdl_gen::SlurmCommand,
        ) -> Result<gdl_gen::Reply, gdl_gen::Error> {
            let reply = gdl_gen::Slurm::run(self.fake, command)?;
            if !self.resumed
                && command.program == "squeue"
                && command.args.iter().any(|arg| arg == "--steps")
                && command.args.iter().any(|arg| arg == "--jobs=1002")
            {
                self.resumed = true;
                self.fake.signal_launchers(1002, Signal::CONT);
            }
            Ok(reply)
        }
    }

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
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.launchers_up(1002);
    let dir = site.fleet_dir.join("global-us");
    std::fs::write(
        dir.join("front.toml"),
        "[system]\nport = 9090\n[runtime]\ndrain_ms = 0\n",
    )
    .expect("the shortest stop grace");
    // A join that posted its request and ended before the start.
    fake.signal_launchers(1002, Signal::STOP);
    launchers::seed(&dir, 1002, "spare-1002-0", "front", "1002.4", "l1", true);
    if started_before_down {
        fake.signal_launchers(1002, Signal::CONT);
        until("l1 registers", || launchers::registered(&dir).len() == 1);
    }
    let mut slow = Slow {
        fake: &mut fake,
        resumed: started_before_down,
    };
    let result = gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&Log::default()),
        &mut slow,
        &mut |_| {},
    );
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        fake.killed,
        Vec::<(i32, bool, usize)>::new(),
        "a joiner cancelled as it ran"
    );
    let joiners = launchers::joiners(&dir);
    assert_eq!(
        joiners.len(),
        usize::from(started_before_down),
        "{joiners:?}"
    );
    for pid in joiners {
        assert_eq!(launchers::terms(&dir, pid), 1);
    }
}
