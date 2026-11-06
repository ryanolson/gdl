use super::*;

#[test]
fn down_signals_replicated_segment_steps_on_fully_qualified_nodes() {
    let mirror = Mirror::new();
    let site = Site::new(&mirror, 1, "");
    let fleet = std::fs::read_to_string(&site.files.fleet).expect("the fleet");
    std::fs::write(
        &site.files.fleet,
        fleet.replace("ready = { front = 1 }", "ready = { front = 2 }"),
    )
    .expect("two front instances");
    let mut fake = Fake::new(&site);
    site.deploy(&mirror, &mut fake, false).expect("deployed");
    fake.calls.borrow_mut().clear();
    let mut slurm = QualifiedNodes { fake: &mut fake };
    let log = Log::default();
    gdl_gen::down(
        &site.files.fleet,
        &slurm_waits(),
        &mut Kube::new(&log),
        &mut slurm,
        &mut |_| {},
    )
    .expect("down");
}

struct QualifiedNodes<'a> {
    fake: &'a mut Fake,
}

impl gdl_gen::Slurm for QualifiedNodes<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        if command.program == "squeue" && command.args.first().is_some_and(|arg| arg == "--steps") {
            assert!(command.args.iter().any(|arg| arg == "--jobs=1002"));
            self.fake.calls.borrow_mut().push(command.clone());
            // srt-slurm appends the node name verbatim for replicated
            // services. Preparation steps alone have the final suffix.
            return Ok(gdl_gen::Reply::ok(
                "1002.0|service_hub\n\
                 1002.1|service_front_node01.cluster\n\
                 1002.2|service_front_node02.cluster\n\
                 1002.3|service_front_node03.cluster.container-prepare\n",
            ));
        }
        if command.program == "curl"
            && command
                .args
                .last()
                .is_some_and(|url| url.ends_with("/instances"))
        {
            let targets: Vec<String> = self
                .fake
                .lines()
                .into_iter()
                .filter(|line| line.starts_with(&["scancel".into(), "--signal=TERM".into()]))
                .flat_map(|line| line.into_iter().skip(2))
                .collect();
            // Check before polling so a missing signal cannot consume
            // the pool's full drain grace.
            assert_eq!(
                targets,
                ["1002.1", "1002.2"],
                "both segment steps must receive TERM before waiting for their instances to leave"
            );
        }
        gdl_gen::Slurm::run(self.fake, command)
    }
}
