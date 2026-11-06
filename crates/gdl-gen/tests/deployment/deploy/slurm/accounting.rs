use super::*;

#[test]
fn an_accounting_cancellation_with_its_actor_is_terminal() {
    let mirror = Mirror::new();
    for actor in [false, true] {
        let site = Site::new(&mirror, 1, "");
        let mut fake = Fake::new(&site);
        let mut slurm = Cancelled {
            fake: &mut fake,
            actor,
        };
        let mut waits = slurm_waits();
        waits.health = Duration::from_millis(150);
        let how = gdl_gen::Deploy {
            spec: site.files.spec.clone(),
            fleet: site.files.fleet.clone(),
            target: mirror.target(),
            out: None,
            waits,
            sources: gdl_gen::Sources::Fetched,
            allow_pool_restart: false,
            images: Some(site.images.clone()),
            replace: false,
        };
        let log = Log::default();
        let mut docker = Docker {
            log: Rc::clone(&log),
            fail: false,
        };
        let error = gdl_gen::deploy(
            &how,
            &mut docker,
            &mut Kube::new(&log),
            &mut slurm,
            &mut |_| {},
        )
        .expect_err("the pool's job was cancelled")
        .to_string();
        assert!(
            error.contains("job 1003 ended: Slurm says CANCELLED"),
            "actor={actor}: accounting cancellation was treated as a live job:\n{error}"
        );
        assert!(error.contains("/outputs/1003/logs"), "{error}");
    }
}

struct Cancelled<'a> {
    fake: &'a mut Fake,
    actor: bool,
}

impl gdl_gen::Slurm for Cancelled<'_> {
    fn run(&mut self, command: &gdl_gen::SlurmCommand) -> Result<gdl_gen::Reply, gdl_gen::Error> {
        // The pool's job is cancelled once the health wait has begun.
        if command.program == "curl"
            && command
                .args
                .last()
                .is_some_and(|url| url.ends_with("/health"))
        {
            self.fake.job(1003).state = "CANCELLED";
        }
        let mut reply = gdl_gen::Slurm::run(self.fake, command)?;
        if command.program == "sacct" && self.actor {
            // sacct's parsable output preserves the cancelling UID.
            reply.stdout = reply.stdout.replace("|CANCELLED|", "|CANCELLED by 1000|");
        }
        Ok(reply)
    }
}
