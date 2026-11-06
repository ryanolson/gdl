//! A fleet with a Slurm site: `deploy` and `down` run Kubernetes clusters
//! only, and no command runs a Slurm site yet, so both refuse the fleet,
//! naming the site, before they run kubectl or Docker once.

use super::*;

/// A Slurm site that `check` takes, with no pool on it.
const SITE: &str = "\n[cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = \"/shared/srt-slurm\"\nrun_dir = \"/shared/gdl-runs\"\nregistry = \"registry.example/gdl\"\nplatform = \"linux/amd64\"\n";

#[test]
fn deploy_and_down_refuse_a_fleet_with_a_slurm_site() {
    let mirror = Mirror::new();
    let fleet = format!("{}{SITE}", plain_fleet());
    let checked = gdl_model::Fleet::from_toml(&fleet)
        .expect("a fleet")
        .check(None);
    assert!(checked.is_ok(), "{checked:?}");
    let files = files(&mirror, &fleet);
    let log = Log::default();
    let mut docker = Docker {
        log: Rc::clone(&log),
        fail: false,
    };
    let mut kube = Kube::new(&log);
    let named = [
        "[cluster.lab]",
        "`lab` is a Slurm site",
        "no command renders or runs a Slurm site yet",
    ];
    let refusal = deploy(&mirror, &files, &mut docker, &mut kube)
        .expect_err("refused")
        .to_string();
    for name in named {
        assert!(refusal.contains(name), "{name} in {refusal}");
    }
    assert!(refusal.contains("`cargo gdl deploy`"), "{refusal}");
    let refusal = down(&files, &mut kube).expect_err("refused").to_string();
    for name in named {
        assert!(refusal.contains(name), "{name} in {refusal}");
    }
    assert_eq!(steps(&log), Vec::<String>::new());
}
