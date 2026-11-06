//! A fleet's Slurm site (`[cluster.<name>] kind = "slurm"`), checked with
//! the spec it deploys: the site's signal lead covers each pool's stop
//! wait; a `[deployment.codegen.<arch>]` table for the site's
//! architecture is taken, since its images apply it; and its registry and
//! platform are an image's.

use super::*;

/// The chat graph's two pools on Slurm site `lab`.
fn fleet(site: &str) -> String {
    format!(
        "[fleet]\nname = \"chat\"\n\
         [cluster.lab]\nkind = \"slurm\"\nregion = \"slurm-lab\"\nsrtctl = \"/shared/srt-slurm\"\nrun_dir = \"/shared/gdl-runs\"\n\
         registry = \"registry.example/gdl\"\n{site}\n\
         [class.global]\nready = {{ front = 1 }}\n[class.model_x]\nready = {{ engine = 1 }}\n\
         [pool.global]\nclass = \"global\"\ncluster = \"lab\"\n\
         [pool.model-x]\nclass = \"model_x\"\ncluster = \"lab\"\nhome = \"global\"\n"
    )
}

const ARM: &str = "platform = \"linux/arm64\"";

/// The problems of `check` of [`spec`] with `extra` and [`fleet`] with
/// `site`; empty when it passes.
fn checked(extra: &str, site: &str) -> Vec<String> {
    let files = Files::new(&spec(extra), &fleet(site));
    let check = files.check();
    if check.status.success() {
        return Vec::new();
    }
    assert_eq!(check.status.code(), Some(1), "{check:?}");
    let at = format!("{}: ", files.fleet().display());
    problems(&check)
        .into_iter()
        .map(|line| line.strip_prefix(&at).unwrap_or(&line).to_string())
        .collect()
}

/// `check` refuses a lead that does not cover a pool's stop wait, 660 s by
/// default, and 120 s more, naming both; the default lead, 900 s, does.
#[test]
fn the_signal_lead_covers_each_pools_stop_wait() {
    assert_eq!(checked("", ARM), Vec::<String>::new());
    let found = checked("", &format!("{ARM}\nsignal_lead_s = 700"));
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].starts_with("[cluster.lab] signal_lead_s: is 700 s, below pool `global`'s stop wait of 660 s plus 120 s, 780 s"),
        "{found:?}"
    );
}

/// A Slurm site's images are built by the path a Kubernetes cluster's
/// are, which applies the codegen table of each image's architecture
/// (`images::a_slurm_sites_images_apply_the_codegen_table`), so `check`
/// takes a table for the site's architecture, as for any other.
#[test]
fn a_codegen_table_for_a_slurm_sites_architecture_is_taken() {
    let codegen = "[deployment.codegen.arm64]\ntarget_features = [\"+lse\"]\n";
    assert_eq!(checked(codegen, ARM), Vec::<String>::new());
    assert_eq!(
        checked(codegen, "platform = \"linux/amd64\""),
        Vec::<String>::new()
    );
}

/// A site's registry and platform name images as a Kubernetes cluster's
/// do; a missing one is the fleet's own problem.
#[test]
fn a_slurm_sites_registry_and_platform_are_an_images() {
    let found = checked("", "platform = \"arm64\"");
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].starts_with("[cluster.lab] platform: "),
        "{found:?}"
    );
    let found = checked("", "");
    assert_eq!(
        found,
        [
            "[cluster.lab] platform: missing; a Slurm site's images are built for it, and its jobs run on its nodes"
        ]
    );
}

/// A site's `base_image` and `hub_image` are image references by the
/// grammar the spec's own images are held to, each refusal naming its key
/// and why.
#[test]
fn a_slurm_sites_images_are_references() {
    let refused = |base: &str, hub: &str| {
        checked(
            "",
            &format!("{ARM}\nbase_image = \"{base}\"\nhub_image = \"{hub}\""),
        )
    };
    assert_eq!(
        refused("-debian", "a b"),
        [
            "[cluster.lab] base_image: `-debian` is not an image reference `[host[:port]/]path[:tag][@digest]`: `-debian` is not a path component: lowercase letters and digits, joined by `.`, `_`, `__`, or runs of `-`",
            "[cluster.lab] hub_image: `a b` is not an image reference `[host[:port]/]path[:tag][@digest]`: `a b` is not a path component: lowercase letters and digits, joined by `.`, `_`, `__`, or runs of `-`",
        ]
    );
    assert_eq!(
        refused(":", "a:"),
        [
            "[cluster.lab] base_image: `:` is not an image reference `[host[:port]/]path[:tag][@digest]`: it names no repository",
            "[cluster.lab] hub_image: `a:` is not an image reference `[host[:port]/]path[:tag][@digest]`: its tag, after `:`, is empty",
        ]
    );
    assert_eq!(
        refused("debian:bookworm-slim", "registry.example:5000/gdl/hub:1"),
        Vec::<String>::new()
    );
}
