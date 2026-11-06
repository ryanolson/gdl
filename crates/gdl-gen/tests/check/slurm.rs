//! A fleet's Slurm site (`[cluster.<name>] kind = "slurm"`), checked with
//! the spec it deploys: the site's signal lead covers each pool's stop
//! wait; a `[deployment.codegen.<arch>]` table
//! for the site's architecture is refused, since no Slurm image is built
//! with it yet; and its registry and platform are an image's.

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

/// No Slurm site's images are built with target features yet, so a
/// codegen table for a site's architecture is refused, never ignored; a
/// table for another architecture is not the site's.
#[test]
fn a_codegen_table_for_a_slurm_sites_architecture_is_refused() {
    let codegen = "[deployment.codegen.arm64]\ntarget_features = [\"+lse\"]\n";
    assert_eq!(
        checked(codegen, ARM),
        [
            "[cluster.lab] platform: is `linux/arm64`, and the spec's `[deployment.codegen.arm64]` builds `arm64` images with target features, which no Slurm site's images are built with yet: leave the table out, or run the fleet's `arm64` pools on Kubernetes clusters"
        ]
    );
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

/// A site's `base_image` and `hub_image` are image references by the rule
/// the spec's own images are held to.
#[test]
fn a_slurm_sites_images_are_references() {
    let found = checked(
        "",
        &format!("{ARM}\nbase_image = \"-debian\"\nhub_image = \"a b\""),
    );
    assert_eq!(
        found,
        [
            "[cluster.lab] base_image: invalid container image reference `-debian`",
            "[cluster.lab] hub_image: invalid container image reference `a b`",
        ]
    );
}
