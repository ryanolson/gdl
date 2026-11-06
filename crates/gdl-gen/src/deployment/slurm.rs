//! What `check` refuses of a fleet's Slurm sites beyond what
//! [`gdl_model::Fleet::check`] does, which needs the generator: a registry
//! or a platform an image cannot be named with, a `base_image` or
//! `hub_image` that is not an image reference, and a
//! `[deployment.codegen.<arch>]` table of the spec that no Slurm site's
//! images are built with yet.

use gdl_model::{Fleet, Problem};

use super::{images, k8s};
use crate::Spec;

/// Every problem of `fleet`'s Slurm sites, each at its site's table, with
/// `spec` when it reads.
pub(crate) fn problems(fleet: &Fleet, spec: Option<&Spec>) -> Vec<Problem> {
    let mut problems = Vec::new();
    for (name, site) in fleet
        .clusters()
        .filter_map(|(name, cluster)| Some((name, cluster.slurm()?)))
    {
        let at = |key: &str, message: String| {
            Problem::at(Some(format!("cluster.{name}")), Some(key.into()), message)
        };
        for (key, image) in [
            ("base_image", &site.base_image),
            ("hub_image", &site.hub_image),
        ] {
            if let Some(Err(why)) = image.as_deref().map(super::reference::check_image) {
                problems.push(at(key, why));
            }
        }
        if let Some(registry) = &site.registry
            && let Err(message) = k8s::check_registry(registry)
        {
            problems.push(at("registry", message));
        }
        if let Some(platform) = &site.platform {
            match k8s::check_platform(platform) {
                Err(message) => problems.push(at("platform", message)),
                Ok(()) => {
                    let arch = images::arch(platform);
                    if spec
                        .and_then(|spec| spec.deployment.as_ref())
                        .is_some_and(|settings| settings.codegen(arch).is_some())
                    {
                        problems.push(at(
                            "platform",
                            format!(
                                "is `{platform}`, and the spec's `[deployment.codegen.{arch}]` builds `{arch}` images with target features, which no Slurm site's images are built with yet: leave the table out, or run the fleet's `{arch}` pools on Kubernetes clusters"
                            ),
                        ));
                    }
                }
            }
        }
    }
    problems
}
