//! [`Docker`] over the `docker` CLI's `buildx`: it builds with the default
//! builder and asks registries with `imagetools`, both as the user's
//! Docker login allows. No argument names a login or a configuration
//! directory, and nothing here reads one.

use std::ffi::OsString;
use std::path::Path;
use std::process::Command;

use super::Docker;
use crate::Error;

/// Runs `docker buildx`.
#[derive(Debug, Default)]
pub struct Buildx;

impl Docker for Buildx {
    fn digest(&mut self, reference: &str) -> Result<Option<String>, Error> {
        let output = Command::new("docker")
            .args(inspect_args(reference))
            .output()
            .map_err(|e| Error::new(format!("running `docker buildx imagetools inspect`: {e}")))?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            if absent(&stderr) {
                return Ok(None);
            }
            return Err(Error::new(format!(
                "`docker buildx imagetools inspect {reference}` failed: {}",
                stderr.trim()
            )));
        }
        manifest_digest(&String::from_utf8_lossy(&output.stdout))
            .map(Some)
            .map_err(|why| Error::new(format!("`{reference}`: {why}")))
    }

    fn build(
        &mut self,
        context: &Path,
        platform: &str,
        reference: &str,
        push: bool,
    ) -> Result<String, Error> {
        let scratch = tempfile::Builder::new()
            .prefix("gdl-buildx-")
            .tempdir()
            .map_err(super::io)?;
        let metadata = scratch.path().join("metadata.json");
        let status = Command::new("docker")
            .args(build_args(context, platform, reference, &metadata, push))
            .status()
            .map_err(|e| Error::new(format!("running `docker buildx build`: {e}")))?;
        if !status.success() {
            return Err(Error::new(format!(
                "`docker buildx build` of `{reference}` for `{platform}` failed ({status})"
            )));
        }
        let text = std::fs::read_to_string(&metadata).map_err(super::io)?;
        built_digest(&text).map_err(|why| Error::new(format!("`{reference}`: {why}")))
    }
}

/// The arguments that ask `reference`'s registry for its manifest.
fn inspect_args(reference: &str) -> Vec<String> {
    [
        "buildx",
        "imagetools",
        "inspect",
        "--format",
        "{{json .Manifest}}",
        reference,
    ]
    .map(String::from)
    .to_vec()
}

/// The arguments that build `context` for `platform` as `reference`, and
/// push it or load it. Provenance and SBOM attestations are off, so that
/// the digest is the image's own manifest's, which a pod's runtime pulls,
/// and not an index's around it.
fn build_args(
    context: &Path,
    platform: &str,
    reference: &str,
    metadata: &Path,
    push: bool,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = [
        "buildx",
        "build",
        "--platform",
        platform,
        "--provenance=false",
        "--sbom=false",
        "--tag",
        reference,
        if push { "--push" } else { "--load" },
        "--metadata-file",
    ]
    .map(OsString::from)
    .to_vec();
    args.push(metadata.into());
    args.push(context.into());
    args
}

/// Whether `imagetools inspect` failed because the registry holds no such
/// image: a missing tag, or a missing repository.
fn absent(stderr: &str) -> bool {
    ["not found", "manifest unknown", "name unknown"]
        .iter()
        .any(|why| stderr.contains(why))
}

/// The digest of the manifest `imagetools inspect` printed as JSON.
fn manifest_digest(stdout: &str) -> Result<String, String> {
    let manifest: serde_json::Value = serde_json::from_str(stdout)
        .map_err(|e| format!("`imagetools inspect` printed no manifest: {e}"))?;
    digest(&manifest["digest"])
}

/// The digest of the image a build's metadata file records.
fn built_digest(metadata: &str) -> Result<String, String> {
    let metadata: serde_json::Value = serde_json::from_str(metadata)
        .map_err(|e| format!("the build's metadata is not JSON: {e}"))?;
    digest(&metadata["containerimage.digest"])
}

fn digest(value: &serde_json::Value) -> Result<String, String> {
    let digest = value.as_str().ok_or("no digest was recorded")?;
    super::check_digest(digest)?;
    Ok(digest.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No command names a login or a configuration directory: the user's
    /// own Docker login is used, and no credential is read.
    #[test]
    fn no_command_names_a_login_or_a_configuration() {
        let reference = "registry.example/gdl/api-hub:0123";
        let mut commands = vec![
            inspect_args(reference),
            build_args(
                Path::new("/ctx"),
                "linux/amd64",
                reference,
                Path::new("/m.json"),
                true,
            )
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect(),
        ];
        commands.push(
            build_args(
                Path::new("/ctx"),
                "linux/amd64",
                reference,
                Path::new("/m"),
                false,
            )
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect(),
        );
        for args in &commands {
            for arg in args {
                for word in [
                    "login",
                    "--config",
                    "DOCKER_CONFIG",
                    "auth",
                    "password",
                    "token",
                ] {
                    assert!(!arg.contains(word), "{args:?}");
                }
            }
        }
    }

    /// A build names its platform, turns attestations off, records its
    /// metadata, and pushes or loads, with the context last.
    #[test]
    fn a_build_names_its_platform_and_where_it_goes() {
        let args = |push| -> Vec<String> {
            build_args(
                Path::new("/ctx"),
                "linux/arm64",
                "r.example/a:t",
                Path::new("/m.json"),
                push,
            )
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
        };
        assert_eq!(
            args(true),
            [
                "buildx",
                "build",
                "--platform",
                "linux/arm64",
                "--provenance=false",
                "--sbom=false",
                "--tag",
                "r.example/a:t",
                "--push",
                "--metadata-file",
                "/m.json",
                "/ctx",
            ]
        );
        assert!(args(false).contains(&"--load".to_string()));
        assert!(!args(false).contains(&"--push".to_string()));
    }

    /// The digests are read from what `imagetools inspect` and a build's
    /// metadata file print, and an image the registry lacks is absent,
    /// while any other failure is an error.
    #[test]
    fn digests_are_read_and_absence_is_told_apart() {
        let digest = format!("sha256:{}", "0f".repeat(32));
        let manifest = format!(
            "{{\"mediaType\": \"application/vnd.docker.distribution.manifest.v2+json\", \"digest\": \"{digest}\", \"size\": 734}}"
        );
        assert_eq!(manifest_digest(&manifest), Ok(digest.clone()));
        let metadata = format!(
            "{{\"containerimage.config.digest\": \"sha256:{}\", \"containerimage.digest\": \"{digest}\"}}",
            "aa".repeat(32)
        );
        assert_eq!(built_digest(&metadata), Ok(digest));
        assert!(built_digest("{}").is_err());
        assert!(manifest_digest("not json").is_err());
        assert!(absent("ERROR: 127.0.0.1:5000/probe:missing: not found"));
        assert!(!absent(
            "ERROR: failed to do request: dial tcp 127.0.0.1:5000: connect: connection refused"
        ));
    }
}
