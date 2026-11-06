//! The one rule an image reference is judged by, wherever the generator
//! reads or writes one: whether Docker, containerd, and Kubernetes read it
//! as a reference, `[host[:port]/]path[:tag][@digest]`.
//!
//! The rule is a port of the parser they share, not a reading of its
//! grammar: `ParseNormalizedNamed` of `github.com/distribution/reference`
//! at tag v0.6.0 (`regexp.go`, `reference.go`, `normalize.go`), and
//! `Digest.Validate` of `github.com/opencontainers/go-digest` at tag
//! v1.0.0 (`digest.go`, `algorithm.go`), both Apache-2.0. Its patterns are
//! upstream's, character for character, and its tests are upstream's own
//! tables. A hand-written reading of the grammar disagreed with the parser
//! on where a path's length is counted, on Docker Hub's `library/`, and on
//! a first component that is no host; a port leaves no such case to find.
//! Only the reason a refusal gives is gdl's ([`why`]): upstream's
//! "invalid reference format" does not say which part is wrong.

use std::sync::LazyLock;

use regex::{Regex, RegexBuilder};

/// The grammar, as each refusal names it.
const GRAMMAR: &str = "`[host[:port]/]path[:tag][@digest]`";

/// Refuse `image` unless Docker reads it as an image reference, saying
/// why.
pub(crate) fn check_image(image: &str) -> Result<(), String> {
    parse_normalized_named(image).map(drop).map_err(|refusal| {
        format!(
            "`{image}` is not an image reference {GRAMMAR}: {}",
            why(image, refusal)
        )
    })
}

/// Refuse `registry` unless an image reference can start with it: a host,
/// with a port if it has one, and then path components, or path
/// components alone, which name a Docker Hub namespace. A trailing `/` is
/// allowed. It is read as the start of `<registry>/<repository>`, as the
/// images it names are.
pub(crate) fn check_registry(registry: &str) -> Result<(), String> {
    let trimmed = registry.trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("it is empty".into());
    }
    let image = format!("{trimmed}/x");
    parse_normalized_named(&image)
        .map(drop)
        .map_err(|refusal| why(&image, refusal))
}

/// Whether `tag` is a tag: upstream's `anchoredTagRegexp`, 1 to 128
/// letters, digits, `_`, `.`, and `-`, starting with a letter, digit, or
/// `_`.
pub(crate) fn tag(tag: &str) -> bool {
    patterns::ANCHORED_TAG.is_match(tag)
}

/// A reference as upstream's `Parse` reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reference {
    domain: String,
    path: String,
    tag: String,
    digest: String,
}

/// Why upstream refuses a reference: its errors, by their names there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// `ErrNameEmpty`.
    NameEmpty,
    /// `ErrNameContainsUppercase`.
    NameContainsUppercase,
    /// `ErrReferenceInvalidFormat`.
    ReferenceInvalidFormat,
    /// `ErrNameTooLong`.
    NameTooLong,
    /// `ParseNormalizedNamed`'s refusal of 64 hex digits, an image ID.
    Identifier,
    /// `ParseNormalizedNamed`'s refusal of a repository path that is not
    /// lowercase.
    RemoteNameUppercase,
    /// go-digest's `ErrDigestInvalidFormat`.
    DigestInvalidFormat,
    /// go-digest's `ErrDigestInvalidLength`.
    DigestInvalidLength,
    /// go-digest's `ErrDigestUnsupported`.
    DigestUnsupported,
}

/// What Docker Hub reads before a path of one component.
const OFFICIAL_REPO_PREFIX: &str = "library/";

/// `ParseNormalizedNamed`: `s` with Docker Hub's domain and `library/`
/// filled in where it leaves them out, parsed.
fn parse_normalized_named(s: &str) -> Result<Reference, Refusal> {
    if patterns::ANCHORED_IDENTIFIER.is_match(s) {
        return Err(Refusal::Identifier);
    }
    let (domain, remainder) = split_docker_domain(s);
    let remote = match remainder.find(':') {
        Some(tag_sep) => &remainder[..tag_sep],
        None => &remainder,
    };
    if to_lower(remote) != remote {
        return Err(Refusal::RemoteNameUppercase);
    }
    parse(&format!("{domain}/{remainder}"))
}

/// `splitDockerDomain`: the domain of `name`, and the rest of it.
fn split_docker_domain(name: &str) -> (&str, String) {
    const LEGACY_DEFAULT_DOMAIN: &str = "index.docker.io";
    const DEFAULT_DOMAIN: &str = "docker.io";
    let Some((maybe_domain, maybe_remote_name)) = name.split_once('/') else {
        return (DEFAULT_DOMAIN, format!("{OFFICIAL_REPO_PREFIX}{name}"));
    };
    let (domain, mut remote_name) = if maybe_domain == patterns::LOCALHOST {
        (maybe_domain, maybe_remote_name.to_string())
    } else if maybe_domain == LEGACY_DEFAULT_DOMAIN {
        (DEFAULT_DOMAIN, maybe_remote_name.to_string())
    } else if maybe_domain.contains(['.', ':']) || to_lower(maybe_domain) != maybe_domain {
        (maybe_domain, maybe_remote_name.to_string())
    } else {
        (DEFAULT_DOMAIN, name.to_string())
    };
    if domain == DEFAULT_DOMAIN && !remote_name.contains('/') {
        remote_name.insert_str(0, OFFICIAL_REPO_PREFIX);
    }
    (domain, remote_name)
}

/// `Parse`: `s` read by the reference pattern, its path at most
/// `RepositoryNameTotalLengthMax` characters, and its digest validated.
fn parse(s: &str) -> Result<Reference, Refusal> {
    const REPOSITORY_NAME_TOTAL_LENGTH_MAX: usize = 255;
    let Some(matches) = patterns::REFERENCE.captures(s) else {
        if s.is_empty() {
            return Err(Refusal::NameEmpty);
        }
        if patterns::REFERENCE.is_match(&to_lower(s)) {
            return Err(Refusal::NameContainsUppercase);
        }
        return Err(Refusal::ReferenceInvalidFormat);
    };
    let group = |i: usize| matches.get(i).map_or("", |m| m.as_str());
    let name = group(1);
    let (domain, path) = match patterns::ANCHORED_NAME.captures(name) {
        Some(name_match) => (
            name_match.get(1).map_or("", |m| m.as_str()),
            name_match.get(2).map_or("", |m| m.as_str()),
        ),
        None => ("", name),
    };
    if path.len() > REPOSITORY_NAME_TOTAL_LENGTH_MAX {
        return Err(Refusal::NameTooLong);
    }
    let digest = group(3);
    if !digest.is_empty() {
        validate_digest(digest)?;
    }
    Ok(Reference {
        domain: domain.into(),
        path: path.into(),
        tag: group(2).into(),
        digest: digest.into(),
    })
}

/// Go's `strings.ToLower`: each character to its simple lowercase, one
/// to one. Rust's `to_lowercase` maps `İ` (U+0130) to `i` and a combining
/// dot, the one character whose full lowercase is longer, and reads a
/// final `Σ` in context; Go maps `İ` to `i`, and `Σ` to `σ` everywhere.
fn to_lower(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\u{130}' => 'i',
            c => c.to_lowercase().next().unwrap_or(c),
        })
        .collect()
}

/// go-digest's `Digest.Validate`, with the algorithms it registers: sha256,
/// sha384, and sha512, each in lowercase hex.
fn validate_digest(s: &str) -> Result<(), Refusal> {
    let Some(i) = s.find(':').filter(|&i| i > 0 && i + 1 != s.len()) else {
        return Err(Refusal::DigestInvalidFormat);
    };
    let (algorithm, encoded) = (&s[..i], &s[i + 1..]);
    let size = match algorithm {
        "sha256" => 32,
        "sha384" => 48,
        "sha512" => 64,
        _ if !patterns::DIGEST_ANCHORED.is_match(s) => return Err(Refusal::DigestInvalidFormat),
        _ => return Err(Refusal::DigestUnsupported),
    };
    if size * 2 != encoded.len() {
        return Err(Refusal::DigestInvalidLength);
    }
    if encoded
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Ok(());
    }
    Err(Refusal::DigestInvalidFormat)
}

/// A path component, as each refusal describes one.
const PATH_COMPONENT: &str =
    "lowercase letters and digits, joined by `.`, `_`, `__`, or runs of `-`";

/// Why `image`, which upstream refuses with `refusal`, is no reference, in
/// words a user can act on. It only words the refusal and never decides
/// it: for a format error, it names the first part of `image`, as the user
/// wrote it, that upstream's own patterns refuse.
fn why(image: &str, refusal: Refusal) -> String {
    match refusal {
        Refusal::NameEmpty => "it names no repository".into(),
        Refusal::RemoteNameUppercase => {
            // The text upstream judged, as the user wrote it: after the
            // host, up to the first `:`, without a `library/` it added.
            let (_, remainder) = split_docker_domain(image);
            let written = if image.ends_with(remainder.as_str()) {
                &remainder
            } else {
                &remainder[OFFICIAL_REPO_PREFIX.len()..]
            };
            let remote = written.split(':').next().unwrap_or_default();
            format!(
                "`{remote}`, which Docker reads as the repository path (after the host, up to the first `:`), has an uppercase letter: each part of a path is {PATH_COMPONENT}"
            )
        }
        // Upstream's patterns are ASCII, so a reference that matches once
        // lowercased holds a character outside ASCII whose lowercase is
        // in it, such as the Kelvin sign.
        Refusal::NameContainsUppercase => match image.chars().find(|c| !c.is_ascii()) {
            Some(c) => format!("`{c}` is not ASCII, and every part of a reference is"),
            None => "it has an uppercase letter where a reference holds only lowercase".into(),
        },
        Refusal::NameTooLong => {
            let (_, remote) = split_docker_domain(image);
            let added =
                remote.starts_with(OFFICIAL_REPO_PREFIX) && !image.contains(OFFICIAL_REPO_PREFIX);
            let hub = if added {
                ", counting the `library/` that Docker Hub reads before a path of one component"
            } else {
                ""
            };
            format!("its repository path is longer than 255 characters{hub}")
        }
        Refusal::Identifier => {
            "it is 64 hex digits, which name an image by its ID, not a repository".into()
        }
        Refusal::DigestInvalidFormat
        | Refusal::DigestInvalidLength
        | Refusal::DigestUnsupported => {
            let digest = image.split_once('@').map_or("", |(_, digest)| digest);
            format!(
                "digest `{digest}` is not `sha256:`, `sha384:`, or `sha512:` and its 64, 96, or 128 lowercase hex digits"
            )
        }
        Refusal::ReferenceInvalidFormat => format_error(image),
    }
}

/// Why `image`, which upstream's reference pattern does not match, is no
/// reference: its first part, as the user wrote it, that upstream's own
/// patterns refuse.
fn format_error(image: &str) -> String {
    let (rest, digest) = match image.split_once('@') {
        Some((rest, digest)) => (rest, Some(digest)),
        None => (image, None),
    };
    // A `:` after the last `/` starts the tag; one before it is a port.
    let path_start = rest.rfind('/').map_or(0, |slash| slash + 1);
    let (name, tag) = match rest[path_start..].find(':') {
        Some(colon) => (
            &rest[..path_start + colon],
            Some(&rest[path_start + colon + 1..]),
        ),
        None => (rest, None),
    };
    if name.is_empty() {
        return "it names no repository".into();
    }
    if let Some(digest) = digest
        && !patterns::ANCHORED_DIGEST.is_match(digest)
    {
        return format!("digest `{digest}` is not `<algorithm>:<hex digits>`");
    }
    match tag {
        Some("") => return "its tag, after `:`, is empty".into(),
        Some(tag) if !self::tag(tag) => {
            return format!(
                "tag `{tag}` is not 1 to 128 letters, digits, `_`, `.`, and `-`, starting with a letter, digit, or `_`"
            );
        }
        _ => {}
    }
    let mut components = name.split('/');
    let first = components.next().unwrap_or_default();
    let more = name.contains('/');
    let component = |part: &str| patterns::ANCHORED_PATH_COMPONENT.is_match(part);
    let host = more && !component(first) && patterns::ANCHORED_DOMAIN.is_match(first);
    if more && !first.is_empty() && !component(first) && !host {
        return format!(
            "`{first}` is neither a host, such as `registry.example:5000`, nor a path component: {PATH_COMPONENT}"
        );
    }
    let path = if host {
        components.collect::<Vec<_>>()
    } else {
        std::iter::once(first).chain(components).collect()
    };
    match path.into_iter().find(|part| !component(part)) {
        Some("") => "it has an empty path component, between two `/` or at an end".into(),
        Some(part) => format!("`{part}` is not a path component: {PATH_COMPONENT}"),
        None => "it does not read as one".into(),
    }
}

/// distribution/reference v0.6.0's `regexp.go`, ported. Each pattern and
/// each helper is upstream's, in its order; only the anchored forms of the
/// domain and path component, which upstream has no names for, are added,
/// for [`why`].
mod patterns {
    use super::{LazyLock, Regex, RegexBuilder};

    pub(super) const ALPHANUMERIC: &str = r"[a-z0-9]+";
    pub(super) const SEPARATOR: &str = r"(?:[._]|__|[-]+)";
    pub(super) const LOCALHOST: &str = r"localhost";
    pub(super) const DOMAIN_NAME_COMPONENT: &str =
        r"(?:[a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])";
    pub(super) const OPTIONAL_PORT: &str = r"(?::[0-9]+)?";
    pub(super) const TAG: &str = r"[\w][\w.-]{0,127}";
    pub(super) const DIGEST_PAT: &str =
        r"[A-Za-z][A-Za-z0-9]*(?:[-_+.][A-Za-z][A-Za-z0-9]*)*[:][[:xdigit:]]{32,}";
    pub(super) const IDENTIFIER: &str = r"([a-f0-9]{64})";
    pub(super) const IPV6ADDRESS: &str = r"\[(?:[a-fA-F0-9:]+)\]";

    pub(super) fn domain_name() -> String {
        DOMAIN_NAME_COMPONENT.to_string() + &any_times(&[r"\.", DOMAIN_NAME_COMPONENT])
    }
    pub(super) fn host() -> String {
        r"(?:".to_string() + &domain_name() + r"|" + IPV6ADDRESS + r")"
    }
    pub(super) fn domain_and_port() -> String {
        host() + OPTIONAL_PORT
    }
    pub(super) fn path_component() -> String {
        ALPHANUMERIC.to_string() + &any_times(&[SEPARATOR, ALPHANUMERIC])
    }
    pub(super) fn remote_name() -> String {
        path_component() + &any_times(&[r"/", &path_component()])
    }
    pub(super) fn name_pat() -> String {
        optional(&[&domain_and_port(), r"/"]) + &remote_name()
    }
    pub(super) fn reference_pat() -> String {
        anchored(&[
            &capture(&[&name_pat()]),
            &optional(&[r":", &capture(&[TAG])]),
            &optional(&[r"@", &capture(&[DIGEST_PAT])]),
        ])
    }

    /// go-digest v1.0.0's `DigestRegexp`.
    pub(super) const GO_DIGEST: &str = r"[a-z0-9]+(?:[.+_-][a-z0-9]+)*:[a-zA-Z0-9=_-]+";

    pub(super) static REFERENCE: LazyLock<Regex> = LazyLock::new(|| compile(&reference_pat()));
    pub(super) static ANCHORED_NAME: LazyLock<Regex> = LazyLock::new(|| {
        compile(&anchored(&[
            &optional(&[&capture(&[&domain_and_port()]), r"/"]),
            &capture(&[&remote_name()]),
        ]))
    });
    pub(super) static ANCHORED_TAG: LazyLock<Regex> = LazyLock::new(|| compile(&anchored(&[TAG])));
    pub(super) static ANCHORED_DIGEST: LazyLock<Regex> =
        LazyLock::new(|| compile(&anchored(&[DIGEST_PAT])));
    pub(super) static ANCHORED_IDENTIFIER: LazyLock<Regex> =
        LazyLock::new(|| compile(&anchored(&[IDENTIFIER])));
    /// go-digest v1.0.0's `DigestRegexpAnchored`.
    pub(super) static DIGEST_ANCHORED: LazyLock<Regex> =
        LazyLock::new(|| compile(&(r"^".to_string() + GO_DIGEST + r"$")));
    pub(super) static ANCHORED_DOMAIN: LazyLock<Regex> =
        LazyLock::new(|| compile(&anchored(&[&domain_and_port()])));
    pub(super) static ANCHORED_PATH_COMPONENT: LazyLock<Regex> =
        LazyLock::new(|| compile(&anchored(&[&path_component()])));

    /// A pattern compiled as Go compiles it: Go's `\w` is ASCII, where the
    /// `regex` crate's is Unicode unless Unicode is off. Both engines match
    /// leftmost-first, so their captures agree.
    fn compile(pattern: &str) -> Regex {
        RegexBuilder::new(pattern)
            .unicode(false)
            .build()
            .expect("upstream's patterns compile")
    }

    pub(super) fn optional(res: &[&str]) -> String {
        r"(?:".to_string() + &res.concat() + r")?"
    }
    pub(super) fn any_times(res: &[&str]) -> String {
        r"(?:".to_string() + &res.concat() + r")*"
    }
    pub(super) fn capture(res: &[&str]) -> String {
        r"(".to_string() + &res.concat() + r")"
    }
    pub(super) fn anchored(res: &[&str]) -> String {
        r"^".to_string() + &res.concat() + r"$"
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod upstream;
