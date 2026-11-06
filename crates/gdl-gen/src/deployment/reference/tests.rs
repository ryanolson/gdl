use super::upstream::{self, Parsed};
use super::*;

/// The name upstream's `Named.Name()` gives: the domain and the path.
fn name(reference: &Reference) -> String {
    match reference.domain.as_str() {
        "" => reference.path.clone(),
        domain => format!("{domain}/{}", reference.path),
    }
}

/// `TestReferenceParse`: `Parse` reads each input as upstream does, or
/// refuses it with upstream's error.
#[test]
fn parse_reads_upstreams_table() {
    let long = |n: usize| "a".repeat(n);
    let deep = format!("{}a", "a/".repeat(127));
    let by_hand = [
        (
            format!("domain/{}:tag", long(256)),
            Err(Refusal::NameTooLong),
        ),
        (
            format!("{deep}:tag-puts-this-over-max"),
            Ok((deep.clone(), "a", "tag-puts-this-over-max", "")),
        ),
        (
            format!("example.com/{}:tag", long(255)),
            Ok((
                format!("example.com/{}", long(255)),
                "example.com",
                "tag",
                "",
            )),
        ),
    ];
    let table = upstream::PARSE.iter().map(|(input, parsed)| {
        let expected = match parsed {
            Parsed::Ok {
                name,
                domain,
                tag,
                digest,
            } => Ok((name.to_string(), *domain, *tag, *digest)),
            Parsed::Err(refusal) => Err(*refusal),
        };
        (input.to_string(), expected)
    });
    let mut count = 0;
    for (input, expected) in table.chain(by_hand) {
        let found = parse(&input).map(|r| {
            let name = name(&r);
            (name, r.domain, r.tag, r.digest)
        });
        let expected = expected.map(|(name, domain, tag, digest)| {
            (
                name,
                domain.to_string(),
                tag.to_string(),
                digest.to_string(),
            )
        });
        assert_eq!(found, expected, "{input}");
        count += 1;
    }
    assert_eq!(count, 47, "every entry of upstream's table");
}

/// `TestValidateReferenceName` and `TestValidateRemoteName`:
/// `ParseNormalizedNamed` accepts each valid name and refuses each
/// invalid one.
#[test]
fn normalized_names_follow_upstreams_table() {
    for name in upstream::VALID.iter().chain(upstream::REMOTE_VALID) {
        assert!(parse_normalized_named(name).is_ok(), "{name}");
    }
    for name in upstream::INVALID.iter().chain(upstream::REMOTE_INVALID) {
        assert!(parse_normalized_named(name).is_err(), "{name}");
    }
}

/// `TestParseRepositoryInfo`, `TestNormalizedSplitHostname`, and
/// `TestParseDockerRef`: each name normalizes to upstream's domain and
/// path, Docker Hub's `docker.io` and `library/` filled in.
#[test]
fn normalizing_fills_in_docker_hub_as_upstream_does() {
    let tables = [
        upstream::REPOSITORY_INFO,
        upstream::SPLIT,
        upstream::DOCKER_REF,
    ];
    for (input, domain, path) in tables.into_iter().flatten() {
        let found = parse_normalized_named(input).expect(input);
        assert_eq!(
            (found.domain.as_str(), found.path.as_str()),
            (*domain, *path),
            "{input}"
        );
    }
}

/// `TestDomainRegexp`, `TestFullNameRegexp`, `TestReferenceRegexp`, and
/// `TestIdentifierRegexp`: the patterns, compiled by the `regex` crate,
/// match and capture as Go's engine does.
#[test]
fn the_patterns_match_as_upstreams_do() {
    let checked =
        |regex: &Regex, input: &str, matched: bool, subs: &[&str]| match regex.captures(input) {
            Some(captures) => {
                assert!(matched, "unexpected match for {input:?}");
                assert_eq!(&captures[0], input);
                for (i, sub) in subs.iter().enumerate() {
                    let found = captures.get(i + 1).map_or("", |m| m.as_str());
                    assert_eq!(found, *sub, "submatch {} of {input:?}", i + 1);
                }
            }
            None => assert!(!matched, "expected a match for {input:?}"),
        };
    for (input, matched) in upstream::DOMAIN {
        checked(&patterns::ANCHORED_DOMAIN, input, *matched, &[]);
    }
    for (input, matched, subs) in upstream::FULL_NAME {
        checked(&patterns::ANCHORED_NAME, input, *matched, subs);
    }
    let deep = format!("{}a", "a/".repeat(128));
    let rest = format!("{}a", "a/".repeat(127));
    checked(&patterns::ANCHORED_NAME, &deep, true, &["a", &rest]);
    for (input, matched, subs) in upstream::REFERENCE {
        checked(&patterns::REFERENCE, input, *matched, subs);
    }
    for (input, matched) in upstream::IDENTIFIER {
        checked(&patterns::ANCHORED_IDENTIFIER, input, *matched, &[]);
    }
    assert_eq!(patterns::ANCHORED_NAME.captures_len(), 3);
    assert_eq!(patterns::REFERENCE.captures_len(), 4);
}

/// Every image the repository's fixtures, examples, and book name, and
/// the forms the generator writes, pass: the rule refuses nothing that
/// worked before it.
#[test]
fn every_image_in_use_is_a_reference() {
    let sha256 = format!("sha256:{}", "0123456789abcdef".repeat(4));
    let images = [
        crate::deployment::DEFAULT_BASE_IMAGE,
        crate::deployment::DEFAULT_BUILD_IMAGE,
        gdl_model::SlurmSite::DEFAULT_BASE_IMAGE,
        "gcr.io/distroless/cc-debian12:nonroot",
        "nvcr.io/nvidia/cuda:12.8.1-runtime-ubuntu24.04",
        "rust:1.93.1-slim-bookworm",
        "rust:1.93-bookworm",
        "rust:1.93",
        "debian:bookworm",
        "debian:bookworm-slim",
        "debian:trixie-slim",
        "ubuntu:24.04",
        "cuda:12",
        "hub:1",
        "debian",
        "library/debian",
        "localhost/gdl/hub",
        "localhost:5000/gdl/hub:pinned",
        "registry.example:5000/gdl/team/chat-hub:linux-arm64-0123456789abcdef",
        "000000000000.dkr.ecr.us-east-1.amazonaws.com/gdl/chat-hub:_x",
        "[::1]:5000/gdl/hub:1",
        "[fe80::1]/gdl",
        "my-org/my__sub.part---x:v1.2.3",
        &format!("debian@{sha256}"),
        &format!("registry.example/gdl/hub:pinned@{sha256}"),
        &format!("debian@sha384:{}", "a".repeat(96)),
        &format!("debian@sha512:{}", "a".repeat(128)),
    ];
    for image in images {
        assert_eq!(check_image(image), Ok(()), "{image}");
    }
}

/// A first component that is no host, such as one with `_`, is read as
/// the first path component, as distribution reads it; containerd then
/// pulls from it as a host. A first component with an uppercase letter is
/// a host, as upstream's `TestParseRepositoryInfo` has it.
#[test]
fn a_first_component_that_is_no_host_is_a_path() {
    assert_eq!(
        check_image("registry_dev.example.com/team/base:dev"),
        Ok(())
    );
    assert_eq!(check_registry("registry_dev.example.com/team"), Ok(()));
    assert_eq!(check_image("Foo/bar"), Ok(()));
}

/// Strings of an image's characters that Docker does not read as one are
/// refused, each with why, in terms of what the user wrote.
#[test]
fn what_is_not_a_reference_is_refused() {
    let neither = "is neither a host, such as `registry.example:5000`, nor a path component";
    let cases = [
        ("", "it names no repository".to_string()),
        (":", "it names no repository".into()),
        ("@sha256:", "it names no repository".into()),
        (":tag", "it names no repository".into()),
        ("a:", "its tag, after `:`, is empty".into()),
        (
            "registry.example/gdl:",
            "its tag, after `:`, is empty".into(),
        ),
        (
            "Debian:12",
            "`Debian`, which Docker reads as the repository path".into(),
        ),
        (
            "registry.example/GDL/hub",
            "`GDL/hub`, which Docker reads as the repository path".into(),
        ),
        ("-debian", "`-debian` is not a path component".into()),
        ("debian-", "`debian-` is not a path component".into()),
        ("a b", "`a b` is not a path component".into()),
        ("a___b", "`a___b` is not a path component".into()),
        ("a..b", "`a..b` is not a path component".into()),
        ("/debian", "it has an empty path component".into()),
        ("gdl//hub", "it has an empty path component".into()),
        ("registry.example/", "it has an empty path component".into()),
        (
            "registry.example:/gdl",
            format!("`registry.example:` {neither}"),
        ),
        (
            "registry.example:50a/gdl",
            format!("`registry.example:50a` {neither}"),
        ),
        ("-bad.example/gdl", format!("`-bad.example` {neither}")),
        ("bad..example/gdl", format!("`bad..example` {neither}")),
        ("[::1/gdl", format!("`[::1` {neither}")),
        ("[]/gdl", format!("`[]` {neither}")),
        ("[::1]x/gdl", format!("`[::1]x` {neither}")),
        (
            "https://registry.example/gdl",
            format!("`https:` {neither}"),
        ),
        ("debian:a/b", format!("`debian:a` {neither}")),
        ("debian:-x", "tag `-x` is not".into()),
        ("debian:.x", "tag `.x` is not".into()),
        ("debian:a:b", "tag `a:b` is not".into()),
        ("debian@sha256:", "digest `sha256:` is not".into()),
        ("debian@sha256:00", "digest `sha256:00` is not".into()),
        (
            "debian@md5:0123456789abcdef0123456789abcdef",
            "digest `md5:".into(),
        ),
        ("debian@@sha256:", "digest `@sha256:` is not".into()),
        (
            &"0123456789abcdef".repeat(4),
            "it is 64 hex digits, which name an image by its ID".into(),
        ),
    ];
    for (image, why) in cases {
        let refusal = check_image(image).expect_err(image);
        let prefix = format!("`{image}` is not an image reference {GRAMMAR}: {why}");
        assert!(refusal.starts_with(&prefix), "{image}: {refusal}");
    }
    let upper = format!("debian@sha256:{}", "0123456789ABCDEF".repeat(4));
    assert!(check_image(&upper).is_err());
    assert!(check_image(&format!("debian:{}", "x".repeat(129))).is_err());
    assert!(check_image(&format!("debian:{}", "x".repeat(128))).is_ok());
}

/// The length limit is the path's, without the host, and counts the
/// `library/` that Docker Hub reads before a path of one component,
/// whether the name has no host or names Docker Hub.
#[test]
fn the_length_limit_counts_the_path_as_docker_reads_it() {
    let long = format!("{}/{}", "a".repeat(127), "b".repeat(127));
    assert_eq!(check_image(&long), Ok(()));
    assert!(
        check_image(&format!("{long}b"))
            .expect_err("a long path")
            .ends_with("its repository path is longer than 255 characters")
    );
    assert_eq!(
        check_image(&format!("registry.example:5000/{long}")),
        Ok(())
    );
    for hub in ["", "docker.io/", "index.docker.io/"] {
        assert_eq!(
            check_image(&format!("{hub}{}", "a".repeat(247))),
            Ok(()),
            "{hub}"
        );
        assert!(
            check_image(&format!("{hub}{}", "a".repeat(248)))
                .expect_err("a long name on Docker Hub")
                .ends_with("its repository path is longer than 255 characters, counting the `library/` that Docker Hub reads before a path of one component"),
            "{hub}"
        );
    }
}

/// Upstream lowercases as Go's `strings.ToLower` does, one rune to one
/// rune: `İ` is `i`, where Rust's `to_lowercase` gives `i` and a
/// combining dot. The error upstream picks follows from it.
#[test]
fn lowercasing_is_gos() {
    assert_eq!(
        parse("İ.example/repo:latest"),
        Err(Refusal::NameContainsUppercase)
    );
}

/// An uppercase refusal names the text upstream judged, as the user
/// wrote it, not a path that has no uppercase letter.
#[test]
fn an_uppercase_refusal_names_what_upstream_judged() {
    let digest = format!("alpine@SHA256:{}", "a".repeat(64));
    let refusal = check_image(&digest).expect_err("an uppercase digest algorithm");
    assert!(
        refusal.ends_with("`alpine@SHA256`, which Docker reads as the repository path (after the host, up to the first `:`), has an uppercase letter: each part of a path is lowercase letters and digits, joined by `.`, `_`, `__`, or runs of `-`"),
        "{refusal}"
    );
    let refusal = check_image("registry.example/GDL/hub:1").expect_err("an uppercase path");
    assert!(refusal.contains("`GDL/hub`"), "{refusal}");
    let refusal = check_image("alpine:\u{212A}").expect_err("a Kelvin sign");
    assert!(
        refusal.ends_with("`\u{212A}` is not ASCII, and every part of a reference is"),
        "{refusal}"
    );
}
