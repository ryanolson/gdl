//! The test tables of `github.com/distribution/reference` at tag v0.6.0
//! (Apache-2.0), ported entry for entry from `reference_test.go`,
//! `normalize_test.go`, and `regexp_test.go`. The few entries upstream
//! builds with `strings.Repeat` are in the tests that read these tables.

use super::Refusal;

/// What `Parse` gives for an input: upstream's repository name (its
/// domain and path), domain, tag, and digest, or its error.
pub(super) enum Parsed {
    Ok {
        name: &'static str,
        domain: &'static str,
        tag: &'static str,
        digest: &'static str,
    },
    Err(Refusal),
}

/// `TestReferenceParse` (reference_test.go): `Parse`'s input, and its
/// name, domain, tag, and digest, or the error it returns.
pub(super) const PARSE: &[(&str, Parsed)] = &[
    (
        "test_com",
        Parsed::Ok {
            name: "test_com",
            domain: "",
            tag: "",
            digest: "",
        },
    ),
    (
        "test.com:tag",
        Parsed::Ok {
            name: "test.com",
            domain: "",
            tag: "tag",
            digest: "",
        },
    ),
    (
        "test.com:5000",
        Parsed::Ok {
            name: "test.com",
            domain: "",
            tag: "5000",
            digest: "",
        },
    ),
    (
        "test.com/repo:tag",
        Parsed::Ok {
            name: "test.com/repo",
            domain: "test.com",
            tag: "tag",
            digest: "",
        },
    ),
    (
        "test:5000/repo",
        Parsed::Ok {
            name: "test:5000/repo",
            domain: "test:5000",
            tag: "",
            digest: "",
        },
    ),
    (
        "test:5000/repo:tag",
        Parsed::Ok {
            name: "test:5000/repo",
            domain: "test:5000",
            tag: "tag",
            digest: "",
        },
    ),
    (
        "test:5000/repo@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        Parsed::Ok {
            name: "test:5000/repo",
            domain: "test:5000",
            tag: "",
            digest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        },
    ),
    (
        "test:5000/repo:tag@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        Parsed::Ok {
            name: "test:5000/repo",
            domain: "test:5000",
            tag: "tag",
            digest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        },
    ),
    (
        "test:5000/repo",
        Parsed::Ok {
            name: "test:5000/repo",
            domain: "test:5000",
            tag: "",
            digest: "",
        },
    ),
    ("", Parsed::Err(Refusal::NameEmpty)),
    (":justtag", Parsed::Err(Refusal::ReferenceInvalidFormat)),
    (
        "@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        Parsed::Err(Refusal::ReferenceInvalidFormat),
    ),
    (
        "repo@sha256:ffffffffffffffffffffffffffffffffff",
        Parsed::Err(Refusal::DigestInvalidLength),
    ),
    (
        "validname@invaliddigest:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        Parsed::Err(Refusal::DigestUnsupported),
    ),
    ("Uppercase:tag", Parsed::Err(Refusal::NameContainsUppercase)),
    (
        "test:5000/Uppercase/lowercase:tag",
        Parsed::Err(Refusal::NameContainsUppercase),
    ),
    (
        "lowercase:Uppercase",
        Parsed::Ok {
            name: "lowercase",
            domain: "",
            tag: "Uppercase",
            digest: "",
        },
    ),
    (
        "aa/asdf$$^/aa",
        Parsed::Err(Refusal::ReferenceInvalidFormat),
    ),
    (
        "sub-dom1.foo.com/bar/baz/quux",
        Parsed::Ok {
            name: "sub-dom1.foo.com/bar/baz/quux",
            domain: "sub-dom1.foo.com",
            tag: "",
            digest: "",
        },
    ),
    (
        "sub-dom1.foo.com/bar/baz/quux:some-long-tag",
        Parsed::Ok {
            name: "sub-dom1.foo.com/bar/baz/quux",
            domain: "sub-dom1.foo.com",
            tag: "some-long-tag",
            digest: "",
        },
    ),
    (
        "b.gcr.io/test.example.com/my-app:test.example.com",
        Parsed::Ok {
            name: "b.gcr.io/test.example.com/my-app",
            domain: "b.gcr.io",
            tag: "test.example.com",
            digest: "",
        },
    ),
    (
        "xn--n3h.com/myimage:xn--n3h.com",
        Parsed::Ok {
            name: "xn--n3h.com/myimage",
            domain: "xn--n3h.com",
            tag: "xn--n3h.com",
            digest: "",
        },
    ),
    (
        "xn--7o8h.com/myimage:xn--7o8h.com@sha512:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        Parsed::Ok {
            name: "xn--7o8h.com/myimage",
            domain: "xn--7o8h.com",
            tag: "xn--7o8h.com",
            digest: "sha512:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        },
    ),
    (
        "foo_bar.com:8080",
        Parsed::Ok {
            name: "foo_bar.com",
            domain: "",
            tag: "8080",
            digest: "",
        },
    ),
    (
        "foo/foo_bar.com:8080",
        Parsed::Ok {
            name: "foo/foo_bar.com",
            domain: "foo",
            tag: "8080",
            digest: "",
        },
    ),
    (
        "192.168.1.1",
        Parsed::Ok {
            name: "192.168.1.1",
            domain: "",
            tag: "",
            digest: "",
        },
    ),
    (
        "192.168.1.1:tag",
        Parsed::Ok {
            name: "192.168.1.1",
            domain: "",
            tag: "tag",
            digest: "",
        },
    ),
    (
        "192.168.1.1:5000",
        Parsed::Ok {
            name: "192.168.1.1",
            domain: "",
            tag: "5000",
            digest: "",
        },
    ),
    (
        "192.168.1.1/repo",
        Parsed::Ok {
            name: "192.168.1.1/repo",
            domain: "192.168.1.1",
            tag: "",
            digest: "",
        },
    ),
    (
        "192.168.1.1:5000/repo",
        Parsed::Ok {
            name: "192.168.1.1:5000/repo",
            domain: "192.168.1.1:5000",
            tag: "",
            digest: "",
        },
    ),
    (
        "192.168.1.1:5000/repo:5050",
        Parsed::Ok {
            name: "192.168.1.1:5000/repo",
            domain: "192.168.1.1:5000",
            tag: "5050",
            digest: "",
        },
    ),
    (
        "[2001:db8::1]",
        Parsed::Err(Refusal::ReferenceInvalidFormat),
    ),
    (
        "[2001:db8::1]:5000",
        Parsed::Err(Refusal::ReferenceInvalidFormat),
    ),
    (
        "[2001:db8::1]:tag",
        Parsed::Err(Refusal::ReferenceInvalidFormat),
    ),
    (
        "[2001:db8::1]/repo",
        Parsed::Ok {
            name: "[2001:db8::1]/repo",
            domain: "[2001:db8::1]",
            tag: "",
            digest: "",
        },
    ),
    (
        "[2001:db8:1:2:3:4:5:6]/repo:tag",
        Parsed::Ok {
            name: "[2001:db8:1:2:3:4:5:6]/repo",
            domain: "[2001:db8:1:2:3:4:5:6]",
            tag: "tag",
            digest: "",
        },
    ),
    (
        "[2001:db8::1]:5000/repo",
        Parsed::Ok {
            name: "[2001:db8::1]:5000/repo",
            domain: "[2001:db8::1]:5000",
            tag: "",
            digest: "",
        },
    ),
    (
        "[2001:db8::1]:5000/repo:tag",
        Parsed::Ok {
            name: "[2001:db8::1]:5000/repo",
            domain: "[2001:db8::1]:5000",
            tag: "tag",
            digest: "",
        },
    ),
    (
        "[2001:db8::1]:5000/repo@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        Parsed::Ok {
            name: "[2001:db8::1]:5000/repo",
            domain: "[2001:db8::1]:5000",
            tag: "",
            digest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        },
    ),
    (
        "[2001:db8::1]:5000/repo:tag@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        Parsed::Ok {
            name: "[2001:db8::1]:5000/repo",
            domain: "[2001:db8::1]:5000",
            tag: "tag",
            digest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        },
    ),
    (
        "[2001:db8::]:5000/repo",
        Parsed::Ok {
            name: "[2001:db8::]:5000/repo",
            domain: "[2001:db8::]:5000",
            tag: "",
            digest: "",
        },
    ),
    (
        "[::1]:5000/repo",
        Parsed::Ok {
            name: "[::1]:5000/repo",
            domain: "[::1]:5000",
            tag: "",
            digest: "",
        },
    ),
    (
        "[fe80::1%eth0]:5000/repo",
        Parsed::Err(Refusal::ReferenceInvalidFormat),
    ),
    (
        "[fe80::1%@invalidzone]:5000/repo",
        Parsed::Err(Refusal::ReferenceInvalidFormat),
    ),
];

/// `TestValidateReferenceName` (normalize_test.go): names `ParseNormalizedNamed` accepts.
pub(super) const VALID: &[&str] = &[
    "docker/docker",
    "library/debian",
    "debian",
    "localhost/library/debian",
    "localhost/debian",
    "LOCALDOMAIN/library/debian",
    "LOCALDOMAIN/debian",
    "docker.io/docker/docker",
    "docker.io/library/debian",
    "docker.io/debian",
    "index.docker.io/docker/docker",
    "index.docker.io/library/debian",
    "index.docker.io/debian",
    "127.0.0.1:5000/docker/docker",
    "127.0.0.1:5000/library/debian",
    "127.0.0.1:5000/debian",
    "192.168.0.1",
    "192.168.0.1:80",
    "192.168.0.1:8/debian",
    "192.168.0.2:25000/debian",
    "thisisthesongthatneverendsitgoesonandonandonthisisthesongthatnev",
    "[fc00::1]:5000/docker",
    "[fc00::1]:5000/docker/docker",
    "[fc00:1:2:3:4:5:6:7]:5000/library/debian",
    "docker.io/1a3f5e7d9c1b3a5f7e9d1c3b5a7f9e1d3c5b7a9f1e3d5d7c9b1a3f5e7d9c1b3a",
    "Docker/docker",
    "DOCKER/docker",
];

/// `TestValidateReferenceName` (normalize_test.go): names `ParseNormalizedNamed` refuses.
pub(super) const INVALID: &[&str] = &[
    "https://github.com/docker/docker",
    "docker/Docker",
    "-docker",
    "-docker/docker",
    "-docker.io/docker/docker",
    "docker///docker",
    "docker.io/docker/Docker",
    "docker.io/docker///docker",
    "[fc00::1]",
    "[fc00::1]:5000",
    "fc00::1:5000/debian",
    "[fe80::1%eth0]:5000/debian",
    "[2001:db8:3:4::192.0.2.33]:5000/debian",
    "1a3f5e7d9c1b3a5f7e9d1c3b5a7f9e1d3c5b7a9f1e3d5d7c9b1a3f5e7d9c1b3a",
];

/// `TestParseRepositoryInfo` (normalize_test.go): each familiar, full,
/// and ambiguous name, and the domain and path it normalizes to.
pub(super) const REPOSITORY_INFO: &[(&str, &str, &str)] = &[
    ("localhost/fooo", "localhost", "fooo"),
    ("localhost/fooo", "localhost", "fooo"),
    ("localhost/fooo", "localhost", "fooo"),
    ("localhost/fooo/bar", "localhost", "fooo/bar"),
    ("localhost/fooo/bar", "localhost", "fooo/bar"),
    ("localhost/fooo/bar", "localhost", "fooo/bar"),
    ("LOCALDOMAIN/fooo", "LOCALDOMAIN", "fooo"),
    ("LOCALDOMAIN/fooo", "LOCALDOMAIN", "fooo"),
    ("LOCALDOMAIN/fooo", "LOCALDOMAIN", "fooo"),
    ("LOCALDOMAIN/fooo/bar", "LOCALDOMAIN", "fooo/bar"),
    ("LOCALDOMAIN/fooo/bar", "LOCALDOMAIN", "fooo/bar"),
    ("LOCALDOMAIN/fooo/bar", "LOCALDOMAIN", "fooo/bar"),
    ("fooo/bar", "docker.io", "fooo/bar"),
    ("docker.io/fooo/bar", "docker.io", "fooo/bar"),
    ("index.docker.io/fooo/bar", "docker.io", "fooo/bar"),
    ("ubuntu", "docker.io", "library/ubuntu"),
    ("docker.io/library/ubuntu", "docker.io", "library/ubuntu"),
    ("library/ubuntu", "docker.io", "library/ubuntu"),
    ("nonlibrary/ubuntu", "docker.io", "nonlibrary/ubuntu"),
    (
        "docker.io/nonlibrary/ubuntu",
        "docker.io",
        "nonlibrary/ubuntu",
    ),
    ("other/library", "docker.io", "other/library"),
    ("docker.io/other/library", "docker.io", "other/library"),
    (
        "127.0.0.1:8000/private/moonbase",
        "127.0.0.1:8000",
        "private/moonbase",
    ),
    (
        "127.0.0.1:8000/private/moonbase",
        "127.0.0.1:8000",
        "private/moonbase",
    ),
    (
        "127.0.0.1:8000/privatebase",
        "127.0.0.1:8000",
        "privatebase",
    ),
    (
        "127.0.0.1:8000/privatebase",
        "127.0.0.1:8000",
        "privatebase",
    ),
    (
        "example.com/private/moonbase",
        "example.com",
        "private/moonbase",
    ),
    (
        "example.com/private/moonbase",
        "example.com",
        "private/moonbase",
    ),
    ("example.com/privatebase", "example.com", "privatebase"),
    ("example.com/privatebase", "example.com", "privatebase"),
    (
        "example.com:8000/private/moonbase",
        "example.com:8000",
        "private/moonbase",
    ),
    (
        "example.com:8000/private/moonbase",
        "example.com:8000",
        "private/moonbase",
    ),
    (
        "example.com:8000/privatebasee",
        "example.com:8000",
        "privatebasee",
    ),
    (
        "example.com:8000/privatebasee",
        "example.com:8000",
        "privatebasee",
    ),
    (
        "ubuntu-12.04-base",
        "docker.io",
        "library/ubuntu-12.04-base",
    ),
    (
        "docker.io/library/ubuntu-12.04-base",
        "docker.io",
        "library/ubuntu-12.04-base",
    ),
    (
        "index.docker.io/library/ubuntu-12.04-base",
        "docker.io",
        "library/ubuntu-12.04-base",
    ),
    ("foo", "docker.io", "library/foo"),
    ("docker.io/library/foo", "docker.io", "library/foo"),
    ("docker.io/foo", "docker.io", "library/foo"),
    ("library/foo/bar", "docker.io", "library/foo/bar"),
    ("docker.io/library/foo/bar", "docker.io", "library/foo/bar"),
    ("store/foo/bar", "docker.io", "store/foo/bar"),
    ("docker.io/store/foo/bar", "docker.io", "store/foo/bar"),
    ("Foo/bar", "Foo", "bar"),
    ("Foo/bar", "Foo", "bar"),
    ("FOO/bar", "FOO", "bar"),
    ("FOO/bar", "FOO", "bar"),
];

/// `TestNormalizedSplitHostname` (normalize_test.go): a name, and the
/// domain and path it normalizes to.
pub(super) const SPLIT: &[(&str, &str, &str)] = &[
    ("test.com/foo", "test.com", "foo"),
    ("test_com/foo", "docker.io", "test_com/foo"),
    ("docker/migrator", "docker.io", "docker/migrator"),
    ("test.com:8080/foo", "test.com:8080", "foo"),
    ("test-com:8080/foo", "test-com:8080", "foo"),
    ("foo", "docker.io", "library/foo"),
    ("xn--n3h.com/foo", "xn--n3h.com", "foo"),
    ("xn--n3h.com:18080/foo", "xn--n3h.com:18080", "foo"),
    ("docker.io/foo", "docker.io", "library/foo"),
    ("docker.io/library/foo", "docker.io", "library/foo"),
    ("docker.io/library/foo/bar", "docker.io", "library/foo/bar"),
];

/// `TestDomainRegexp` (regexp_test.go): whether the anchored domain pattern matches.
pub(super) const DOMAIN: &[(&str, bool)] = &[
    ("test.com", true),
    ("test.com:10304", true),
    ("test.com:http", false),
    ("localhost", true),
    ("localhost:8080", true),
    ("a", true),
    ("a.b", true),
    ("ab.cd.com", true),
    ("a-b.com", true),
    ("-ab.com", false),
    ("ab-.com", false),
    ("ab.c-om", true),
    ("ab.-com", false),
    ("ab.com-", false),
    ("0101.com", true),
    ("001a.com", true),
    ("b.gbc.io:443", true),
    ("b.gbc.io", true),
    ("xn--n3h.com", true),
    ("Asdf.com", true),
    ("192.168.1.1:75050", true),
    ("192.168.1.1:750050", true),
    ("[fd00:1:2::3]:75050", true),
    ("[fd00:1:2::3]75050", false),
    ("[fd00:1:2::3]::75050", false),
    ("[fd00:1:2::3%eth0]:75050", false),
    ("[fd00123123123]:75050", true),
    ("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:75050", true),
    ("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:750505", true),
    ("fd00:1:2::3:75050", false),
];

/// `TestFullNameRegexp` (regexp_test.go): whether the anchored name pattern matches, and its domain and path.
pub(super) const FULL_NAME: &[(&str, bool, &[&str])] = &[
    ("", false, &[]),
    ("short", true, &["", "short"]),
    ("simple/name", true, &["simple", "name"]),
    ("library/ubuntu", true, &["library", "ubuntu"]),
    ("docker/stevvooe/app", true, &["docker", "stevvooe/app"]),
    (
        "aa/aa/aa/aa/aa/aa/aa/aa/aa/bb/bb/bb/bb/bb/bb",
        true,
        &["aa", "aa/aa/aa/aa/aa/aa/aa/aa/bb/bb/bb/bb/bb/bb"],
    ),
    ("aa/aa/bb/bb/bb", true, &["aa", "aa/bb/bb/bb"]),
    ("a/a/a/a", true, &["a", "a/a/a"]),
    ("a/a/a/a/", false, &[]),
    ("a//a/a", false, &[]),
    ("a", true, &["", "a"]),
    ("a/aa", true, &["a", "aa"]),
    ("a/aa/a", true, &["a", "aa/a"]),
    ("foo.com", true, &["", "foo.com"]),
    ("foo.com/", false, &[]),
    ("foo.com:8080/bar", true, &["foo.com:8080", "bar"]),
    ("foo.com:http/bar", false, &[]),
    ("foo.com/bar", true, &["foo.com", "bar"]),
    ("foo.com/bar/baz", true, &["foo.com", "bar/baz"]),
    ("localhost:8080/bar", true, &["localhost:8080", "bar"]),
    (
        "sub-dom1.foo.com/bar/baz/quux",
        true,
        &["sub-dom1.foo.com", "bar/baz/quux"],
    ),
    ("blog.foo.com/bar/baz", true, &["blog.foo.com", "bar/baz"]),
    ("a^a", false, &[]),
    ("aa/asdf$$^/aa", false, &[]),
    ("asdf$$^/aa", false, &[]),
    ("aa-a/a", true, &["aa-a", "a"]),
    ("a-/a/a/a", false, &[]),
    ("foo.com/a-/a/a", false, &[]),
    ("-foo/bar", false, &[]),
    ("foo/bar-", false, &[]),
    ("foo-/bar", false, &[]),
    ("foo/-bar", false, &[]),
    ("_foo/bar", false, &[]),
    ("foo_bar", true, &["", "foo_bar"]),
    ("foo_bar.com", true, &["", "foo_bar.com"]),
    ("foo_bar.com:8080", false, &[]),
    ("foo_bar.com:8080/app", false, &[]),
    ("foo.com/foo_bar", true, &["foo.com", "foo_bar"]),
    ("____/____", false, &[]),
    ("_docker/_docker", false, &[]),
    ("docker_/docker_", false, &[]),
    (
        "b.gcr.io/test.example.com/my-app",
        true,
        &["b.gcr.io", "test.example.com/my-app"],
    ),
    ("xn--n3h.com/myimage", true, &["xn--n3h.com", "myimage"]),
    ("xn--7o8h.com/myimage", true, &["xn--7o8h.com", "myimage"]),
    (
        "example.com/xn--7o8h.com/myimage",
        true,
        &["example.com", "xn--7o8h.com/myimage"],
    ),
    (
        "example.com/some_separator__underscore/myimage",
        true,
        &["example.com", "some_separator__underscore/myimage"],
    ),
    ("example.com/__underscore/myimage", false, &[]),
    ("example.com/..dots/myimage", false, &[]),
    ("example.com/.dots/myimage", false, &[]),
    ("example.com/nodouble..dots/myimage", false, &[]),
    ("example.com/nodouble..dots/myimage", false, &[]),
    ("docker./docker", false, &[]),
    (".docker/docker", false, &[]),
    ("docker-/docker", false, &[]),
    ("-docker/docker", false, &[]),
    ("do..cker/docker", false, &[]),
    ("do__cker:8080/docker", false, &[]),
    ("do__cker/docker", true, &["", "do__cker/docker"]),
    (
        "b.gcr.io/test.example.com/my-app",
        true,
        &["b.gcr.io", "test.example.com/my-app"],
    ),
    (
        "registry.io/foo/project--id.module--name.ver---sion--name",
        true,
        &[
            "registry.io",
            "foo/project--id.module--name.ver---sion--name",
        ],
    ),
    ("Asdf.com/foo/bar", true, &[]),
    ("Foo/FarB", false, &[]),
];

/// `TestReferenceRegexp` (regexp_test.go): whether the reference pattern matches, and its name, tag, and digest.
pub(super) const REFERENCE: &[(&str, bool, &[&str])] = &[
    (
        "registry.com:8080/myapp:tag",
        true,
        &["registry.com:8080/myapp", "tag", ""],
    ),
    (
        "registry.com:8080/myapp@sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        true,
        &[
            "registry.com:8080/myapp",
            "",
            "sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        ],
    ),
    (
        "registry.com:8080/myapp:tag2@sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        true,
        &[
            "registry.com:8080/myapp",
            "tag2",
            "sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        ],
    ),
    ("registry.com:8080/myapp@sha256:badbadbadbad", false, &[]),
    ("registry.com:8080/myapp:invalid~tag", false, &[]),
    ("bad_hostname.com:8080/myapp:tag", false, &[]),
    (
        "localhost:8080@sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        true,
        &[
            "localhost",
            "8080",
            "sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        ],
    ),
    (
        "localhost:8080/name@sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        true,
        &[
            "localhost:8080/name",
            "",
            "sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        ],
    ),
    (
        "localhost:http/name@sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        false,
        &[],
    ),
    (
        "localhost@sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        true,
        &[
            "localhost",
            "",
            "sha256:be178c0543eb17f5f3043021c9e5fcf30285e557a4fc309cce97ff9ca6182912",
        ],
    ),
    ("registry.com:8080/myapp@bad", false, &[]),
    ("registry.com:8080/myapp@2bad", false, &[]),
];

/// `TestIdentifierRegexp` (regexp_test.go): whether the anchored identifier pattern matches.
pub(super) const IDENTIFIER: &[(&str, bool)] = &[
    (
        "da304e823d8ca2b9d863a3c897baeb852ba21ea9a9f1414736394ae7fcaf9821",
        true,
    ),
    (
        "7EC43B381E5AEFE6E04EFB0B3F0693FF2A4A50652D64AEC573905F2DB5889A1C",
        false,
    ),
    (
        "da304e823d8ca2b9d863a3c897baeb852ba21ea9a9f1414736394ae7fcaf",
        false,
    ),
    (
        "sha256:da304e823d8ca2b9d863a3c897baeb852ba21ea9a9f1414736394ae7fcaf9821",
        false,
    ),
    (
        "da304e823d8ca2b9d863a3c897baeb852ba21ea9a9f1414736394ae7fcaf98218482",
        false,
    ),
];

/// `TestValidateRemoteName` (normalize_test.go): names `ParseNormalizedNamed` accepts.
pub(super) const REMOTE_VALID: &[&str] = &[
    "docker/docker",
    "thisisthesongthatneverendsitgoesonandonandonthisisthesongthatnev",
    "docker-rules/docker",
    "docker---rules/docker",
    "doc/docker",
    "d/docker",
    "jess/t",
    "dock__er/docker",
];

/// `TestValidateRemoteName` (normalize_test.go): names `ParseNormalizedNamed` refuses.
pub(super) const REMOTE_INVALID: &[&str] = &[
    "docker/Docker",
    "docker///docker",
    "1a3f5e7d9c1b3a5f7e9d1c3b5a7f9e1d3c5b7a9f1e3d5d7c9b1a3f5e7d9c1b3a",
    "-docker/docker",
    "docker-/docker",
    "-docker-/docker",
    "____/____",
    "_docker/_docker",
    "dock..er/docker",
    "dock_.er/docker",
    "dock-.er/docker",
    "docker/",
    "this_is_not_a_valid_namespace_because_its_lenth_is_greater_than_255_this_is_not_a_valid_namespace_because_its_lenth_is_greater_than_255_this_is_not_a_valid_namespace_because_its_lenth_is_greater_than_255_this_is_not_a_valid_namespace_because_its_lenth_is_greater_than_255/docker",
];

/// `TestParseDockerRef` (normalize_test.go): a reference, and the domain
/// and path of the name it normalizes to.
pub(super) const DOCKER_REF: &[(&str, &str, &str)] = &[
    ("busybox", "docker.io", "library/busybox"),
    ("busybox:latest", "docker.io", "library/busybox"),
    (
        "busybox@sha256:e6693c20186f837fc393390135d8a598a96a833917917789d63766cab6c59582",
        "docker.io",
        "library/busybox",
    ),
    ("library/busybox", "docker.io", "library/busybox"),
    ("docker.io/busybox", "docker.io", "library/busybox"),
    ("docker.io/library/busybox", "docker.io", "library/busybox"),
    ("docker.io/busybox:latest", "docker.io", "library/busybox"),
    ("library/busybox:latest", "docker.io", "library/busybox"),
    (
        "docker.io/library/busybox:latest",
        "docker.io",
        "library/busybox",
    ),
    ("gcr.io/library/busybox", "gcr.io", "library/busybox"),
    (
        "gcr.io/library/busybox:latest@sha256:e6693c20186f837fc393390135d8a598a96a833917917789d63766cab6c59582",
        "gcr.io",
        "library/busybox",
    ),
];
