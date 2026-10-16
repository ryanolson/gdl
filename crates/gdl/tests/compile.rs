//! Mistakes that must not compile, and graphs that must.

#[test]
fn compile_failures() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}

#[test]
fn compile_passes() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/pass/*.rs");
}
