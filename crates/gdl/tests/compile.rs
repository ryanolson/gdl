//! Node, configuration, and metrics definitions that must not compile.

#[test]
fn compile_failures() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
