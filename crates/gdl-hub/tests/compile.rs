//! Mistakes that must not compile.

/// A report reaches a parent's directory only through
/// [`gdl_hub::VerifiedReport`], which only the report's verification
/// returns: no code outside the hub builds one. It does not implement
/// `Deserialize` either, so none is read from the wire.
#[test]
fn compile_failures() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
