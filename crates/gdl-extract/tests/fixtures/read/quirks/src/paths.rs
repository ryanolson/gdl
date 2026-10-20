//! Not `mod.rs`: an inline module's `#[path]` here is relative to `src/`.

#[path = "alternate"]
pub mod nested {
    #[path = "leaf.rs"]
    pub mod leaf;
}
