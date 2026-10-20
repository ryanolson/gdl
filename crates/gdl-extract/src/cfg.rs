//! `cfg` as a build for the host would see it, with a package's enabled
//! features. A name that is not set is false, as it is to the compiler;
//! settings that compiler flags add are not seen.

use std::collections::HashSet;
use std::process::Command;

use syn::punctuated::Punctuated;
use syn::{Meta, Token};

use crate::Error;

/// The host's `cfg` settings: names, and `key = "value"` pairs.
pub(crate) struct Settings {
    names: HashSet<String>,
    pairs: HashSet<(String, String)>,
}

impl Settings {
    pub(crate) fn host() -> Result<Self, Error> {
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let output = Command::new(rustc)
            .args(["--print", "cfg"])
            .output()
            .map_err(|e| Error(format!("running rustc: {e}")))?;
        Ok(Self::parse(&String::from_utf8_lossy(&output.stdout)))
    }

    fn parse(text: &str) -> Self {
        let mut names = HashSet::new();
        let mut pairs = HashSet::new();
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
            match line.split_once('=') {
                Some((key, value)) => {
                    pairs.insert((key.to_string(), value.trim_matches('"').to_string()));
                }
                None => {
                    names.insert(line.to_string());
                }
            }
        }
        Self { names, pairs }
    }

    /// Whether `predicate` holds for a package with `features` enabled.
    pub(crate) fn holds(&self, predicate: &Meta, features: &[String]) -> bool {
        let name = |path: &syn::Path| path.get_ident().map(ToString::to_string);
        match predicate {
            Meta::Path(path) => name(path).is_some_and(|n| self.names.contains(&n)),
            Meta::NameValue(pair) => {
                let (
                    Some(key),
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(value),
                        ..
                    }),
                ) = (name(&pair.path), &pair.value)
                else {
                    return false;
                };
                let value = value.value();
                if key == "feature" {
                    features.contains(&value)
                } else {
                    self.pairs.contains(&(key, value))
                }
            }
            Meta::List(list) => {
                let Ok(args) =
                    list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                else {
                    return false;
                };
                match name(&list.path).as_deref() {
                    Some("all") => args.iter().all(|m| self.holds(m, features)),
                    Some("any") => args.iter().any(|m| self.holds(m, features)),
                    Some("not") => args.len() == 1 && !self.holds(&args[0], features),
                    _ => false,
                }
            }
        }
    }

    /// The attributes that apply, with `cfg_attr` expanded, or `None` when
    /// a `cfg` leaves the item out.
    pub(crate) fn attributes(
        &self,
        attrs: &[syn::Attribute],
        features: &[String],
    ) -> Option<Vec<Meta>> {
        let mut out = Vec::new();
        let mut pending: Vec<Meta> = attrs.iter().map(|a| a.meta.clone()).collect();
        pending.reverse();
        while let Some(meta) = pending.pop() {
            let Meta::List(list) = &meta else {
                out.push(meta);
                continue;
            };
            if list.path.is_ident("cfg") {
                match list.parse_args::<Meta>() {
                    Ok(predicate) if self.holds(&predicate, features) => continue,
                    _ => return None,
                }
            }
            if list.path.is_ident("cfg_attr") {
                let Ok(args) =
                    list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                else {
                    continue;
                };
                let mut args = args.into_iter();
                if let Some(predicate) = args.next()
                    && self.holds(&predicate, features)
                {
                    let mut inner: Vec<Meta> = args.collect();
                    inner.reverse();
                    pending.extend(inner);
                }
                continue;
            }
            out.push(meta);
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn linux() -> Settings {
        Settings::parse("unix\ndebug_assertions\ntarget_os=\"linux\"\n")
    }

    #[test]
    fn a_predicate_holds_as_the_compiler_would_read_it() {
        let features = vec!["extra".to_string()];
        for (predicate, holds) in [
            ("unix", true),
            ("windows", false),
            ("test", false),
            ("target_os = \"linux\"", true),
            ("feature = \"extra\"", true),
            ("feature = \"other\"", false),
            ("not(any(unix, windows))", false),
            ("all(unix, feature = \"extra\")", true),
            ("all()", true),
            ("any()", false),
        ] {
            let meta: Meta = syn::parse_str(predicate).expect("predicate");
            assert_eq!(linux().holds(&meta, &features), holds, "{predicate}");
        }
    }

    #[test]
    fn cfg_attr_applies_its_attributes_when_it_holds() {
        let item: syn::ItemStruct = syn::parse_str(
            "#[cfg_attr(unix, derive(Message))] #[cfg_attr(windows, derive(Other))] struct A;",
        )
        .expect("item");
        let attrs = linux().attributes(&item.attrs, &[]).expect("kept");
        assert_eq!(attrs.len(), 1);
        assert!(attrs[0].path().is_ident("derive"));
        let item: syn::ItemStruct = syn::parse_str("#[cfg(windows)] struct A;").expect("item");
        assert!(linux().attributes(&item.attrs, &[]).is_none());
    }
}
