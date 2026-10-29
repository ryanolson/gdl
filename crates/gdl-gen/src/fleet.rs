//! A spec and the fleet file that deploys it, checked together: what
//! `cargo gdl check` reports, and what `cargo gdl compose` refuses.

use std::collections::BTreeMap;
use std::path::Path;

use gdl_model::{Fleet, Layout, Problem, Problems};

use crate::{Offers, Spec};

/// Every problem with the spec at `spec`, as [`crate::render`] finds
/// them, with nothing written, and, with `fleet`, every problem with that fleet
/// file against the spec's layouts. Each names its file, and its table and
/// key where it has them. A file that does not read is one problem, and
/// the other is still checked: a fleet file without the spec's layouts for
/// what needs none.
pub fn check_files(spec: &Path, fleet: Option<&Path>, target: &Path) -> Problems {
    let mut problems = Vec::new();
    let layouts = match Spec::load(spec) {
        Err(error) => {
            problems.extend(error.in_file(spec.display()).0);
            None
        }
        Ok(loaded) => {
            let (checked, layouts) = crate::checked(&loaded, target, Offers::Read);
            if let Err(error) = checked {
                problems.extend(error.in_file(spec.display()).0);
            }
            Some(layouts)
        }
    };
    if let Some(fleet) = fleet {
        problems.extend(check_fleet(fleet, layouts.as_ref()).0);
    }
    Problems(problems)
}

/// Every problem with the fleet file at `path`, against the spec's
/// `layouts` when the spec reads, and one note when a pool's children could
/// not be judged for want of its layout. The note comes only with a spec
/// that has problems of its own, so it never fails a check alone.
fn check_fleet(path: &Path, layouts: Option<&BTreeMap<String, Option<Layout>>>) -> Problems {
    let located = |problems: Problems| problems.in_file(path.display());
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => return located(Problem::from(format!("cannot be read: {error}")).into()),
    };
    match Fleet::from_toml(&text) {
        Err(problems) => located(problems),
        Ok(fleet) => {
            let mut problems = match fleet.check(layouts) {
                Ok(()) => Problems::default(),
                Err(errors) => errors.into(),
            };
            problems.0.extend(fleet.children_unchecked(layouts));
            located(problems)
        }
    }
}
