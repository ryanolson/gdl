use std::collections::BTreeMap;

use serde::Deserialize;

use super::{ClassSpec, Placement, PlacementError, PlacementErrors, PoolPolicy, RequestRouter};
use super::{SegmentSpec, check_name_shape, pools};
use crate::{Problem, Problems, read_toml};

/// A spec's layouts: one graph placed in one or more ways, each placement
/// by its layout's name.
///
/// A spec without `[layout]` tables has one layout, [`Placements::DEFAULT`],
/// whose placement is its top-level `[segment]`, `[routing]`, `[class]`,
/// and `[pools]` tables, read as [`Placement::from_toml`] reads them. A
/// spec with `[layout.<name>]` tables has one layout for each, holding
/// those four tables, and none at the top level:
///
/// ```toml
/// [layout.regional.segment.front]
/// nodes = ["Ask", "Preprocess"]
/// [layout.regional.class.global]
/// segments = ["front"]
///
/// [layout.compact.segment.all]
/// nodes = ["Ask", "Preprocess"]
/// ```
///
/// Each layout is its own placement, with its own hash: nothing is shared
/// between layouts but the graph they place. Two layouts that place the
/// graph alike have the same hash, whatever they are called.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placements {
    layouts: BTreeMap<String, Placement>,
    tabled: bool,
}

/// One `[layout.<name>]` table as written. Unknown keys are refused, as
/// they are not at a spec's top level, where its other tables are: a
/// layout holds nothing but a placement.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Tables {
    #[serde(rename = "segment", default)]
    segments: BTreeMap<String, SegmentSpec>,
    #[serde(default)]
    routing: BTreeMap<String, RequestRouter>,
    #[serde(rename = "class", default)]
    classes: BTreeMap<String, ClassSpec>,
    #[serde(default, deserialize_with = "pools::recorded")]
    pools: BTreeMap<String, PoolPolicy>,
}

/// The `[layout]` table of a spec, the one key this reader needs.
#[derive(Deserialize)]
struct Written {
    #[serde(default)]
    layout: Option<BTreeMap<String, Tables>>,
}

/// The tables of a placement, which a spec with `[layout]` tables holds in
/// them rather than at its top level.
const PLACEMENT_TABLES: [&str; 4] = ["segment", "routing", "class", "pools"];

impl Placements {
    /// The name of the one layout of a spec without `[layout]` tables, by
    /// which a pool's settings and the fleet file may name it.
    pub const DEFAULT: &'static str = "default";

    /// Read the layouts of a spec, `text`. Every problem with the names and
    /// with where the tables are is reported, each where it is; a file that
    /// does not parse is one problem.
    pub fn from_toml(text: &str) -> Result<Self, Problems> {
        let Written { layout } = read_toml(text)?;
        let Some(layouts) = layout else {
            return Ok(Placement::from_toml(text)?.into());
        };
        let written: toml::Table = toml::from_str(text).unwrap_or_default();
        let mut problems = Vec::new();
        for table in PLACEMENT_TABLES
            .iter()
            .filter(|t| written.contains_key(**t))
        {
            problems.push(Problem::at(
                Some(table.to_string()),
                None,
                "a spec with [layout] tables places its graph in them; move this table into a layout",
            ));
        }
        if layouts.is_empty() {
            problems.push(Problem::at(
                Some("layout".into()),
                None,
                "lists no layout; a spec with a [layout] table has a [layout.<name>] table for each",
            ));
        }
        let mut placed = BTreeMap::new();
        for (name, tables) in layouts {
            if let Err(why) = check_layout_name(&name) {
                problems.push(Problem::at(Some(format!("layout.{name}")), None, why));
            }
            let own = written
                .get("layout")
                .and_then(|layouts| layouts.get(&name))
                .and_then(toml::Value::as_table)
                .cloned()
                .unwrap_or_default();
            let placement = Placement {
                segments: tables.segments,
                routing: tables.routing,
                classes: tables.classes,
                pools: tables.pools,
                spelled: Default::default(),
            };
            match placement.finish(&own) {
                Ok(placement) => {
                    placed.insert(name, placement);
                }
                Err(found) => problems.extend(found.0.into_iter().map(|p| within(&name, p))),
            }
        }
        if problems.is_empty() {
            Ok(Self {
                layouts: placed,
                tabled: true,
            })
        } else {
            Err(Problems(problems))
        }
    }

    /// How many layouts there are: at least one.
    pub fn len(&self) -> usize {
        self.layouts.len()
    }

    /// Whether there are none: never, for placements that were read.
    pub fn is_empty(&self) -> bool {
        self.layouts.is_empty()
    }

    /// Whether the layouts were written in `[layout]` tables, rather than
    /// as one placement at the spec's top level.
    pub fn tabled(&self) -> bool {
        self.tabled
    }

    /// The layouts' names, in name order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.layouts.keys().map(String::as_str)
    }

    /// Each layout's name and placement, in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Placement)> {
        self.layouts
            .iter()
            .map(|(name, placement)| (name.as_str(), placement))
    }

    /// The placement of layout `name`.
    pub fn get(&self, name: &str) -> Option<&Placement> {
        self.layouts.get(name)
    }

    /// The problems of layout `name`'s placement against its graph, each
    /// in the layout's tables: a problem of a table is the layout's
    /// table's, and a problem of no table (a type that cannot cross where
    /// this layout makes it cross) is the layout's own. A manifest that is
    /// wrong whatever the layout stays the graph's, so that it reads the
    /// same from every layout. Without `[layout]` tables, the problems are
    /// where a placement's always were.
    pub fn problems(&self, name: &str, errors: &PlacementErrors) -> Vec<Problem> {
        errors
            .0
            .iter()
            .map(|error| match error {
                _ if !self.tabled => error.problem(),
                PlacementError::InvalidManifest(_) => error.problem(),
                _ => within(name, error.problem()),
            })
            .collect()
    }
}

impl From<Placement> for Placements {
    /// The one layout of a spec without `[layout]` tables.
    fn from(placement: Placement) -> Self {
        Self {
            layouts: BTreeMap::from([(Self::DEFAULT.to_string(), placement)]),
            tabled: false,
        }
    }
}

/// `problem`, of a table of layout `name`, located in that layout's table.
fn within(name: &str, mut problem: Problem) -> Problem {
    problem.table = Some(match problem.table.take() {
        Some(table) => format!("layout.{name}.{table}"),
        None => format!("layout.{name}"),
    });
    problem
}

/// Whether `name` can name a layout. A pool's settings name the layout the
/// pool runs, in a file or in the environment (`GDL_POOL__LAYOUT`), and a
/// spec names it as a TOML key, so it has a settings key's shape.
pub fn check_layout_name(name: &str) -> Result<(), String> {
    check_name_shape("layout", name)
}
