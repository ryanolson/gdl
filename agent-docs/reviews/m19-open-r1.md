# The items left open by milestone 19: review pass 1

Scope: `ea0670a` (ruling 102, `ConfigSchema` sealed to its derive) and `005b52b` (ruling 103, the reader's cost).

Reviewer: a fresh Opus subagent, read-only, running the thermo-nuclear code-quality review over `cce783b..005b52b`. It worked in a clone in scratch, and probed further shapes with the counting allocator of `cost.rs`. It mutated the fixes to see which tests go red, and ran an independent Sonnet pass. It ran fmt, clippy, `gdl-extract`'s tests, and `gdl`'s trybuild cases. Verdict: REJECT, 2 must fix, 5 should fix, 4 nits. The seal is sound; the reader's behavior is unchanged; its claim of linear cost for every shape was not.

| # | Finding | Ruling | Test | Fix |
|---|---|---|---|---|
| M1 | a path inside braces (`p::{x::a}`) and a group nested in a group still copied the prefix | valid (probe: ×4.0) | `a_path_in_braces_adds_one_segment`, `nested_groups_share_their_prefix`; both fail on `005b52b` | a `use` path is a chain of links (`Link`), one link per segment written, and the names in braces share what is before them. The chain unlinks one link at a time when dropped, since its length is the source's to choose |
| M2 | nested inline modules built a directory path per level | valid (probe: ×3.96) | `an_inline_module_does_not_build_its_directory`; fails on `005b52b` | a directory is a chain of names and `#[path]`s (`Dir`), made a path only when a module file is looked up; an inline module no longer reopens its file |
| S1 | the export check ran per name over every attribute of the item | valid (probe: ×3.98) | `the_export_check_runs_once_per_use_item`; fails on `005b52b` | decided once per `use` item, keyed by its shared attributes |
| S2 | resolution keeps nothing between queries, so N resolutions through K segments or K globs cost N×K | valid; the claim narrowed | none | ruling 103's addendum and the perf record name it, bounded by the import child. A cache across queries is not sound as the resolver stands: its answers through cyclic globs hold only within one query |
| S3 | every location copied its file's path | valid (probe: +400 B per name) | `locations_share_their_files_path`; fails on `005b52b` (×1.58) | `Location::file` is an `Rc<Path>` |
| S4 | no test guarded the name indexes or the role index | valid (mutation) | `a_type_is_found_by_its_name` (time; catches the linear scan in most runs, 3.6 to 3.8×), `the_role_check_finds_a_nodes_impl_without_walking_the_crate` (fails on `ea0670a`: allocations ×3.9) | the tests |
| S5 | ruling 103, the perf record, and the budget comment overstated the bound | valid | none | all three corrected |
| N1 | the impl index lived in `Resolver`, written by `markers`: a module cycle | valid | none | `Impls` in `markers`, passed beside the resolver |
| N2 | a binding named `first` held the last segment | valid | none | renamed with the rewrite |
| N3 | `impls_for` cloned a `Vec` of names per call | valid | none | `Impls::of` returns a slice of `Copy` entries naming the trait by an enum |
| N4 | `public_path` is quadratic for a deep chain with a message per level, as its output is | not actionable | none | stated in ruling 103's addendum |

A stale binary misled the coordinator once during the fixes, and is worth recording. A scratch worktree of this repository built into the checkout's `target` directory. Cargo names a path dependency by its path relative to the workspace root, so the two checkouts' crates collided, and one overwrote the other's artifacts. Build a worktree into a target directory of its own.

The workspace suite, clippy, and fmt pass after the fixes; the example workspaces, the Node tests, and `generate --check` too.
