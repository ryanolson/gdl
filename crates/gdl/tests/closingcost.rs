//! The guide quotes the current scoped form against the current request
//! scope, from the table in `docs/src/operations/feature-costs.md`. Each
//! number is found by its row's and its column's header text, units
//! included, so a renamed row or a changed unit fails here instead of
//! leaving the guide's quote wrong.

const MEASUREMENTS: &str = include_str!("../../../docs/src/operations/feature-costs.md");
const GUIDE: &str = include_str!("../../../docs/src/guides/write-nodes.md");

/// The row the guide quotes, and the row it is measured against.
const SCOPED: &str = "`scoped` (change only; base's is `operator`)";
const UNSCOPED: &str = "`operator`";

/// The columns the guide quotes, in the guide's units.
const PER_ITEM: &str = "Change: Ir per item";
const PER_REQUEST: &str = "Change: Ir per request";
const ALLOCATIONS: &str = "Change: allocations per request";

/// Whether the guide's `CALL_SCOPE` cost quote matches the table in
/// `measurements`: per item to the whole Ir, per request and allocations
/// to the hundredth. The error is one line that names what differs or
/// what is missing.
fn check(guide: &str, measurements: &str) -> Result<(), String> {
    let (header, rows) = table(measurements)?;
    let mut measured = [0.0; 3];
    for (slot, column) in measured
        .iter_mut()
        .zip([PER_ITEM, PER_REQUEST, ALLOCATIONS])
    {
        *slot = cell(&header, &rows, SCOPED, column)? - cell(&header, &rows, UNSCOPED, column)?;
    }
    let quoted = quote(guide)?;
    let hundredths = |n: f64| (n * 100.0).round();
    let same = [
        measured[0].round() == quoted[0],
        hundredths(measured[1]) == hundredths(quoted[1]),
        hundredths(measured[2]) == hundredths(quoted[2]),
    ];
    if same.iter().all(|same| *same) {
        Ok(())
    } else {
        Err(format!(
            "the guide quotes {quoted:?} (Ir per item, Ir per request, allocations per request); feature-costs.md gives {measured:?}"
        ))
    }
}

/// The cells of a table line, trimmed.
fn cells(line: &str) -> Vec<&str> {
    let line = line.trim();
    let line = line.strip_prefix('|').unwrap_or(line);
    let line = line.strip_suffix('|').unwrap_or(line);
    line.split('|').map(str::trim).collect()
}

/// The table that holds the row `SCOPED`: its header cells and its rows.
fn table(measurements: &str) -> Result<(Vec<&str>, Vec<Vec<&str>>), String> {
    let lines: Vec<&str> = measurements.lines().collect();
    let at = lines
        .iter()
        .position(|line| line.starts_with('|') && cells(line)[0] == SCOPED)
        .ok_or_else(|| format!("feature-costs.md has no row {SCOPED:?}"))?;
    let start = (0..at)
        .rev()
        .find(|&i| !lines[i].starts_with('|'))
        .map_or(0, |i| i + 1);
    let end = (at..lines.len())
        .find(|&i| !lines[i].starts_with('|'))
        .unwrap_or(lines.len());
    let header = cells(lines[start]);
    let rows = lines[start + 2..end]
        .iter()
        .map(|line| cells(line))
        .collect();
    Ok((header, rows))
}

/// The number in row `row`, column `column`, matched by header text.
fn cell(header: &[&str], rows: &[Vec<&str>], row: &str, column: &str) -> Result<f64, String> {
    let index = header
        .iter()
        .position(|cell| *cell == column)
        .ok_or_else(|| {
            format!("feature-costs.md: the table with {SCOPED:?} has no column {column:?}, the guide's unit")
        })?;
    let found = rows
        .iter()
        .find(|cells| cells[0] == row)
        .ok_or_else(|| format!("feature-costs.md: the table with {SCOPED:?} has no row {row:?}"))?;
    let value = found.get(index).copied().unwrap_or_default();
    value.replace(',', "").parse().map_err(|_| {
        format!("feature-costs.md: row {row:?}, column {column:?} is {value:?}, not a number")
    })
}

/// The guide's quote: Ir per item, Ir per request, allocations per request.
fn quote(guide: &str) -> Result<[f64; 3], String> {
    let missing = |what: &str| format!("write-nodes.md: the `CALL_SCOPE` bullet has no {what}");
    let quote = guide
        .lines()
        .find(|line| line.starts_with("- **`CALL_SCOPE`**"))
        .ok_or_else(|| missing("line"))?
        .split_once("It costs about ")
        .ok_or_else(|| missing("\"It costs about\""))?
        .1;
    let (item, rest) = quote
        .split_once(" Ir more per item and ")
        .ok_or_else(|| missing("cost per item"))?;
    let (request, rest) = rest
        .split_once(" Ir and ")
        .ok_or_else(|| missing("cost per request"))?;
    let (allocations, _) = rest
        .split_once(" allocations more per request")
        .ok_or_else(|| missing("allocations per request"))?;
    let number = |text: &str| {
        text.replace(',', "")
            .parse::<f64>()
            .map_err(|_| missing(&format!("number in {text:?}")))
    };
    Ok([number(item)?, number(request)?, number(allocations)?])
}

#[test]
fn the_guide_quotes_the_measured_costs() {
    check(GUIDE, MEASUREMENTS).unwrap_or_else(|error| panic!("{error}"));
}

/// `MEASUREMENTS` with `from` replaced by `to`, which must occur.
fn edited(from: &str, to: &str) -> String {
    assert!(MEASUREMENTS.contains(from), "the table holds {from:?}");
    MEASUREMENTS.replace(from, to)
}

#[test]
fn a_changed_number_fails() {
    let changed = edited(
        "| | 55.78 | | 2,802 | | 6.05 |",
        "| | 55.78 | | 2,803 | | 6.05 |",
    );
    let error = check(GUIDE, &changed).expect_err("a changed number");
    assert!(error.contains("1982"), "scoped minus operator: {error}");
}

#[test]
fn each_changed_unit_fails() {
    for column in [PER_ITEM, PER_REQUEST, ALLOCATIONS] {
        let changed = edited(column, &column.replace("per ", "per 100 "));
        let error = check(GUIDE, &changed).expect_err(column);
        assert!(error.contains(&format!("no column {column:?}")), "{error}");
        assert!(!error.contains('\n'), "one line: {error}");
    }
}

#[test]
fn a_missing_row_fails() {
    let changed = edited("| `operator` | 55.78 |", "| `operator`, renamed | 55.78 |");
    let error = check(GUIDE, &changed).expect_err("a missing row");
    assert!(error.contains(&format!("no row {UNSCOPED:?}")), "{error}");
    let changed = edited(SCOPED, "`scoped`");
    let error = check(GUIDE, &changed).expect_err("a missing quoted row");
    assert!(error.contains(&format!("no row {SCOPED:?}")), "{error}");
}

#[test]
fn a_cell_that_is_not_a_number_fails() {
    let changed = edited(
        "| | 55.78 | | 2,802 | | 6.05 |",
        "| | 55.78 | | about 2,802 | | 6.05 |",
    );
    let error = check(GUIDE, &changed).expect_err("not a number");
    assert!(error.contains("not a number"), "{error}");
}
