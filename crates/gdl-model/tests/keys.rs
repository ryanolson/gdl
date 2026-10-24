//! Every table of a placement and of a fleet file refuses a key it does
//! not know, nested tables included, and the refusal says where: the line,
//! the table, and the key.

use gdl_model::{Fleet, Placement};

const SEGMENTS: &str = "[segment.a]\nnodes = ['S']\n[segment.d]\nnodes = ['D']\n";
const CLASSES: &str = "[class.global]\nsegments = ['a']\n[class.model_x]\nsegments = ['d']\n";

/// `text`'s refusal, which must exist.
fn refused<T: std::fmt::Debug, E: std::fmt::Display>(text: &str, read: Result<T, E>) -> String {
    match read {
        Ok(value) => panic!("accepted {value:?}:\n{text}"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn a_placement_refuses_an_unknown_key_in_every_table() {
    let base = format!("{SEGMENTS}{CLASSES}");
    for (extra, expected) in [
        (
            "[segment.e]\nnodes = ['E']\nnode = 'F'\n",
            "line 11: [segment.e] node: unknown field `node`",
        ),
        (
            "[class.spare]\nsegments = []\nsegment = 'a'\n",
            "line 11: [class.spare] segment: unknown field `segment`",
        ),
        (
            "[cells.model_x]\npolicyy = 'home_first'\n",
            "line 10: [cells.model_x] policyy: unknown field `policyy`",
        ),
        // They chose near cells, which a chooser does not follow.
        (
            "[cells.model_x]\npolicy = 'home_first'\nspill_rtt_ms = 80\n",
            "line 11: [cells.model_x] spill_rtt_ms: unknown field `spill_rtt_ms`, expected `policy`",
        ),
        (
            "[cells.model_x]\nmax_near = 16\n",
            "line 10: [cells.model_x] max_near: unknown field `max_near`, expected `policy`",
        ),
        (
            "[routing]\nD = { selector = 'least_loaded', weight = 2 }\n",
            "line 10: [routing.D] weight: unknown field `weight`, expected `selector`",
        ),
        (
            "[routing]\nD = 'fastest'\n",
            "line 10: [routing] D: unknown variant `fastest`",
        ),
    ] {
        let text = format!("{base}{extra}");
        let error = refused(&text, Placement::from_toml(&text));
        assert!(error.starts_with(expected), "{expected}\nin: {error}");
    }
}

/// The checks made as the placement is read name their table and key.
#[test]
fn a_placements_names_are_refused_where_they_are() {
    let text = format!(
        "{SEGMENTS}[class.Global]\nsegments = ['a']\n[class.model_x]\nsegments = ['d']\n\
         [cells.model_x]\npolicy = 'Home'\n[routing]\nD = {{ selector = 'Near' }}\n"
    );
    let error = refused(&text, Placement::from_toml(&text));
    for expected in [
        "[class.Global]: class name `Global` must be",
        "[cells.model_x] policy: policy name `Home` must be",
        "[routing] D: selector name `Near` must be",
    ] {
        assert!(
            error.lines().any(|line| line.starts_with(expected)),
            "{expected}\nin: {error}"
        );
    }
    assert_eq!(error.lines().count(), 3, "every name at once: {error}");
}

#[test]
fn a_fleet_refuses_an_unknown_key_in_every_table() {
    for (text, expected) in [
        (
            "[region.us-east-1]\n",
            "line 1: region: unknown field `region`",
        ),
        (
            "[class.global]\nready = { a = 1 }\nquorum = { a = 1 }\n",
            "line 3: [class.global] quorum: unknown field `quorum`",
        ),
        (
            "[cell.g]\nclass = 'global'\nregion = 'r'\nzone = 'a'\n",
            "line 4: [cell.g] zone: unknown field `zone`",
        ),
        // The relay mesh among global hubs is not built, so nothing reads
        // seeds; a fleet file never took them.
        (
            "[cell.g]\nclass = 'global'\nregion = 'r'\nseeds = ['hub-a:1337']\n",
            "line 4: [cell.g] seeds: unknown field `seeds`",
        ),
        (
            "[cell.g]\nclass = 'global'\n",
            "line 1: [cell.g] region: missing field `region`",
        ),
        (
            "[class.global]\nready = { a = 'two' }\n",
            "line 2: [class.global.ready] a: invalid type",
        ),
        (
            "[class.global]\nready = { a = -1 }\n",
            "line 2: [class.global.ready] a: invalid value",
        ),
        ("[cell.g]\nclass = 'global' x\n", "line 2: [cell.g] class: "),
        // A syntax error outside a key's value, and one in a header.
        ("[cell.g]\nclass = 'global'\nregion\n", "line 3: [cell.g]: "),
        (
            "[cell.g]\nclass = 'global'\n[cell.h\n",
            "line 3: unclosed table",
        ),
    ] {
        let error = refused(text, Fleet::from_toml(text));
        assert!(error.starts_with(expected), "{expected}\nin: {error}");
        assert_eq!(error.lines().count(), 1, "one line: {error}");
    }
}

/// A `[cell]` table refuses a key it does not know, and every problem in
/// reading one has its key in table `cell`, which its reader turns into
/// the file or the variable.
#[test]
fn a_cells_settings_refuse_an_unknown_key_and_name_it() {
    const LEAST: &str = "name = 'g'\nclass = 'global'\nregion = 'r'\n";
    for (extra, expected) in [
        ("hone = 'h'\n", "[cell] hone: unknown field `hone`"),
        (
            "seeds = ['hub-a:1337']\n",
            "[cell] seeds: unknown field `seeds`",
        ),
        ("[ready]\ne = 'x'\n", "[cell.ready] e: invalid type"),
        (
            "summary_ms = 500\n",
            "[cell] summary_ms: is 500; the period is at least 1000 ms",
        ),
        (
            "hub = 'http://h:1'\n",
            "[cell] hub: \"http://h:1\" must be `host:port`",
        ),
        (
            "directory = 'dir'\n",
            "[cell] directory: is \"dir\", which needs `dir`",
        ),
        (
            "dir = '/srv'\n",
            "[cell] dir: \"/srv\" is read only by the `dir` backend",
        ),
    ] {
        let text = format!("{LEAST}{extra}");
        let value: toml::Value = toml::from_str(&text).expect("toml");
        let error = refused(&text, gdl_model::CellSettings::read(value));
        assert!(error.starts_with(expected), "{expected}\nin: {error}");
    }
    let value: toml::Value = toml::from_str("class = 'global'\nregion = 'r'\n").expect("toml");
    let error = refused("no name", gdl_model::CellSettings::read(value));
    assert!(
        error.starts_with("[cell] name: missing field `name`"),
        "{error}"
    );
    assert_eq!(
        gdl_model::cell_variable("ready.e"),
        "GDL_CELL__READY__E",
        "the variable that sets a key"
    );
}

/// A syntax error's table and key are those of TOML's structure: a
/// header inside a multi-line string is text, and a key's value that
/// spans lines is still that key's.
#[test]
fn a_syntax_error_is_located_through_strings_and_arrays() {
    for (text, expected, repaired) in [
        (
            "[config.A]\nnote = \"\"\"\n[cells.phantom]\n\"\"\"\nlimit = 3 x\n",
            "line 5: [config.A] limit: ",
            "[config.A]\nnote = \"\"\"\n[cells.phantom]\n\"\"\"\nlimit = 3\n",
        ),
        (
            "[config.A]\nnote = '''\n[cells.phantom]\n'''\nlimit = 3 x\n",
            "line 5: [config.A] limit: ",
            "[config.A]\nnote = '''\n[cells.phantom]\n'''\nlimit = 3\n",
        ),
        (
            "[segment.front]\nnodes = [\n  \"A\",\n  \"B\" x\n]\n",
            "line 4: [segment.front] nodes: ",
            "[segment.front]\nnodes = [\n  \"A\",\n  \"B\"\n]\n",
        ),
        (
            "[segment.front]\nnodes = [\n  [\"A\"],\n]\n[segment.back]\nnodes = 3 x\n",
            "line 6: [segment.back] nodes: ",
            "[segment.front]\nnodes = [\n  [\"A\"],\n]\n[segment.back]\nnodes = 3\n",
        ),
        (
            "[a]\nk = \"has # and [x] and \\\" quote\"\nm = 1 x\n",
            "line 3: [a] m: ",
            "[a]\nk = \"has # and [x] and \\\" quote\"\nm = 1\n",
        ),
    ] {
        gdl_model::read_toml::<toml::Table>(repaired).expect("the repaired text parses");
        let error = refused(text, gdl_model::read_toml::<toml::Table>(text));
        assert!(
            error.starts_with(expected),
            "{expected}\nin: {error}\n{text}"
        );
    }
}

/// Text that is not ASCII, in each kind of string, in a comment, and in a
/// string the error is in, is scanned as text: the locator never splits
/// a character, and the error is reported, not a panic.
#[test]
fn a_syntax_error_after_text_that_is_not_ascii_is_located() {
    for (text, expected, repaired) in [
        (
            "[config.A]\nnote = \"café\"\nlimit = 3 x\n",
            "line 3: [config.A] limit: ",
            "[config.A]\nnote = \"café\"\nlimit = 3\n",
        ),
        (
            r#"[config.A] # é
note = '''
naïve ☃
'''
other = 'ü'
limit = 3 x
"#,
            "line 6: [config.A] limit: ",
            r#"[config.A] # é
note = '''
naïve ☃
'''
other = 'ü'
limit = 3
"#,
        ),
        (
            "[config.A]\nnote = \"\"\"\ncafé\n\"\"\"\nlimit = 3 x\n",
            "line 5: [config.A] limit: ",
            "[config.A]\nnote = \"\"\"\ncafé\n\"\"\"\nlimit = 3\n",
        ),
        (
            "[config.A]\nlimit = \"café x\n",
            "line 2: ",
            "[config.A]\nlimit = \"café x\"\n",
        ),
    ] {
        gdl_model::read_toml::<toml::Table>(repaired).expect("the repaired text parses");
        let error = refused(text, gdl_model::read_toml::<toml::Table>(text));
        assert!(
            error.starts_with(expected),
            "{expected}\nin: {error}\n{text}"
        );
    }
}

/// An error at any place in text that is not ASCII is reported: a control
/// character, which TOML refuses everywhere, is put at each character
/// boundary of a file with such text in every kind of string, a comment,
/// and a key.
#[test]
fn an_error_anywhere_in_text_that_is_not_ascii_is_reported() {
    let sample = "[config.\"ä\"] # é\nnote = '''\nnaïve ☃\n'''\nother = \"ü\\\"ß\"\nlist = [\"ö\",\n  'å']\n\"ключ\" = 1\n";
    gdl_model::read_toml::<toml::Table>(sample).expect("the sample parses");
    for (at, _) in sample.char_indices() {
        let text = format!("{}\u{1}{}", &sample[..at], &sample[at..]);
        let error = refused(&text, gdl_model::read_toml::<toml::Table>(&text));
        assert!(error.starts_with("line "), "{error}");
    }
}
