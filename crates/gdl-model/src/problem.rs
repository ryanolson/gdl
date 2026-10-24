use std::fmt;

use serde::de::DeserializeOwned;

/// One thing wrong with a deployment's settings, and where it is. Every
/// reader of the spec, the fleet file, and a cell's settings reports in
/// this shape, so that a problem reads the same from every command:
///
/// ```text
/// fleet.toml:12: [cell.model-x-01] zone: unknown field `zone`, expected one of `class`, `region`, `home`, `ready`
/// gdl.toml: [cells.model_x]: cell policy `[cells.model_x]`: the placement has no class `model_x`
/// GDL_CELL__SUMMARY_MS: is 500; the period is at least 1000 ms
/// ```
///
/// A file's problem names the file, the line when the parser gives one
/// (a problem found after parsing has none), the table, and the key. A
/// variable's names the variable, which spells its table and key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    /// Where the setting was read from: `None` until the reader, which
    /// knows the file or the variable, says.
    pub origin: Option<Origin>,
    /// The line, counted from 1, when the parser gives one.
    pub line: Option<usize>,
    /// The column, counted from 1, when the parser gives one with the
    /// line: a graph file's parser does, TOML's reader is given none.
    pub column: Option<usize>,
    /// The table, as a dotted path without its brackets
    /// (`cell.model-x-01`); `None` for the file's top level.
    pub table: Option<String>,
    /// The key in the table; `None` when the problem is the table's own.
    pub key: Option<String>,
    /// What is wrong.
    pub message: String,
}

/// Where a setting was read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// A file, by the path its reader was given.
    File(String),
    /// An environment variable, by its name.
    Variable(String),
}

impl Problem {
    /// A problem of `key` in `table`, from no origin yet.
    pub fn at(table: Option<String>, key: Option<String>, message: impl Into<String>) -> Self {
        Self {
            origin: None,
            line: None,
            column: None,
            table,
            key,
            message: message.into(),
        }
    }

    /// The key's whole dotted path, its table's included (`cell.ready.e`):
    /// `None` for a problem of a table or of no key.
    pub fn path(&self) -> Option<String> {
        let key = self.key.as_deref()?;
        Some(match &self.table {
            Some(table) => format!("{table}.{key}"),
            None => key.to_string(),
        })
    }

    /// The problem a reader met at `path`: the table and the key from the
    /// path it had reached. An unknown key's path ends with that key; a
    /// missing one's ends at its table, and the key is the one serde's
    /// message names. The table ends where an array begins, since TOML
    /// addresses an array's elements from the key that holds it
    /// (`[messages] crates[0].rev`).
    pub(crate) fn from_path(path: &serde_path_to_error::Path, message: String) -> Self {
        let mut keys = segments(path);
        let missing = message
            .strip_prefix("missing field `")
            .and_then(|rest| rest.split_once('`'))
            .map(|(key, _)| key.to_string());
        let array = keys.iter().position(|key| key.contains('['));
        let split = match (array, &missing) {
            (Some(array), _) => array,
            (None, Some(missing)) => {
                keys.push(missing.clone());
                keys.len() - 1
            }
            (None, None) => keys.len().saturating_sub(1),
        };
        let (table, key) = keys.split_at(split);
        Self::at(
            (!table.is_empty()).then(|| table.join(".")),
            (!key.is_empty()).then(|| key.join(".")),
            message,
        )
    }
}

/// Read `text`, a TOML file, as a `T`. A file that does not parse is one
/// problem, located by its line, table, and key; its reader adds the file.
pub fn read_toml<T: DeserializeOwned>(text: &str) -> Result<T, Problems> {
    let line = |error: &toml::de::Error| {
        error
            .span()
            .map(|span| text[..span.start.min(text.len())].matches('\n').count() + 1)
    };
    let deserializer = toml::Deserializer::parse(text).map_err(|error| {
        let (table, key) = error
            .span()
            .map_or((None, None), |span| surroundings(text, span.start));
        Problems::from(Problem {
            line: line(&error),
            ..Problem::at(table, key, error.message())
        })
    })?;
    serde_path_to_error::deserialize(deserializer).map_err(|error| {
        let path = error.path().clone();
        let error = error.into_inner();
        Problems::from(Problem {
            line: line(&error),
            ..Problem::from_path(&path, error.message().to_string())
        })
    })
}

/// The table and the key that a syntax error at `offset` of `text` is in.
/// The parser gives only the place, so the text before it is scanned for
/// TOML's structure: strings (a header inside a multi-line string is
/// text), comments, and the brackets of a value that spans lines (an
/// error inside a multi-line array is its key's). The table is the last
/// header, and the key the one whose value is open at the error. Where the
/// scan cannot tell, as inside a header or a string left open on its
/// line, neither is named: a missing location is better than a wrong one.
fn surroundings(text: &str, offset: usize) -> (Option<String>, Option<String>) {
    let mut end = offset.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let before = &text[..end];
    let bytes = before.as_bytes();
    let (mut table, mut key) = (None, None);
    // Where the statement on the current line began, and whether a key's
    // value is open, with how many brackets deep.
    let (mut statement, mut value, mut depth) = (0, false, 0usize);
    let mut at = 0;
    while at < bytes.len() {
        let rest = &before[at..];
        match bytes[at] {
            b'"' | b'\'' => {
                let quote = bytes[at];
                let triple = if quote == b'"' { "\"\"\"" } else { "'''" };
                let (close, open) = match rest.starts_with(triple) {
                    true => (triple, 3),
                    false => (&triple[..1], 1),
                };
                let mut inner = at + open;
                loop {
                    let Some(&byte) = bytes.get(inner) else {
                        // The error is inside this string: its key's, if
                        // it is a value, and nothing certain otherwise.
                        return match value {
                            true => (table, key),
                            false => (None, None),
                        };
                    };
                    if quote == b'"' && byte == b'\\' {
                        inner += 2;
                    } else if before[inner..].starts_with(close) {
                        break;
                    } else if byte == b'\n' && open == 1 {
                        return (None, None);
                    } else {
                        inner += 1;
                    }
                }
                at = inner + close.len();
                continue;
            }
            b'#' => {
                at += rest.find('\n').unwrap_or(rest.len());
                continue;
            }
            b'[' if !value && before[statement..at].trim().is_empty() => {
                let line = &rest[..rest.find('\n').unwrap_or(rest.len())];
                let header = line.split('#').next().unwrap_or_default().trim();
                let Some(inner) = header
                    .strip_prefix('[')
                    .and_then(|header| header.strip_suffix(']'))
                else {
                    return (None, None);
                };
                let inner = inner.trim_start_matches('[').trim_end_matches(']').trim();
                if inner.is_empty() || line.len() == rest.len() {
                    // A header the error is in.
                    return (None, None);
                }
                (table, key) = (Some(inner.to_string()), None);
                at += line.len();
                continue;
            }
            b'[' | b'{' if value => depth += 1,
            b']' | b'}' if value => depth = depth.saturating_sub(1),
            b'=' if !value => {
                key = Some(before[statement..at].trim().to_string()).filter(|k| !k.is_empty());
                value = true;
            }
            b'\n' => {
                if value && depth == 0 {
                    (value, key) = (false, None);
                }
                if !value {
                    statement = at + 1;
                }
            }
            _ => {}
        }
        at += 1;
    }
    (table, key.filter(|_| value))
}

/// The keys of a reader's path, each array element written after the key
/// that holds it (`crates[0]`), as TOML addresses it.
fn segments(path: &serde_path_to_error::Path) -> Vec<String> {
    use serde_path_to_error::Segment;
    let mut keys: Vec<String> = Vec::new();
    for segment in path.iter() {
        match segment {
            Segment::Map { key } => keys.push(key.clone()),
            Segment::Seq { index } => match keys.last_mut() {
                Some(last) => last.push_str(&format!("[{index}]")),
                None => keys.push(format!("[{index}]")),
            },
            Segment::Enum { variant } => keys.push(variant.clone()),
            Segment::Unknown => {}
        }
    }
    keys
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.origin, self.line) {
            // The variable's name spells the table and the key.
            (Some(Origin::Variable(name)), _) => return write!(f, "{name}: {}", self.message),
            (Some(Origin::File(path)), Some(line)) => match self.column {
                Some(column) => write!(f, "{path}:{line}:{column}: ")?,
                None => write!(f, "{path}:{line}: ")?,
            },
            (Some(Origin::File(path)), None) => write!(f, "{path}: ")?,
            (None, Some(line)) => write!(f, "line {line}: ")?,
            (None, None) => {}
        }
        match (&self.table, &self.key) {
            (Some(table), Some(key)) => write!(f, "[{table}] {key}: ")?,
            (Some(table), None) => write!(f, "[{table}]: ")?,
            (None, Some(key)) => write!(f, "{key}: ")?,
            (None, None) => {}
        }
        f.write_str(&self.message)
    }
}

impl std::error::Error for Problem {}

impl From<String> for Problem {
    /// A problem that names no table or key.
    fn from(message: String) -> Self {
        Self::at(None, None, message)
    }
}

/// Every problem a reader or a check found, in the order found; printed
/// one to a line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Problems(pub Vec<Problem>);

impl Problems {
    /// Each problem without an origin, read from the file at `path`.
    pub fn in_file(mut self, path: impl fmt::Display) -> Self {
        let path = path.to_string();
        for problem in &mut self.0 {
            problem
                .origin
                .get_or_insert_with(|| Origin::File(path.clone()));
        }
        self
    }

    /// Whether there is none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Problem> for Problems {
    fn from(problem: Problem) -> Self {
        Self(vec![problem])
    }
}

impl fmt::Display for Problems {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, problem) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{problem}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Problems {}

impl From<Problems> for crate::Error {
    fn from(problems: Problems) -> Self {
        crate::Error::new(problems.to_string())
    }
}
