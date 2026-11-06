//! A cluster table is read whole before its `kind` says which keys it
//! takes. Read so, it must refuse what the struct read directly refuses,
//! keep each value as its format wrote it, and name a problem's table and
//! key in words a reader of TOML or JSON can read.

use gdl_model::{Cluster, Fleet, KubernetesCluster};

const REQUIRED: &str = "context = 'c'\nnamespace = 'n'\nregion = 'r'\n";

/// A float that is no number is refused where a string or an integer
/// goes, as the struct read directly refuses it, and never read as unset.
#[test]
fn a_float_that_is_no_number_is_refused() {
    for extra in [
        "registry = nan\n",
        "platform = inf\n",
        "resources = -inf\n",
        "tolerations = [{key = 'dedicated', operator = 'Exists', effect = 'NoExecute', toleration_seconds = nan}]\n",
    ] {
        let fields = format!("{REQUIRED}{extra}");
        assert!(
            toml::from_str::<KubernetesCluster>(&fields).is_err(),
            "{extra}"
        );
        assert!(
            Fleet::from_toml(&format!("[cluster.a]\n{fields}")).is_err(),
            "accepted: {extra}"
        );
    }
}

/// A key written twice in JSON is refused, as the struct read directly
/// refuses it, and so is a second `kind`: neither is overwritten.
#[test]
fn a_key_written_twice_is_refused() {
    let fields = r#"{"context":"c","namespace":"n","region":"r","operator":true,"operator":false}"#;
    assert!(serde_json::from_str::<KubernetesCluster>(fields).is_err());
    let text = format!(r#"{{"cluster":{{"a":{fields}}}}}"#);
    let error = serde_json::from_str::<Fleet>(&text).expect_err("operator twice");
    assert!(
        error.to_string().contains("duplicate field `operator`"),
        "{error}"
    );
    let twice =
        r#"{"kind":"slurm","kind":"kubernetes","context":"c","namespace":"n","region":"r"}"#;
    let error = serde_json::from_str::<Cluster>(twice).expect_err("kind twice");
    assert!(
        error.to_string().contains("duplicate field `kind`"),
        "{error}"
    );
}

/// A problem inside the table says where it is in words: in JSON, which
/// has no table and key, in the message; in TOML, as the table and the
/// key, with no character of the reader's own left in the message, even
/// for a key that holds such a character.
#[test]
fn a_problem_inside_the_table_names_where_in_words() {
    let error = serde_json::from_str::<Fleet>(r#"{"cluster":{"a":{"kind":false}}}"#)
        .expect_err("kind false")
        .to_string();
    assert!(error.starts_with("in `kind`: is false"), "{error}");
    assert!(!error.contains(char::is_control), "{error:?}");

    let nested =
        format!("[cluster.a]\n{REQUIRED}[cluster.a.resources.default.requests]\ncpu = false\n");
    let problem = &Fleet::from_toml(&nested).expect_err("cpu false").0[0];
    assert_eq!(
        problem.table.as_deref(),
        Some("cluster.a.resources.default.requests")
    );
    assert_eq!(problem.key.as_deref(), Some("cpu"));
    assert!(problem.message.starts_with("invalid type"), "{problem}");

    for key in ["\\u001F", "\\u001E", "a`b", "x.y", "in `z`: w"] {
        let text = format!("[cluster.a]\n{REQUIRED}[cluster.a.env]\n\"{key}\" = false\n");
        let problem = &Fleet::from_toml(&text).expect_err(key).0[0];
        assert_eq!(problem.table.as_deref(), Some("cluster.a.env"), "{key}");
        assert!(
            problem.message.starts_with("invalid type"),
            "{key}: {problem}"
        );
        assert!(!problem.message.contains(char::is_control), "{problem:?}");
    }
}

/// What reads writes as the struct read directly writes it, for every
/// shape of a value: integers at their bounds, inline tables, arrays of
/// tables, and tables of keys that are a cluster's own keys elsewhere.
#[test]
fn every_shape_of_value_writes_as_the_struct_writes_it() {
    for extra in [
        "tolerations = [{key = 'dedicated', operator = 'Exists', effect = 'NoExecute', toleration_seconds = 9223372036854775807}]\n",
        "[[tolerations]]\nkey = 'dedicated'\noperator = 'Exists'\neffect = 'NoExecute'\ntoleration_seconds = -9223372036854775808\n",
        "resources = {default = {requests = {cpu = '250m', memory = '256Mi'}, limits = {cpu = '1', memory = '1Gi'}}}\n",
        "[env]\nKIND = 'slurm'\nkind = 'kubernetes'\n",
        "[resources.default.requests]\ncpu = '250m'\nmemory = '256Mi'\n[resources.default.limits]\ncpu = '1'\nmemory = '1Gi'\n",
    ] {
        let fields = format!("{REQUIRED}{extra}");
        let direct: KubernetesCluster = toml::from_str(&fields).expect("the struct");
        let table = fields
            .replace("[[tolerations]]", "[[cluster.a.tolerations]]")
            .replace("[env]", "[cluster.a.env]")
            .replace("[resources.", "[cluster.a.resources.");
        let fleet = Fleet::from_toml(&format!("[cluster.a]\n{table}")).expect("a fleet");
        let cluster = fleet.cluster("a").expect("a cluster");
        assert_eq!(cluster.kubernetes(), Some(&direct), "{extra}");
        assert_eq!(
            toml::to_string(cluster).expect("toml"),
            toml::to_string(&direct).expect("toml")
        );
        assert_eq!(
            serde_json::to_string(cluster).expect("json"),
            serde_json::to_string(&direct).expect("json")
        );
    }
}
