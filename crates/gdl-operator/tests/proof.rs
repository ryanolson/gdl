//! Exercise the kind proof's final assertions with completed and pending requests.

use std::process::{Command, Output};

fn finish_load(initial_bad: u8, final_bad: u8) -> Output {
    let script = include_str!("../../../scripts/kind-operator.sh");
    let (_, finish) = script
        .rsplit_once("\ncounts=\"$(load_counts)\"\n")
        .expect("the kind proof's final load check");
    let shell = format!(
        r#"
set -euo pipefail
load_bad={initial_bad}
fail() {{ exit 1; }}
say() {{ :; }}
load_counts() {{ printf '12 %s\n' "${{load_bad}}"; }}
k() {{
  if [[ "$1" == exec ]]; then
    load_bad={final_bad}
  elif [[ "$1" == logs ]]; then
    printf 'ok 1\n'
    if [[ "${{load_bad}}" != 0 ]]; then printf 'fail 2 28 0 timeout\n'; fi
    printf 'done\n'
  else
    exit 2
  fi
}}
counts="$(load_counts)"
{finish}
"#
    );
    Command::new("bash")
        .args(["-c", &shell])
        .output()
        .expect("run the kind proof's final assertions with a completed load")
}

#[test]
fn the_kind_proof_accepts_successes_and_rejects_reported_failures() {
    assert!(finish_load(0, 0).status.success());
    assert!(!finish_load(1, 1).status.success());
}

#[test]
fn the_kind_proof_rejects_a_request_that_fails_as_the_load_stops() {
    assert!(
        !finish_load(0, 1).status.success(),
        "a pending request failed when the load stopped, but the proof reported success"
    );
}
