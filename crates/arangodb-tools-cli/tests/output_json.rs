//! Hermetic checks for the `--output json` mode.
//!
//! These drive the `arangox` binary with an input that fails *before* any
//! network call (a missing import file), so they need no server. They assert
//! that the output mode controls how errors are rendered: a JSON object on
//! stderr in `json` mode, and a `error: ...` line in text mode.

use std::process::Command;

/// Path to the freshly built `arangox` binary under test.
const ARANGOX: &str = env!("CARGO_BIN_EXE_arangox");

#[test]
fn json_mode_renders_errors_as_json_on_stderr() {
    let output = Command::new(ARANGOX)
        .args(["--output", "json", "import"])
        .args(["--collection", "c"])
        .args(["--input", "/no/such/arangox-test-file.jsonl"])
        .output()
        .expect("run arangox");

    assert!(!output.status.success(), "missing input should fail");
    assert!(
        output.stdout.is_empty(),
        "no result object on stdout for an early failure; got: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let last = stderr.lines().last().unwrap_or_default();
    let value: serde_json::Value =
        serde_json::from_str(last).expect("stderr ends with a JSON error object");
    assert_eq!(value["status"], "error");
    assert!(
        value["message"]
            .as_str()
            .unwrap_or_default()
            .contains("/no/such/"),
        "error message names the bad path: {value}"
    );
}

#[test]
fn text_mode_renders_errors_as_plain_text() {
    let output = Command::new(ARANGOX)
        .args(["import", "--collection", "c"])
        .args(["--input", "/no/such/arangox-test-file.jsonl"])
        .output()
        .expect("run arangox");

    assert!(!output.status.success(), "missing input should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error: "),
        "text mode prints a plain error line; got: {stderr}"
    );
    assert!(
        serde_json::from_str::<serde_json::Value>(stderr.trim()).is_err(),
        "text-mode error is not JSON"
    );
}
