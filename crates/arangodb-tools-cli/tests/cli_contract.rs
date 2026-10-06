//! Hermetic checks on the CLI's argument surface.
//!
//! These need no server: they assert that the parser itself is well-formed and
//! that credential conflicts are rejected before any connection is attempted.

use std::process::Command;

const ARANGOX: &str = env!("CARGO_BIN_EXE_arangox");

/// Runs `arangox` with `args` and returns `(stdout + stderr, exit_ok)`.
fn run(args: &[&str]) -> (String, bool) {
    let out = Command::new(ARANGOX)
        .args(args)
        .output()
        .expect("arangox binary runs");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (text, out.status.success())
}

/// `--output` is a destination path on `dump`/`export`, while the global
/// presentation flag is `--output-format`. When the global flag was also named
/// `--output`, clap's `global = true` propagation produced two definitions of
/// the id `output` with different types, and *every* `dump`/`export` run
/// panicked at parse time with "Mismatch between definition and access".
///
/// The panic was invisible to the rest of the suite because the only CLI test
/// exercised `import`, which has no `--output`. These assertions pin the shape
/// that keeps the two apart.
#[test]
fn dump_and_export_accept_output_as_a_destination_path() {
    for args in [
        vec!["dump", "--output", "/nonexistent-path-for-parse-test"],
        vec!["export", "--query", "RETURN 1", "--output", "-"],
    ] {
        let (text, _) = run(&args);
        assert!(
            !text.contains("Mismatch between definition and access"),
            "clap arg-id collision regressed for `{}`:\n{text}",
            args[0]
        );
        assert!(
            !text.contains("unexpected argument '--output'"),
            "`{}` must accept --output as its destination:\n{text}",
            args[0]
        );
    }
}

#[test]
fn global_output_format_flag_is_accepted_before_a_subcommand() {
    let (text, _) = run(&["--output-format", "json", "import", "--collection", "c"]);
    assert!(
        !text.contains("unexpected argument"),
        "--output-format must be a global flag:\n{text}"
    );
}

#[test]
fn conflicting_credential_sources_are_rejected_by_name() {
    let (text, ok) = run(&[
        "export",
        "--query",
        "RETURN 1",
        "--output",
        "-",
        "--username",
        "root",
        "--jwt-secret-env",
        "SOME_VAR",
    ]);
    assert!(!ok, "conflicting credentials must fail");
    assert!(
        text.contains("conflicting authentication options"),
        "error must name the conflict:\n{text}"
    );
    // Both offending flags are named, so the user does not have to guess which
    // one was ignored — the ambiguity this check exists to prevent.
    assert!(text.contains("--username"), "names --username:\n{text}");
    assert!(
        text.contains("--jwt-secret-env"),
        "names --jwt-secret-env:\n{text}"
    );
}

#[test]
fn auth_jwt_without_a_username_explains_the_alternatives() {
    let (text, ok) = run(&[
        "export", "--query", "RETURN 1", "--output", "-", "--auth", "jwt",
    ]);
    assert!(!ok, "--auth jwt without --username must fail");
    assert!(
        text.contains("--auth jwt requires --username"),
        "error must say what is missing:\n{text}"
    );
    assert!(
        text.contains("--jwt-secret-file"),
        "error must point at the superuser-JWT alternative:\n{text}"
    );
}

#[test]
fn an_unset_credential_variable_names_the_variable() {
    let (text, ok) = run(&[
        "export",
        "--query",
        "RETURN 1",
        "--output",
        "-",
        "--jwt-secret-env",
        "DEFINITELY_UNSET_VAR_FOR_TEST",
    ]);
    assert!(!ok, "an unset credential variable must fail");
    assert!(
        text.contains("DEFINITELY_UNSET_VAR_FOR_TEST"),
        "error must name the missing variable:\n{text}"
    );
}
