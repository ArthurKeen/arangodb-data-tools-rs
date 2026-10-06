# Proposed PRD patch (NOT applied) — REQ-110 / §13.1

**delta_type:** wrong-signature
**review_state:** proposed
**created:** 2026-10-05

## Observed
§13.1 lists the global option as `--output text|json`. That spelling is
**unimplementable**: `dump` (dump.rs:24) and `export` (export.rs:41) both take
`--output` for their destination path, and clap's `global = true` propagation
makes the two definitions collide. Any `arangox dump --output ...` or
`arangox export --output ...` invocation panics at parse time:

    Mismatch between definition and access of `output`.

The flag has been renamed in code to `--output-format`, keeping `--output`
as the destination on dump/export.

## Proposed patch
In §13.1, replace `--output text|json` with `--output-format text|json`, and
note that `dump`/`export` keep `--output` for their destination path.

## Justification
The previous §13.1 patch (applied 2026-08-18) changed `--log-format` to
`--output` to reflect that the flag governs result rendering, not just logs.
That was right about the semantics but picked a name already taken by two
subcommands. `--output-format` keeps the corrected semantics without the
collision, and leaves `--output` meaning a destination — which matches
arangodump/arangoexport convention.
