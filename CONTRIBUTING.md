# Contributing

Thanks for your interest in `arangodb-data-tools-rs`. The project is pre-alpha and the architecture is still solidifying, so please open an issue to discuss substantial changes before sending a large PR.

## Ground rules

- This is a **clean-room** project. Do **not** copy or paste code from ArangoDB's source (it is licensed under BSL 1.1). Implement behavior from public APIs, public documentation, and black-box observation only.
- Keep the design aligned with `RUST_ARANGODB_TOOLS_PRD.md` and `docs/IMPLEMENTATION_PLAN.md`. If a change diverges from those, update them in the same PR.

## Development

A stable Rust toolchain is pinned via `rust-toolchain.toml`.

Before pushing, make sure the following all pass:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Integration tests run against an ArangoDB instance in Docker. The CI workflow starts one automatically; locally you can run your own and point tests at it via the `ARANGO_ENDPOINT` environment variable (added as the integration test suite lands).

## Commit and PR conventions

- Write focused commits with clear messages explaining the "why".
- Keep PRs small and reviewable where possible.
- Add or update tests for behavior changes.
- Update relevant docs (`README.md`, PRD, implementation plan, `docs/`).

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
