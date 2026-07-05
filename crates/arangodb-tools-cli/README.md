# arangodb-tools-cli (`arangox`)

The `arangox` command-line tool for ArangoDB bulk data workflows, part of the
[arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

A single binary with `import`, `export`, `dump`, `restore`, and `rdf`
subcommands. It reads and writes local files or object storage (S3-compatible),
supports gzip/zstd compression, live progress reporting, and machine-readable
output via `--output json`.

## Install

```bash
cargo install arangodb-tools-cli
```

## Usage

```bash
# Import JSONL into a collection
arangox import --database mydb --collection people --input people.jsonl

# Dump and restore a database
arangox dump    --database mydb --output s3://backups/mydb
arangox restore --database mydb --input  s3://backups/mydb
```

Common connection flags (`--endpoint`, `--database`, `--username`,
`--password-env`/`--auth-token-env`, `--tls-ca`, `--insecure`) are shared by all
subcommands. See the [repository README](https://github.com/ArthurKeen/arangodb-data-tools-rs)
for the full command reference.

## License

MIT
