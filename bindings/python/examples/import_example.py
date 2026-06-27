"""Minimal example of the arangox Python bindings.

Run after `maturin develop` against a reachable ArangoDB:

    ARANGO_PASSWORD=... python examples/import_example.py users.jsonl

Prints the import summary dict.
"""

import os
import sys

import arangox


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: import_example.py <input-file> [collection]", file=sys.stderr)
        return 2

    input_path = sys.argv[1]
    collection = sys.argv[2] if len(sys.argv) > 2 else "users"

    summary = arangox.import_file(
        collection,
        input_path,
        endpoint=os.environ.get("ARANGO_ENDPOINT", "http://localhost:8529"),
        database=os.environ.get("ARANGO_DATABASE", "_system"),
        username=os.environ.get("ARANGO_USERNAME", "root"),
        password=os.environ.get("ARANGO_PASSWORD", ""),
        create_collection=True,
        on_duplicate="update",
    )
    print(summary)
    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
