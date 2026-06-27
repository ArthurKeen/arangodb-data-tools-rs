"""Type stubs for the `arangox` extension module (sketch).

Only `import_file` is implemented; `export`/`dump`/`restore` raise
`NotImplementedError` for now.
"""

from typing import Any, Optional, TypedDict

__version__: str

class ImportSummary(TypedDict):
    documents_sent: int
    batches: int
    created: int
    errors: int
    updated: int
    ignored: int
    empty: int
    bytes_sent: int

def import_file(
    collection: str,
    input: str,
    *,
    endpoint: Optional[str] = ...,
    database: Optional[str] = ...,
    username: Optional[str] = ...,
    password: Optional[str] = ...,
    token: Optional[str] = ...,
    insecure: bool = ...,
    request_timeout_secs: int = ...,
    create_collection: bool = ...,
    edge: bool = ...,
    on_duplicate: Optional[str] = ...,
    overwrite: bool = ...,
    from_collection_prefix: Optional[str] = ...,
    to_collection_prefix: Optional[str] = ...,
    format: Optional[str] = ...,
    batch_size_bytes: Optional[int] = ...,
    max_docs: Optional[int] = ...,
    threads: Optional[int] = ...,
    max_in_flight_bytes: Optional[int] = ...,
) -> ImportSummary: ...
def export(**kwargs: Any) -> None: ...
def dump(**kwargs: Any) -> None: ...
def restore(**kwargs: Any) -> None: ...
