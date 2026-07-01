"""Type stubs for the `arangox` extension module.

Each function builds a client, runs the corresponding async pipeline to
completion (releasing the GIL during I/O), and returns a result dict.
"""

from typing import Any, Dict, List, Optional, TypedDict

__version__: str

class ImportSummary(TypedDict):
    operation: str
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
def export(
    output: str,
    *,
    collection: Optional[str] = ...,
    query: Optional[str] = ...,
    bind_vars: Optional[str] = ...,
    format: Optional[str] = ...,
    fields: Optional[List[str]] = ...,
    compression: Optional[str] = ...,
    batch_size: int = ...,
    split_bytes: Optional[int] = ...,
    endpoint: Optional[str] = ...,
    database: Optional[str] = ...,
    username: Optional[str] = ...,
    password: Optional[str] = ...,
    token: Optional[str] = ...,
    insecure: bool = ...,
    request_timeout_secs: int = ...,
) -> Dict[str, Any]: ...
def dump(
    output: str,
    *,
    include_system: bool = ...,
    compression: Optional[str] = ...,
    batch_ttl_secs: int = ...,
    endpoint: Optional[str] = ...,
    database: Optional[str] = ...,
    username: Optional[str] = ...,
    password: Optional[str] = ...,
    token: Optional[str] = ...,
    insecure: bool = ...,
    request_timeout_secs: int = ...,
) -> Dict[str, Any]: ...
def restore(
    input: str,
    *,
    create_database: bool = ...,
    overwrite: bool = ...,
    endpoint: Optional[str] = ...,
    database: Optional[str] = ...,
    username: Optional[str] = ...,
    password: Optional[str] = ...,
    token: Optional[str] = ...,
    insecure: bool = ...,
    request_timeout_secs: int = ...,
) -> Dict[str, Any]: ...
