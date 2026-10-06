# Authentication

ArangoDB accepts several credential shapes, and its own client tools spell them
inconsistently. This page states exactly what `arangox` does, what the server
actually requires, and how to diagnose a rejection — because "not authorized" is
the same message whether the secret is wrong, the claims are malformed, or the
user simply lacks rights.

Everything below was verified against **ArangoDB 3.12.4**.

## The four modes

`arangox` accepts exactly one credential source. Supplying two is an error that
names both, rather than silently preferring one.

| Mode | Flags | Identity | Expiry |
|---|---|---|---|
| Basic | `--username` + `--password-env` | that user | n/a |
| User JWT (login) | `--username` + `--password-env` + `--auth jwt` | that user | server-set; refreshed automatically |
| Superuser JWT | `--jwt-secret-file` or `--jwt-secret-env` | **superuser** | minted hourly; re-minted automatically |
| Supplied token | `--auth-token-env` | whatever the token says | **not refreshable** |

Secrets are always read from a file or a named environment variable, never from
the command line, so they do not appear in shell history or process listings.

### Basic — the default

```bash
export ARANGO_PASSWORD='...'
arangox import --username root --password-env ARANGO_PASSWORD \
  --collection users --input users.jsonl
```

### User JWT — log in once instead of resending the password

```bash
arangox import --username root --password-env ARANGO_PASSWORD --auth jwt \
  --collection users --input users.jsonl
```

`arangox` posts to `/_open/auth`, receives a token, and uses it as a bearer
credential. Same user, same permissions as basic — the difference is that the
password crosses the wire once rather than on every request.

**The token is short-lived.** ArangoDB 3.12 issues one-hour tokens, which is
shorter than many dumps and imports. `arangox` reads the token's `exp` and
re-authenticates a minute before it lapses, and again if a request is ever
refused, so a long operation does not die partway through.

### Superuser JWT — what `--server.jwt-secret-keyfile` is for

If you hold the server's JWT secret, `arangox` mints its own superuser token:

```bash
arangox dump --jwt-secret-file /etc/arangodb/jwtsecret \
  --database mydb --output s3://backups/mydb
```

or, when secrets arrive as environment variables:

```bash
arangox dump --jwt-secret-env ARANGO_JWT_SECRET \
  --database mydb --output s3://backups/mydb
```

This is the mode that matches ArangoDB's own `--server.jwt-secret-keyfile`. A
superuser token **bypasses user permissions entirely**, so prefer a real user
account where one will do.

The secret must match the server's keyfile byte for byte. A trailing newline —
from `echo` or an editor — produces a signature the server rejects with an
opaque 401; `arangox` trims one trailing newline from `--jwt-secret-file` for
exactly this reason.

### A token you already hold

```bash
export ARANGO_TOKEN='eyJhbGciOi...'
arangox export --auth-token-env ARANGO_TOKEN --collection users --output users.jsonl
```

The token is sent unchanged. `arangox` cannot re-obtain it, so **if it expires
mid-run the operation fails.** For anything long-running, prefer
`--jwt-secret-file` or `--auth jwt`.

## What the server actually requires

Both token shapes are HS256 JWTs signed with the server's JWT secret. They
differ **only by payload** — and the difference is not obvious from the error
messages, which is what makes hand-rolled tokens so frustrating to debug.

| Claims | Result |
|---|---|
| `server_id` + `iss: "arangodb"` | **superuser** |
| `preferred_username` + `iss: "arangodb"` | that user's permissions |

Verified behavior, all observed against 3.12.4:

- **`server_id` is required for a superuser token.** A token carrying only
  `iss: "arangodb"` is rejected with 401. This is the trap: the payload looks
  reasonable and the error says nothing about a missing claim.
- **`exp` is optional on a superuser token.** Omit it and the token never
  expires. `arangox` sets one anyway and re-mints, since minting is local
  and free.
- A token from `POST /_open/auth` carries `preferred_username` and a
  server-chosen `exp` — one hour by default.
- A wrong `iss`, an unknown `preferred_username`, an elapsed `exp`, or a
  signature under the wrong secret each produce an identical 401.

For reference, this is the superuser payload `arangox` signs:

```json
{"server_id": "arangodb-data-tools-rs", "iss": "arangodb", "iat": 1791243953, "exp": 1791247553}
```

## Diagnosing a 401

When `arangox` is using a mode it can refresh, it retries once with a fresh
token before giving up — so a 401 that reaches you is a genuine credential
failure, and the message says which mode failed and what to check.

To confirm a token outside the tool:

```bash
# Mint and test a superuser token by hand
SECRET=$(cat /etc/arangodb/jwtsecret)
# ... sign {"server_id":"x","iss":"arangodb"} with HS256 using $SECRET ...
curl -H "Authorization: bearer $TOKEN" http://localhost:8529/_api/version
```

```bash
# Check a username/password pair against the login endpoint directly
curl -X POST http://localhost:8529/_open/auth \
  -d '{"username":"root","password":"..."}'
```

A `200` with a `jwt` field means the credentials are good and any remaining
failure is a permissions problem on the target database, not an authentication
problem.

Both `bearer` and `Bearer` are accepted in the `Authorization` header.

## Library use

```rust
use arangodb_client::ArangoClient;

// Superuser JWT, minted and refreshed by the client.
let client = ArangoClient::builder()
    .endpoint("http://localhost:8529")
    .database("mydb")
    .jwt_secret(std::fs::read_to_string("/etc/arangodb/jwtsecret")?.trim_end())
    .build()?;

// Or log in as a user and let the client refresh the token.
let client = ArangoClient::builder()
    .endpoint("http://localhost:8529")
    .database("mydb")
    .jwt_login("root", std::env::var("ARANGO_PASSWORD")?)
    .build()?;
```

Clients produced by `with_database` share the token cache with their parent, so
a multi-database operation logs in once rather than per database.
