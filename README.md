# Milvus Connector

Adds Milvus vector database connectivity as an installable connector extension.

This connector is listed in the public Irodori extension marketplace.

## Connector

- Extension ID: `irodori.milvus`
- Engine ID: `milvus`
- Wire: `milvus`
- Default port: `19530`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `true`

No desktop adapter source exists yet; this package starts from the refactored ABI shim and connector metadata.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust code keeps native ABI exports in `src/lib.rs`, shared buffer/JSON helpers in `src/abi.rs`, and Milvus REST API behavior in `src/driver.rs`.

## Connection Metadata

- Endpoint modes: `hostPort`, `connectionString`
- Transport modes: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS supported: `true`
- Custom driver options: `true`

| Auth method | Label | Secret purposes |
|---|---|---|
| `none` | No authentication | none |
| `connectionString` | Connection string / DSN | none |
| `apiKey` | API key | `token` |
| `bearerToken` | Bearer token | `token` |
| `clientCertificate` | Client certificate / mTLS | `privateKey`, `privateKeyPassphrase` |
| `userPassword` | User/password | `password` |
| `customDriverOptions` | Custom driver options | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## Experience Metadata

- Domains: `vector`
- Result views: `vectorNeighbors`, `table`, `json`
- Inspired by: `Milvus collections`, `Milvus indexes`, `Milvus scalar filtering`

| Workflow | Result view | Templates |
|---|---|---|
| Similarity search | vectorNeighbors | vector-similarity |
| Filtered ANN search | vectorNeighbors | vector-filtered |
| Collection or index health | table | vector-health |

| Template | Label | Language | Result view |
|---|---|---|---|
| `vector-similarity` | Milvus vector search | `python` | `vectorNeighbors` |
| `vector-filtered` | Milvus filtered search | `python` | `vectorNeighbors` |
| `vector-health` | Milvus collection stats | `text` | `table` |

## ABI Calls

The driver handles these JSON requests today:

| Method | Response |
|---|---|
| `health` / `ping` | Connector health, engine id, ABI version, and driver link status. |
| `describe` / `capabilities` | Embedded manifest and connector config. |
| `manifest` | Raw `irodori.extension.json`. |
| `config` | Raw `connector.config.json`. |
| `connect` | Lists Milvus collections through REST API. |
| `query` | Runs collection query/search requests through Milvus REST API. |
| `metadata` | Reads collection schema from Milvus REST API. |
| `close` | Removes the cached native connection. |

## Development


Generated extension repositories share `../target` across sibling repositories so Rust dependencies are compiled once per checkout. DuckDB and MotherDuck are driver-linked by default; set `IRODORI_CONNECTOR_LINK_DUCKDB=0` only when you need metadata-only DuckDB-compatible scaffolds.


```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.
