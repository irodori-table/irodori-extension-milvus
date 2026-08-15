use irodori_connector_abi::{collect_url_auth, option_bool, option_string, push_sensitive};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use reqwest::{Client, RequestBuilder};
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, MilvusConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct MilvusConnection {
    client: Client,
    config: MilvusConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MilvusConfig {
    base_url: String,
    token: Option<String>,
    tls: TlsConfig,
    redaction_values: Vec<String>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, MilvusConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match MilvusConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let client = match config.tls.build_client() {
        Ok(client) => client,
        Err(err) => return abi::error("connector.invalidRequest", config.redact(&err)),
    };
    let connection = MilvusConnection { client, config };
    let collection_count = match runtime().and_then(|runtime| runtime.block_on(probe(&connection)))
    {
        Ok(count) => count,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "endpoint".to_string(),
            Value::String(connection.config.base_url.clone()),
        ),
        ("collectionCount".to_string(), json!(collection_count)),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(input) = abi::string_field(request, "query")
        .or_else(|| abi::string_field(request, "sql"))
        .or_else(|| abi::string_field(request, "statement"))
        .or_else(|| abi::string_field(request, "collection"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a collection name or JSON Milvus query.",
        );
    };
    let query = match MilvusQuery::from_input(input, request, abi::max_rows(request)) {
        Ok(query) => query,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(run_milvus_query(&connection, query))) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl MilvusConnection {
    fn auth(&self, builder: RequestBuilder) -> RequestBuilder {
        if let Some(token) = self.config.token.as_deref() {
            builder.bearer_auth(token)
        } else {
            builder
        }
    }
}

/// Transport security, as `connector.config.json` declares it under
/// `clientCertificate`.
///
/// Paths, never key material: connector options persist to the workspace in the
/// clear, so the profile carries a path and the driver reads the file at
/// connect time.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TlsConfig {
    root_cert_path: Option<String>,
    client_cert_path: Option<String>,
    client_key_path: Option<String>,
    accept_invalid_certs: bool,
}

impl TlsConfig {
    fn from_request(request: &Value) -> Self {
        Self {
            root_cert_path: option_string(
                request,
                &["sslRootCert", "sslrootcert", "ssl-ca", "caCert"],
            ),
            client_cert_path: option_string(
                request,
                &["sslCert", "sslcert", "ssl-cert", "clientCert"],
            ),
            client_key_path: option_string(request, &["sslKey", "sslkey", "ssl-key", "clientKey"]),
            accept_invalid_certs: option_string(request, &["sslInsecure", "tlsInsecure"])
                .is_some_and(|value| matches!(value.trim(), "1" | "true" | "yes")),
        }
    }

    /// A client honouring the profile's TLS material.
    ///
    /// Returns the default client untouched when nothing is configured, so a
    /// plain `http://` endpoint keeps working exactly as before.
    fn build_client(&self) -> Result<Client, String> {
        if self.root_cert_path.is_none()
            && self.client_cert_path.is_none()
            && self.client_key_path.is_none()
            && !self.accept_invalid_certs
        {
            return Client::builder()
                .build()
                .map_err(|err| format!("HTTP client setup failed: {err}"));
        }

        let mut builder = Client::builder();

        if let Some(path) = &self.root_cert_path {
            let pem = read_pem(path, "SSL root certificate")?;
            let bundle = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|err| format!("SSL root certificate at {path} is not valid PEM: {err}"))?;
            // `from_pem_bundle` answers Ok(vec![]) for a file with no PEM
            // blocks in it. Adding nothing and carrying on would fall back to
            // the system roots — the connection would succeed while verifying
            // against something other than the CA the user named.
            if bundle.is_empty() {
                return Err(format!(
                    "SSL root certificate at {path} contains no PEM certificate."
                ));
            }
            for certificate in bundle {
                builder = builder.add_root_certificate(certificate);
            }
        }

        // reqwest wants one PEM carrying both halves. Accept them as separate
        // files, which is how every other tool spells it, and join them here.
        match (&self.client_cert_path, &self.client_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let mut pem = read_pem(cert_path, "SSL client certificate")?;
                if !pem.ends_with(b"\n") {
                    pem.push(b'\n');
                }
                pem.extend_from_slice(&read_pem(key_path, "SSL client key")?);
                builder = builder.identity(
                    reqwest::Identity::from_pem(&pem)
                        .map_err(|err| format!("SSL client identity is not usable: {err}"))?,
                );
            }
            (Some(_), None) => {
                return Err("SSL client certificate needs a matching client key.".to_string())
            }
            (None, Some(_)) => {
                return Err("SSL client key needs a matching client certificate.".to_string())
            }
            (None, None) => {}
        }

        if self.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        builder
            .build()
            .map_err(|err| format!("TLS client setup failed: {err}"))
    }
}

fn read_pem(path: &str, label: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|err| format!("{label} at {path} could not be read: {err}"))
}

impl MilvusConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let base_url = option_string(request, &["connectionString", "url", "dsn"])
            .unwrap_or_else(|| build_url(request));
        let token = option_string(request, &["token", "apiKey", "bearerToken", "accessToken"])
            .or_else(|| {
                let user = option_string(request, &["user", "username"])?;
                let password = option_string(request, &["password"])?;
                Some(format!("{user}:{password}"))
            });
        let tls = TlsConfig::from_request(request);
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, token.as_deref());
        collect_url_auth(&base_url, &mut redaction_values);
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            token,
            tls,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values.iter().fold(
            message.replace(&self.base_url, "<milvus-url>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

struct MilvusQuery {
    endpoint: &'static str,
    body: Value,
    cap: usize,
}

impl MilvusQuery {
    fn from_input(input: &str, request: &Value, cap: usize) -> Result<Self, String> {
        let input = input.trim();
        let mut value = if input.starts_with('{') {
            serde_json::from_str::<Value>(input)
                .map_err(|err| format!("invalid Milvus query JSON: {err}"))?
        } else {
            json!({ "collectionName": input })
        };
        if value.get("collectionName").is_none() {
            if let Some(collection) = option_string(request, &["collection", "collectionName"]) {
                value["collectionName"] = Value::String(collection);
            }
        }
        let collection = value
            .get("collectionName")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "Milvus query needs collectionName.".to_string())?
            .to_string();
        let endpoint = if value.get("data").is_some() || value.get("vector").is_some() {
            if value.get("data").is_none() {
                value["data"] = json!([value.get("vector").cloned().unwrap_or(Value::Null)]);
            }
            "/v2/vectordb/entities/search"
        } else {
            value["filter"] = value
                .get("filter")
                .cloned()
                .unwrap_or_else(|| Value::String(String::new()));
            "/v2/vectordb/entities/query"
        };
        value["collectionName"] = Value::String(collection);
        if value.get("limit").is_none() {
            value["limit"] = json!(cap);
        }
        if endpoint.ends_with("/query") && value.get("outputFields").is_none() {
            value["outputFields"] = json!(["*"]);
        }
        Ok(Self {
            endpoint,
            body: value,
            cap,
        })
    }
}

async fn probe(connection: &MilvusConnection) -> Result<usize, String> {
    Ok(collection_names(connection).await?.len())
}

async fn run_milvus_query(
    connection: &MilvusConnection,
    query: MilvusQuery,
) -> Result<QueryOutput, String> {
    let value = post_json(connection, query.endpoint, query.body).await?;
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let rows_json = match data {
        Value::Array(values) => values,
        Value::Object(object) => object
            .get("data")
            .or_else(|| object.get("results"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| vec![Value::Object(object)]),
        other => vec![other],
    };
    let truncated = rows_json.len() > query.cap;
    Ok(rows_to_output(
        rows_json.into_iter().take(query.cap).collect(),
        truncated,
    ))
}

async fn load_metadata(connection: &MilvusConnection) -> Result<Value, String> {
    let mut objects = Vec::new();
    for name in collection_names(connection).await? {
        let value = post_json(
            connection,
            "/v2/vectordb/collections/describe",
            json!({ "collectionName": name }),
        )
        .await?;
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        let fields = data
            .pointer("/schema/fields")
            .or_else(|| data.get("fields"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let columns = fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                json!({
                    "name": field.get("name").and_then(Value::as_str).unwrap_or("field"),
                    "dataType": field.get("dataType")
                        .or_else(|| field.get("type"))
                        .map(Value::to_string)
                        .unwrap_or_else(|| "unknown".to_string()),
                    "nullable": true,
                    "ordinal": index + 1,
                    "primaryKey": field.get("isPrimary").and_then(Value::as_bool).unwrap_or(false)
                })
            })
            .collect::<Vec<_>>();
        objects.push(json!({
            "schema": "default",
            "name": data.get("collectionName").and_then(Value::as_str).unwrap_or(&name),
            "kind": "collection",
            "columns": columns,
            "indexes": data.get("indexes").cloned().unwrap_or_else(|| json!([])),
            "primaryKey": [],
            "foreignKeys": []
        }));
    }
    Ok(json!({ "schemas": [{ "name": "default", "objects": objects }] }))
}

async fn collection_names(connection: &MilvusConnection) -> Result<Vec<String>, String> {
    let value = post_json(connection, "/v2/vectordb/collections/list", json!({})).await?;
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    if let Some(values) = data.as_array() {
        return Ok(values
            .iter()
            .filter_map(|value| {
                value.as_str().map(str::to_string).or_else(|| {
                    value
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
            })
            .collect());
    }
    Ok(data
        .get("collectionNames")
        .or_else(|| data.get("collection_names"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect())
}

async fn post_json(
    connection: &MilvusConnection,
    path: &str,
    body: Value,
) -> Result<Value, String> {
    let response = connection
        .auth(
            connection
                .client
                .post(format!("{}{}", connection.config.base_url, path)),
        )
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("Milvus request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Milvus response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("Milvus returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("Milvus JSON response parse failed: {err}: {text}"))?;
    let code = value.get("code").and_then(Value::as_i64).unwrap_or(0);
    if code != 0 && code != 200 {
        return Err(value
            .get("message")
            .or_else(|| value.get("reason"))
            .and_then(Value::as_str)
            .unwrap_or("Milvus API returned an error.")
            .to_string());
    }
    Ok(value)
}

fn rows_to_output(rows_json: Vec<Value>, truncated: bool) -> QueryOutput {
    let mut columns = Vec::new();
    for row in &rows_json {
        if let Some(object) = row.as_object() {
            for key in object.keys() {
                if !columns.iter().any(|column| column == key) {
                    columns.push(key.clone());
                }
            }
        }
    }
    let rows = rows_json
        .iter()
        .map(|row| {
            if let Some(object) = row.as_object() {
                columns
                    .iter()
                    .map(|column| object.get(column).cloned().unwrap_or(Value::Null))
                    .collect()
            } else {
                vec![row.clone()]
            }
        })
        .collect::<Vec<_>>();
    if columns.is_empty() && !rows_json.is_empty() {
        (vec!["value".to_string()], rows, truncated)
    } else {
        (columns, rows, truncated)
    }
}

fn connection(connection_id: &str) -> Result<MilvusConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn build_url(request: &Value) -> String {
    let host = option_string(request, &["host", "endpoint"]).unwrap_or_else(|| "127.0.0.1".into());
    let port = option_string(request, &["port"]).unwrap_or_else(|| "19530".into());
    let scheme = if option_bool(request, &["tls", "ssl"]).unwrap_or(false) {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{host}:{port}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_http_url_by_default() {
        let request = json!({"profile": {"host": "localhost"}});
        assert_eq!(build_url(&request), "http://localhost:19530");
    }

    #[test]
    fn parses_collection_query() {
        let request = json!({});
        let query = MilvusQuery::from_input("items", &request, 10).unwrap();
        assert_eq!(query.endpoint, "/v2/vectordb/entities/query");
        assert_eq!(query.body["collectionName"], "items");
    }

    #[test]
    fn reads_tls_paths_from_the_connector_options() {
        let tls = TlsConfig::from_request(&json!({
            "profile": {
                "options": {
                    "sslRootCert": "/etc/ssl/ca.pem",
                    "sslCert": "/etc/ssl/client.pem",
                    "sslKey": "/etc/ssl/client.key"
                }
            }
        }));
        assert_eq!(tls.root_cert_path.as_deref(), Some("/etc/ssl/ca.pem"));
        assert_eq!(tls.client_cert_path.as_deref(), Some("/etc/ssl/client.pem"));
        assert_eq!(tls.client_key_path.as_deref(), Some("/etc/ssl/client.key"));
        assert!(!tls.accept_invalid_certs);
    }

    #[test]
    fn accepts_the_driver_spellings_of_the_tls_options() {
        let tls = TlsConfig::from_request(&json!({
            "profile": { "options": { "sslrootcert": "/ca.pem", "ssl-cert": "/c.pem" } }
        }));
        assert_eq!(tls.root_cert_path.as_deref(), Some("/ca.pem"));
        assert_eq!(tls.client_cert_path.as_deref(), Some("/c.pem"));
    }

    #[test]
    fn a_profile_without_tls_options_keeps_the_plain_client() {
        let tls = TlsConfig::from_request(&json!({ "profile": {} }));
        assert_eq!(tls, TlsConfig::default());
        assert!(tls.build_client().is_ok());
    }

    #[test]
    fn half_a_client_identity_is_rejected_with_a_usable_message() {
        // Silently ignoring the half that was supplied would connect without
        // the certificate the user asked for.
        let cert_only = TlsConfig {
            client_cert_path: Some("/etc/ssl/client.pem".into()),
            ..TlsConfig::default()
        };
        assert_eq!(
            cert_only.build_client().unwrap_err(),
            "SSL client certificate needs a matching client key."
        );

        let key_only = TlsConfig {
            client_key_path: Some("/etc/ssl/client.key".into()),
            ..TlsConfig::default()
        };
        assert_eq!(
            key_only.build_client().unwrap_err(),
            "SSL client key needs a matching client certificate."
        );
    }

    #[test]
    fn an_unreadable_certificate_names_the_file_and_the_field() {
        let tls = TlsConfig {
            root_cert_path: Some("/definitely/not/here.pem".into()),
            ..TlsConfig::default()
        };
        let err = tls.build_client().unwrap_err();
        assert!(
            err.starts_with("SSL root certificate at /definitely/not/here.pem"),
            "{err}"
        );
    }

    #[test]
    fn a_certificate_file_with_no_pem_block_is_rejected() {
        // reqwest answers Ok(vec![]) rather than an error for a file with no
        // PEM blocks, so without an explicit emptiness check this connection
        // would silently verify against the system roots instead of the named
        // CA.
        let dir = std::env::temp_dir().join("irodori-milvus-tls-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("not-a-cert.pem");
        std::fs::write(&path, b"this is not a certificate").unwrap();

        let tls = TlsConfig {
            root_cert_path: Some(path.to_string_lossy().into_owned()),
            ..TlsConfig::default()
        };
        let err = tls.build_client().unwrap_err();
        assert!(err.contains("contains no PEM certificate"), "{err}");

        std::fs::remove_file(&path).ok();
    }
}
