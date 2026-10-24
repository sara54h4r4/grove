//! Private PHP/MySQL store adapter. No SQL or database credentials are exposed
//! to the agents, and no second implementation of agent command semantics.
use crate::{DbReply, Result, integer, storage};
use reqwest::{Url, header::HeaderValue};
use serde_json::{Value, json};

pub const INITIALIZE: &str = "bluehost:initialize";
pub const USER_AGENT: &str = concat!("Grove-Coordination/", env!("CARGO_PKG_VERSION"));

pub fn configuration(endpoint: &str, token: &str) -> Result<(Url, HeaderValue)> {
    let url = Url::parse(endpoint).map_err(|_| "invalid GROVE_STORE_URL")?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "GROVE_STORE_URL must be an HTTPS endpoint without credentials or query".into(),
        );
    }
    if token.len() < 48 || !token.bytes().all(|b| b.is_ascii_graphic()) || token.contains("SET_") {
        return Err("invalid GROVE_STORE_TOKEN".into());
    }
    let mut token = HeaderValue::from_str(token).map_err(|_| "invalid store credential")?;
    token.set_sensitive(true);
    Ok((url, token))
}

pub fn request(sql: &str, params: Vec<Value>) -> Result<Value> {
    let repo = params.first().ok_or("missing store repository")?;
    Ok(match sql {
        INITIALIZE => json!({"action":"initialize","repository_id":repo}),
        storage::LOAD => json!({"action":"load","repository_id":repo,"command":
            serde_json::from_str::<Value>(params[1].as_str().ok_or("invalid store command")?)
                .map_err(|_| "invalid store command")?}),
        storage::CHECK => {
            json!({"action":"check","repository_id":repo,"revision":integer(&params[1])?})
        }
        storage::COMMIT => {
            json!({"action":"commit","repository_id":repo,"revision":integer(&params[1])?,
            "patch":serde_json::from_str::<Value>(params[2].as_str().ok_or("invalid store patch")?)
                .map_err(|_| "invalid store patch")?,"high":integer(&params[3])?,"context":params[4]})
        }
        _ => return Err("unsupported Bluehost store operation".into()),
    })
}

pub fn decode(value: Value, bytes: usize) -> Result<DbReply> {
    if value["ok"] != true || !value["rows"].is_array() {
        // Never forward arbitrary upstream errors, bodies, paths or credentials.
        return Err("coordination store unavailable; check server configuration".into());
    }
    Ok(DbReply {
        data: json!({"rows":value["rows"]}),
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configuration_and_failures_do_not_disclose_credentials() {
        let token = "fixture-only-do-not-disclose-".repeat(3);
        let (_, header) = configuration("https://example.test/grove-store/", &token).unwrap();
        assert!(header.is_sensitive());
        for url in [
            "http://example.test/",
            "https://user:password@example.test/",
            "https://example.test/?token=private",
            "https://example.test/#private",
        ] {
            assert!(configuration(url, &token).is_err());
        }
        let error = decode(json!({"ok":false,"error":token}), 100)
            .err()
            .unwrap();
        assert!(!error.contains(&token));
        assert!(request("arbitrary SQL", vec![json!("repo")]).is_err());
    }
}
