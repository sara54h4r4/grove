use grove_cloud::{Config, Envelope, MAX_REQUEST_BYTES};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::Arc;
use vercel_runtime::{Error, Request, Response, ResponseBody, run, service_fn};

static CONFIG: tokio::sync::OnceCell<Arc<Config>> = tokio::sync::OnceCell::const_new();

fn response(status: u16, value: Value) -> Result<Response<ResponseBody>, Error> {
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(value.into())?)
}

async fn handler(request: Request) -> Result<Response<ResponseBody>, Error> {
    if request.method() != "POST" {
        return response(
            405,
            json!({"ok":false,"error":"POST a coordination command"}),
        );
    }
    let config = match CONFIG
        .get_or_try_init(|| async { Config::from_environment(false).map(Arc::new) })
        .await
    {
        Ok(c) => c.clone(),
        Err(_) => {
            eprintln!("Grove coordination configuration unavailable");
            return response(
                503,
                json!({"ok":false,"error":"service configuration unavailable"}),
            );
        }
    };
    let bearer = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned);
    let approver = request
        .headers()
        .get("x-grove-approver-token")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    // Authenticate before reading or deserializing untrusted bodies.
    if config
        .authenticate(
            bearer.as_deref(),
            None,
            &grove_lib::coordination::CoordCommand::Hello,
        )
        .is_err()
    {
        return response(401, json!({"ok":false,"error":"authentication required"}));
    }
    let bytes = match http_body_util::Limited::new(request.into_body(), MAX_REQUEST_BYTES)
        .collect()
        .await
    {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return response(
                413,
                json!({"ok":false,"error":"request exceeds size bound"}),
            );
        }
    };
    let envelope: Envelope = match serde_json::from_slice(&bytes) {
        Ok(e) => e,
        Err(_) => {
            return response(
                400,
                json!({"ok":false,"error":"invalid coordination request"}),
            );
        }
    };
    if envelope.repository_id != config.repository_id {
        return response(
            403,
            json!({"ok":false,"error":"repository identity mismatch"}),
        );
    }
    let operator =
        match config.authenticate(bearer.as_deref(), approver.as_deref(), &envelope.command) {
            Ok(v) => v,
            Err(_) => {
                return response(
                    403,
                    json!({"ok":false,"error":"operator credential required"}),
                );
            }
        };
    match config.coordinate(envelope, operator).await {
        Ok(data) => response(200, json!({"ok":true,"data":data})),
        Err(error) => {
            eprintln!("Grove coordination request failed");
            response(200, json!({"ok":false,"error":error}))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    if let Some(index) = std::env::args().position(|a| a == "--import-snapshot") {
        let path = std::env::args()
            .nth(index + 1)
            .ok_or("--import-snapshot requires a private archive path")?;
        if std::fs::metadata(&path)?.len() > (grove_cloud::MAX_SNAPSHOT_BYTES + 1_048_576) as u64 {
            return Err("archive exceeds import size bound".into());
        }
        let archive = serde_json::from_slice(&std::fs::read(path)?)?;
        let config = Config::from_environment(true).map_err(std::io::Error::other)?;
        config
            .import_snapshot(archive)
            .await
            .map_err(std::io::Error::other)?;
        println!("Coordination archive imported into empty destination");
        return Ok(());
    }
    if std::env::args().any(|a| a == "--migrate") {
        let config = Config::from_environment(true).map_err(std::io::Error::other)?;
        config.migrate().await.map_err(std::io::Error::other)?;
        println!("Grove coordination schema initialized");
        return Ok(());
    }
    run(service_fn(handler)).await
}
