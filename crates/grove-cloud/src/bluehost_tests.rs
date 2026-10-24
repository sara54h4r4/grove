//! Actual PHP 8.3 + InnoDB over HTTP, compared to the complete SQLite engine.
use super::*;

struct Harness {
    config: Arc<Config>,
    now: i64,
}

impl Harness {
    async fn new() -> Self {
        let endpoint =
            std::env::var("GROVE_TEST_STORE_ENDPOINT").expect("run hosting/bluehost/tests/run.py");
        let endpoint = reqwest::Url::parse(&endpoint).unwrap();
        assert_eq!(endpoint.host_str(), Some("127.0.0.1"));
        let config = Arc::new(Config {
            repository_id: std::env::var("GROVE_TEST_STORE_REPOSITORY").unwrap(),
            token: "fixture-participant-only".repeat(2),
            approver: Some("fixture-operator-only".repeat(2)),
            bluehost: true,
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            endpoint,
            connection: reqwest::header::HeaderValue::from_str(
                &std::env::var("GROVE_TEST_STORE_TOKEN").unwrap(),
            )
            .unwrap(),
            cache: Mutex::new(cache::Cache::default()),
        });
        config.migrate().await.unwrap();
        Self {
            config,
            now: chrono::Utc::now().timestamp_millis(),
        }
    }
    async fn private(&self, mut request: Value) -> Value {
        request["repository_id"] = json!(self.config.repository_id);
        let r = self
            .config
            .client
            .post(self.config.endpoint.clone())
            .header("x-grove-store-token", self.config.connection.clone())
            .json(&request)
            .send()
            .await
            .unwrap();
        let status = r.status();
        let result: Value = r.json().await.unwrap();
        assert!(
            status.is_success(),
            "private fixture request failed: {result}"
        );
        result
    }
    async fn snapshot(&self) -> Snapshot {
        let mut snapshot = evaluate(
            &self.config.repository_id,
            None,
            CoordCommand::Hello,
            false,
            self.now,
        )
        .unwrap()
        .0;
        snapshot.tables.values_mut().for_each(Vec::clear);
        let (mut table, mut hash, mut revision) = (json!(""), json!(""), Value::Null);
        loop {
            let page = self
                .private(json!({"action":"export","table":table,"hash":hash,"revision":revision}))
                .await;
            let row = &page["rows"][0];
            revision = row[0].clone();
            let records = row[4].as_array().unwrap();
            if records.is_empty() {
                break;
            }
            for record in records {
                snapshot
                    .tables
                    .get_mut(record["table_name"].as_str().unwrap())
                    .unwrap()
                    .push(record["data"].as_array().unwrap().clone());
            }
            table = row[5].clone();
            hash = row[6].clone();
        }
        snapshot
    }
    async fn run(&self, value: Value, operator: bool) -> (Result<Value>, RequestMetrics) {
        let mut metrics = RequestMetrics::default();
        let result = self
            .config
            .coordinate_inner(
                Envelope {
                    repository_id: self.config.repository_id.clone(),
                    command: serde_json::from_value(value).unwrap(),
                },
                operator,
                &mut metrics,
                Some(self.now),
            )
            .await;
        (result, metrics)
    }
    async fn check(&mut self, value: Value, operator: bool) -> Result<Value> {
        self.now += 100;
        let before = self.snapshot().await;
        let expected = evaluate(
            &self.config.repository_id,
            Some(&before),
            serde_json::from_value(value.clone()).unwrap(),
            operator,
            self.now,
        );
        let (actual, _) = self.run(value, operator).await;
        match (expected, &actual) {
            (Err(expected), Err(actual)) => assert_eq!(expected, *actual),
            (Ok((snapshot, mut expected)), Ok(actual)) => {
                let mut snapshot = serde_json::to_value(snapshot).unwrap();
                for key in ["message_id", "claim_id"] {
                    if let (Some(old), Some(new)) = (expected[key].as_str(), actual[key].as_str()) {
                        let (old, new) = (old.to_string(), new.to_string());
                        super::postgres_tests::replace_id(&mut snapshot, &old, &new);
                        super::postgres_tests::replace_id(&mut expected, &old, &new);
                    }
                }
                let mut core = actual.clone();
                for field in [
                    "history_available_after",
                    "history_truncated",
                    "poll_after_seconds",
                ] {
                    core.as_object_mut().unwrap().remove(field);
                }
                assert_eq!(expected, core);
                let mut expected: Snapshot = serde_json::from_value(snapshot).unwrap();
                let mut stored = self.snapshot().await;
                for rows in expected
                    .tables
                    .values_mut()
                    .chain(stored.tables.values_mut())
                {
                    rows.sort_by_key(|row| serde_json::to_string(row).unwrap());
                }
                assert_eq!(
                    expected.tables, stored.tables,
                    "every unrelated row must survive"
                );
            }
            (expected, actual) => panic!(
                "engine/store differs: {:?} / {:?}",
                expected.err(),
                actual.as_ref().err()
            ),
        }
        actual
    }
    async fn register(&mut self, id: &str, scope: &str) {
        self.check(json!({"op":"register","session_id":id,"task":"fixture","scope":[scope],"acceptance":["preserve policy"],
            "observation":{"worktree_id":id,"worktree":"/fixture","branch":"main","commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dirty":false,"verification":"client_reported"}}),false).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires disposable PHP/MySQL fixture; run hosting/bluehost/tests/run.py"]
async fn bluehost_projection_atomicity_retention_and_clock_deltas() {
    let mut h = Harness::new().await;
    h.check(json!({"op":"hello"}), false).await.unwrap();
    h.register("codex:a", "src/a").await;
    h.register("codex:b", "src/a").await;
    h.register("codex:c", "src/ab").await;
    let msg=h.check(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"request","body":"literal / paths and policy","key":"once"}),false).await.unwrap();
    h.check(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"request","body":"literal / paths and policy","key":"once"}),false).await.unwrap();
    h.check(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"request","body":"different","key":"once"}),false).await.unwrap_err();
    let view = h
        .check(json!({"op":"sync","session_id":"codex:b","limit":2}), false)
        .await
        .unwrap();
    h.check(
        json!({"op":"checkpoint","session_id":"codex:b","cursor":view["next_cursor"]}),
        false,
    )
    .await
    .unwrap();
    h.check(
        json!({"op":"sync","session_id":"codex:c","limit":100}),
        false,
    )
    .await
    .unwrap();
    h.check(
        json!({"op":"ack_message","session_id":"codex:c","message_id":msg["message_id"]}),
        false,
    )
    .await
    .unwrap_err();
    h.check(
        json!({"op":"ack_message","session_id":"codex:b","message_id":msg["message_id"]}),
        false,
    )
    .await
    .unwrap();
    for revision in 0..2 {
        h.check(json!({"op":"propose","session_id":"codex:a","decision_id":"policy","expected_revision":revision,"scope":["src/a"],"body":"include AGENTS.md","provenance":"fixture"}),false).await.unwrap();
        h.check(json!({"op":"approve","decision_id":"policy","revision":revision+1,"authority":"operator"}),false).await.unwrap_err();
        h.check(json!({"op":"approve","decision_id":"policy","revision":revision+1,"authority":"operator"}),true).await.unwrap();
        h.check(json!({"op":"ack_decision","session_id":"codex:b","decision_id":"policy","revision":revision+1}),false).await.unwrap();
    }
    let claim = h
        .check(
            json!({"op":"claim","session_id":"codex:a","path":"src/a","ttl_seconds":5}),
            false,
        )
        .await
        .unwrap();
    h.check(json!({"op":"renew_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":1,"ttl_seconds":5}),false).await.unwrap();
    h.check(
        json!({"op":"release_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":1}),
        false,
    )
    .await
    .unwrap();
    let oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    h.check(json!({"op":"capture_transfer","session_id":"codex:a","transfer_id":"whole-pr","manifest":{
        "source_ref":"feature","base_ref":"base","target_ref":"HEAD","source_commit":oid,"base_commit":oid,"initial_target_commit":oid,
        "items":[{"path":"AGENTS.md","before":null,"after":{"oid":oid,"mode":"100644"},"initial_target":null}]}}),false).await.unwrap();
    h.check(
        json!({"op":"get_transfer","session_id":"codex:b","transfer_id":"whole-pr"}),
        false,
    )
    .await
    .unwrap();
    h.check(json!({"op":"check_transfer","session_id":"codex:a","transfer_id":"whole-pr","observation":{
        "source_commit":oid,"base_commit":oid,"target_commit":oid,"target_dirty":false,"target_entries":{"AGENTS.md":null}}}),false).await.unwrap();
    h.check(json!({"op":"review_transfer","transfer_id":"whole-pr","review":{"source_commit":oid,"base_commit":oid,"target_commit":oid,"reviewer":"fixture operator",
        "items":[],"criteria":[{"index":0,"accepted":false,"evidence":"policy missing"}]}}),true).await.unwrap();
    let command = json!({"op":"sync","session_id":"codex:b","limit":100});
    h.run(command.clone(), false).await.0.unwrap();
    let (_, warm) = h.run(command.clone(), false).await;
    assert_eq!(warm.projection_cache_hits, 1);
    h.now += 100;
    h.run(json!({"op":"heartbeat","session_id":"codex:a"}), false)
        .await
        .0
        .unwrap();
    let (updated, metrics) = h.run(command, false).await;
    let updated = updated.unwrap();
    assert_eq!(metrics.projection_cache_hits, 1);
    assert_eq!(
        updated["peers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["session_id"] == "codex:a")
            .unwrap()["heartbeat_at"],
        h.now
    );
    assert!(metrics.db_response_bytes < 2048);
    let mut jobs = Vec::new();
    for _ in 0..8 {
        let cfg = h.config.clone();
        jobs.push(tokio::spawn(async move { cfg.coordinate(Envelope{repository_id:cfg.repository_id.clone(),command:serde_json::from_value(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"progress","body":"one effect","key":"concurrent"})).unwrap()},false).await.unwrap() }));
    }
    let mut ids = Vec::new();
    for job in jobs {
        ids.push(job.await.unwrap()["message_id"].clone());
    }
    assert!(ids.iter().all(|id| *id == ids[0]));
    let snapshot = h.snapshot().await;
    assert_eq!(
        snapshot.tables["coord_messages"]
            .iter()
            .filter(|r| r[3] == "concurrent")
            .count(),
        1
    );
    assert_eq!(
        snapshot.tables["coord_events"]
            .iter()
            .filter(
                |r| r[1] == "message" && r[5].as_str().unwrap().contains(ids[0].as_str().unwrap())
            )
            .count(),
        1
    );
    let mut parallel = Vec::new();
    for n in 0..4 {
        let cfg = h.config.clone();
        parallel.push(tokio::spawn(async move {
            cfg.coordinate(Envelope{repository_id:cfg.repository_id.clone(),command:serde_json::from_value(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"progress","body":"distinct effect","key":format!("distinct-{n}")})).unwrap()},false).await.unwrap()
        }));
    }
    for job in parallel {
        job.await.unwrap();
    }
    let mut state = h.private(json!({"action":"fingerprint"})).await;
    let old = h.now - 9 * 86_400_000;
    let prototype: Value =
        serde_json::from_str(snapshot.tables["coord_sessions"][0][1].as_str().unwrap()).unwrap();
    let mut patch = Vec::new();
    for n in 0..200 {
        let id = format!("codex:archived-{n:04}");
        let mut session = prototype.clone();
        session["session_id"] = json!(id);
        session["native_session_id"] = json!(format!("archived-{n:04}"));
        session["status"] = json!("stopped");
        session["heartbeat_at"] = json!(old);
        patch.push(json!({"table_name":"coord_sessions","row_key":[id],"data":[id,session.to_string(),0,0]}));
    }
    h.private(json!({"action":"commit","revision":state["rows"][0][0],"high":state["rows"][0][1],"context":true,"patch":patch})).await;
    for start in (1..=1003).step_by(32) {
        state = h.private(json!({"action":"fingerprint"})).await;
        let mut patch = Vec::new();
        let end = (start + 31).min(1003);
        for n in start..=end {
            let id = format!("history-{n}");
            let body = json!({"body":"x".repeat(8192)}).to_string();
            patch.push(json!({"table_name":"coord_messages","row_key":[id],"data":[id,"codex:a","codex:b",format!("history-{n}"),body,old,old,old]}));
            patch.push(json!({"table_name":"coord_events","row_key":[10000+n],"data":[10000+n,"message","codex:c","codex:b","[\"src/ab\"]",body,old]}));
        }
        h.private(json!({"action":"commit","revision":state["rows"][0][0],"high":10000+end,"context":true,"patch":patch})).await;
    }
    let command = json!({"op":"sync","session_id":"codex:b","after":11003,"limit":50});
    let (cold, cold_metrics) = h.run(command.clone(), false).await;
    cold.unwrap();
    let (_, warm_metrics) = h.run(command.clone(), false).await;
    assert_eq!(warm_metrics.projection_cache_hits, 1);
    assert!(
        warm_metrics.db_response_bytes < 2048,
        "old peer clocks must not cross the API on unchanged polls"
    );
    h.now += 100;
    h.run(json!({"op":"heartbeat","session_id":"codex:a"}), false)
        .await
        .0
        .unwrap();
    let (view, delta) = h.run(command, false).await;
    assert_eq!(delta.projection_cache_hits, 1);
    assert!(delta.db_response_bytes < 2048);
    let view = view.unwrap();
    assert_eq!(
        view["peers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["session_id"] == "codex:a")
            .unwrap()["heartbeat_at"],
        h.now
    );
    let current = h.config.as_ref();
    let restarted = Arc::new(Config {
        repository_id: current.repository_id.clone(),
        token: current.token.clone(),
        approver: current.approver.clone(),
        bluehost: true,
        client: current.client.clone(),
        endpoint: current.endpoint.clone(),
        connection: current.connection.clone(),
        cache: Mutex::new(cache::Cache::default()),
    });
    let mut recovered_metrics = RequestMetrics::default();
    let recovered = restarted
        .coordinate_inner(
            Envelope {
                repository_id: restarted.repository_id.clone(),
                command: serde_json::from_value(
                    json!({"op":"sync","session_id":"codex:b","after":11003,"limit":50}),
                )
                .unwrap(),
            },
            false,
            &mut recovered_metrics,
            Some(h.now),
        )
        .await
        .unwrap();
    assert_eq!(
        view, recovered,
        "process recovery must not depend on cache memory"
    );
    assert_eq!(recovered_metrics.projection_cache_hits, 0);
    let (scan, scan_metrics) = h
        .run(
            json!({"op":"sync","session_id":"codex:b","after":10000,"limit":50}),
            false,
        )
        .await;
    let scan = scan.unwrap();
    assert!(
        scan["updates"].as_array().unwrap().is_empty(),
        "src/a must not overlap src/ab"
    );
    assert_eq!(scan["next_cursor"], 11000);
    assert_eq!(scan["has_more"], true);
    assert!(
        scan_metrics.db_response_bytes < 400_000,
        "irrelevant 8KB bodies stay beside MySQL"
    );
    for _ in 0..4 {
        h.private(json!({"action":"prune"})).await;
    }
    let (trimmed, _) = h
        .run(
            json!({"op":"sync","session_id":"codex:b","after":0,"limit":100}),
            false,
        )
        .await;
    let trimmed = trimmed.unwrap();
    assert_eq!(trimmed["history_truncated"], true);
    assert_eq!(trimmed["high_watermark"], 11003);
    assert!(!trimmed["current_decisions"].as_array().unwrap().is_empty());
    h.run(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"progress","body":"after prune","key":"after-prune"}),false).await.0.unwrap();
    let (after, _) = h
        .run(
            json!({"op":"sync","session_id":"codex:b","after":11003,"limit":50}),
            false,
        )
        .await;
    assert_eq!(after.unwrap()["updates"][0]["cursor"], 11004);
    println!(
        "BLUEHOST_RECEIPT {}",
        json!({"php_mysql_engine_equivalence":"passed","concurrent_same_key_effects":1,"concurrent_distinct_key_effects":4,
            "historical_sessions":200,"historical_8kb_events":1003,"cold_sync_db_bytes":cold_metrics.db_response_bytes,
            "warm_sync_db_bytes":warm_metrics.db_response_bytes,"clock_delta_db_bytes":delta.db_response_bytes,
            "header_scan_db_bytes":scan_metrics.db_response_bytes,"retention_keeps_authority_and_watermark":true})
    );
}
